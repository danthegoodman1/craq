package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	grpcproto "github.com/danthegoodman1/craq/proto/craq/v1"
	"github.com/danthegoodman1/craq/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	replicationSessionQueueDepth   = 8192
	replicationReconnectMinBackoff = 50 * time.Millisecond
	replicationReconnectMaxBackoff = time.Second
	replicationBackpressureRetry   = 250 * time.Microsecond
)

type outboundReplicationRequest struct {
	id    uint64
	ctx   context.Context
	frame *grpcproto.ReplicationFrame
	done  chan error
	kind  string
	at    time.Time
	slot  int
}

func (r *outboundReplicationRequest) complete(err error) {
	select {
	case r.done <- err:
	default:
	}
}

type replicationPeerSession struct {
	transport *ReplicationTransport
	target    string
	sendCh    chan *outboundReplicationRequest
	closeCh   chan struct{}
	closedCh  chan struct{}
	metricsMu sync.Mutex
	highWater map[string]int
}

func newReplicationPeerSession(transport *ReplicationTransport, target string) *replicationPeerSession {
	session := &replicationPeerSession{
		transport: transport,
		target:    target,
		sendCh:    make(chan *outboundReplicationRequest, replicationSessionQueueDepth),
		closeCh:   make(chan struct{}),
		closedCh:  make(chan struct{}),
		highWater: map[string]int{},
	}
	go session.run()
	return session
}

func (s *replicationPeerSession) close() {
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
	}
	<-s.closedCh
}

func (s *replicationPeerSession) enqueue(req *outboundReplicationRequest) error {
	select {
	case <-s.closeCh:
		return context.Canceled
	default:
	}
	select {
	case s.sendCh <- req:
		depth := len(s.sendCh)
		s.metricsMu.Lock()
		if depth > s.highWater[req.kind] {
			s.highWater[req.kind] = depth
			if observer := s.transport.currentObserver(); observer != nil {
				observer.ObserveReplicationSessionQueueDepthHighWater(req.kind, s.target, depth)
			}
		}
		s.metricsMu.Unlock()
		return nil
	default:
		return fmt.Errorf("%w: replication session queue full for %q", storage.ErrReplicaBackpressure, s.target)
	}
}

func (t *ReplicationTransport) sessionForTarget(target string) *replicationPeerSession {
	t.mu.Lock()
	defer t.mu.Unlock()
	if session, ok := t.sessions[target]; ok {
		return session
	}
	session := newReplicationPeerSession(t, target)
	t.sessions[target] = session
	return session
}

func (t *ReplicationTransport) nextRequestID() uint64 {
	return atomic.AddUint64(&t.nextID, 1)
}

func (t *ReplicationTransport) submitReplicationRequest(
	ctx context.Context,
	target string,
	payload isReplicationFramePayload,
) error {
	req := &outboundReplicationRequest{
		id:   t.nextRequestID(),
		ctx:  ctx,
		done: make(chan error, 1),
		kind: payload.kind(),
		at:   time.Now(),
		slot: payload.slot(),
	}
	req.frame = payload.toFrame(req.id)
	if err := t.sessionForTarget(target).enqueue(req); err != nil {
		return err
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

type isReplicationFramePayload interface {
	toFrame(requestID uint64) *grpcproto.ReplicationFrame
	kind() string
	slot() int
}

type replicationForwardPayload struct {
	req storage.ForwardWriteRequest
}

func (p replicationForwardPayload) toFrame(requestID uint64) *grpcproto.ReplicationFrame {
	return &grpcproto.ReplicationFrame{
		RequestId: requestID,
		Payload: &grpcproto.ReplicationFrame_ForwardWrite{
			ForwardWrite: &grpcproto.ForwardWriteRequest{
				Operation: &grpcproto.WriteOperation{
					Slot:     int32(p.req.Operation.Slot),
					Sequence: p.req.Operation.Sequence,
					Kind:     string(p.req.Operation.Kind),
					Key:      p.req.Operation.Key,
					Value:    p.req.Operation.Value,
					Metadata: protoObjectMetadata(&p.req.Operation.Metadata),
				},
				FromNodeId:   p.req.FromNodeID,
				ChainVersion: p.req.ChainVersion,
			},
		},
	}
}

func (replicationForwardPayload) kind() string {
	return "forward"
}

func (p replicationForwardPayload) slot() int {
	return p.req.Operation.Slot
}

type replicationCommitPayload struct {
	req storage.CommitWriteRequest
}

func (p replicationCommitPayload) toFrame(requestID uint64) *grpcproto.ReplicationFrame {
	return &grpcproto.ReplicationFrame{
		RequestId: requestID,
		Payload: &grpcproto.ReplicationFrame_CommitWrite{
			CommitWrite: &grpcproto.CommitWriteRequest{
				Slot:         int32(p.req.Slot),
				Sequence:     p.req.Sequence,
				FromNodeId:   p.req.FromNodeID,
				ChainVersion: p.req.ChainVersion,
			},
		},
	}
}

func (replicationCommitPayload) kind() string {
	return "commit"
}

func (p replicationCommitPayload) slot() int {
	return p.req.Slot
}

func (s *replicationPeerSession) run() {
	defer close(s.closedCh)

	var (
		backlog []*outboundReplicationRequest
		pending = map[uint64]*outboundReplicationRequest{}
		blocked = map[int]time.Time{}
		stream  grpc.BidiStreamingClient[grpcproto.ReplicationFrame, grpcproto.ReplicationFrame]
		errCh   <-chan error
		ackCh   <-chan *grpcproto.ReplicationFrame
		cancel  context.CancelFunc
		backoff = replicationReconnectMinBackoff
	)

	closeStream := func() {
		if cancel != nil {
			cancel()
			cancel = nil
		}
		stream = nil
		errCh = nil
		ackCh = nil
	}

	requeuePending := func() {
		if len(pending) == 0 {
			return
		}
		ids := make([]uint64, 0, len(pending))
		for id := range pending {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		replayed := make([]*outboundReplicationRequest, 0, len(ids))
		for _, id := range ids {
			replayed = append(replayed, pending[id])
			delete(pending, id)
		}
		backlog = append(replayed, backlog...)
	}

	prependBacklog := func(req *outboundReplicationRequest) {
		backlog = append(backlog, nil)
		copy(backlog[1:], backlog[:len(backlog)-1])
		backlog[0] = req
	}

	var nextSendableIndex func(time.Time) (int, time.Duration)
	nextSendableIndex = func(now time.Time) (int, time.Duration) {
		nextWait := time.Duration(0)
		waitSet := false
		for i, req := range backlog {
			if req.ctx != nil {
				if err := req.ctx.Err(); err != nil {
					req.complete(err)
					backlog = append(backlog[:i], backlog[i+1:]...)
					return nextSendableIndex(now)
				}
			}
			until, ok := blocked[req.slot]
			if ok && now.Before(until) {
				wait := until.Sub(now)
				if !waitSet || wait < nextWait {
					nextWait = wait
					waitSet = true
				}
				continue
			}
			delete(blocked, req.slot)
			return i, 0
		}
		if !waitSet {
			return -1, 0
		}
		return -1, nextWait
	}

	for {
		if stream == nil {
			for {
				select {
				case req := <-s.sendCh:
					backlog = append(backlog, req)
				case <-s.closeCh:
					for _, req := range backlog {
						req.complete(context.Canceled)
					}
					for _, req := range pending {
						req.complete(context.Canceled)
					}
					return
				default:
				}
				if len(backlog) == 0 {
					break
				}
				conn, err := s.transport.pool.DialContext(context.Background(), s.target)
				if err != nil {
					timer := time.NewTimer(backoff)
					select {
					case req := <-s.sendCh:
						backlog = append(backlog, req)
					case <-timer.C:
					case <-s.closeCh:
						timer.Stop()
						for _, req := range backlog {
							req.complete(context.Canceled)
						}
						return
					}
					if backoff < replicationReconnectMaxBackoff {
						backoff *= 2
						if backoff > replicationReconnectMaxBackoff {
							backoff = replicationReconnectMaxBackoff
						}
					}
					continue
				}
				client := grpcproto.NewStorageServiceClient(conn)
				streamCtx, streamCancel := context.WithCancel(context.Background())
				nextStream, err := client.Replicate(streamCtx)
				if err != nil {
					streamCancel()
					timer := time.NewTimer(backoff)
					select {
					case req := <-s.sendCh:
						backlog = append(backlog, req)
					case <-timer.C:
					case <-s.closeCh:
						timer.Stop()
						for _, req := range backlog {
							req.complete(context.Canceled)
						}
						return
					}
					if backoff < replicationReconnectMaxBackoff {
						backoff *= 2
						if backoff > replicationReconnectMaxBackoff {
							backoff = replicationReconnectMaxBackoff
						}
					}
					continue
				}
				frames := make(chan *grpcproto.ReplicationFrame, 128)
				streamErrs := make(chan error, 1)
				go func() {
					for {
						frame, recvErr := nextStream.Recv()
						if recvErr != nil {
							streamErrs <- recvErr
							return
						}
						frames <- frame
					}
				}()
				stream = nextStream
				cancel = streamCancel
				ackCh = frames
				errCh = streamErrs
				backoff = replicationReconnectMinBackoff
				break
			}
		}

		if stream != nil && len(backlog) > 0 {
			idx, wait := nextSendableIndex(time.Now())
			if idx == -1 {
				if wait > 0 {
					timer := time.NewTimer(wait)
					select {
					case req := <-s.sendCh:
						timer.Stop()
						backlog = append(backlog, req)
					case frame := <-ackCh:
						timer.Stop()
						if frame == nil {
							continue
						}
						req, ok := pending[frame.GetRequestId()]
						if !ok {
							continue
						}
						delete(pending, frame.GetRequestId())
						ack := frame.GetAck()
						if ack == nil || ack.Success {
							req.complete(nil)
							continue
						}
						err := unmarshalEncodedError(ack.EncodedError)
						var bp *storage.BackpressureError
						if errors.As(err, &bp) && errors.Is(bp.Cause, storage.ErrReplicaBackpressure) {
							blocked[req.slot] = time.Now().Add(replicationBackpressureRetry)
							req.at = time.Now()
							prependBacklog(req)
							continue
						}
						req.complete(err)
					case streamErr := <-errCh:
						timer.Stop()
						if streamErr == nil || errors.Is(streamErr, io.EOF) || status.Code(streamErr) == codes.Unavailable {
							closeStream()
							requeuePending()
							continue
						}
						closeStream()
						requeuePending()
					case <-timer.C:
					case <-s.closeCh:
						timer.Stop()
						closeStream()
						for _, req := range backlog {
							req.complete(context.Canceled)
						}
						for _, req := range pending {
							req.complete(context.Canceled)
						}
						return
					}
				}
				continue
			}
			req := backlog[idx]
			if err := stream.Send(req.frame); err != nil {
				closeStream()
				requeuePending()
				continue
			}
			if observer := s.transport.currentObserver(); observer != nil {
				observer.ObserveReplicationSessionQueueWait(req.kind, s.target, time.Since(req.at))
			}
			pending[req.id] = req
			backlog = append(backlog[:idx], backlog[idx+1:]...)
			continue
		}

		select {
		case req := <-s.sendCh:
			backlog = append(backlog, req)
		case frame := <-ackCh:
			if frame == nil {
				continue
			}
			req, ok := pending[frame.GetRequestId()]
			if !ok {
				continue
			}
			delete(pending, frame.GetRequestId())
			ack := frame.GetAck()
			if ack == nil || ack.Success {
				req.complete(nil)
				continue
			}
			err := unmarshalEncodedError(ack.EncodedError)
			var bp *storage.BackpressureError
			if errors.As(err, &bp) && errors.Is(bp.Cause, storage.ErrReplicaBackpressure) {
				blocked[req.slot] = time.Now().Add(replicationBackpressureRetry)
				req.at = time.Now()
				prependBacklog(req)
				continue
			}
			req.complete(err)
		case streamErr := <-errCh:
			if streamErr == nil || errors.Is(streamErr, io.EOF) || status.Code(streamErr) == codes.Unavailable {
				closeStream()
				requeuePending()
				continue
			}
			closeStream()
			requeuePending()
		case <-s.closeCh:
			closeStream()
			for _, req := range backlog {
				req.complete(context.Canceled)
			}
			for _, req := range pending {
				req.complete(context.Canceled)
			}
			return
		}
	}
}
