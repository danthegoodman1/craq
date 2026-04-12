package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	grpcproto "github.com/danthegoodman1/craq/proto/craq/v1"
	"github.com/danthegoodman1/craq/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	replicationSessionQueueDepth   = 4096
	replicationReconnectMinBackoff = 50 * time.Millisecond
	replicationReconnectMaxBackoff = time.Second
)

type outboundReplicationRequest struct {
	id    uint64
	ctx   context.Context
	frame *grpcproto.ReplicationFrame
	done  chan error
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
}

func newReplicationPeerSession(transport *ReplicationTransport, target string) *replicationPeerSession {
	session := &replicationPeerSession{
		transport: transport,
		target:    target,
		sendCh:    make(chan *outboundReplicationRequest, replicationSessionQueueDepth),
		closeCh:   make(chan struct{}),
		closedCh:  make(chan struct{}),
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

func (s *replicationPeerSession) run() {
	defer close(s.closedCh)

	var (
		backlog []*outboundReplicationRequest
		pending = map[uint64]*outboundReplicationRequest{}
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
			req := backlog[0]
			if req.ctx != nil {
				if err := req.ctx.Err(); err != nil {
					req.complete(err)
					backlog = backlog[1:]
					continue
				}
			}
			if err := stream.Send(req.frame); err != nil {
				closeStream()
				requeuePending()
				continue
			}
			pending[req.id] = req
			backlog = backlog[1:]
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
			req.complete(unmarshalEncodedError(ack.EncodedError))
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
