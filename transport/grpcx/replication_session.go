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
)

type outboundReplicationRequest struct {
	id    uint64
	ctx   context.Context
	frame *grpcproto.ReplicationFrame
	done  chan error
	kind  string
	at    time.Time
	slot  int
	seq   uint64
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
	diagMu    sync.Mutex
	diag      map[replicationSlotKindKey]*replicationSlotState
}

type replicationSlotKindKey struct {
	kind string
	slot int
}

type replicationSlotState struct {
	kind             string
	slot             int
	advertisedCredit int
	localCredit      int
	localSpoolDepth  int
	pending          map[uint64]uint64
	lastEnqueuedSeq  uint64
	lastSentSeq      uint64
	lastAckedSeq     uint64
	lastCreditUpdate time.Time
	blockedSince     time.Time
}

func newReplicationPeerSession(transport *ReplicationTransport, target string) *replicationPeerSession {
	session := &replicationPeerSession{
		transport: transport,
		target:    target,
		sendCh:    make(chan *outboundReplicationRequest, replicationSessionQueueDepth),
		closeCh:   make(chan struct{}),
		closedCh:  make(chan struct{}),
		highWater: map[string]int{},
		diag:      map[replicationSlotKindKey]*replicationSlotState{},
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
		s.recordEnqueue(req)
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
		seq:  payload.sequence(),
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
	sequence() uint64
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

func (p replicationForwardPayload) sequence() uint64 {
	return p.req.Operation.Sequence
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

func (p replicationCommitPayload) sequence() uint64 {
	return p.req.Sequence
}

func (s *replicationPeerSession) debugStateFor(kind string, slot int) *replicationSlotState {
	key := replicationSlotKindKey{kind: kind, slot: slot}
	state, ok := s.diag[key]
	if !ok {
		state = &replicationSlotState{
			kind:    kind,
			slot:    slot,
			pending: map[uint64]uint64{},
		}
		s.diag[key] = state
	}
	return state
}

func (s *replicationPeerSession) recordEnqueue(req *outboundReplicationRequest) {
	if req == nil {
		return
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	state := s.debugStateFor(req.kind, req.slot)
	state.localSpoolDepth++
	state.lastEnqueuedSeq = req.seq
	if state.localCredit <= 0 && state.blockedSince.IsZero() {
		state.blockedSince = time.Now().UTC()
	}
}

func (s *replicationPeerSession) recordCredit(slot int, available int) {
	now := time.Now().UTC()
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	for _, kind := range []string{"forward", "commit"} {
		state := s.debugStateFor(kind, slot)
		state.advertisedCredit = available
		state.localCredit = available
		state.lastCreditUpdate = now
		if available > 0 && state.localSpoolDepth == 0 {
			state.blockedSince = time.Time{}
		}
	}
}

func (s *replicationPeerSession) recordSent(req *outboundReplicationRequest, localCredit int) {
	if req == nil {
		return
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	state := s.debugStateFor(req.kind, req.slot)
	if state.localSpoolDepth > 0 {
		state.localSpoolDepth--
	}
	state.localCredit = localCredit
	state.pending[req.id] = req.seq
	state.lastSentSeq = req.seq
	if state.localCredit > 0 && state.localSpoolDepth == 0 {
		state.blockedSince = time.Time{}
	}
}

func (s *replicationPeerSession) recordAck(req *outboundReplicationRequest) {
	if req == nil {
		return
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	state := s.debugStateFor(req.kind, req.slot)
	delete(state.pending, req.id)
	if req.seq > state.lastAckedSeq {
		state.lastAckedSeq = req.seq
	}
	if state.localCredit > 0 && state.localSpoolDepth == 0 {
		state.blockedSince = time.Time{}
	}
}

func (s *replicationPeerSession) recordBlocked(req *outboundReplicationRequest) {
	if req == nil {
		return
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	state := s.debugStateFor(req.kind, req.slot)
	if state.blockedSince.IsZero() {
		state.blockedSince = time.Now().UTC()
	}
}

func (s *replicationPeerSession) recordRequeued(req *outboundReplicationRequest) {
	if req == nil {
		return
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	state := s.debugStateFor(req.kind, req.slot)
	delete(state.pending, req.id)
	state.localSpoolDepth++
	if state.blockedSince.IsZero() {
		state.blockedSince = time.Now().UTC()
	}
}

func (s *replicationPeerSession) recordCanceledBacklog(req *outboundReplicationRequest) {
	if req == nil {
		return
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	state := s.debugStateFor(req.kind, req.slot)
	if state.localSpoolDepth > 0 {
		state.localSpoolDepth--
	}
	if state.localCredit > 0 && state.localSpoolDepth == 0 {
		state.blockedSince = time.Time{}
	}
}

func (s *replicationPeerSession) slotSnapshots(slot int) []storage.ReplicationSessionSlotSnapshot {
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	out := make([]storage.ReplicationSessionSlotSnapshot, 0, 2)
	for _, kind := range []string{"forward", "commit"} {
		state, ok := s.diag[replicationSlotKindKey{kind: kind, slot: slot}]
		if !ok {
			continue
		}
		snapshot := storage.ReplicationSessionSlotSnapshot{
			Target:             s.target,
			Kind:               kind,
			Slot:               slot,
			AdvertisedCredit:   state.advertisedCredit,
			LocalCredit:        state.localCredit,
			LocalSpoolDepth:    state.localSpoolDepth,
			LastEnqueuedSeq:    state.lastEnqueuedSeq,
			LastTransmittedSeq: state.lastSentSeq,
			LastAckedSeq:       state.lastAckedSeq,
		}
		if !state.lastCreditUpdate.IsZero() {
			at := state.lastCreditUpdate
			snapshot.LastCreditUpdateAt = &at
		}
		if !state.blockedSince.IsZero() {
			at := state.blockedSince
			snapshot.BlockedSince = &at
		}
		if len(state.pending) > 0 {
			ids := make([]uint64, 0, len(state.pending))
			for id := range state.pending {
				ids = append(ids, id)
			}
			sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
			snapshot.PendingRequests = make([]storage.ReplicationSessionRequestSnapshot, 0, len(ids))
			for _, id := range ids {
				snapshot.PendingRequests = append(snapshot.PendingRequests, storage.ReplicationSessionRequestSnapshot{
					RequestID: id,
					Sequence:  state.pending[id],
				})
			}
		}
		out = append(out, snapshot)
	}
	return out
}

func (s *replicationPeerSession) run() {
	defer close(s.closedCh)

	var (
		backlog []*outboundReplicationRequest
		pending = map[uint64]*outboundReplicationRequest{}
		credits = map[int]int{}
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
			req := pending[id]
			replayed = append(replayed, req)
			s.recordRequeued(req)
			delete(pending, id)
		}
		backlog = append(replayed, backlog...)
	}

	slotCredit := func(slot int) int {
		credit, ok := credits[slot]
		if !ok {
			return 1
		}
		return credit
	}

	setSlotCredit := func(slot int, available int) {
		if available < 0 {
			available = 0
		}
		credits[slot] = available
	}

	var nextSendableIndex func() int
		nextSendableIndex = func() int {
			for i, req := range backlog {
				if req.ctx != nil {
					if err := req.ctx.Err(); err != nil {
						req.complete(err)
						s.recordCanceledBacklog(req)
						backlog = append(backlog[:i], backlog[i+1:]...)
						return nextSendableIndex()
					}
				}
				if slotCredit(req.slot) <= 0 {
					s.recordBlocked(req)
					continue
				}
				return i
			}
		return -1
	}

	handleInboundFrame := func(frame *grpcproto.ReplicationFrame) {
		if frame == nil {
			return
		}
		switch payload := frame.GetPayload().(type) {
		case *grpcproto.ReplicationFrame_SlotCredit:
			slot := int(payload.SlotCredit.Slot)
			available := int(payload.SlotCredit.Available)
			setSlotCredit(slot, available)
			s.recordCredit(slot, available)
		case *grpcproto.ReplicationFrame_Ack:
			req, ok := pending[frame.GetRequestId()]
			if !ok {
				return
			}
			delete(pending, frame.GetRequestId())
			s.recordAck(req)
			if payload.Ack == nil || payload.Ack.Success {
				req.complete(nil)
				return
			}
			req.complete(unmarshalEncodedError(payload.Ack.EncodedError))
		}
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
			if idx := nextSendableIndex(); idx != -1 {
				req := backlog[idx]
				if err := stream.Send(req.frame); err != nil {
					closeStream()
					requeuePending()
					continue
				}
				if observer := s.transport.currentObserver(); observer != nil {
					observer.ObserveReplicationSessionQueueWait(req.kind, s.target, time.Since(req.at))
				}
				nextCredit := slotCredit(req.slot) - 1
				setSlotCredit(req.slot, nextCredit)
				s.recordSent(req, nextCredit)
				pending[req.id] = req
				backlog = append(backlog[:idx], backlog[idx+1:]...)
				continue
			}
		}

		select {
		case req := <-s.sendCh:
			backlog = append(backlog, req)
		case frame := <-ackCh:
			handleInboundFrame(frame)
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
