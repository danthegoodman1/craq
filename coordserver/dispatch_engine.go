package coordserver

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync/atomic"
	"time"

	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
)

type dispatchPeerRefreshChange struct {
	slot  int
	state activePeerRefreshState
	clear bool
}

type engineContextKey struct{}

type engineResponse struct {
	value any
	err   error
}

type dispatchRequest struct {
	ctx                context.Context
	run                func(context.Context) (any, error)
	peerRefreshChanges []dispatchPeerRefreshChange
	reconcile          bool
	done               chan engineResponse
}

type dispatchTask struct {
	index int
}

type dispatchResult struct {
	index int
	err   error
}

type dispatchEngine struct {
	server            *Server
	requests          chan dispatchRequest
	signals           chan struct{}
	backgroundEnabled bool
	busy              atomic.Int32
	reconcilePending  atomic.Int32
	done              chan struct{}
}

func newDispatchEngine(server *Server, backgroundEnabled bool) *dispatchEngine {
	return &dispatchEngine{
		server:            server,
		requests:          make(chan dispatchRequest, 128),
		signals:           make(chan struct{}, 1),
		backgroundEnabled: backgroundEnabled,
		done:              make(chan struct{}),
	}
}

func withEngineContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, engineContextKey{}, true)
}

func contextInCoordinatorEngine(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	inEngine, _ := ctx.Value(engineContextKey{}).(bool)
	return inEngine
}

func (e *dispatchEngine) start() {
	go e.run()
}

func (e *dispatchEngine) isBusy() bool {
	return e != nil && e.busy.Load() != 0
}

func (e *dispatchEngine) wake(reconcile bool) {
	if e == nil {
		return
	}
	if reconcile {
		e.reconcilePending.Store(1)
	}
	select {
	case e.signals <- struct{}{}:
	default:
	}
}

func (e *dispatchEngine) drainUntilIdle(ctx context.Context, changes []dispatchPeerRefreshChange, reconcile bool) error {
	if e == nil {
		return nil
	}
	return e.submit(ctx, changes, reconcile, true)
}

func (e *dispatchEngine) submitCall(ctx context.Context, run func(context.Context) (any, error)) (any, error) {
	if e == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req := dispatchRequest{
		ctx:  ctx,
		run:  run,
		done: make(chan engineResponse, 1),
	}
	select {
	case <-e.server.closeCh:
		return nil, context.Canceled
	case <-ctx.Done():
		return nil, ctx.Err()
	case e.requests <- req:
	}
	select {
	case <-e.server.closeCh:
		return nil, context.Canceled
	case resp := <-req.done:
		return resp.value, resp.err
	}
}

func (e *dispatchEngine) submit(ctx context.Context, changes []dispatchPeerRefreshChange, reconcile bool, wait bool) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	req := dispatchRequest{
		ctx:                ctx,
		peerRefreshChanges: append([]dispatchPeerRefreshChange(nil), changes...),
		reconcile:          reconcile,
	}
	if wait {
		req.done = make(chan engineResponse, 1)
	}

	select {
	case <-e.server.closeCh:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	case e.requests <- req:
	}
	if !wait {
		return nil
	}

	select {
	case <-e.server.closeCh:
		return context.Canceled
	case resp := <-req.done:
		return resp.err
	}
}

func (e *dispatchEngine) run() {
	defer close(e.done)

	var ticker *time.Ticker
	var tick <-chan time.Time
	if e.backgroundEnabled {
		ticker = time.NewTicker(e.server.dispatchRetryInterval)
		tick = ticker.C
		defer ticker.Stop()
	}

	for {
		select {
		case <-e.server.closeCh:
			return
		case <-tick:
			if err := e.runBackgroundPass(e.server.backgroundContext(), e.consumeReconcilePending()); err != nil {
				e.server.logDispatchLoopError(err)
			}
		case <-e.signals:
			if err := e.runBackgroundPass(e.server.backgroundContext(), e.consumeReconcilePending()); err != nil {
				e.server.logDispatchLoopError(err)
			}
		case req := <-e.requests:
			engineCtx := withEngineContext(req.ctx)
			var (
				value any
				err   error
			)
			if req.run != nil {
				value, err = req.run(engineCtx)
			} else {
				e.applyPeerRefreshChanges(req.peerRefreshChanges)
				err = e.runUntilIdle(engineCtx, req.reconcile || e.consumeReconcilePending())
			}
			if req.done != nil {
				req.done <- engineResponse{value: value, err: err}
				close(req.done)
				continue
			}
			if err != nil {
				e.server.logDispatchLoopError(err)
			}
		}
	}
}

func (e *dispatchEngine) runBackgroundPass(ctx context.Context, reconcile bool) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.busy.Store(1)
	defer e.busy.Store(0)

	if err := ctx.Err(); err != nil {
		return err
	}
	e.server.syncViewsFromRuntime()
	e.server.rebuildRoutingSnapshot()
	if reconcile {
		if err := e.server.reconcileState(ctx); err != nil {
			return err
		}
		e.server.syncViewsFromRuntime()
		e.server.rebuildRoutingSnapshot()
	}
	moreOutbox, err := e.server.dispatchRuntimeOutboxBatchOwned(ctx, 0)
	if err != nil {
		return err
	}
	moreRefresh, err := e.server.dispatchQueuedActivePeerRefreshesBatchOwned(ctx, 0)
	if err != nil {
		return err
	}
	if moreOutbox || moreRefresh {
		e.wake(false)
	}
	return nil
}

func (e *dispatchEngine) runUntilIdle(ctx context.Context, reconcile bool) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.busy.Store(1)
	defer e.busy.Store(0)

	for pass := 0; pass < runtimeVersionRetryLimit; pass++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		before := e.server.backgroundDispatchSignature()
		e.server.syncViewsFromRuntime()
		e.server.rebuildRoutingSnapshot()
		if reconcile {
			if err := e.server.reconcileState(ctx); err != nil {
				return err
			}
			reconcile = false
			e.server.syncViewsFromRuntime()
			e.server.rebuildRoutingSnapshot()
		}
		err := e.server.dispatchRuntimeOutboxOwned(ctx)
		if err == nil {
			err = e.server.dispatchQueuedActivePeerRefreshesOwned(ctx)
		}
		if err != nil {
			return err
		}
		after := e.server.backgroundDispatchSignature()
		if after == before || !after.hasWork() {
			return nil
		}
	}
	return nil
}

func (e *dispatchEngine) applyPeerRefreshChanges(changes []dispatchPeerRefreshChange) {
	for _, change := range changes {
		if change.clear {
			e.server.clearActivePeerRefresh(change.slot)
			continue
		}
		e.server.enqueueActivePeerRefresh(change.slot, change.state)
	}
}

func (e *dispatchEngine) consumeReconcilePending() bool {
	return e.reconcilePending.Swap(0) != 0
}

func (s *Server) syncAndSubmitDispatch(ctx context.Context, changes []dispatchPeerRefreshChange, wait bool, reconcile bool) error {
	if s.dispatchEngine == nil {
		return nil
	}
	if contextInCoordinatorEngine(ctx) {
		s.syncViewsFromRuntime()
		s.dispatchEngine.applyPeerRefreshChanges(changes)
		s.rebuildRoutingSnapshot()
		if !wait {
			if reconcile {
				s.dispatchEngine.reconcilePending.Store(1)
			}
			s.dispatchEngine.wake(reconcile)
			return nil
		}
		return s.dispatchEngine.runUntilIdle(ctx, reconcile || s.dispatchEngine.consumeReconcilePending())
	}
	s.syncViewsFromRuntime()
	if !wait {
		for _, change := range changes {
			if change.clear {
				s.clearActivePeerRefresh(change.slot)
				continue
			}
			s.enqueueActivePeerRefresh(change.slot, change.state)
		}
		s.rebuildRoutingSnapshot()
		s.dispatchEngine.wake(reconcile)
		return nil
	}
	return s.dispatchEngine.drainUntilIdle(ctx, changes, reconcile)
}

func (s *Server) logDispatchLoopError(err error) {
	if err == nil {
		return
	}
	if errors.Is(err, ErrDispatchFailed) || errors.Is(err, ErrDispatchTimeout) {
		after := s.backgroundDispatchSignature()
		s.logger.Warn().
			Err(err).
			Str("component", "coordserver").
			Int("pending_entries", after.PendingEntries).
			Int("outbox_entries", after.OutboxEntries).
			Int("active_peer_refreshes", after.ActivePeerRefreshes).
			Msg("background dispatch loop will retry after partial failure")
		return
	}
	s.logger.Warn().Err(err).Str("component", "coordserver").Msg("non-ha dispatch loop observed error")
}

func submitEngineCall[T any](ctx context.Context, engine *dispatchEngine, run func(context.Context) (T, error)) (T, error) {
	var zero T
	if engine == nil || contextInCoordinatorEngine(ctx) {
		return run(ctx)
	}
	value, err := engine.submitCall(ctx, func(engineCtx context.Context) (any, error) {
		return run(engineCtx)
	})
	if err != nil {
		return zero, err
	}
	if value == nil {
		return zero, nil
	}
	typed, ok := value.(T)
	if !ok {
		return zero, fmt.Errorf("%w: unexpected engine response type %T", ErrStateMismatch, value)
	}
	return typed, nil
}

func acknowledgedOutboxEntryIDs(entries []coordruntime.OutboxEntry, results []error) []string {
	successIDs := make([]string, 0, len(entries))
	for index, entry := range entries {
		if results[index] == nil {
			successIDs = append(successIDs, entry.ID)
		}
	}
	sort.Strings(successIDs)
	return successIDs
}

func outboxAckCommandID(expectedVersion uint64, entryIDs []string) string {
	normalized := append([]string(nil), entryIDs...)
	sort.Strings(normalized)
	h := fnv.New64a()
	for _, entryID := range normalized {
		_, _ = h.Write([]byte(entryID))
		_, _ = h.Write([]byte{0})
	}
	return fmt.Sprintf("server-ack-outbox-v%d-n%d-h%x", expectedVersion, len(normalized), h.Sum64())
}
