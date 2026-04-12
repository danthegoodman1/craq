package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

const slotOwnerMailboxSize = 128

type slotOwner struct {
	node *Node
	slot int
	ch   chan func(*slotRuntime)
	completionCh chan func(*slotRuntime)
	done chan struct{}
	once sync.Once
}

type slotRuntime struct {
	owner  *slotOwner
	node   *Node
	slot   int
	exists bool
	record replicaRecord

	commitEffectInFlight   bool
	upstreamCommitInFlight bool
	catchupSyncInFlight    bool
}

type predecessorSyncPlan struct {
	sourceNodeID   string
	sourceTarget   string
	chainVersion   uint64
	currentHighest uint64
}

type predecessorSyncResult struct {
	plan     predecessorSyncPlan
	snapshot Snapshot
	highest  uint64
}

type slotWriteWaiter struct {
	once sync.Once
	done chan struct{}
	mu   sync.Mutex
	err  error
}

type slotSubmitWriteResponse struct {
	result CommitResult
	waiter *slotWriteWaiter
	role   ReplicaRole
	err    error
}

type slotReadResponse struct {
	result ReadResult
	err    error
}

type slotSnapshotResponse struct {
	snapshot Snapshot
	highest  uint64
	err      error
}

type slotSequencesResponse struct {
	sequences []uint64
	err       error
}

type slotAddReplicaResponse struct {
	autoActivate bool
	err          error
}

func newSlotWriteWaiter() *slotWriteWaiter {
	return &slotWriteWaiter{
		done: make(chan struct{}),
	}
}

func (w *slotWriteWaiter) complete(err error) {
	if w == nil {
		return
	}
	w.once.Do(func() {
		w.mu.Lock()
		w.err = err
		w.mu.Unlock()
		close(w.done)
	})
}

func (w *slotWriteWaiter) wait(ctx context.Context, nodeDone <-chan struct{}) error {
	if w == nil {
		return nil
	}
	for {
		select {
		case <-w.done:
			return w.result()
		case <-ctx.Done():
			if w.ready() {
				return w.result()
			}
			return fmt.Errorf("%w: %w", ErrWriteTimeout, ctx.Err())
		case <-nodeDone:
			if w.ready() {
				return w.result()
			}
			return fmt.Errorf("%w: %w", ErrWriteTimeout, context.Canceled)
		}
	}
}

func (w *slotWriteWaiter) ready() bool {
	if w == nil {
		return true
	}
	select {
	case <-w.done:
		return true
	default:
		return false
	}
}

func (w *slotWriteWaiter) result() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func newSlotOwner(node *Node, slot int, exists bool, record replicaRecord) *slotOwner {
	owner := &slotOwner{
		node:         node,
		slot:         slot,
		ch:           make(chan func(*slotRuntime), slotOwnerMailboxSize),
		completionCh: make(chan func(*slotRuntime), slotOwnerMailboxSize),
		done:         make(chan struct{}),
	}
	go owner.loop(exists, record)
	return owner
}

func (o *slotOwner) loop(exists bool, record replicaRecord) {
	runtime := &slotRuntime{
		owner:  o,
		node:   o.node,
		slot:   o.slot,
		exists: exists,
		record: record,
	}
	for {
		select {
		case <-o.node.done:
			return
		case <-o.done:
			return
		case fn := <-o.completionCh:
			fn(runtime)
			continue
		default:
		}
		select {
		case <-o.node.done:
			return
		case <-o.done:
			return
		case fn := <-o.completionCh:
			fn(runtime)
		case fn := <-o.ch:
			fn(runtime)
		}
	}
}

func (o *slotOwner) dispatch(ctx context.Context, fn func(*slotRuntime)) error {
	select {
	case <-o.node.done:
		return context.Canceled
	case <-o.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	case o.ch <- fn:
		return nil
	}
}

func (o *slotOwner) enqueue(fn func(*slotRuntime)) bool {
	select {
	case <-o.node.done:
		return false
	case <-o.done:
		return false
	case o.ch <- fn:
		return true
	}
}

func (o *slotOwner) enqueueCompletion(fn func(*slotRuntime)) bool {
	select {
	case <-o.node.done:
		return false
	case <-o.done:
		return false
	case o.completionCh <- fn:
		return true
	}
}

func (o *slotOwner) close() {
	o.once.Do(func() {
		close(o.done)
	})
}

func (rt *slotRuntime) bufferedCount() int {
	if !rt.exists {
		return 0
	}
	record := ensureProtocolReplicaState(rt.record)
	return len(record.bufferedForwards) + len(record.bufferedCommits)
}

func (rt *slotRuntime) publish(prevBuffered int) {
	if rt.exists {
		rt.node.publishReplicaRecord(rt.slot, rt.record)
	} else {
		rt.node.deletePublishedReplica(rt.slot)
	}
	if prevBuffered != rt.bufferedCount() {
		rt.node.refreshMetricGauges()
	}
}

func (rt *slotRuntime) setRecord(record replicaRecord) {
	prevBuffered := rt.bufferedCount()
	rt.exists = true
	rt.record = record
	rt.node.recordTimeoutMaterializerLag(rt.slot, record.highestCommittedSequence, record.materializedCommittedSequence)
	rt.publish(prevBuffered)
}

func (rt *slotRuntime) removeRecord() {
	prevBuffered := rt.bufferedCount()
	rt.exists = false
	rt.record = replicaRecord{}
	rt.node.recordTimeoutMaterializerLag(rt.slot, 0, 0)
	rt.publish(prevBuffered)
}

func (rt *slotRuntime) backgroundContext() context.Context {
	return rt.node.runtimeCtx
}

func (rt *slotRuntime) runAsync(run func() error, apply func(*slotRuntime, error)) {
	go func() {
		err := run()
		_ = rt.owner.enqueue(func(runtime *slotRuntime) {
			apply(runtime, err)
		})
	}()
}

func (rt *slotRuntime) activeRecord() (replicaRecord, error) {
	if !rt.exists {
		return replicaRecord{}, fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.state != ReplicaStateActive {
		return replicaRecord{}, fmt.Errorf("%w: slot %d is %q", ErrWriteRejected, rt.slot, record.state)
	}
	return record, nil
}

func (rt *slotRuntime) replicationRecord() (replicaRecord, error) {
	if !rt.exists {
		return replicaRecord{}, fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.state != ReplicaStateActive && record.state != ReplicaStateCatchingUp {
		return replicaRecord{}, fmt.Errorf("%w: slot %d is %q", ErrWriteRejected, rt.slot, record.state)
	}
	return record, nil
}

func (rt *slotRuntime) prepareActivation(ctx context.Context) error {
	if !rt.exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.state != ReplicaStateCatchingUp {
		return nil
	}
	return rt.syncFromPredecessor(ctx)
}

func (rt *slotRuntime) buildPredecessorSyncPlan() (predecessorSyncPlan, bool, error) {
	if !rt.exists {
		return predecessorSyncPlan{}, false, fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	sourceNodeID := record.assignment.Peers.PredecessorNodeID
	sourceTarget := record.assignment.Peers.PredecessorTarget
	if sourceNodeID == "" {
		return predecessorSyncPlan{}, false, nil
	}
	return predecessorSyncPlan{
		sourceNodeID:   sourceNodeID,
		sourceTarget:   sourceTarget,
		chainVersion:   record.assignment.ChainVersion,
		currentHighest: record.highestCommittedSequence,
	}, true, nil
}

func (rt *slotRuntime) fetchPredecessorSnapshot(ctx context.Context, plan predecessorSyncPlan) (Snapshot, uint64, error) {
	snapshot, highestCommittedSequence, err := rt.node.repl.FetchSnapshot(
		ctx,
		peerTransportTarget(plan.sourceTarget, plan.sourceNodeID),
		rt.slot,
	)
	if err != nil {
		return Snapshot{}, 0, fmt.Errorf("err in n.repl.FetchSnapshot: %w", err)
	}
	return snapshot, highestCommittedSequence, nil
}

func (rt *slotRuntime) reduceSyncedPredecessorState(record replicaRecord, highestCommittedSequence uint64) replicaRecord {
	record = ensureProtocolReplicaState(record)
	record.highestCommittedSequence = highestCommittedSequence
	record.materializedCommittedSequence = highestCommittedSequence
	record.highestUpstreamConfirmedSequence = highestCommittedSequence
	if nextSequence := highestCommittedSequence + 1; record.nextSequence < nextSequence {
		record.nextSequence = nextSequence
	}
	for sequence := range record.pendingWrites {
		if sequence <= highestCommittedSequence {
			delete(record.pendingWrites, sequence)
		}
	}
	for sequence := range record.stagedForwards {
		if sequence <= highestCommittedSequence {
			delete(record.stagedForwards, sequence)
		}
	}
	for sequence := range record.bufferedForwards {
		if sequence <= highestCommittedSequence {
			delete(record.bufferedForwards, sequence)
		}
	}
	for sequence := range record.bufferedCommits {
		if sequence <= highestCommittedSequence {
			delete(record.bufferedCommits, sequence)
		}
	}
	record.dirtyByKey = map[string][]dirtyReadEntry{}
	record.committedOverlay = map[string]dirtyReadEntry{}
	for _, pending := range record.pendingWrites {
		if pending.operation != nil {
			record = reduceAddDirtyEntry(record, *pending.operation)
		}
	}
	for _, staged := range record.stagedForwards {
		record = reduceAddDirtyEntry(record, staged.Operation)
	}
	return record
}

func (rt *slotRuntime) applyFetchedPredecessorSnapshot(ctx context.Context, result predecessorSyncResult) error {
	if !rt.exists {
		return nil
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.ChainVersion != result.plan.chainVersion ||
		record.assignment.Peers.PredecessorNodeID != result.plan.sourceNodeID ||
		record.assignment.Peers.PredecessorTarget != result.plan.sourceTarget {
		return nil
	}
	if result.highest <= record.highestCommittedSequence {
		return nil
	}
	if err := rt.node.backend.InstallSnapshot(rt.slot, result.snapshot); err != nil {
		return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
	}
	if err := rt.node.backend.SetHighestCommittedSequence(rt.slot, result.highest); err != nil {
		return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
	}
	record = rt.reduceSyncedPredecessorState(record, result.highest)
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	return nil
}

func (rt *slotRuntime) syncFromPredecessor(ctx context.Context) error {
	plan, ok, err := rt.buildPredecessorSyncPlan()
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	snapshot, highestCommittedSequence, err := rt.fetchPredecessorSnapshot(ctx, plan)
	if err != nil {
		return err
	}
	if highestCommittedSequence <= plan.currentHighest {
		return nil
	}
	return rt.applyFetchedPredecessorSnapshot(ctx, predecessorSyncResult{
		plan:     plan,
		snapshot: snapshot,
		highest:  highestCommittedSequence,
	})
}

func (rt *slotRuntime) hasBufferedGap() bool {
	if !rt.exists {
		return false
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.Peers.PredecessorNodeID == "" {
		return false
	}
	nextSequence := record.highestCommittedSequence + 1
	for sequence := range record.bufferedForwards {
		if sequence > nextSequence {
			return true
		}
	}
	for sequence := range record.bufferedCommits {
		if sequence > nextSequence {
			return true
		}
	}
	return false
}

func (rt *slotRuntime) ensureGapCatchup(ctx context.Context) {
	if !rt.hasBufferedGap() || rt.catchupSyncInFlight {
		return
	}
	plan, ok, err := rt.buildPredecessorSyncPlan()
	if err != nil || !ok {
		return
	}
	rt.catchupSyncInFlight = true
	var result predecessorSyncResult
	rt.runAsync(func() error {
		snapshot, highest, err := rt.fetchPredecessorSnapshot(ctx, plan)
		if err != nil {
			return err
		}
		if highest <= plan.currentHighest {
			return nil
		}
		result = predecessorSyncResult{
			plan:     plan,
			snapshot: snapshot,
			highest:  highest,
		}
		return nil
	}, func(runtime *slotRuntime, err error) {
		runtime.catchupSyncInFlight = false
		if err != nil {
			return
		}
		if result.highest > 0 {
			if err := runtime.applyFetchedPredecessorSnapshot(runtime.backgroundContext(), result); err != nil {
				return
			}
		}
		runtime.drainReadyBuffered(runtime.backgroundContext(), nil)
	})
}

func (rt *slotRuntime) handleSubmitWrite(
	ctx context.Context,
	kind OperationKind,
	key string,
	value string,
	conditions WriteConditions,
	resp chan<- slotSubmitWriteResponse,
) {
	record, err := rt.activeRecord()
	if err != nil {
		resp <- slotSubmitWriteResponse{err: err}
		return
	}
	if record.assignment.Role != ReplicaRoleHead && record.assignment.Role != ReplicaRoleSingle {
		resp <- slotSubmitWriteResponse{err: fmt.Errorf(
			"%w: slot %d role %q cannot accept writes",
			ErrWriteRejected,
			rt.slot,
			record.assignment.Role,
		)}
		return
	}
	getCommittedStarted := time.Now()
	current, found, err := rt.committedObject(key)
	rt.node.observeWriteStage(writeStageHeadGetCommitted, record.assignment.Role, writeStageResult(err), time.Since(getCommittedStarted))
	if err != nil {
		resp <- slotSubmitWriteResponse{err: err}
		return
	}
	if err := evaluateWriteConditions(conditions, found, current); err != nil {
		resp <- slotSubmitWriteResponse{err: err}
		return
	}
	if kind == OperationKindDelete && !found {
		resp <- slotSubmitWriteResponse{result: CommitResult{Slot: rt.slot}}
		return
	}
	if err := rt.node.tryAdmitNodeClientWrite(rt.slot, record.inFlightClientWrites); err != nil {
		resp <- slotSubmitWriteResponse{err: err}
		return
	}
	stageStarted := time.Now()
	record.inFlightClientWrites++
	operation := WriteOperation{
		Slot:     rt.slot,
		Sequence: record.nextSequence,
		Kind:     kind,
		Key:      key,
		Value:    value,
		Metadata: rt.node.nextObjectMetadata(found, current),
	}
	reduction := reduceSubmitWrite(record, operation)
	waiter := newSlotWriteWaiter()
	pending := reduction.Record.pendingWrites[operation.Sequence]
	pending.waiter = waiter
	reduction.Record.pendingWrites[operation.Sequence] = pending
	reduction.Record.inFlightClientWrites = record.inFlightClientWrites
	rt.setRecord(reduction.Record)
	rt.node.observeWriteStage(writeStageHeadStageOp, reduction.Record.assignment.Role, writeStageResultSuccess, time.Since(stageStarted))
	rt.node.refreshMetricGauges()
	if reduction.Record.assignment.Role == ReplicaRoleHead {
		rt.node.traceWriteEvent(reduction.Record.assignment, operation.Sequence, "head_accepted_write")
	}

	role := reduction.Record.assignment.Role
	switch role {
	case ReplicaRoleSingle:
		commitResp := make(chan error, 1)
		rt.startCommitEffect(rt.backgroundContext(), ctx, operation.Sequence, commitResp)
		go func() {
			select {
			case <-rt.node.done:
			case <-commitResp:
				resp <- slotSubmitWriteResponse{
					result: reduction.Result,
					waiter: waiter,
					role:   role,
				}
			}
		}()
		return
	case ReplicaRoleHead:
		successorNodeID := reduction.Record.assignment.Peers.SuccessorNodeID
		successorTarget := reduction.Record.assignment.Peers.SuccessorTarget
		if successorNodeID == "" {
			waiter.complete(fmt.Errorf("%w: slot %d head has no successor", ErrStateMismatch, rt.slot))
			resp <- slotSubmitWriteResponse{
				result: reduction.Result,
				waiter: waiter,
				role:   role,
			}
			return
		}
		req := ForwardWriteRequest{
			Operation:    cloneWriteOperation(operation),
			FromNodeID:   rt.node.nodeID,
			ChainVersion: reduction.Record.assignment.ChainVersion,
		}
		rt.runAsync(func() error {
			forwardStarted := time.Now()
			err := rt.node.repl.ForwardWrite(
				ctx,
				peerTransportTarget(successorTarget, successorNodeID),
				req,
			)
			rt.node.observeWriteStage(writeStageHeadForwardRPC, role, writeStageResult(err), time.Since(forwardStarted))
			return err
		}, func(runtime *slotRuntime, err error) {
			if err != nil {
				waiter.complete(fmt.Errorf("err in n.repl.ForwardWrite: %w", err))
			} else {
				runtime.node.traceWriteEvent(reduction.Record.assignment, operation.Sequence, "head_forward_accepted")
			}
			resp <- slotSubmitWriteResponse{
				result: reduction.Result,
				waiter: waiter,
				role:   role,
			}
		})
		return
	default:
		resp <- slotSubmitWriteResponse{
			result: reduction.Result,
			waiter: waiter,
			role:   role,
		}
	}
}

func (rt *slotRuntime) startCommitEffect(applyCtx context.Context, resultCtx context.Context, sequence uint64, resp chan<- error) {
	if !rt.exists {
		if resp != nil {
			resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		}
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	operation, err := reduceCommittableOperation(record, sequence)
	if err != nil {
		if resp != nil {
			resp <- err
		}
		return
	}
	applied := record
	applied.highestCommittedSequence = sequence
	applied.localDataPresent = true
	if applied.state != ReplicaStateRecovered {
		applied.lastKnownState = applied.state
	}
	applied.highestUpstreamConfirmedSequence = normalizeUpstreamConfirmedSequence(record)
	persisted := persistedReplica(applied)
	role := record.assignment.Role
	stage, recordStage := writeCommitApplyStage(role)
	commit := DurableCommit{
		Operation:                 operation,
		Persisted:                 persisted,
		UpstreamConfirmedSequence: upstreamConfirmedSequenceForLocalCommit(record, sequence),
	}
	if stage := writeTraceCommitIntentStage(role); stage != "" {
		rt.node.traceWriteEvent(record.assignment, sequence, stage)
	}
	rt.commitEffectInFlight = true
	applyStarted := time.Now()
	if err := rt.node.submitCommittedOperation(applyCtx, rt.owner, commit, func(runtime *slotRuntime, err error, completedAt time.Time) {
		runtime.commitEffectInFlight = false
		if recordStage {
			runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		runtime.handleCommitEffectResult(resultCtx, sequence, operation, nil, err, resp)
	}); err != nil {
		rt.commitEffectInFlight = false
		if recordStage {
			rt.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		rt.handleCommitEffectResult(resultCtx, sequence, operation, nil, err, resp)
	}
}

func (rt *slotRuntime) handleCommitEffectResult(
	ctx context.Context,
	sequence uint64,
	operation WriteOperation,
	commitReq *CommitWriteRequest,
	err error,
	resp chan<- error,
) {
	if !rt.exists {
		if resp != nil {
			resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		}
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	pending := record.pendingWrites[sequence]
	committed := err == nil || rt.node.writeActuallyCommitted(rt.slot, sequence)
	if committed {
		record = reduceApplyCommittedSequence(record, operation, sequence, rt.node.maxBufferedReplicaMessagesPerSlot)
		if commitReq != nil {
			record = reduceRecordCommitApplied(record, *commitReq, rt.node.maxBufferedReplicaMessagesPerSlot)
		}
		record = recordWithCommittedOverlay(record, operation)
		if record.assignment.Peers.PredecessorNodeID == "" {
			record.highestUpstreamConfirmedSequence = sequence
		}
		rt.setRecord(record)
		if pending.waiter != nil {
			rt.node.traceWriteEvent(record.assignment, sequence, "waiter_released")
			pending.waiter.complete(err)
		}
	} else if pending.waiter != nil {
		rt.node.traceWriteEvent(record.assignment, sequence, "waiter_released")
		pending.waiter.complete(err)
	}
	if err != nil {
		if resp != nil {
			resp <- err
		}
		return
	}
	if !rt.exists {
		if resp != nil {
			resp <- nil
		}
		return
	}
	record = ensureProtocolReplicaState(rt.record)
	if ctx != nil && ctx.Err() != nil {
		if resp != nil {
			resp <- ctx.Err()
		}
		return
	}
	predecessorNodeID := record.assignment.Peers.PredecessorNodeID
	predecessorTarget := record.assignment.Peers.PredecessorTarget
	if predecessorNodeID == "" {
		rt.drainReadyBuffered(rt.backgroundContext(), resp)
		return
	}
	upstreamCommitReq := CommitWriteRequest{
		Slot:         rt.slot,
		Sequence:     sequence,
		FromNodeID:   rt.node.nodeID,
		ChainVersion: record.assignment.ChainVersion,
	}
	role := record.assignment.Role
	rt.runAsync(func() error {
		commitStarted := time.Now()
		err := rt.node.repl.CommitWrite(
			ctx,
			peerTransportTarget(predecessorTarget, predecessorNodeID),
			upstreamCommitReq,
		)
		rt.node.observeWriteStage(writeStageCommitUpstreamRPC, role, writeStageResult(err), time.Since(commitStarted))
		return err
	}, func(runtime *slotRuntime, commitErr error) {
		if commitErr != nil {
			if resp != nil {
				resp <- fmt.Errorf("err in n.repl.CommitWrite: %w", commitErr)
			}
			return
		}
		record := ensureProtocolReplicaState(runtime.record)
		if err := runtime.node.submitUpstreamConfirmedSequence(runtime.backgroundContext(), runtime.owner, record.assignment, sequence, func(applyRuntime *slotRuntime, err error, _ time.Time) {
			if err != nil {
				if resp != nil {
					resp <- fmt.Errorf("err in n.commitJournal.submitUpstreamConfirm: %w", err)
				}
				return
			}
			if applyRuntime.exists {
				record := ensureProtocolReplicaState(applyRuntime.record)
				if sequence > record.highestUpstreamConfirmedSequence {
					record.highestUpstreamConfirmedSequence = sequence
					applyRuntime.setRecord(record)
				}
			}
			applyRuntime.drainReadyBuffered(applyRuntime.backgroundContext(), resp)
		}); err != nil {
			if resp != nil {
				resp <- err
			}
		}
	})
}

func (rt *slotRuntime) handleForwardWrite(
	ctx context.Context,
	req ForwardWriteRequest,
	resp chan<- error,
) {
	record, err := rt.replicationRecord()
	if err != nil {
		resp <- err
		return
	}
	if record.assignment.Peers.SuccessorNodeID == "" && rt.commitEffectInFlight && req.Operation.Sequence == record.nextSequence {
		buffered, err := reduceBufferFutureForward(record, req, slotProtocolBufferLimits{
			perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
			perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
			nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
		})
		if err != nil {
			rt.node.observeBackpressure(err)
			resp <- err
			return
		}
		rt.setRecord(buffered)
		rt.node.refreshMetricGauges()
		resp <- nil
		return
	}
	reduction, err := reduceForwardWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
	})
	if err != nil {
		if req.Operation.Sequence > record.nextSequence {
			rt.node.observeBackpressure(err)
		}
		resp <- err
		return
	}
	switch reduction.Action {
	case slotReducerActionIgnore:
		resp <- nil
		return
	case slotReducerActionBuffer:
		rt.setRecord(reduction.Record)
		rt.node.refreshMetricGauges()
		rt.ensureGapCatchup(rt.backgroundContext())
		resp <- nil
		return
	}
	rt.setRecord(reduction.Record)
	rt.applyForward(ctx, req, resp)
}

func (rt *slotRuntime) handleForwardWriteAccepted(
	ctx context.Context,
	req ForwardWriteRequest,
	resp chan<- error,
) {
	record, err := rt.replicationRecord()
	if err != nil {
		resp <- err
		return
	}
	if record.assignment.Peers.SuccessorNodeID == "" && rt.commitEffectInFlight && req.Operation.Sequence == record.nextSequence {
		buffered, err := reduceBufferFutureForward(record, req, slotProtocolBufferLimits{
			perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
			perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
			nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
		})
		if err != nil {
			rt.node.observeBackpressure(err)
			resp <- err
			return
		}
		rt.setRecord(buffered)
		rt.node.refreshMetricGauges()
		resp <- nil
		return
	}
	reduction, err := reduceForwardWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
	})
	if err != nil {
		if req.Operation.Sequence > record.nextSequence {
			rt.node.observeBackpressure(err)
		}
		resp <- err
		return
	}
	switch reduction.Action {
	case slotReducerActionIgnore:
		resp <- nil
		return
	case slotReducerActionBuffer:
		rt.setRecord(reduction.Record)
		rt.node.refreshMetricGauges()
		rt.ensureGapCatchup(rt.backgroundContext())
		resp <- nil
		return
	}
	rt.setRecord(reduction.Record)
	rt.applyForwardAccepted(ctx, req, resp)
}

func (rt *slotRuntime) applyForward(
	ctx context.Context,
	req ForwardWriteRequest,
	resp chan<- error,
) {
	if !rt.exists {
		resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.Peers.SuccessorNodeID == "" {
		rt.startCommitEffect(rt.backgroundContext(), ctx, req.Operation.Sequence, resp)
		return
	}
	role := record.assignment.Role
	successorNodeID := record.assignment.Peers.SuccessorNodeID
	successorTarget := record.assignment.Peers.SuccessorTarget
	forwardReq := ForwardWriteRequest{
		Operation:    cloneWriteOperation(req.Operation),
		FromNodeID:   rt.node.nodeID,
		ChainVersion: record.assignment.ChainVersion,
	}
	rt.runAsync(func() error {
		forwardStarted := time.Now()
		err := rt.node.repl.ForwardWrite(
			ctx,
			peerTransportTarget(successorTarget, successorNodeID),
			forwardReq,
		)
		rt.node.observeWriteStage(writeStageHeadForwardRPC, role, writeStageResult(err), time.Since(forwardStarted))
		return err
	}, func(runtime *slotRuntime, err error) {
		if err != nil {
			if resp != nil {
				resp <- fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
			}
			return
		}
		runtime.drainReadyBuffered(runtime.backgroundContext(), resp)
	})
}

func (rt *slotRuntime) applyForwardAccepted(
	ctx context.Context,
	req ForwardWriteRequest,
	resp chan<- error,
) {
	if !rt.exists {
		resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.Peers.SuccessorNodeID == "" {
		rt.startCommitEffect(rt.backgroundContext(), rt.backgroundContext(), req.Operation.Sequence, nil)
		resp <- nil
		return
	}
	role := record.assignment.Role
	successorNodeID := record.assignment.Peers.SuccessorNodeID
	successorTarget := record.assignment.Peers.SuccessorTarget
	forwardReq := ForwardWriteRequest{
		Operation:    cloneWriteOperation(req.Operation),
		FromNodeID:   rt.node.nodeID,
		ChainVersion: record.assignment.ChainVersion,
	}
	rt.runAsync(func() error {
		forwardStarted := time.Now()
		err := rt.node.repl.ForwardWrite(
			ctx,
			peerTransportTarget(successorTarget, successorNodeID),
			forwardReq,
		)
		rt.node.observeWriteStage(writeStageForwardAcceptRPC, role, writeStageResult(err), time.Since(forwardStarted))
		return err
	}, func(_ *slotRuntime, err error) {
		if err != nil {
			resp <- fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
			return
		}
		resp <- nil
	})
}

func (rt *slotRuntime) handleCommitWrite(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	record, err := rt.replicationRecord()
	if err != nil {
		resp <- err
		return
	}
	if rt.commitEffectInFlight && req.Sequence == record.highestCommittedSequence+1 {
		buffered, err := reduceBufferFutureCommit(record, req, slotProtocolBufferLimits{
			perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
			perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
			nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
		})
		if err != nil {
			rt.node.observeBackpressure(err)
			resp <- err
			return
		}
		rt.setRecord(buffered)
		rt.node.refreshMetricGauges()
		resp <- nil
		return
	}
	reduction, err := reduceCommitWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
	})
	if err != nil {
		if req.Sequence > record.highestCommittedSequence+1 || !reduceHasCommittableSequence(record, req.Sequence) {
			rt.node.observeBackpressure(err)
		}
		resp <- err
		return
	}
	switch reduction.Action {
	case slotReducerActionIgnore:
		resp <- nil
		return
	case slotReducerActionBuffer:
		rt.setRecord(reduction.Record)
		rt.node.refreshMetricGauges()
		rt.ensureGapCatchup(rt.backgroundContext())
		resp <- nil
		return
	}
	rt.applyCommit(ctx, req, resp)
}

func (rt *slotRuntime) handleCommitWriteAccepted(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	record, err := rt.activeRecord()
	if err != nil {
		resp <- err
		return
	}
	if rt.commitEffectInFlight && req.Sequence == record.highestCommittedSequence+1 {
		buffered, err := reduceBufferFutureCommit(record, req, slotProtocolBufferLimits{
			perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
			perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
			nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
		})
		if err != nil {
			rt.node.observeBackpressure(err)
			resp <- err
			return
		}
		rt.setRecord(buffered)
		rt.node.refreshMetricGauges()
		resp <- nil
		return
	}
	reduction, err := reduceCommitWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
	})
	if err != nil {
		if req.Sequence > record.highestCommittedSequence+1 || !reduceHasCommittableSequence(record, req.Sequence) {
			rt.node.observeBackpressure(err)
		}
		resp <- err
		return
	}
	switch reduction.Action {
	case slotReducerActionIgnore:
		resp <- nil
		return
	case slotReducerActionBuffer:
		rt.setRecord(reduction.Record)
		rt.node.refreshMetricGauges()
		rt.ensureGapCatchup(rt.backgroundContext())
		resp <- nil
		return
	}
	rt.applyCommitAccepted(ctx, req, resp)
}

func (rt *slotRuntime) applyCommit(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	if !rt.exists {
		if resp != nil {
			resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		}
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	operation, err := reduceCommittableOperation(record, req.Sequence)
	if err != nil {
		if resp != nil {
			resp <- err
		}
		return
	}
	applied := record
	applied.highestCommittedSequence = req.Sequence
	applied.localDataPresent = true
	if applied.state != ReplicaStateRecovered {
		applied.lastKnownState = applied.state
	}
	applied.highestUpstreamConfirmedSequence = normalizeUpstreamConfirmedSequence(record)
	persisted := persistedReplica(applied)
	reqCopy := req
	role := record.assignment.Role
	stage, recordStage := writeCommitApplyStage(role)
	commit := DurableCommit{
		Operation:                 operation,
		Persisted:                 persisted,
		UpstreamConfirmedSequence: upstreamConfirmedSequenceForLocalCommit(record, req.Sequence),
	}
	if stage := writeTraceCommitIntentStage(role); stage != "" {
		rt.node.traceWriteEvent(record.assignment, req.Sequence, stage)
	}
	rt.commitEffectInFlight = true
	applyStarted := time.Now()
	if err := rt.node.submitCommittedOperation(rt.backgroundContext(), rt.owner, commit, func(runtime *slotRuntime, err error, completedAt time.Time) {
		runtime.commitEffectInFlight = false
		if recordStage {
			runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		runtime.handleCommitEffectResult(ctx, req.Sequence, operation, &reqCopy, err, resp)
	}); err != nil {
		rt.commitEffectInFlight = false
		if recordStage {
			rt.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		rt.handleCommitEffectResult(ctx, req.Sequence, operation, &reqCopy, err, resp)
	}
}

func (rt *slotRuntime) applyCommitAccepted(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	if !rt.exists {
		resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	operation, err := reduceCommittableOperation(record, req.Sequence)
	if err != nil {
		resp <- err
		return
	}
	applied := record
	applied.highestCommittedSequence = req.Sequence
	applied.localDataPresent = true
	if applied.state != ReplicaStateRecovered {
		applied.lastKnownState = applied.state
	}
	applied.highestUpstreamConfirmedSequence = normalizeUpstreamConfirmedSequence(record)
	reqCopy := req
	role := record.assignment.Role
	if stage := writeTraceCommitAcceptReceivedStage(role); stage != "" {
		rt.node.traceWriteEvent(record.assignment, req.Sequence, stage)
	}
	stage, recordStage := writeCommitApplyStage(role)
	commit := DurableCommit{
		Operation:                 operation,
		Persisted:                 persistedReplica(applied),
		UpstreamConfirmedSequence: upstreamConfirmedSequenceForLocalCommit(record, req.Sequence),
	}
	if stage := writeTraceCommitIntentStage(role); stage != "" {
		rt.node.traceWriteEvent(record.assignment, req.Sequence, stage)
	}
	rt.commitEffectInFlight = true
	applyStarted := time.Now()
	if err := rt.node.submitCommittedOperation(rt.backgroundContext(), rt.owner, commit, func(runtime *slotRuntime, err error, completedAt time.Time) {
		runtime.commitEffectInFlight = false
		if recordStage {
			runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		if !runtime.exists {
			resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, runtime.slot)
			return
		}
		record := ensureProtocolReplicaState(runtime.record)
		pending := record.pendingWrites[req.Sequence]
		committed := err == nil || runtime.node.writeActuallyCommitted(runtime.slot, req.Sequence)
		if committed {
			record = reduceApplyCommittedSequence(record, operation, req.Sequence, runtime.node.maxBufferedReplicaMessagesPerSlot)
			record = reduceRecordCommitApplied(record, reqCopy, runtime.node.maxBufferedReplicaMessagesPerSlot)
			record = recordWithCommittedOverlay(record, operation)
			if record.assignment.Peers.PredecessorNodeID == "" {
				record.highestUpstreamConfirmedSequence = req.Sequence
			}
			runtime.setRecord(record)
			if pending.waiter != nil {
				runtime.node.traceWriteEvent(record.assignment, req.Sequence, "waiter_released")
				pending.waiter.complete(err)
			}
		} else if pending.waiter != nil {
			runtime.node.traceWriteEvent(record.assignment, req.Sequence, "waiter_released")
			pending.waiter.complete(err)
		}
		if err != nil {
			resp <- err
			return
		}
		resp <- nil
		runtime.ensureUpstreamCommitReplay(runtime.backgroundContext())
		runtime.drainReadyBuffered(runtime.backgroundContext(), nil)
	}); err != nil {
		rt.commitEffectInFlight = false
		if recordStage {
			rt.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		resp <- err
	}
}

func (rt *slotRuntime) drainReadyBuffered(ctx context.Context, resp chan<- error) {
	if !rt.exists {
		if resp != nil {
			resp <- nil
		}
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if req, ok := record.bufferedForwards[record.nextSequence]; ok {
		if rt.commitEffectInFlight && record.assignment.Peers.SuccessorNodeID == "" {
			if resp != nil {
				resp <- nil
			}
			return
		}
		reduction, err := reduceForwardWrite(record, req, slotProtocolBufferLimits{
			perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
			perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
			nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
		})
		if err != nil || reduction.Action != slotReducerActionApply {
			if resp != nil {
				resp <- nil
			}
			return
		}
		rt.setRecord(reduction.Record)
		rt.applyForward(ctx, req, resp)
		return
	}
	nextCommit := record.highestCommittedSequence + 1
	if req, ok := record.bufferedCommits[nextCommit]; ok && reduceHasCommittableSequence(record, nextCommit) {
		if rt.commitEffectInFlight {
			if resp != nil {
				resp <- nil
			}
			return
		}
		reduction, err := reduceCommitWrite(record, req, slotProtocolBufferLimits{
			perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
			perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
			nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
		})
		if err != nil || reduction.Action != slotReducerActionApply {
			if resp != nil {
				resp <- nil
			}
			return
		}
		rt.applyCommit(ctx, req, resp)
		return
	}
	rt.ensureGapCatchup(rt.backgroundContext())
	if resp != nil {
		resp <- nil
	}
}

func (rt *slotRuntime) ensureUpstreamCommitReplay(ctx context.Context) {
	if !rt.exists || rt.upstreamCommitInFlight {
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.Peers.PredecessorNodeID == "" {
		if record.highestUpstreamConfirmedSequence != record.highestCommittedSequence {
			record.highestUpstreamConfirmedSequence = record.highestCommittedSequence
			rt.setRecord(record)
		}
		return
	}
	nextSequence := record.highestUpstreamConfirmedSequence + 1
	if nextSequence == 0 || nextSequence > record.highestCommittedSequence {
		return
	}
	predecessorNodeID := record.assignment.Peers.PredecessorNodeID
	predecessorTarget := record.assignment.Peers.PredecessorTarget
	role := record.assignment.Role
	req := CommitWriteRequest{
		Slot:         rt.slot,
		Sequence:     nextSequence,
		FromNodeID:   rt.node.nodeID,
		ChainVersion: record.assignment.ChainVersion,
	}
	rt.upstreamCommitInFlight = true
	rt.runAsync(func() error {
		started := time.Now()
		err := rt.node.repl.CommitWrite(
			ctx,
			peerTransportTarget(predecessorTarget, predecessorNodeID),
			req,
		)
		rt.node.observeWriteStage(writeStageCommitUpstreamAcceptRPC, role, writeStageResult(err), time.Since(started))
		return err
	}, func(runtime *slotRuntime, err error) {
		if err != nil {
			runtime.upstreamCommitInFlight = false
			runtime.node.events.record(runtime.node.logger, zerolog.ErrorLevel, "replication_commit_replay_failed", "storage upstream commit replay failed", &runtime.slot, nil, &nextSequence, predecessorNodeID, "", err)
			return
		}
		if !runtime.exists {
			runtime.upstreamCommitInFlight = false
			return
		}
		record := ensureProtocolReplicaState(runtime.record)
		if err := runtime.node.submitUpstreamConfirmedSequence(runtime.backgroundContext(), runtime.owner, record.assignment, nextSequence, func(applyRuntime *slotRuntime, err error, _ time.Time) {
			applyRuntime.upstreamCommitInFlight = false
			if err != nil {
				applyRuntime.node.events.record(applyRuntime.node.logger, zerolog.ErrorLevel, "replication_commit_confirm_failed", "storage upstream commit confirmation persist failed", &applyRuntime.slot, nil, &nextSequence, predecessorNodeID, "", err)
				return
			}
			if !applyRuntime.exists {
				return
			}
			record := ensureProtocolReplicaState(applyRuntime.record)
			if nextSequence > record.highestUpstreamConfirmedSequence {
				record.highestUpstreamConfirmedSequence = nextSequence
				applyRuntime.setRecord(record)
			}
			applyRuntime.ensureUpstreamCommitReplay(applyRuntime.backgroundContext())
		}); err != nil {
			runtime.upstreamCommitInFlight = false
			runtime.node.events.record(runtime.node.logger, zerolog.ErrorLevel, "replication_commit_confirm_failed", "storage upstream commit confirmation persist failed", &runtime.slot, nil, &nextSequence, predecessorNodeID, "", err)
			return
		}
	})
}

func (rt *slotRuntime) releaseClientWrite() {
	if !rt.exists {
		rt.node.releaseNodeClientWrite()
		return
	}
	record := rt.record
	if record.inFlightClientWrites > 0 {
		record.inFlightClientWrites--
	}
	rt.setRecord(record)
	rt.node.releaseNodeClientWrite()
}

func (rt *slotRuntime) handleClientGet(ctx context.Context, req ClientGetRequest) (ReadResult, error) {
	if !rt.exists {
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, ReplicaAssignment{}, ReplicaState(""), RoutingMismatchReasonUnknownSlot)
	}
	record := rt.record
	if record.state != ReplicaStateActive {
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, record.assignment, record.state, RoutingMismatchReasonInactiveReplica)
	}
	if record.assignment.ChainVersion != req.ExpectedChainVersion {
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, record.assignment, record.state, RoutingMismatchReasonWrongVersion)
	}
	assignment := cloneAssignment(record.assignment)
	dirtyEntries := rt.node.dirtyEntriesForKey(record, req.Key)
	consistency := normalizeReadConsistency(req.Consistency)
	object, found, err := rt.node.resolveRead(ctx, req, record, assignment, dirtyEntries, consistency)
	if err != nil {
		return ReadResult{}, err
	}
	result := ReadResult{
		Slot:         req.Slot,
		ChainVersion: assignment.ChainVersion,
		Found:        found,
	}
	if found {
		result.Value = object.Value
		result.Metadata = cloneObjectMetadataPtr(&object.Metadata)
	}
	return result, nil
}

func (rt *slotRuntime) handleCommittedSnapshotWithSequence() (Snapshot, uint64, error) {
	snapshot, err := rt.node.backend.CommittedSnapshot(rt.slot)
	if err != nil {
		return nil, 0, fmt.Errorf("err in n.backend.CommittedSnapshot: %w", err)
	}
	if !rt.exists {
		return snapshot, 0, nil
	}
	return recordMergedCommittedSnapshot(rt.record, snapshot), rt.record.highestCommittedSequence, nil
}

func (rt *slotRuntime) handleStagedSequences() ([]uint64, error) {
	if !rt.exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	unique := map[uint64]struct{}{}
	for sequence := range record.pendingWrites {
		unique[sequence] = struct{}{}
	}
	for sequence := range record.stagedForwards {
		unique[sequence] = struct{}{}
	}
	if sequences, err := rt.node.backend.StagedSequences(rt.slot); err == nil {
		for _, sequence := range sequences {
			unique[sequence] = struct{}{}
		}
	} else if !errors.Is(err, ErrUnknownReplica) {
		return nil, fmt.Errorf("err in n.backend.StagedSequences: %w", err)
	}
	sequences := make([]uint64, 0, len(unique))
	for sequence := range unique {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (rt *slotRuntime) handleBufferedForwardSequences() ([]uint64, error) {
	if !rt.exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	sequences := make([]uint64, 0, len(record.bufferedForwards))
	for sequence := range record.bufferedForwards {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (rt *slotRuntime) handleBufferedCommitSequences() ([]uint64, error) {
	if !rt.exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	sequences := make([]uint64, 0, len(record.bufferedCommits))
	for sequence := range record.bufferedCommits {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (rt *slotRuntime) addReplicaAsTail(ctx context.Context, cmd AddReplicaAsTailCommand) (bool, error) {
	start := time.Now()
	if cmd.Assignment.Slot < 0 {
		return false, fmt.Errorf("%w: slot must be >= 0", ErrInvalidConfig)
	}
	if rt.exists {
		if reflect.DeepEqual(rt.record.assignment, cmd.Assignment) && rt.record.state != ReplicaStateRemoved {
			return false, nil
		}
		return false, fmt.Errorf("%w: slot %d", ErrReplicaExists, cmd.Assignment.Slot)
	}
	needsCatchup := cmd.Assignment.Peers.PredecessorNodeID != ""
	autoActivate := !needsCatchup
	if needsCatchup {
		if err := rt.node.admitCatchup(); err != nil {
			rt.node.observeBackpressure(err)
			return false, err
		}
		defer rt.node.releaseCatchup()
	}

	if err := rt.node.backend.CreateReplica(cmd.Assignment.Slot); err != nil {
		if errors.Is(err, ErrReplicaExists) && rt.node.waitForReplicaCreationReplay(ctx, cmd.Assignment) {
			return false, nil
		}
		return false, fmt.Errorf("err in n.backend.CreateReplica: %w", err)
	}

	rollback := true
	defer func() {
		if rollback {
			_ = rt.node.backend.DeleteReplica(cmd.Assignment.Slot)
		}
	}()

	record := replicaRecord{
		assignment:                       cloneAssignment(cmd.Assignment),
		state:                            ReplicaStatePending,
		nextSequence:                     1,
		materializedCommittedSequence:    0,
		localDataPresent:                 true,
		lastKnownState:                   ReplicaStatePending,
		highestUpstreamConfirmedSequence: 0,
	}
	record = ensureProtocolReplicaState(record)
	rt.setRecord(record)

	if sourceNodeID := cmd.Assignment.Peers.PredecessorNodeID; sourceNodeID != "" {
		snapshot, highestCommittedSequence, err := rt.node.repl.FetchSnapshot(
			ctx,
			peerTransportTarget(cmd.Assignment.Peers.PredecessorTarget, sourceNodeID),
			cmd.Assignment.Slot,
		)
		if err != nil {
			rt.removeRecord()
			return false, fmt.Errorf("err in n.repl.FetchSnapshot: %w", err)
		}
		if err := rt.node.backend.InstallSnapshot(cmd.Assignment.Slot, snapshot); err != nil {
			rt.removeRecord()
			return false, fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
		}
		if err := rt.node.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
			rt.removeRecord()
			return false, fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
		}
		record.highestCommittedSequence = highestCommittedSequence
		record.materializedCommittedSequence = highestCommittedSequence
		record.highestUpstreamConfirmedSequence = highestCommittedSequence
		record.nextSequence = highestCommittedSequence + 1
		autoActivate = len(snapshot) == 0 && highestCommittedSequence == 0
	}

	record.state = ReplicaStateCatchingUp
	record.lastKnownState = ReplicaStateCatchingUp
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		rt.removeRecord()
		return false, fmt.Errorf("err in n.persistReplica: %w", err)
	}
	rollback = false
	if rt.node.metrics != nil {
		rt.node.metrics.catchupOps.WithLabelValues("add_replica_as_tail", "success").Inc()
		rt.node.metrics.catchupDuration.Observe(time.Since(start).Seconds())
	}
	rt.node.refreshMetricGauges()
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "add_replica", "storage replica added as tail", &cmd.Assignment.Slot, &cmd.Assignment.ChainVersion, nil, cmd.Assignment.Peers.PredecessorNodeID, "", nil)

	return autoActivate && rt.node.autoActivateEmptyReplicas, nil
}

func (rt *slotRuntime) activateReplicaReady(ctx context.Context) error {
	if !rt.exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := rt.record
	if record.state == ReplicaStateActive {
		return nil
	}
	if record.state != ReplicaStateCatchingUp {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, rt.slot, record.state)
	}
	record.state = ReplicaStateActive
	record.lastKnownState = ReplicaStateActive
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "activate_replica", "storage replica activated", &rt.slot, &record.assignment.ChainVersion, nil, "", "", nil)
	rt.drainReadyBuffered(rt.backgroundContext(), nil)
	return nil
}

func (rt *slotRuntime) markReplicaLeaving(ctx context.Context) error {
	if !rt.exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := rt.record
	if record.state == ReplicaStateLeaving || record.state == ReplicaStateRemoved {
		return nil
	}
	if record.state != ReplicaStateActive {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, rt.slot, record.state)
	}
	record.state = ReplicaStateLeaving
	record.lastKnownState = ReplicaStateLeaving
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "mark_leaving", "storage replica marked leaving", &rt.slot, &record.assignment.ChainVersion, nil, "", "", nil)
	return nil
}

func (rt *slotRuntime) removeReplica(ctx context.Context) error {
	if !rt.exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := rt.record
	if record.state != ReplicaStateLeaving && record.state != ReplicaStateRemoved {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, rt.slot, record.state)
	}
	if err := rt.node.coord.ReportReplicaRemoved(ctx, rt.slot, rt.node.HighestAcceptedCoordinatorEpoch()); err != nil {
		return fmt.Errorf("err in n.coord.ReportReplicaRemoved: %w", err)
	}
	if record.state == ReplicaStateLeaving {
		if err := rt.node.backend.DeleteReplica(rt.slot); err != nil {
			return fmt.Errorf("err in n.backend.DeleteReplica: %w", err)
		}
		if err := rt.node.local.DeleteReplica(ctx, rt.node.nodeID, rt.slot); err != nil {
			return fmt.Errorf("err in n.local.DeleteReplica: %w", err)
		}
	}
	rt.removeRecord()
	rt.node.evictSlotOwner(rt.slot, rt.owner)
	rt.node.refreshMetricGauges()
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "remove_replica", "storage replica removed", &rt.slot, nil, nil, "", "", nil)
	return nil
}

func (rt *slotRuntime) updateChainPeers(ctx context.Context, assignment ReplicaAssignment) error {
	if !rt.exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, assignment.Slot)
	}
	record := rt.record
	if reflect.DeepEqual(record.assignment, assignment) {
		return nil
	}
	if record.assignment.ChainVersion != assignment.ChainVersion || record.assignment.Peers.PredecessorNodeID != assignment.Peers.PredecessorNodeID {
		record.highestUpstreamConfirmedSequence = record.highestCommittedSequence
	}
	record.assignment = cloneAssignment(assignment)
	if record.state != ReplicaStateRecovered {
		record.lastKnownState = record.state
	}
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "update_chain_peers", "storage replica peers updated", &assignment.Slot, &assignment.ChainVersion, nil, "", "", nil)
	if record.state == ReplicaStateActive {
		rt.ensureUpstreamCommitReplay(rt.backgroundContext())
	}
	return nil
}

func (rt *slotRuntime) resumeRecoveredReplica(ctx context.Context, cmd ResumeRecoveredReplicaCommand) error {
	if !rt.exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Assignment.Slot)
	}
	record := rt.record
	if record.state == ReplicaStateActive && record.localDataPresent && reflect.DeepEqual(record.assignment, cmd.Assignment) {
		return nil
	}
	if record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Assignment.Slot, record.state)
	}
	if !record.localDataPresent {
		return fmt.Errorf("%w: slot %d has no committed data to resume", ErrStateMismatch, cmd.Assignment.Slot)
	}
	record.assignment = cloneAssignment(cmd.Assignment)
	record.state = ReplicaStateActive
	record.lastKnownState = ReplicaStateActive
	record.nextSequence = record.highestCommittedSequence + 1
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "resume_recovered_replica", "storage recovered replica resumed", &cmd.Assignment.Slot, &cmd.Assignment.ChainVersion, nil, "", "", nil)
	rt.ensureUpstreamCommitReplay(rt.backgroundContext())
	return nil
}

func (rt *slotRuntime) recoverReplica(ctx context.Context, cmd RecoverReplicaCommand) error {
	start := time.Now()
	if rt.exists && rt.record.state == ReplicaStateActive && reflect.DeepEqual(rt.record.assignment, cmd.Assignment) {
		return nil
	}
	if rt.exists && rt.record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Assignment.Slot, rt.record.state)
	}
	if err := rt.node.admitCatchup(); err != nil {
		rt.node.observeBackpressure(err)
		return err
	}
	defer rt.node.releaseCatchup()
	if err := rt.node.ensureBackendReplica(cmd.Assignment.Slot); err != nil {
		return fmt.Errorf("err in n.ensureBackendReplica: %w", err)
	}
	snapshot, highestCommittedSequence, err := rt.node.repl.FetchSnapshot(
		ctx,
		peerTransportTarget(cmd.Assignment.Peers.PredecessorTarget, cmd.SourceNodeID),
		cmd.Assignment.Slot,
	)
	if err != nil {
		return fmt.Errorf("err in n.repl.FetchSnapshot: %w", err)
	}
	if err := rt.node.backend.InstallSnapshot(cmd.Assignment.Slot, snapshot); err != nil {
		return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
	}
	if err := rt.node.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
		return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
	}
	record := replicaRecord{
		assignment:                       cloneAssignment(cmd.Assignment),
		state:                            ReplicaStateActive,
		nextSequence:                     highestCommittedSequence + 1,
		highestCommittedSequence:         highestCommittedSequence,
		materializedCommittedSequence:    highestCommittedSequence,
		highestUpstreamConfirmedSequence: highestCommittedSequence,
		localDataPresent:                 true,
		lastKnownState:                   ReplicaStateActive,
	}
	record = ensureProtocolReplicaState(record)
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	if rt.node.metrics != nil {
		rt.node.metrics.catchupOps.WithLabelValues("recover_replica", "success").Inc()
		rt.node.metrics.catchupDuration.Observe(time.Since(start).Seconds())
	}
	rt.node.refreshMetricGauges()
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "recover_replica", "storage replica recovered from peer", &cmd.Assignment.Slot, &cmd.Assignment.ChainVersion, nil, cmd.SourceNodeID, "", nil)
	return nil
}

func (rt *slotRuntime) dropRecoveredReplica(ctx context.Context, slot int) error {
	if !rt.exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	record := rt.record
	if record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, slot, record.state)
	}
	if err := rt.node.backend.DeleteReplica(slot); err != nil && !errors.Is(err, ErrUnknownReplica) {
		return fmt.Errorf("err in n.backend.DeleteReplica: %w", err)
	}
	if err := rt.node.local.DeleteReplica(ctx, rt.node.nodeID, slot); err != nil {
		return fmt.Errorf("err in n.local.DeleteReplica: %w", err)
	}
	rt.removeRecord()
	rt.node.evictSlotOwner(rt.slot, rt.owner)
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "drop_recovered_replica", "storage recovered replica dropped", &slot, nil, nil, "", "", nil)
	return nil
}

func (n *Node) ensureSlotOwner(slot int) *slotOwner {
	n.mu.Lock()
	defer n.mu.Unlock()
	if owner, ok := n.slotOwners[slot]; ok {
		return owner
	}
	record := replicaRecord{}
	ok := false
	if published, exists := n.publishedReplicas[slot]; exists {
		record = replicaRecordFromPublished(published)
		ok = true
	}
	owner := newSlotOwner(n, slot, ok, record)
	n.slotOwners[slot] = owner
	return owner
}

func (rt *slotRuntime) committedObject(key string) (CommittedObject, bool, error) {
	if overlay, overlayFound, ok := recordCommittedOverlayObject(rt.record, key); ok {
		return overlay, overlayFound, nil
	}
	object, found, err := rt.node.backend.GetCommitted(rt.slot, key)
	if err != nil {
		return CommittedObject{}, false, fmt.Errorf("err in n.backend.GetCommitted: %w", err)
	}
	return object, found, nil
}

func (rt *slotRuntime) markMaterialized(highest uint64) {
	if !rt.exists {
		return
	}
	record := recordPruneMaterializedOverlay(rt.record, highest)
	rt.setRecord(record)
}

func (n *Node) evictSlotOwner(slot int, owner *slotOwner) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if existing, ok := n.slotOwners[slot]; ok && existing == owner {
		delete(n.slotOwners, slot)
		owner.close()
	}
}

func (n *Node) tryAdmitNodeClientWrite(slot int, slotCurrent int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.maxInFlightClientWritesPerNode > 0 && n.inFlightClientWrites >= n.maxInFlightClientWritesPerNode {
		return newWriteBackpressureError(slot, n.inFlightClientWrites, n.maxInFlightClientWritesPerNode)
	}
	if n.maxInFlightClientWritesPerSlot > 0 && slotCurrent >= n.maxInFlightClientWritesPerSlot {
		return newWriteBackpressureError(slot, slotCurrent, n.maxInFlightClientWritesPerSlot)
	}
	n.inFlightClientWrites++
	n.refreshMetricGaugesLocked()
	return nil
}

func (n *Node) releaseNodeClientWrite() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.inFlightClientWrites > 0 {
		n.inFlightClientWrites--
	}
	n.refreshMetricGaugesLocked()
}

func (n *Node) releaseClientWriteOwned(owner *slotOwner) {
	if owner == nil {
		return
	}
	done := make(chan struct{}, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.releaseClientWrite()
		done <- struct{}{}
	}); err != nil {
		return
	}
	select {
	case <-n.done:
	case <-done:
	}
}

func (n *Node) submitWriteOwned(
	ctx context.Context,
	slot int,
	kind OperationKind,
	key string,
	value string,
	conditions WriteConditions,
) (CommitResult, error) {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan slotSubmitWriteResponse, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.handleSubmitWrite(ctx, kind, key, value, conditions, respCh)
	}); err != nil {
		return CommitResult{}, err
	}
	var response slotSubmitWriteResponse
	select {
	case <-n.done:
		return CommitResult{}, context.Canceled
	case response = <-respCh:
	}
	if response.err != nil {
		return CommitResult{}, response.err
	}
	if response.waiter == nil {
		return response.result, nil
	}
	if err := ctx.Err(); err != nil {
		n.releaseClientWriteOwned(owner)
		timeoutErr := fmt.Errorf("%w: %w", ErrWriteTimeout, err)
		n.captureWriteTimeout(slot, response.result.Sequence, response.role, timeoutErr)
		return CommitResult{}, timeoutErr
	}
	waitCtx, cancel := withDefaultTimeout(ctx, n.writeCommitTimeout)
	defer cancel()
	waitStarted := time.Now()
	err := response.waiter.wait(waitCtx, n.done)
	n.observeWriteStage(writeStageHeadWaitForCommit, response.role, writeStageResult(err), time.Since(waitStarted))
	n.releaseClientWriteOwned(owner)
	if err != nil {
		if errors.Is(err, ErrWriteTimeout) {
			n.captureWriteTimeout(slot, response.result.Sequence, response.role, err)
		}
		return CommitResult{}, err
	}
	return response.result, nil
}

func (n *Node) handleClientGetOwned(ctx context.Context, req ClientGetRequest) (ReadResult, error) {
	owner := n.ensureSlotOwner(req.Slot)
	respCh := make(chan slotReadResponse, 1)
	if err := owner.dispatch(ctx, func(runtime *slotRuntime) {
		result, err := runtime.handleClientGet(ctx, req)
		respCh <- slotReadResponse{result: result, err: err}
	}); err != nil {
		return ReadResult{}, err
	}
	select {
	case <-n.done:
		return ReadResult{}, context.Canceled
	case <-ctx.Done():
		return ReadResult{}, ctx.Err()
	case response := <-respCh:
		return response.result, response.err
	}
}

func (n *Node) committedObjectOwned(ctx context.Context, slot int, key string) (CommittedObject, bool, error) {
	type committedObjectResponse struct {
		object CommittedObject
		found  bool
		err    error
	}
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan committedObjectResponse, 1)
	if err := owner.dispatch(ctx, func(runtime *slotRuntime) {
		object, found, err := runtime.committedObject(key)
		respCh <- committedObjectResponse{object: object, found: found, err: err}
	}); err != nil {
		return CommittedObject{}, false, err
	}
	select {
	case <-n.done:
		return CommittedObject{}, false, context.Canceled
	case <-ctx.Done():
		return CommittedObject{}, false, ctx.Err()
	case response := <-respCh:
		return response.object, response.found, response.err
	}
}

func (n *Node) handleForwardWriteOwned(ctx context.Context, req ForwardWriteRequest) error {
	owner := n.ensureSlotOwner(req.Operation.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.handleForwardWrite(ctx, req, respCh)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) drainBufferedOwned(owner *slotOwner) error {
	for attempt := 0; attempt < 64; attempt++ {
		respCh := make(chan error, 1)
		if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
			runtime.drainReadyBuffered(runtime.backgroundContext(), respCh)
		}); err != nil {
			return err
		}
		select {
		case <-n.done:
			return context.Canceled
		case err := <-respCh:
			if err != nil {
				return err
			}
		}
		type slotDrainStatus struct {
			busy bool
		}
		statusCh := make(chan slotDrainStatus, 1)
		if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
			record := ensureProtocolReplicaState(runtime.record)
			statusCh <- slotDrainStatus{
				busy: runtime.commitEffectInFlight ||
					runtime.upstreamCommitInFlight ||
					runtime.catchupSyncInFlight ||
					len(record.bufferedForwards) > 0 ||
					len(record.bufferedCommits) > 0,
			}
		}); err != nil {
			return err
		}
		select {
		case <-n.done:
			return context.Canceled
		case status := <-statusCh:
			if !status.busy {
				return nil
			}
		}
		time.Sleep(50 * time.Microsecond)
	}
	return nil
}

func (n *Node) handleCommitWriteOwned(ctx context.Context, req CommitWriteRequest) error {
	owner := n.ensureSlotOwner(req.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.handleCommitWrite(ctx, req, respCh)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		if err != nil {
			return err
		}
		return n.drainBufferedOwned(owner)
	}
}

func (n *Node) handleForwardWriteAcceptedOwned(ctx context.Context, req ForwardWriteRequest) error {
	owner := n.ensureSlotOwner(req.Operation.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.handleForwardWriteAccepted(ctx, req, respCh)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) handleCommitWriteAcceptedOwned(ctx context.Context, req CommitWriteRequest) error {
	owner := n.ensureSlotOwner(req.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.handleCommitWriteAccepted(ctx, req, respCh)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) addReplicaAsTailOwned(ctx context.Context, cmd AddReplicaAsTailCommand) error {
	owner := n.ensureSlotOwner(cmd.Assignment.Slot)
	respCh := make(chan slotAddReplicaResponse, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		autoActivate, err := runtime.addReplicaAsTail(ctx, cmd)
		respCh <- slotAddReplicaResponse{autoActivate: autoActivate, err: err}
	}); err != nil {
		return err
	}
	var response slotAddReplicaResponse
	select {
	case <-n.done:
		return context.Canceled
	case response = <-respCh:
	}
	if response.err != nil {
		return response.err
	}
	if response.autoActivate {
		activateCtx, cancel := autoActivationReadyContext(ctx)
		defer cancel()
		if err := n.activateReplicaOwned(activateCtx, cmd.Assignment.Slot); err != nil &&
			!errors.Is(err, errReplicaActivationInFlight) &&
			!errors.Is(err, errReplicaAlreadyActive) &&
			!errors.Is(err, ErrUnknownReplica) {
			return err
		}
	}
	return nil
}

func (n *Node) activateReplicaOwned(ctx context.Context, slot int) error {
	if err := n.beginReplicaActivation(slot); err != nil {
		if errors.Is(err, errReplicaAlreadyActive) || errors.Is(err, errReplicaActivationInFlight) {
			return nil
		}
		return err
	}
	defer n.endReplicaActivation(slot)
	owner := n.ensureSlotOwner(slot)
	prepareCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		prepareCh <- runtime.prepareActivation(ctx)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-prepareCh:
		if err != nil {
			return err
		}
	}
	if err := n.coord.ReportReplicaReady(ctx, slot, n.HighestAcceptedCoordinatorEpoch()); err != nil {
		return fmt.Errorf("err in n.coord.ReportReplicaReady: %w", err)
	}
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.activateReplicaReady(ctx)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) markReplicaLeavingOwned(ctx context.Context, slot int) error {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.markReplicaLeaving(ctx)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) removeReplicaOwned(ctx context.Context, slot int) error {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.removeReplica(ctx)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) updateChainPeersOwned(ctx context.Context, assignment ReplicaAssignment) error {
	owner := n.ensureSlotOwner(assignment.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.updateChainPeers(ctx, assignment)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) resumeRecoveredReplicaOwned(ctx context.Context, cmd ResumeRecoveredReplicaCommand) error {
	owner := n.ensureSlotOwner(cmd.Assignment.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.resumeRecoveredReplica(ctx, cmd)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) recoverReplicaOwned(ctx context.Context, cmd RecoverReplicaCommand) error {
	owner := n.ensureSlotOwner(cmd.Assignment.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.recoverReplica(ctx, cmd)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) dropRecoveredReplicaOwned(ctx context.Context, slot int) error {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.dropRecoveredReplica(ctx, slot)
	}); err != nil {
		return err
	}
	select {
	case <-n.done:
		return context.Canceled
	case err := <-respCh:
		return err
	}
}

func (n *Node) committedSnapshotWithSequenceOwned(ctx context.Context, slot int) (Snapshot, uint64, error) {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan slotSnapshotResponse, 1)
	if err := owner.dispatch(ctx, func(runtime *slotRuntime) {
		snapshot, highest, err := runtime.handleCommittedSnapshotWithSequence()
		respCh <- slotSnapshotResponse{snapshot: snapshot, highest: highest, err: err}
	}); err != nil {
		return nil, 0, err
	}
	select {
	case <-n.done:
		return nil, 0, context.Canceled
	case <-owner.done:
		return nil, 0, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	case response := <-respCh:
		return response.snapshot, response.highest, response.err
	}
}

func (n *Node) stagedSequencesOwned(ctx context.Context, slot int) ([]uint64, error) {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan slotSequencesResponse, 1)
	if err := owner.dispatch(ctx, func(runtime *slotRuntime) {
		sequences, err := runtime.handleStagedSequences()
		respCh <- slotSequencesResponse{sequences: sequences, err: err}
	}); err != nil {
		return nil, err
	}
	select {
	case <-n.done:
		return nil, context.Canceled
	case <-owner.done:
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	case <-ctx.Done():
		return nil, ctx.Err()
	case response := <-respCh:
		return response.sequences, response.err
	}
}

func (n *Node) bufferedForwardSequencesOwned(ctx context.Context, slot int) ([]uint64, error) {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan slotSequencesResponse, 1)
	if err := owner.dispatch(ctx, func(runtime *slotRuntime) {
		sequences, err := runtime.handleBufferedForwardSequences()
		respCh <- slotSequencesResponse{sequences: sequences, err: err}
	}); err != nil {
		return nil, err
	}
	select {
	case <-n.done:
		return nil, context.Canceled
	case <-owner.done:
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	case <-ctx.Done():
		return nil, ctx.Err()
	case response := <-respCh:
		return response.sequences, response.err
	}
}

func (n *Node) bufferedCommitSequencesOwned(ctx context.Context, slot int) ([]uint64, error) {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan slotSequencesResponse, 1)
	if err := owner.dispatch(ctx, func(runtime *slotRuntime) {
		sequences, err := runtime.handleBufferedCommitSequences()
		respCh <- slotSequencesResponse{sequences: sequences, err: err}
	}); err != nil {
		return nil, err
	}
	select {
	case <-n.done:
		return nil, context.Canceled
	case <-owner.done:
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	case <-ctx.Done():
		return nil, ctx.Err()
	case response := <-respCh:
		return response.sequences, response.err
	}
}
