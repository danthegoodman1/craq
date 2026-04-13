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

const (
	slotPrepareWindow        = 32
	slotCommitTokenWindow    = 16
	preparedReplayRetryDelay = 2 * time.Millisecond
)

type acceptedCommitStage string

const (
	acceptedCommitWaitingForTurn         acceptedCommitStage = "accepted_waiting_for_turn"
	acceptedCommitDurableInFlight        acceptedCommitStage = "durable_in_flight"
	acceptedCommitDurableCompleteWaiting acceptedCommitStage = "durable_complete_waiting_apply"
	acceptedCommitApplied                acceptedCommitStage = "applied"
)

type acceptedCommitEntry struct {
	request CommitWriteRequest
	stage   acceptedCommitStage
	strict  bool
	waiters []acceptedCommitWaiter
}

type acceptedCommitWaiter struct {
	resp chan<- error
	ctx  context.Context
}

type slotOwner struct {
	node         *Node
	slot         int
	ch           chan func(*slotRuntime)
	completionCh chan func(*slotRuntime)
	done         chan struct{}
	once         sync.Once
}

type slotRuntime struct {
	owner  *slotOwner
	node   *Node
	slot   int
	exists bool
	record replicaRecord

	committedMetadata map[string]committedMetadataEntry

	commitEffectInFlight         bool
	commitEffectSequence         uint64
	highestWatermarkSent         uint64
	acceptedCommits              map[uint64]*acceptedCommitEntry
	upstreamCommitInFlight       bool
	upstreamCommitHighSent       uint64
	upstreamCommitAcked          map[uint64]struct{}
	preparedReplayRetryScheduled bool
	catchupSyncInFlight          bool
	drainScheduled               bool
	progressionGap               bool
	progressionGapSequence       uint64

	lastAcceptCommitReceived  slotSequenceBreadcrumb
	lastDuplicateCommitParked slotSequenceBreadcrumb
	lastReconciledFromJournal slotSequenceBreadcrumb
	lastAppliedLocally        slotSequenceBreadcrumb
	lastWaiterReleased        slotSequenceBreadcrumb
}

type prepareForwardPipeline struct {
	mu          sync.Mutex
	prepareDone bool
	forwardDone bool
	finished    bool
}

type committedMetadataEntry struct {
	found    bool
	metadata ObjectMetadata
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
		owner:               o,
		node:                o.node,
		slot:                o.slot,
		exists:              exists,
		record:              record,
		committedMetadata:   map[string]committedMetadataEntry{},
		upstreamCommitAcked: map[uint64]struct{}{},
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
	wasMissing := !rt.exists
	rt.exists = true
	rt.record = record
	rt.pruneAcceptedCommits(record)
	if wasMissing && rt.node.commitJournal != nil {
		rt.node.commitJournal.allowSlot(rt.slot)
	}
	rt.node.recordTimeoutMaterializerLag(rt.slot, record.highestCommittedSequence, record.materializedCommittedSequence)
	rt.publish(prevBuffered)
}

func (rt *slotRuntime) removeRecord() {
	prevBuffered := rt.bufferedCount()
	rt.exists = false
	rt.record = replicaRecord{}
	rt.clearCommittedMetadata()
	rt.clearAcceptedCommits(context.Canceled)
	rt.upstreamCommitHighSent = 0
	rt.upstreamCommitAcked = nil
	rt.preparedReplayRetryScheduled = false
	rt.progressionGap = false
	rt.progressionGapSequence = 0
	rt.node.recordTimeoutMaterializerLag(rt.slot, 0, 0)
	rt.publish(prevBuffered)
}

func (rt *slotRuntime) backgroundContext() context.Context {
	return rt.node.runtimeCtx
}

func (rt *slotRuntime) clearCommittedMetadata() {
	rt.committedMetadata = map[string]committedMetadataEntry{}
}

func (rt *slotRuntime) rememberCommittedMetadata(key string, found bool, metadata ObjectMetadata) {
	if key == "" {
		return
	}
	if rt.committedMetadata == nil {
		rt.committedMetadata = map[string]committedMetadataEntry{}
	}
	entry := committedMetadataEntry{found: found}
	if found {
		entry.metadata = cloneObjectMetadata(metadata)
	}
	rt.committedMetadata[key] = entry
}

func (rt *slotRuntime) rememberCommittedOperation(operation WriteOperation) {
	switch operation.Kind {
	case OperationKindPut:
		rt.rememberCommittedMetadata(operation.Key, true, operation.Metadata)
	case OperationKindDelete:
		rt.rememberCommittedMetadata(operation.Key, false, ObjectMetadata{})
	}
}

func (rt *slotRuntime) committedMetadataState(key string) (*ObjectMetadata, bool, bool, error) {
	if overlay, found, ok := recordCommittedOverlayObject(rt.record, key); ok {
		if found {
			rt.rememberCommittedMetadata(key, true, overlay.Metadata)
			return cloneObjectMetadataPtr(&overlay.Metadata), true, true, nil
		}
		rt.rememberCommittedMetadata(key, false, ObjectMetadata{})
		return nil, false, true, nil
	}
	if entry, ok := rt.committedMetadata[key]; ok {
		if entry.found {
			return cloneObjectMetadataPtr(&entry.metadata), true, true, nil
		}
		return nil, false, true, nil
	}
	object, found, err := rt.node.backend.GetCommitted(rt.slot, key)
	if err != nil {
		return nil, false, false, fmt.Errorf("err in n.backend.GetCommitted: %w", err)
	}
	if found {
		rt.rememberCommittedMetadata(key, true, object.Metadata)
		return cloneObjectMetadataPtr(&object.Metadata), true, false, nil
	}
	rt.rememberCommittedMetadata(key, false, ObjectMetadata{})
	return nil, false, false, nil
}

func (rt *slotRuntime) ensureAcceptedCommits() {
	if rt.acceptedCommits == nil {
		rt.acceptedCommits = map[uint64]*acceptedCommitEntry{}
	}
}

func (rt *slotRuntime) acceptedCommit(sequence uint64) *acceptedCommitEntry {
	if len(rt.acceptedCommits) == 0 {
		return nil
	}
	return rt.acceptedCommits[sequence]
}

func (rt *slotRuntime) acceptedCommitEntry(sequence uint64, req CommitWriteRequest) *acceptedCommitEntry {
	return rt.acceptedCommitEntryWithStrict(sequence, req, false)
}

func (rt *slotRuntime) strictAcceptedCommitEntry(sequence uint64, req CommitWriteRequest) *acceptedCommitEntry {
	return rt.acceptedCommitEntryWithStrict(sequence, req, true)
}

func (rt *slotRuntime) acceptedCommitEntryWithStrict(sequence uint64, req CommitWriteRequest, strict bool) *acceptedCommitEntry {
	rt.ensureAcceptedCommits()
	entry, ok := rt.acceptedCommits[sequence]
	if !ok {
		entry = &acceptedCommitEntry{
			request: cloneCommitRequest(req),
			stage:   acceptedCommitWaitingForTurn,
			strict:  strict,
		}
		rt.acceptedCommits[sequence] = entry
	}
	if strict {
		entry.strict = true
	}
	return entry
}

func (rt *slotRuntime) pruneAcceptedCommits(record replicaRecord) {
	if len(rt.acceptedCommits) == 0 {
		return
	}
	record = ensureProtocolReplicaState(record)
	for sequence, entry := range rt.acceptedCommits {
		if entry == nil {
			delete(rt.acceptedCommits, sequence)
			continue
		}
		if sequence <= record.highestCommittedSequence || entry.stage == acceptedCommitApplied {
			rt.releaseAcceptedCommitWaiters(sequence, nil)
			delete(rt.acceptedCommits, sequence)
			continue
		}
	}
	if len(rt.acceptedCommits) == 0 {
		rt.acceptedCommits = nil
	}
}

func (rt *slotRuntime) clearAcceptedCommits(err error) {
	if len(rt.acceptedCommits) == 0 {
		return
	}
	for sequence := range rt.acceptedCommits {
		rt.releaseAcceptedCommitWaiters(sequence, err)
		delete(rt.acceptedCommits, sequence)
	}
	rt.acceptedCommits = nil
}

func acceptedCommitWaiterResult(waiter acceptedCommitWaiter, err error) error {
	if err != nil {
		return err
	}
	if waiter.ctx != nil {
		if ctxErr := waiter.ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	return nil
}

func (rt *slotRuntime) parkAcceptedCommitWaiter(sequence uint64, resp chan<- error, ctx context.Context) {
	if resp == nil {
		return
	}
	entry := rt.acceptedCommit(sequence)
	if entry == nil {
		return
	}
	entry.waiters = append(entry.waiters, acceptedCommitWaiter{
		resp: resp,
		ctx:  ctx,
	})
}

func (rt *slotRuntime) markBreadcrumb(dst *slotSequenceBreadcrumb, sequence uint64) {
	if dst == nil || sequence == 0 {
		return
	}
	dst.Sequence = sequence
	dst.At = rt.node.clock.Now().UTC()
}

func (rt *slotRuntime) releaseAcceptedCommitWaiters(sequence uint64, err error) {
	entry := rt.acceptedCommit(sequence)
	if entry == nil {
		return
	}
	waiters := rt.takeAcceptedCommitWaiters(sequence)
	for _, waiter := range waiters {
		if waiter.resp == nil {
			continue
		}
		waiter.resp <- acceptedCommitWaiterResult(waiter, err)
	}
}

func (rt *slotRuntime) takeAcceptedCommitWaiters(sequence uint64) []acceptedCommitWaiter {
	entry := rt.acceptedCommit(sequence)
	if entry == nil {
		return nil
	}
	waiters := entry.waiters
	entry.waiters = nil
	return waiters
}

func (rt *slotRuntime) deleteAcceptedCommit(sequence uint64) {
	if len(rt.acceptedCommits) == 0 {
		return
	}
	delete(rt.acceptedCommits, sequence)
}

func (rt *slotRuntime) acceptedCommitWindow() []uint64 {
	if len(rt.acceptedCommits) == 0 {
		return nil
	}
	sequences := make([]uint64, 0, len(rt.acceptedCommits))
	for sequence, entry := range rt.acceptedCommits {
		if entry == nil || entry.stage == acceptedCommitApplied {
			continue
		}
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences
}

func (rt *slotRuntime) earliestAcceptedCommitSequence(after uint64) uint64 {
	sequences := rt.acceptedCommitWindow()
	for _, sequence := range sequences {
		if sequence > after {
			return sequence
		}
	}
	return 0
}

func (rt *slotRuntime) earliestStrictAcceptedCommitSequence(after uint64) uint64 {
	sequences := rt.acceptedCommitWindow()
	for _, sequence := range sequences {
		entry := rt.acceptedCommit(sequence)
		if entry != nil && entry.strict && sequence > after {
			return sequence
		}
	}
	return 0
}

func (rt *slotRuntime) earliestAcceptedCommitInFlightSequence() uint64 {
	sequences := rt.acceptedCommitWindow()
	for _, sequence := range sequences {
		entry := rt.acceptedCommit(sequence)
		if entry != nil && entry.stage == acceptedCommitDurableInFlight {
			return sequence
		}
	}
	return 0
}

func (rt *slotRuntime) syncAcceptedCommitsFromRecord(record replicaRecord) {
	record = ensureProtocolReplicaState(record)
	if len(record.bufferedCommits) == 0 {
		rt.pruneAcceptedCommits(record)
		return
	}
	rt.ensureAcceptedCommits()
	for sequence, req := range record.bufferedCommits {
		if sequence <= record.highestCommittedSequence {
			continue
		}
		if _, ok := rt.acceptedCommits[sequence]; ok {
			continue
		}
		rt.acceptedCommits[sequence] = &acceptedCommitEntry{
			request: cloneCommitRequest(req),
			stage:   acceptedCommitWaitingForTurn,
			strict:  false,
		}
	}
	rt.pruneAcceptedCommits(record)
}

func (rt *slotRuntime) mirrorAcceptedCommit(record replicaRecord, req CommitWriteRequest) (replicaRecord, error) {
	record = ensureProtocolReplicaState(record)
	if req.Sequence <= record.highestCommittedSequence {
		return record, nil
	}
	if existing, ok := record.bufferedCommits[req.Sequence]; ok {
		if sameCommitRequest(existing, req) {
			return record, nil
		}
		return record, fmt.Errorf("%w: slot %d sequence %d buffered commit conflict", ErrProtocolConflict, req.Slot, req.Sequence)
	}
	return reduceBufferFutureCommit(record, req, slotProtocolBufferLimits{
		perSlotLimit:    rt.node.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    rt.node.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: rt.node.bufferedReplicaMessagesForNode(),
	})
}

func (rt *slotRuntime) contiguousPreparedHighWater(record replicaRecord) uint64 {
	record = ensureProtocolReplicaState(record)
	return record.highestPreparedDurable
}

func (rt *slotRuntime) ingestCommitToken(record replicaRecord, req CommitWriteRequest) replicaRecord {
	record = ensureProtocolReplicaState(record)
	if req.Sequence <= record.highestCommitTokenReceived {
		delete(record.bufferedCommits, req.Sequence)
		return record
	}
	if req.Sequence == record.highestCommitTokenReceived+1 {
		record.highestCommitTokenReceived = req.Sequence
		delete(record.bufferedCommits, req.Sequence)
		for {
			next := record.highestCommitTokenReceived + 1
			if _, ok := record.bufferedCommits[next]; !ok {
				break
			}
			delete(record.bufferedCommits, next)
			record.highestCommitTokenReceived = next
		}
		return record
	}
	record.bufferedCommits[req.Sequence] = cloneCommitRequest(req)
	return record
}

func (rt *slotRuntime) startPrepareEffect(
	ctx context.Context,
	record replicaRecord,
	operation WriteOperation,
	afterPrepare func(*slotRuntime, error),
) {
	record = ensureProtocolReplicaState(record)
	assignment := cloneAssignment(record.assignment)
	prepare := DurableCommit{
		Operation: cloneWriteOperation(operation),
		Persisted: persistedReplica(record),
	}
	role := assignment.Role
	started := time.Now()
	if err := rt.node.submitPreparedOperation(rt.backgroundContext(), rt.owner, prepare, func(runtime *slotRuntime, err error, completedAt time.Time) {
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		runtime.node.observeWriteStage(writeStagePrepareFlush, role, writeStageResult(err), time.Since(started))
		if err == nil && runtime.exists {
			current := ensureProtocolReplicaState(runtime.record)
			changed := false
			if operation.Sequence > current.highestPreparedDurable {
				current.highestPreparedDurable = operation.Sequence
				changed = true
			}
			if current.assignment.Peers.SuccessorNodeID == "" && operation.Sequence > current.highestCommitTokenReceived {
				current.highestCommitTokenReceived = operation.Sequence
				changed = true
			}
			if changed {
				runtime.setRecord(current)
			}
			runtime.submitCommitWatermarkReady(current)
		}
		if afterPrepare != nil {
			afterPrepare(runtime, err)
		}
	}); err != nil {
		rt.node.observeWriteStage(writeStagePrepareFlush, role, writeStageResult(err), time.Since(started))
		if afterPrepare != nil {
			afterPrepare(rt, err)
		}
	}
}

func (p *prepareForwardPipeline) resolve(
	runtime *slotRuntime,
	prepareDone bool,
	err error,
	onComplete func(*slotRuntime, error),
) {
	if onComplete == nil {
		return
	}
	call := false
	p.mu.Lock()
	switch {
	case p.finished:
		p.mu.Unlock()
		return
	case err != nil:
		p.finished = true
		call = true
	case prepareDone:
		p.prepareDone = true
		if p.forwardDone {
			p.finished = true
			call = true
		}
	default:
		p.forwardDone = true
		if p.prepareDone {
			p.finished = true
			call = true
		}
	}
	p.mu.Unlock()
	if call {
		onComplete(runtime, err)
	}
}

func (rt *slotRuntime) dispatchForwardAsync(
	ctx context.Context,
	target string,
	req ForwardWriteRequest,
	role ReplicaRole,
	stage writeStage,
	onComplete func(*slotRuntime, error),
) error {
	if asyncRepl, ok := rt.node.repl.(asyncForwardReplicationTransport); ok {
		started := time.Now()
		return asyncRepl.ForwardWriteAsync(
			ctx,
			target,
			req,
			func(err error) {
				_ = rt.owner.enqueueCompletion(func(runtime *slotRuntime) {
					runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(started))
					if onComplete != nil {
						onComplete(runtime, err)
					}
				})
			},
		)
	}
	rt.runAsync(func() error {
		started := time.Now()
		err := rt.node.repl.ForwardWrite(ctx, target, req)
		rt.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(started))
		return err
	}, onComplete)
	return nil
}

func (rt *slotRuntime) startPreparedForwardPipeline(
	ctx context.Context,
	record replicaRecord,
	operation WriteOperation,
	stage writeStage,
	onComplete func(*slotRuntime, error),
) error {
	record = ensureProtocolReplicaState(record)
	if record.assignment.Peers.SuccessorNodeID == "" {
		return fmt.Errorf("%w: slot %d replica has no successor", ErrStateMismatch, rt.slot)
	}
	pipeline := &prepareForwardPipeline{}
	var dispatchForward func(*slotRuntime, replicaRecord) error
	dispatchForward = func(runtime *slotRuntime, current replicaRecord) error {
		current = ensureProtocolReplicaState(current)
		assignment := cloneAssignment(current.assignment)
		successorNodeID := assignment.Peers.SuccessorNodeID
		if successorNodeID == "" {
			pipeline.resolve(runtime, false, nil, onComplete)
			return nil
		}
		successorTarget := assignment.Peers.SuccessorTarget
		req := ForwardWriteRequest{
			Operation:    cloneWriteOperation(operation),
			FromNodeID:   runtime.node.nodeID,
			ChainVersion: assignment.ChainVersion,
		}
		return runtime.dispatchForwardAsync(
			ctx,
			peerTransportTarget(successorTarget, successorNodeID),
			req,
			assignment.Role,
			stage,
			func(nextRuntime *slotRuntime, err error) {
				if err != nil {
					pipeline.resolve(nextRuntime, false, err, onComplete)
					return
				}
				if !nextRuntime.exists {
					pipeline.resolve(nextRuntime, false, fmt.Errorf("%w: slot %d", ErrUnknownReplica, nextRuntime.slot), onComplete)
					return
				}
				latest := ensureProtocolReplicaState(nextRuntime.record)
				if latest.assignment.Peers.SuccessorNodeID == "" {
					pipeline.resolve(nextRuntime, false, nil, onComplete)
					return
				}
				if latest.assignment.ChainVersion != assignment.ChainVersion ||
					latest.assignment.Peers.SuccessorNodeID != successorNodeID ||
					latest.assignment.Peers.SuccessorTarget != successorTarget {
					if err := dispatchForward(nextRuntime, latest); err != nil {
						pipeline.resolve(nextRuntime, false, err, onComplete)
					}
					return
				}
				pipeline.resolve(nextRuntime, false, nil, onComplete)
			},
		)
	}
	rt.startPrepareEffect(rt.backgroundContext(), record, operation, func(runtime *slotRuntime, err error) {
		pipeline.resolve(runtime, true, err, onComplete)
	})
	if err := dispatchForward(rt, record); err != nil {
		pipeline.resolve(rt, false, err, onComplete)
		return err
	}
	return nil
}

func (rt *slotRuntime) submitCommitWatermarkReady(record replicaRecord) {
	record = ensureProtocolReplicaState(record)
	if record.assignment.Peers.SuccessorNodeID == "" && record.highestPreparedDurable > record.highestCommitTokenReceived {
		record.highestCommitTokenReceived = record.highestPreparedDurable
		rt.setRecord(record)
	}
	ready := min(record.highestCommitTokenReceived, rt.contiguousPreparedHighWater(record))
	if record.assignment.Role != ReplicaRoleHead && record.assignment.Role != ReplicaRoleSingle {
		if ready > record.highestCommittedSequence {
			rt.applyCommitWatermark(ready)
		} else {
			rt.ensureUpstreamCommitReplay(rt.backgroundContext())
		}
		return
	}
	if ready <= record.highestCommittedSequence || ready <= rt.highestWatermarkSent {
		return
	}
	assignment := cloneAssignment(record.assignment)
	for seq := record.highestCommittedSequence + 1; seq <= ready; seq++ {
		if entry := rt.acceptedCommit(seq); entry != nil && entry.stage == acceptedCommitWaitingForTurn {
			entry.stage = acceptedCommitDurableInFlight
		}
	}
	rt.commitEffectInFlight = true
	if ready > rt.commitEffectSequence {
		rt.commitEffectSequence = ready
	}
	rt.highestWatermarkSent = ready
	rt.node.traceWriteEvent(assignment, ready, "commit_watermark_flush_start")
	started := time.Now()
	if err := rt.node.submitHeadCommitRange(rt.backgroundContext(), rt.owner, assignment, ready, func(runtime *slotRuntime, err error, completedAt time.Time) {
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(assignment.Role, writeStageResult(err), time.Since(completedAt))
		}
		if err != nil {
			runtime.node.traceWriteEvent(assignment, ready, "commit_watermark_flush_error")
		} else {
			runtime.node.traceWriteEvent(assignment, ready, "commit_watermark_flush_end")
		}
		runtime.node.observeWriteStage(writeStageCommitWatermarkFlush, assignment.Role, writeStageResult(err), time.Since(started))
		if err != nil {
			if runtime.highestWatermarkSent == ready {
				record := ensureProtocolReplicaState(runtime.record)
				runtime.highestWatermarkSent = record.highestCommittedSequence
			}
			if runtime.commitEffectSequence <= ready {
				runtime.commitEffectInFlight = false
				runtime.commitEffectSequence = 0
			}
			for seq := ensureProtocolReplicaState(runtime.record).highestCommittedSequence + 1; seq <= ready; seq++ {
				if entry := runtime.acceptedCommit(seq); entry != nil && entry.stage == acceptedCommitDurableInFlight {
					entry.stage = acceptedCommitWaitingForTurn
				}
			}
			return
		}
		runtime.applyCommitWatermark(ready)
	}); err != nil {
		rt.highestWatermarkSent = record.highestCommittedSequence
		rt.commitEffectInFlight = false
		rt.commitEffectSequence = 0
		for seq := record.highestCommittedSequence + 1; seq <= ready; seq++ {
			if entry := rt.acceptedCommit(seq); entry != nil && entry.stage == acceptedCommitDurableInFlight {
				entry.stage = acceptedCommitWaitingForTurn
			}
		}
		rt.node.observeWriteStage(writeStageCommitWatermarkFlush, assignment.Role, writeStageResult(err), time.Since(started))
	}
}

func (rt *slotRuntime) applyCommitWatermark(sequence uint64) {
	if !rt.exists {
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if sequence <= record.highestCommittedSequence {
		return
	}
	if sequence > record.highestCommitTokenReceived {
		sequence = record.highestCommitTokenReceived
	}
	if sequence > rt.contiguousPreparedHighWater(record) {
		sequence = rt.contiguousPreparedHighWater(record)
	}
	if sequence <= record.highestCommittedSequence {
		return
	}
	rt.node.traceWriteEvent(record.assignment, sequence, "commit_watermark_applied")
	start := record.highestCommittedSequence + 1
	for seq := start; seq <= sequence; seq++ {
		pending := record.pendingWrites[seq]
		commitReq := syntheticCommitRequest(record, seq)
		operation, err := reduceCommittableOperation(record, seq)
		if err != nil {
			rt.enterProgressionGap(record, seq)
			return
		}
		record = reduceApplyCommittedSequence(record, operation, seq, rt.node.maxBufferedReplicaMessagesPerSlot)
		rt.markBreadcrumb(&rt.lastAppliedLocally, seq)
		if commitReq != nil {
			record = reduceRecordCommitApplied(record, *commitReq, rt.node.maxBufferedReplicaMessagesPerSlot)
		}
		record = recordWithCommittedOverlay(record, operation)
		rt.rememberCommittedOperation(operation)
		if pending.waiter != nil {
			rt.node.traceWriteEvent(record.assignment, seq, "waiter_released")
			rt.markBreadcrumb(&rt.lastWaiterReleased, seq)
			pending.waiter.complete(nil)
		}
		rt.releaseAcceptedCommitWaiters(seq, nil)
		rt.deleteAcceptedCommit(seq)
	}
	if rt.commitEffectInFlight && rt.commitEffectSequence != 0 && rt.commitEffectSequence <= sequence {
		rt.commitEffectInFlight = false
		rt.commitEffectSequence = 0
	}
	if record.assignment.Peers.PredecessorNodeID == "" {
		record.highestUpstreamConfirmedSequence = sequence
	}
	rt.setRecord(record)
	rt.enqueueMaterializationUpTo(sequence)
	rt.ensureUpstreamCommitReplay(rt.backgroundContext())
	rt.drainReadyBuffered(rt.backgroundContext(), nil)
}

func (rt *slotRuntime) enqueueMaterializationUpTo(sequence uint64) {
	if !rt.exists || rt.node.commitJournal == nil || sequence == 0 {
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	commits := make([]DurableCommit, 0, int(sequence-record.materializedCommittedSequence))
	for seq := record.materializedCommittedSequence + 1; seq <= sequence; seq++ {
		operation, ok := record.preparedEntries[seq]
		if !ok {
			continue
		}
		commits = append(commits, DurableCommit{
			Operation: cloneWriteOperation(operation),
			Persisted: persistedReplica(record),
		})
	}
	rt.node.commitJournal.enqueueMaterialized(commits)
}

func (rt *slotRuntime) enterProgressionGap(record replicaRecord, sequence uint64) {
	if sequence == 0 {
		return
	}
	if rt.progressionGap && rt.progressionGapSequence == sequence {
		return
	}
	rt.progressionGap = true
	rt.progressionGapSequence = sequence
	chainVersion := record.assignment.ChainVersion
	rt.node.observeCommitGapDetected()
	rt.node.events.record(rt.node.logger, zerolog.ErrorLevel, "commit_gap_detected", "storage commit progression gap detected", &rt.slot, &chainVersion, &sequence, "", "", nil)
}

func (rt *slotRuntime) clearProgressionGap(record replicaRecord) {
	if !rt.progressionGap {
		return
	}
	sequence := rt.progressionGapSequence
	chainVersion := record.assignment.ChainVersion
	rt.progressionGap = false
	rt.progressionGapSequence = 0
	rt.node.observeCommitGapRepaired()
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "commit_gap_repaired", "storage commit progression gap repaired", &rt.slot, &chainVersion, &sequence, "", "", nil)
}

func (rt *slotRuntime) detectAcceptedCommitGap(record replicaRecord) bool {
	record = ensureProtocolReplicaState(record)
	rt.syncAcceptedCommitsFromRecord(record)
	nextSequence := record.highestCommittedSequence + 1
	if rt.acceptedCommit(nextSequence) != nil {
		rt.clearProgressionGap(record)
		return false
	}
	if !rt.commitEffectInFlight && len(rt.acceptedCommits) == 0 {
		rt.clearProgressionGap(record)
		return false
	}
	// Out-of-order future accepts are valid and stay buffered in the ledger until
	// the missing next sequence arrives. Treat this as a progression gap only when
	// local durable state is already ahead of the in-memory committed cursor and
	// reconciliation cannot repair it.
	if rt.node.durableCommittedSequence(rt.slot) < nextSequence {
		rt.clearProgressionGap(record)
		return false
	}
	if rt.reconcileDurableCommitProgress(rt.backgroundContext()) {
		rt.clearProgressionGap(record)
		return false
	}
	rt.enterProgressionGap(record, nextSequence)
	return rt.progressionGap
}

func (rt *slotRuntime) startAcceptedCommitEffect(ctx context.Context, sequence uint64, entry *acceptedCommitEntry) {
	if entry == nil || !rt.exists {
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	operation, err := reduceCommittableOperation(record, sequence)
	if err != nil {
		rt.releaseAcceptedCommitWaiters(sequence, err)
		rt.deleteAcceptedCommit(sequence)
		return
	}
	applied := record
	applied.highestCommittedSequence = sequence
	applied.localDataPresent = true
	if applied.state != ReplicaStateRecovered {
		applied.lastKnownState = applied.state
	}
	applied.highestUpstreamConfirmedSequence = normalizeUpstreamConfirmedSequence(record)
	reqCopy := cloneCommitRequest(entry.request)
	role := record.assignment.Role
	stage, recordStage := writeCommitApplyStage(role)
	commit := DurableCommit{
		Operation:                 operation,
		Persisted:                 persistedReplica(applied),
		UpstreamConfirmedSequence: upstreamConfirmedSequenceForLocalCommit(record, sequence),
	}
	if traceStage := writeTraceCommitAcceptReceivedStage(role); traceStage != "" {
		rt.node.traceWriteEvent(record.assignment, sequence, traceStage)
	}
	if traceStage := writeTraceCommitIntentStage(role); traceStage != "" {
		rt.node.traceWriteEvent(record.assignment, sequence, traceStage)
	}
	entry.stage = acceptedCommitDurableInFlight
	rt.commitEffectInFlight = true
	rt.commitEffectSequence = sequence
	applyStarted := time.Now()
	if err := rt.node.submitCommittedOperation(rt.backgroundContext(), rt.owner, commit, func(runtime *slotRuntime, err error, completedAt time.Time) {
		if recordStage {
			runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		runtime.finishAcceptedCommitEffect(sequence, operation, &reqCopy, err)
	}); err != nil {
		rt.commitEffectInFlight = false
		rt.commitEffectSequence = 0
		entry.stage = acceptedCommitWaitingForTurn
		if recordStage {
			rt.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		rt.releaseAcceptedCommitWaiters(sequence, err)
		rt.deleteAcceptedCommit(sequence)
	}
}

func (rt *slotRuntime) advanceAcceptedCommitProgress(ctx context.Context) {
	for rt.exists {
		record := ensureProtocolReplicaState(rt.record)
		rt.syncAcceptedCommitsFromRecord(record)
		if rt.detectAcceptedCommitGap(record) {
			return
		}
		if rt.commitEffectInFlight && rt.reconcileDurableCommitProgress(ctx) {
			continue
		}
		ready := min(record.highestCommitTokenReceived, rt.contiguousPreparedHighWater(record))
		if ready > record.highestCommittedSequence && ready > rt.highestWatermarkSent {
			rt.submitCommitWatermarkReady(record)
		}
		return
	}
}

func syntheticCommitRequest(record replicaRecord, sequence uint64) *CommitWriteRequest {
	record = ensureProtocolReplicaState(record)
	expected, ok := record.expectedCommitSources[sequence]
	if !ok {
		return nil
	}
	return &CommitWriteRequest{
		Slot:         record.assignment.Slot,
		Sequence:     sequence,
		FromNodeID:   expected.FromNodeID,
		ChainVersion: expected.ChainVersion,
	}
}

func (rt *slotRuntime) finishAcceptedCommitEffect(sequence uint64, operation WriteOperation, commitReq *CommitWriteRequest, err error) {
	rt.commitEffectInFlight = false
	rt.commitEffectSequence = 0
	entry := rt.acceptedCommit(sequence)
	waiters := rt.takeAcceptedCommitWaiters(sequence)
	if entry != nil {
		entry.stage = acceptedCommitDurableCompleteWaiting
	}
	if !rt.exists {
		for _, waiter := range waiters {
			if waiter.resp != nil {
				waiter.resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
			}
		}
		rt.deleteAcceptedCommit(sequence)
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if sequence <= record.highestCommittedSequence {
		if entry != nil {
			entry.stage = acceptedCommitApplied
		}
		for _, waiter := range waiters {
			if waiter.resp != nil {
				waiter.resp <- acceptedCommitWaiterResult(waiter, nil)
			}
		}
		rt.deleteAcceptedCommit(sequence)
		rt.ensureUpstreamCommitReplay(rt.backgroundContext())
		rt.advanceAcceptedCommitProgress(rt.backgroundContext())
		rt.drainReadyBuffered(rt.backgroundContext(), nil)
		return
	}
	pending := record.pendingWrites[sequence]
	committed := err == nil || rt.node.writeActuallyCommitted(rt.slot, sequence)
	if committed {
		record = reduceApplyCommittedSequence(record, operation, sequence, rt.node.maxBufferedReplicaMessagesPerSlot)
		rt.markBreadcrumb(&rt.lastAppliedLocally, sequence)
		if commitReq == nil {
			commitReq = syntheticCommitRequest(record, sequence)
		}
		if commitReq != nil {
			record = reduceRecordCommitApplied(record, *commitReq, rt.node.maxBufferedReplicaMessagesPerSlot)
		}
		record = recordWithCommittedOverlay(record, operation)
		rt.rememberCommittedOperation(operation)
		if record.assignment.Peers.PredecessorNodeID == "" {
			record.highestUpstreamConfirmedSequence = sequence
		}
		rt.setRecord(record)
		if pending.waiter != nil {
			rt.node.traceWriteEvent(record.assignment, sequence, "waiter_released")
			rt.markBreadcrumb(&rt.lastWaiterReleased, sequence)
			pending.waiter.complete(err)
		}
		if entry != nil {
			entry.stage = acceptedCommitApplied
		}
	} else if pending.waiter != nil {
		rt.node.traceWriteEvent(record.assignment, sequence, "waiter_released")
		rt.markBreadcrumb(&rt.lastWaiterReleased, sequence)
		pending.waiter.complete(err)
	}
	for _, waiter := range waiters {
		if waiter.resp != nil {
			waiter.resp <- acceptedCommitWaiterResult(waiter, err)
		}
	}
	rt.deleteAcceptedCommit(sequence)
	if err != nil {
		return
	}
	rt.ensureUpstreamCommitReplay(rt.backgroundContext())
	rt.advanceAcceptedCommitProgress(rt.backgroundContext())
	rt.drainReadyBuffered(rt.backgroundContext(), nil)
}

func (rt *slotRuntime) assignmentMatchesReplayTarget(record replicaRecord, predecessorNodeID, predecessorTarget string, chainVersion uint64) bool {
	record = ensureProtocolReplicaState(record)
	return record.assignment.ChainVersion == chainVersion &&
		record.assignment.Peers.PredecessorNodeID == predecessorNodeID &&
		record.assignment.Peers.PredecessorTarget == predecessorTarget
}

func (rt *slotRuntime) recordUpstreamCommitAccepted(sequence uint64) bool {
	if !rt.exists {
		return false
	}
	record := ensureProtocolReplicaState(rt.record)
	if sequence <= record.highestUpstreamConfirmedSequence {
		return true
	}
	record.highestUpstreamConfirmedSequence = sequence
	rt.setRecord(record)
	return true
}

func (rt *slotRuntime) persistUpstreamConfirmedSequenceAsync(
	assignment ReplicaAssignment,
	sequence uint64,
	peerNodeID string,
) {
	if err := rt.node.submitUpstreamConfirmedSequence(rt.backgroundContext(), rt.owner, assignment, sequence, func(applyRuntime *slotRuntime, err error, _ time.Time) {
		if err != nil {
			applyRuntime.node.events.record(applyRuntime.node.logger, zerolog.ErrorLevel, "replication_commit_confirm_failed", "storage upstream commit confirmation persist failed", &applyRuntime.slot, nil, &sequence, peerNodeID, "", err)
		}
	}); err != nil {
		rt.node.events.record(rt.node.logger, zerolog.ErrorLevel, "replication_commit_confirm_failed", "storage upstream commit confirmation persist failed", &rt.slot, nil, &sequence, peerNodeID, "", err)
	}
}

func (rt *slotRuntime) reconcileDurableCommitProgress(_ context.Context) bool {
	if !rt.exists || !rt.commitEffectInFlight {
		return false
	}
	sequence := rt.commitEffectSequence
	if sequence == 0 {
		return false
	}
	durable := rt.node.durableCommittedSequence(rt.slot)
	if durable < sequence {
		return false
	}
	record := ensureProtocolReplicaState(rt.record)
	if durable > record.highestCommitTokenReceived {
		durable = record.highestCommitTokenReceived
	}
	if prepared := rt.contiguousPreparedHighWater(record); durable > prepared {
		durable = prepared
	}
	if durable <= record.highestCommittedSequence {
		rt.markBreadcrumb(&rt.lastReconciledFromJournal, sequence)
		rt.commitEffectInFlight = false
		rt.commitEffectSequence = 0
		return true
	}
	rt.markBreadcrumb(&rt.lastReconciledFromJournal, durable)
	rt.applyCommitWatermark(durable)
	return true
}

func (rt *slotRuntime) runAsync(run func() error, apply func(*slotRuntime, error)) {
	go func() {
		err := run()
		// Async replication/storage completions must not sit behind new mailbox work,
		// otherwise pipelined replay windows can stall under sustained load.
		_ = rt.owner.enqueueCompletion(func(runtime *slotRuntime) {
			apply(runtime, err)
		})
	}()
}

func (rt *slotRuntime) scheduleDrainReadyBuffered() {
	if rt == nil || rt.owner == nil || rt.drainScheduled {
		return
	}
	rt.drainScheduled = true
	_ = rt.owner.enqueueCompletion(func(runtime *slotRuntime) {
		runtime.drainScheduled = false
		runtime.drainReadyBuffered(runtime.backgroundContext(), nil)
	})
}

func (rt *slotRuntime) activeRecord() (replicaRecord, error) {
	if !rt.exists {
		return replicaRecord{}, fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
	}
	record := ensureProtocolReplicaState(rt.record)
	if rt.progressionGap {
		return replicaRecord{}, fmt.Errorf("%w: slot %d commit progression gap at sequence %d", ErrStateMismatch, rt.slot, rt.progressionGapSequence)
	}
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

func (rt *slotRuntime) replicationAvailableCredit(kind string) int {
	if !rt.exists {
		return 0
	}
	if rt.progressionGap {
		return 0
	}
	record := ensureProtocolReplicaState(rt.record)
	available := rt.node.maxBufferedReplicaMessagesPerSlot - reduceTotalBufferedMessages(record)
	switch kind {
	case "forward":
		inFlight := int(record.nextSequence - 1 - record.highestPreparedDurable)
		windowRemaining := slotPrepareWindow - inFlight
		if windowRemaining < available {
			available = windowRemaining
		}
	case "commit":
		inFlight := int(record.highestCommitTokenReceived - record.highestCommittedSequence)
		windowRemaining := slotCommitTokenWindow - inFlight
		if windowRemaining < available {
			available = windowRemaining
		}
	case "commit_advance":
		if rt.upstreamCommitInFlight {
			available = 0
		} else if available > 1 {
			available = 1
		}
	}
	if rt.node.maxBufferedReplicaMessagesPerNode > 0 {
		nodeRemaining := rt.node.maxBufferedReplicaMessagesPerNode - rt.node.bufferedReplicaMessagesForNode()
		if nodeRemaining < available {
			available = nodeRemaining
		}
	}
	if available < 0 {
		return 0
	}
	return available
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
	record.highestPreparedDurable = highestCommittedSequence
	record.highestCommitTokenReceived = highestCommittedSequence
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

func retargetUncommittedCommitSources(record replicaRecord) replicaRecord {
	record = ensureProtocolReplicaState(record)
	for sequence := range record.expectedCommitSources {
		if sequence <= record.highestCommittedSequence {
			delete(record.expectedCommitSources, sequence)
			continue
		}
		if record.assignment.Peers.SuccessorNodeID == "" {
			delete(record.expectedCommitSources, sequence)
			continue
		}
		record.expectedCommitSources[sequence] = expectedCommitSource{
			FromNodeID:   record.assignment.Peers.SuccessorNodeID,
			ChainVersion: record.assignment.ChainVersion,
		}
	}
	return record
}

func retargetUncommittedForwardSources(record replicaRecord) replicaRecord {
	record = ensureProtocolReplicaState(record)
	for sequence, req := range record.stagedForwards {
		if sequence <= record.highestCommittedSequence {
			delete(record.stagedForwards, sequence)
			continue
		}
		if record.assignment.Peers.PredecessorNodeID == "" {
			continue
		}
		if req.FromNodeID != record.assignment.Peers.PredecessorNodeID {
			continue
		}
		req.ChainVersion = record.assignment.ChainVersion
		record.stagedForwards[sequence] = req
	}
	for sequence, req := range record.bufferedForwards {
		if sequence <= record.highestCommittedSequence {
			delete(record.bufferedForwards, sequence)
			continue
		}
		if record.assignment.Peers.PredecessorNodeID == "" {
			continue
		}
		if req.FromNodeID != record.assignment.Peers.PredecessorNodeID {
			continue
		}
		req.ChainVersion = record.assignment.ChainVersion
		record.bufferedForwards[sequence] = req
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
	if rt.node.commitJournal != nil {
		rt.node.commitJournal.dropSlot(rt.slot)
	}
	if err := rt.node.backend.InstallSnapshot(rt.slot, result.snapshot); err != nil {
		return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
	}
	rt.clearCommittedMetadata()
	if err := rt.node.backend.SetHighestCommittedSequence(rt.slot, result.highest); err != nil {
		return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
	}
	record = rt.reduceSyncedPredecessorState(record, result.highest)
	rt.setRecord(record)
	if rt.node.commitJournal != nil {
		rt.node.commitJournal.allowSlot(rt.slot)
	}
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
	currentMetadata, found, cacheHit, err := rt.committedMetadataState(key)
	rt.node.observeWriteStage(writeStageHeadGetCommitted, record.assignment.Role, writeStageResult(err), time.Since(getCommittedStarted))
	if err != nil {
		resp <- slotSubmitWriteResponse{err: err}
		return
	}
	if cacheHit {
		rt.node.observeHeadCommittedMetadataLookup("hit")
	} else {
		rt.node.observeHeadCommittedMetadataLookup("miss")
	}
	current := CommittedObject{}
	if currentMetadata != nil {
		current.Metadata = cloneObjectMetadata(*currentMetadata)
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
		Metadata: rt.node.nextObjectMetadata(found, currentMetadata),
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
		rt.startPrepareEffect(rt.backgroundContext(), reduction.Record, operation, func(runtime *slotRuntime, err error) {
			if err != nil {
				waiter.complete(err)
			}
		})
		resp <- slotSubmitWriteResponse{
			result: reduction.Result,
			waiter: waiter,
			role:   role,
		}
		return
	case ReplicaRoleHead:
		if reduction.Record.assignment.Peers.SuccessorNodeID == "" {
			waiter.complete(fmt.Errorf("%w: slot %d head has no successor", ErrStateMismatch, rt.slot))
			resp <- slotSubmitWriteResponse{
				result: reduction.Result,
				waiter: waiter,
				role:   role,
			}
			return
		}
		if err := rt.startPreparedForwardPipeline(
			rt.backgroundContext(),
			reduction.Record,
			operation,
			writeStageHeadForwardRPC,
			func(runtime *slotRuntime, err error) {
				if err != nil {
					waiter.complete(fmt.Errorf("err in n.repl.ForwardWrite: %w", err))
					return
				}
				if !runtime.exists {
					waiter.complete(fmt.Errorf("%w: slot %d", ErrUnknownReplica, runtime.slot))
					return
				}
				assignment := cloneAssignment(ensureProtocolReplicaState(runtime.record).assignment)
				runtime.node.traceWriteEvent(assignment, operation.Sequence, "head_forward_accepted")
			},
		); err != nil {
			waiter.complete(fmt.Errorf("err in n.repl.ForwardWrite: %w", err))
		}
		resp <- slotSubmitWriteResponse{
			result: reduction.Result,
			waiter: waiter,
			role:   role,
		}
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
	rt.commitEffectSequence = sequence
	applyStarted := time.Now()
	if err := rt.node.submitCommittedOperation(applyCtx, rt.owner, commit, func(runtime *slotRuntime, err error, completedAt time.Time) {
		runtime.commitEffectInFlight = false
		runtime.commitEffectSequence = 0
		if recordStage {
			runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		runtime.handleCommitEffectResult(resultCtx, sequence, operation, nil, err, resp)
	}); err != nil {
		rt.commitEffectInFlight = false
		rt.commitEffectSequence = 0
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
		rt.markBreadcrumb(&rt.lastAppliedLocally, sequence)
		if commitReq != nil {
			record = reduceRecordCommitApplied(record, *commitReq, rt.node.maxBufferedReplicaMessagesPerSlot)
		}
		record = recordWithCommittedOverlay(record, operation)
		rt.rememberCommittedOperation(operation)
		if record.assignment.Peers.PredecessorNodeID == "" {
			record.highestUpstreamConfirmedSequence = sequence
		}
		rt.setRecord(record)
		if pending.waiter != nil {
			rt.node.traceWriteEvent(record.assignment, sequence, "waiter_released")
			rt.markBreadcrumb(&rt.lastWaiterReleased, sequence)
			pending.waiter.complete(err)
		}
	} else if pending.waiter != nil {
		rt.node.traceWriteEvent(record.assignment, sequence, "waiter_released")
		rt.markBreadcrumb(&rt.lastWaiterReleased, sequence)
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
	if predecessorNodeID == "" {
		rt.drainReadyBuffered(rt.backgroundContext(), resp)
		return
	}
	rt.ensureUpstreamCommitReplay(rt.backgroundContext())
	rt.drainReadyBuffered(rt.backgroundContext(), resp)
}

func (rt *slotRuntime) handleForwardWrite(
	ctx context.Context,
	req ForwardWriteRequest,
	resp chan<- error,
) {
	rt.reconcileDurableCommitProgress(rt.backgroundContext())
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
	rt.reconcileDurableCommitProgress(rt.backgroundContext())
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
	_ context.Context,
	req ForwardWriteRequest,
	resp chan<- error,
) {
	if !rt.exists {
		resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.Peers.SuccessorNodeID == "" {
		rt.startPrepareEffect(rt.backgroundContext(), record, req.Operation, func(runtime *slotRuntime, err error) {
			if err != nil {
				if resp != nil {
					resp <- err
				}
				return
			}
			current := ensureProtocolReplicaState(runtime.record)
			runtime.submitCommitWatermarkReady(current)
			if resp != nil {
				resp <- nil
			}
			runtime.scheduleDrainReadyBuffered()
		})
		return
	}
	if err := rt.startPreparedForwardPipeline(
		rt.backgroundContext(),
		record,
		req.Operation,
		writeStageHeadForwardRPC,
		func(runtime *slotRuntime, err error) {
			if err != nil {
				if resp != nil {
					resp <- fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
				}
				return
			}
			if !runtime.exists {
				if resp != nil {
					resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, runtime.slot)
				}
				return
			}
			current := ensureProtocolReplicaState(runtime.record)
			if current.assignment.Peers.SuccessorNodeID == "" {
				runtime.submitCommitWatermarkReady(current)
				if resp != nil {
					resp <- nil
				}
				runtime.scheduleDrainReadyBuffered()
				return
			}
			if resp != nil {
				resp <- nil
			}
			runtime.scheduleDrainReadyBuffered()
		},
	); err != nil && resp != nil {
		resp <- fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
	}
}

func (rt *slotRuntime) applyForwardAccepted(
	_ context.Context,
	req ForwardWriteRequest,
	resp chan<- error,
) {
	if !rt.exists {
		resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, rt.slot)
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.Peers.SuccessorNodeID == "" {
		rt.startPrepareEffect(rt.backgroundContext(), record, req.Operation, func(runtime *slotRuntime, err error) {
			if err != nil {
				resp <- err
				return
			}
			current := ensureProtocolReplicaState(runtime.record)
			runtime.submitCommitWatermarkReady(current)
			resp <- nil
			runtime.scheduleDrainReadyBuffered()
		})
		return
	}
	if err := rt.startPreparedForwardPipeline(
		rt.backgroundContext(),
		record,
		req.Operation,
		writeStageForwardAcceptRPC,
		func(runtime *slotRuntime, err error) {
			if err != nil {
				chainVersion := uint64(0)
				successorNodeID := ""
				if runtime.exists {
					current := ensureProtocolReplicaState(runtime.record)
					chainVersion = current.assignment.ChainVersion
					successorNodeID = current.assignment.Peers.SuccessorNodeID
				}
				runtime.node.events.record(
					runtime.node.logger,
					zerolog.ErrorLevel,
					"replication_forward_async_failed",
					"storage forward replay failed after local prepare acceptance",
					&runtime.slot,
					&chainVersion,
					&req.Operation.Sequence,
					successorNodeID,
					"",
					err,
				)
				resp <- fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
				return
			}
			if !runtime.exists {
				resp <- fmt.Errorf("%w: slot %d", ErrUnknownReplica, runtime.slot)
				return
			}
			current := ensureProtocolReplicaState(runtime.record)
			if current.assignment.Peers.SuccessorNodeID == "" {
				runtime.submitCommitWatermarkReady(current)
			}
			resp <- nil
			runtime.scheduleDrainReadyBuffered()
		},
	); err != nil {
		resp <- fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
	}
}

func (rt *slotRuntime) handleCommitAdvance(
	ctx context.Context,
	req CommitAdvanceRequest,
	resp chan<- error,
) {
	rt.handleCommitAdvanceRequest(ctx, req, resp, false)
}

func (rt *slotRuntime) handleCommitAdvanceAccepted(
	ctx context.Context,
	req CommitAdvanceRequest,
	resp chan<- error,
) {
	rt.handleCommitAdvanceRequest(ctx, req, resp, true)
}

func (rt *slotRuntime) handleCommitAdvanceRequest(
	_ context.Context,
	req CommitAdvanceRequest,
	resp chan<- error,
	accepted bool,
) {
	rt.reconcileDurableCommitProgress(rt.backgroundContext())
	record, err := rt.replicationRecord()
	if err != nil {
		resp <- err
		return
	}
	record = ensureProtocolReplicaState(record)
	if err := validateCommitAdvanceSource(record, req); err != nil {
		resp <- err
		return
	}
	if accepted {
		rt.markBreadcrumb(&rt.lastAcceptCommitReceived, req.CommittedThrough)
	}
	if req.CommittedThrough > record.highestCommitTokenReceived {
		record.highestCommitTokenReceived = req.CommittedThrough
		rt.setRecord(record)
	}
	rt.node.traceWriteEvent(record.assignment, req.CommittedThrough, "commit_token_received")
	if req.CommittedThrough <= record.highestCommittedSequence {
		resp <- nil
		return
	}
	rt.submitCommitWatermarkReady(record)
	resp <- nil
}

func (rt *slotRuntime) handleCommitWrite(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	rt.handleCommitTokenRequest(ctx, req, resp, false)
}

func (rt *slotRuntime) handleCommitWriteAccepted(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	rt.markBreadcrumb(&rt.lastAcceptCommitReceived, req.Sequence)
	rt.handleCommitTokenRequest(ctx, req, resp, true)
}

func (rt *slotRuntime) handleCommitTokenRequest(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
	strict bool,
) {
	rt.reconcileDurableCommitProgress(rt.backgroundContext())
	record, err := rt.replicationRecord()
	if err != nil {
		resp <- err
		return
	}
	record = ensureProtocolReplicaState(record)
	if err := validateCommitSource(record, req); err != nil {
		resp <- err
		return
	}
	if req.Sequence <= record.highestCommittedSequence {
		if err := reduceHandlePastCommit(record, req); err != nil {
			resp <- err
			return
		}
		resp <- nil
		return
	}
	entry := rt.acceptedCommitEntryWithStrict(req.Sequence, req, strict)
	if !sameCommitRequest(entry.request, req) {
		resp <- fmt.Errorf("%w: slot %d sequence %d accepted commit conflict", ErrProtocolConflict, req.Slot, req.Sequence)
		return
	}
	if req.Sequence > record.highestCommitTokenReceived+1 {
		buffered, err := rt.mirrorAcceptedCommit(record, req)
		if err != nil {
			rt.node.observeBackpressure(err)
			resp <- err
			return
		}
		record = buffered
	}
	record = rt.ingestCommitToken(record, req)
	rt.setRecord(record)
	rt.node.traceWriteEvent(record.assignment, req.Sequence, "commit_token_received")
	if req.Sequence <= record.highestCommittedSequence {
		if strict {
			rt.releaseAcceptedCommitWaiters(req.Sequence, nil)
			rt.deleteAcceptedCommit(req.Sequence)
		}
		resp <- nil
		return
	}
	if strict {
		rt.parkAcceptedCommitWaiter(req.Sequence, resp, ctx)
	}
	rt.submitCommitWatermarkReady(record)
	if !strict {
		resp <- nil
	}
}

func (rt *slotRuntime) handleBufferedCommitRequest(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	rt.reconcileDurableCommitProgress(rt.backgroundContext())
	record, err := rt.replicationRecord()
	if err != nil {
		resp <- err
		return
	}
	record = ensureProtocolReplicaState(record)
	if rt.progressionGap {
		resp <- fmt.Errorf("%w: slot %d commit progression gap at sequence %d", ErrStateMismatch, rt.slot, rt.progressionGapSequence)
		return
	}
	rt.syncAcceptedCommitsFromRecord(record)
	if err := validateCommitSource(record, req); err != nil {
		resp <- err
		return
	}
	if req.Sequence <= record.highestCommittedSequence {
		if err := reduceHandlePastCommit(record, req); err != nil {
			resp <- err
			return
		}
		resp <- nil
		return
	}
	if entry := rt.acceptedCommit(req.Sequence); entry != nil {
		if !sameCommitRequest(entry.request, req) {
			resp <- fmt.Errorf("%w: slot %d sequence %d accepted commit conflict", ErrProtocolConflict, req.Slot, req.Sequence)
			return
		}
		if entry.stage != acceptedCommitWaitingForTurn {
			rt.parkAcceptedCommitWaiter(req.Sequence, resp, ctx)
			return
		}
		if req.Sequence == record.highestCommittedSequence+1 &&
			!rt.commitEffectInFlight &&
			reduceHasCommittableSequence(record, req.Sequence) {
			rt.parkAcceptedCommitWaiter(req.Sequence, resp, ctx)
			rt.advanceAcceptedCommitProgress(ctx)
			return
		}
		resp <- nil
		return
	}
	buffered, err := rt.mirrorAcceptedCommit(record, req)
	if err != nil {
		if req.Sequence > record.highestCommittedSequence+1 || !reduceHasCommittableSequence(record, req.Sequence) {
			rt.node.observeBackpressure(err)
		}
		resp <- err
		return
	}
	record = buffered
	rt.setRecord(record)
	if req.Sequence != record.highestCommittedSequence+1 ||
		rt.commitEffectInFlight ||
		!reduceHasCommittableSequence(record, req.Sequence) {
		resp <- nil
		return
	}
	rt.acceptedCommitEntry(req.Sequence, req)
	rt.parkAcceptedCommitWaiter(req.Sequence, resp, ctx)
	rt.advanceAcceptedCommitProgress(ctx)
}

func (rt *slotRuntime) handleAcceptedCommitRequest(
	ctx context.Context,
	req CommitWriteRequest,
	resp chan<- error,
) {
	rt.reconcileDurableCommitProgress(rt.backgroundContext())
	record, err := rt.replicationRecord()
	if err != nil {
		resp <- err
		return
	}
	record = ensureProtocolReplicaState(record)
	if rt.progressionGap {
		resp <- fmt.Errorf("%w: slot %d commit progression gap at sequence %d", ErrStateMismatch, rt.slot, rt.progressionGapSequence)
		return
	}
	rt.syncAcceptedCommitsFromRecord(record)
	if err := validateCommitSource(record, req); err != nil {
		resp <- err
		return
	}
	if req.Sequence <= record.highestCommittedSequence {
		if err := reduceHandlePastCommit(record, req); err != nil {
			resp <- err
			return
		}
		resp <- nil
		return
	}
	entry := rt.acceptedCommit(req.Sequence)
	recordChanged := false
	if entry != nil {
		if !sameCommitRequest(entry.request, req) {
			resp <- fmt.Errorf("%w: slot %d sequence %d accepted commit conflict", ErrProtocolConflict, req.Slot, req.Sequence)
			return
		}
		entry.strict = true
		rt.markBreadcrumb(&rt.lastDuplicateCommitParked, req.Sequence)
		rt.parkAcceptedCommitWaiter(req.Sequence, resp, ctx)
		rt.advanceAcceptedCommitProgress(ctx)
		return
	}
	buffered, err := rt.mirrorAcceptedCommit(record, req)
	if err != nil {
		if req.Sequence > record.highestCommittedSequence+1 || !reduceHasCommittableSequence(record, req.Sequence) {
			rt.node.observeBackpressure(err)
		}
		resp <- err
		return
	}
	if !reflect.DeepEqual(buffered.bufferedCommits, record.bufferedCommits) {
		recordChanged = true
	}
	record = buffered
	entry = rt.strictAcceptedCommitEntry(req.Sequence, req)
	rt.parkAcceptedCommitWaiter(req.Sequence, resp, ctx)
	if entry.stage == acceptedCommitApplied {
		rt.releaseAcceptedCommitWaiters(req.Sequence, nil)
		rt.deleteAcceptedCommit(req.Sequence)
		return
	}
	if recordChanged {
		rt.setRecord(record)
	}
	rt.advanceAcceptedCommitProgress(ctx)
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
	rt.commitEffectSequence = req.Sequence
	applyStarted := time.Now()
	if err := rt.node.submitCommittedOperation(rt.backgroundContext(), rt.owner, commit, func(runtime *slotRuntime, err error, completedAt time.Time) {
		runtime.commitEffectInFlight = false
		runtime.commitEffectSequence = 0
		if recordStage {
			runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		runtime.handleCommitEffectResult(ctx, req.Sequence, operation, &reqCopy, err, resp)
	}); err != nil {
		rt.commitEffectInFlight = false
		rt.commitEffectSequence = 0
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
	entry := rt.acceptedCommitEntry(req.Sequence, req)
	rt.commitEffectInFlight = true
	rt.commitEffectSequence = req.Sequence
	rt.parkAcceptedCommitWaiter(req.Sequence, resp, ctx)
	entry.stage = acceptedCommitDurableInFlight
	applyStarted := time.Now()
	if err := rt.node.submitCommittedOperation(rt.backgroundContext(), rt.owner, commit, func(runtime *slotRuntime, err error, completedAt time.Time) {
		if recordStage {
			runtime.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		if !completedAt.IsZero() {
			runtime.node.observeCommitOwnerCallbackDelay(role, writeStageResult(err), time.Since(completedAt))
		}
		runtime.finishAcceptedCommitEffect(req.Sequence, operation, &reqCopy, err)
	}); err != nil {
		rt.commitEffectInFlight = false
		rt.commitEffectSequence = 0
		entry.stage = acceptedCommitWaitingForTurn
		if recordStage {
			rt.node.observeWriteStage(stage, role, writeStageResult(err), time.Since(applyStarted))
		}
		rt.releaseAcceptedCommitWaiters(req.Sequence, err)
		rt.deleteAcceptedCommit(req.Sequence)
	}
}

func (rt *slotRuntime) drainReadyBuffered(ctx context.Context, resp chan<- error) {
	if !rt.exists {
		if resp != nil {
			resp <- nil
		}
		return
	}
	rt.reconcileDurableCommitProgress(rt.backgroundContext())
	if !rt.exists {
		if resp != nil {
			resp <- nil
		}
		return
	}
	rt.advanceAcceptedCommitProgress(ctx)
	if rt.commitEffectInFlight {
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
		rt.acceptedCommitEntry(nextCommit, req)
		rt.advanceAcceptedCommitProgress(ctx)
		if resp != nil {
			resp <- nil
		}
		return
	}
	rt.ensureGapCatchup(rt.backgroundContext())
	if resp != nil {
		resp <- nil
	}
}

func (rt *slotRuntime) ensureUpstreamCommitReplay(ctx context.Context) {
	if !rt.exists {
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
	if rt.upstreamCommitInFlight {
		return
	}
	committedThrough := record.highestCommittedSequence
	if committedThrough <= record.highestUpstreamConfirmedSequence {
		return
	}
	predecessorNodeID := record.assignment.Peers.PredecessorNodeID
	predecessorTarget := record.assignment.Peers.PredecessorTarget
	role := record.assignment.Role
	req := CommitAdvanceRequest{
		Slot:             rt.slot,
		CommittedThrough: committedThrough,
		FromNodeID:       rt.node.nodeID,
		ChainVersion:     record.assignment.ChainVersion,
	}
	rt.upstreamCommitInFlight = true
	rt.upstreamCommitHighSent = committedThrough
	rt.node.traceWriteEvent(record.assignment, committedThrough, "commit_token_send_start")
	rt.runAsync(func() error {
		started := time.Now()
		err := rt.node.repl.CommitAdvance(
			ctx,
			peerTransportTarget(predecessorTarget, predecessorNodeID),
			req,
		)
		rt.node.observeWriteStage(writeStageCommitTokenQueueWait, role, writeStageResult(err), time.Since(started))
		return err
	}, func(runtime *slotRuntime, err error) {
		runtime.upstreamCommitInFlight = false
		if err != nil {
			runtime.node.traceWriteEvent(record.assignment, committedThrough, "commit_token_send_error")
			runtime.node.events.record(runtime.node.logger, zerolog.ErrorLevel, "replication_commit_replay_failed", "storage upstream commit advance failed", &runtime.slot, nil, &committedThrough, predecessorNodeID, "", err)
			if committedThrough <= runtime.upstreamCommitHighSent {
				runtime.upstreamCommitHighSent = 0
			}
			return
		}
		if !runtime.exists {
			return
		}
		current := ensureProtocolReplicaState(runtime.record)
		if !runtime.assignmentMatchesReplayTarget(current, predecessorNodeID, predecessorTarget, req.ChainVersion) {
			runtime.node.traceWriteEvent(current.assignment, committedThrough, "commit_token_send_retarget")
			if committedThrough > current.highestUpstreamConfirmedSequence {
				runtime.upstreamCommitHighSent = current.highestUpstreamConfirmedSequence
			}
			runtime.ensureUpstreamCommitReplay(runtime.backgroundContext())
			return
		}
		runtime.node.traceWriteEvent(current.assignment, committedThrough, "commit_token_sent")
		if committedThrough > current.highestUpstreamConfirmedSequence {
			current.highestUpstreamConfirmedSequence = committedThrough
			runtime.setRecord(current)
			runtime.persistUpstreamConfirmedSequenceAsync(cloneAssignment(current.assignment), current.highestUpstreamConfirmedSequence, predecessorNodeID)
		}
		runtime.ensureUpstreamCommitReplay(runtime.backgroundContext())
	})
}

func (rt *slotRuntime) schedulePreparedForwardReplayRetry() {
	if !rt.exists || rt.preparedReplayRetryScheduled {
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	if record.assignment.Peers.SuccessorNodeID == "" {
		return
	}
	if rt.contiguousPreparedHighWater(record) <= record.highestCommittedSequence {
		return
	}
	rt.preparedReplayRetryScheduled = true
	time.AfterFunc(preparedReplayRetryDelay, func() {
		_ = rt.owner.enqueueCompletion(func(runtime *slotRuntime) {
			runtime.preparedReplayRetryScheduled = false
			runtime.replayPreparedForwards(runtime.backgroundContext())
		})
	})
}

func (rt *slotRuntime) replayPreparedForwards(ctx context.Context) {
	if !rt.exists {
		return
	}
	record := ensureProtocolReplicaState(rt.record)
	successorNodeID := record.assignment.Peers.SuccessorNodeID
	if successorNodeID == "" {
		return
	}
	high := rt.contiguousPreparedHighWater(record)
	if high <= record.highestCommittedSequence {
		return
	}
	successorTarget := record.assignment.Peers.SuccessorTarget
	role := record.assignment.Role
	chainVersion := record.assignment.ChainVersion
	rt.preparedReplayRetryScheduled = false
	for sequence := record.highestCommittedSequence + 1; sequence <= high; sequence++ {
		operation, ok := record.preparedEntries[sequence]
		if !ok {
			break
		}
		req := ForwardWriteRequest{
			Operation:    cloneWriteOperation(operation),
			FromNodeID:   rt.node.nodeID,
			ChainVersion: chainVersion,
		}
		seq := sequence
		rt.runAsync(func() error {
			started := time.Now()
			err := rt.node.repl.ForwardWrite(
				ctx,
				peerTransportTarget(successorTarget, successorNodeID),
				req,
			)
			rt.node.observeWriteStage(writeStageForwardAcceptRPC, role, writeStageResult(err), time.Since(started))
			return err
		}, func(runtime *slotRuntime, err error) {
			if err != nil {
				runtime.node.events.record(
					runtime.node.logger,
					zerolog.ErrorLevel,
					"replication_prepare_replay_failed",
					"storage prepared forward replay failed after chain update",
					&runtime.slot,
					&chainVersion,
					&seq,
					successorNodeID,
					"",
					err,
				)
				if runtime.exists {
					current := ensureProtocolReplicaState(runtime.record)
					if current.assignment.ChainVersion == chainVersion &&
						current.assignment.Peers.SuccessorNodeID == successorNodeID &&
						current.assignment.Peers.SuccessorTarget == successorTarget &&
						seq > current.highestCommittedSequence {
						runtime.schedulePreparedForwardReplayRetry()
					}
				}
				return
			}
			runtime.drainReadyBuffered(runtime.backgroundContext(), nil)
		})
	}
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
		highestPreparedDurable:           0,
		highestCommitTokenReceived:       0,
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
		rt.clearCommittedMetadata()
		if err := rt.node.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
			rt.removeRecord()
			return false, fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
		}
		record.highestPreparedDurable = highestCommittedSequence
		record.highestCommitTokenReceived = highestCommittedSequence
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
		if rt.node.commitJournal != nil {
			rt.node.commitJournal.dropSlot(rt.slot)
		}
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
	predecessorPeerChanged := record.assignment.Peers.PredecessorNodeID != assignment.Peers.PredecessorNodeID ||
		record.assignment.Peers.PredecessorTarget != assignment.Peers.PredecessorTarget
	predecessorReplayTargetChanged := record.assignment.ChainVersion != assignment.ChainVersion ||
		predecessorPeerChanged
	successorChanged := record.assignment.ChainVersion != assignment.ChainVersion ||
		record.assignment.Peers.SuccessorNodeID != assignment.Peers.SuccessorNodeID ||
		record.assignment.Peers.SuccessorTarget != assignment.Peers.SuccessorTarget
	if predecessorPeerChanged {
		record.highestUpstreamConfirmedSequence = record.highestCommittedSequence
	}
	record.assignment = cloneAssignment(assignment)
	if predecessorReplayTargetChanged {
		rt.upstreamCommitHighSent = record.highestUpstreamConfirmedSequence
		rt.upstreamCommitAcked = map[uint64]struct{}{}
		record = retargetUncommittedForwardSources(record)
	}
	if successorChanged {
		record = retargetUncommittedCommitSources(record)
		if record.assignment.Peers.SuccessorNodeID == "" && record.highestPreparedDurable > record.highestCommitTokenReceived {
			record.highestCommitTokenReceived = record.highestPreparedDurable
		}
	}
	if record.state != ReplicaStateRecovered {
		record.lastKnownState = record.state
	}
	rt.setRecord(record)
	if err := rt.node.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	rt.node.events.record(rt.node.logger, zerolog.InfoLevel, "update_chain_peers", "storage replica peers updated", &assignment.Slot, &assignment.ChainVersion, nil, "", "", nil)
	if record.state == ReplicaStateActive {
		rt.submitCommitWatermarkReady(record)
		if successorChanged {
			rt.replayPreparedForwards(rt.backgroundContext())
		}
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
	if record.assignment.Peers.SuccessorNodeID == "" {
		if committed := rt.contiguousPreparedHighWater(record); committed > record.highestCommittedSequence {
			record.highestCommittedSequence = committed
			record.localDataPresent = true
		}
	} else {
		tailNodeID := record.assignment.Peers.TailNodeID
		tailTarget := peerTransportTarget(record.assignment.Peers.TailTarget, tailNodeID)
		if tailNodeID != "" && tailNodeID != rt.node.nodeID && tailTarget != "" {
			if tailCommitted, err := rt.node.repl.FetchCommittedSequence(ctx, tailTarget, rt.slot); err == nil {
				recoverableCommitted := rt.contiguousPreparedHighWater(record)
				if tailCommitted < recoverableCommitted {
					recoverableCommitted = tailCommitted
				}
				if recoverableCommitted > record.highestCommittedSequence {
					record.highestCommittedSequence = recoverableCommitted
					record.localDataPresent = true
				}
			}
		}
	}
	record.state = ReplicaStateActive
	record.lastKnownState = ReplicaStateActive
	record.highestPreparedDurable = record.highestCommittedSequence
	record.highestCommitTokenReceived = record.highestCommittedSequence
	record.highestUpstreamConfirmedSequence = normalizeUpstreamConfirmedSequence(record)
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
	if rt.node.commitJournal != nil {
		rt.node.commitJournal.dropSlot(cmd.Assignment.Slot)
	}
	if err := rt.node.backend.InstallSnapshot(cmd.Assignment.Slot, snapshot); err != nil {
		return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
	}
	rt.clearCommittedMetadata()
	if err := rt.node.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
		return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
	}
	record := replicaRecord{
		assignment:                       cloneAssignment(cmd.Assignment),
		state:                            ReplicaStateActive,
		nextSequence:                     highestCommittedSequence + 1,
		highestPreparedDurable:           highestCommittedSequence,
		highestCommitTokenReceived:       highestCommittedSequence,
		highestCommittedSequence:         highestCommittedSequence,
		materializedCommittedSequence:    highestCommittedSequence,
		highestUpstreamConfirmedSequence: highestCommittedSequence,
		localDataPresent:                 true,
		lastKnownState:                   ReplicaStateActive,
	}
	record = ensureProtocolReplicaState(record)
	rt.setRecord(record)
	if rt.node.commitJournal != nil {
		rt.node.commitJournal.allowSlot(cmd.Assignment.Slot)
	}
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
	if rt.node.commitJournal != nil {
		rt.node.commitJournal.dropSlot(slot)
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
	n.observeWriteStage(writeStageHeadWaitForCommitWatermark, response.role, writeStageResult(err), time.Since(waitStarted))
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

func (n *Node) handleCommitAdvanceOwned(ctx context.Context, req CommitAdvanceRequest) error {
	owner := n.ensureSlotOwner(req.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.handleCommitAdvance(ctx, req, respCh)
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

func (n *Node) handleCommitAdvanceAcceptedOwned(ctx context.Context, req CommitAdvanceRequest) error {
	owner := n.ensureSlotOwner(req.Slot)
	respCh := make(chan error, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		runtime.handleCommitAdvanceAccepted(ctx, req, respCh)
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

func (n *Node) ReplicationSlotCredit(slot int) int {
	return n.ReplicationSlotCreditByKind(slot, "forward")
}

func (n *Node) ReplicationSlotCreditByKind(slot int, kind string) int {
	owner := n.ensureSlotOwner(slot)
	respCh := make(chan int, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.replicationAvailableCredit(kind)
	}); err != nil {
		return 0
	}
	select {
	case <-n.done:
		return 0
	case credit := <-respCh:
		return credit
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
