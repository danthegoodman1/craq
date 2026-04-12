package storage

import (
	"fmt"
	"sort"
)

type slotProtocolBufferLimits struct {
	perSlotLimit    int
	perNodeLimit    int
	nodeBufferedNow int
}

type slotReducerAction string

const (
	slotReducerActionApply  slotReducerAction = "apply"
	slotReducerActionBuffer slotReducerAction = "buffer"
	slotReducerActionIgnore slotReducerAction = "ignore"
)

type slotForwardReduction struct {
	Record replicaRecord
	Action slotReducerAction
}

type slotCommitReduction struct {
	Record replicaRecord
	Action slotReducerAction
}

type slotSubmitReduction struct {
	Record replicaRecord
	Result CommitResult
}

// The reducer layer is the single source of truth for slot-owned protocol
// state transitions. The lock-based Node facade should delegate mutations here
// so we do not maintain parallel imperative copies in the hot path.
func reduceSubmitWrite(record replicaRecord, operation WriteOperation) slotSubmitReduction {
	record = ensureProtocolReplicaState(record)
	opCopy := cloneWriteOperation(operation)
	record.pendingWrites[operation.Sequence] = pendingWrite{
		result: CommitResult{
			Slot:     operation.Slot,
			Sequence: operation.Sequence,
			Applied:  true,
			Metadata: cloneObjectMetadataPtr(&operation.Metadata),
		},
		operation: &opCopy,
	}
	record.expectedCommitSources[operation.Sequence] = expectedCommitSource{
		FromNodeID:   record.assignment.Peers.SuccessorNodeID,
		ChainVersion: record.assignment.ChainVersion,
	}
	record = reduceAddDirtyEntry(record, operation)
	record.nextSequence++
	return slotSubmitReduction{
		Record: record,
		Result: CommitResult{
			Slot:     operation.Slot,
			Sequence: operation.Sequence,
			Applied:  true,
			Metadata: cloneObjectMetadataPtr(&operation.Metadata),
		},
	}
}

func reduceForwardWrite(
	record replicaRecord,
	req ForwardWriteRequest,
	limits slotProtocolBufferLimits,
) (slotForwardReduction, error) {
	record = ensureProtocolReplicaState(record)
	if err := validateForwardSource(record, req); err != nil {
		return slotForwardReduction{}, err
	}
	switch {
	case req.Operation.Sequence < record.nextSequence:
		if err := reduceHandlePastForward(record, req); err != nil {
			return slotForwardReduction{}, err
		}
		return slotForwardReduction{Record: record, Action: slotReducerActionIgnore}, nil
	case req.Operation.Sequence > record.nextSequence:
		next, err := reduceBufferFutureForward(record, req, limits)
		if err != nil {
			return slotForwardReduction{}, err
		}
		return slotForwardReduction{Record: next, Action: slotReducerActionBuffer}, nil
	default:
		record = reduceStageForward(record, req)
		return slotForwardReduction{Record: record, Action: slotReducerActionApply}, nil
	}
}

func reduceCommitWrite(
	record replicaRecord,
	req CommitWriteRequest,
	limits slotProtocolBufferLimits,
) (slotCommitReduction, error) {
	record = ensureProtocolReplicaState(record)
	if err := validateCommitSource(record, req); err != nil {
		return slotCommitReduction{}, err
	}
	switch {
	case req.Sequence <= record.highestCommittedSequence:
		if err := reduceHandlePastCommit(record, req); err != nil {
			return slotCommitReduction{}, err
		}
		return slotCommitReduction{Record: record, Action: slotReducerActionIgnore}, nil
	case req.Sequence > record.highestCommittedSequence+1 || !reduceHasCommittableSequence(record, req.Sequence):
		next, err := reduceBufferFutureCommit(record, req, limits)
		if err != nil {
			return slotCommitReduction{}, err
		}
		return slotCommitReduction{Record: next, Action: slotReducerActionBuffer}, nil
	default:
		return slotCommitReduction{Record: record, Action: slotReducerActionApply}, nil
	}
}

func reduceApplyCommittedSequence(
	record replicaRecord,
	operation WriteOperation,
	sequence uint64,
	retentionLimit int,
) replicaRecord {
	record = ensureProtocolReplicaState(record)
	record.highestCommittedSequence = sequence
	record.localDataPresent = true
	if record.state != ReplicaStateRecovered {
		record.lastKnownState = record.state
	}
	if staged, ok := record.stagedForwards[sequence]; ok {
		delete(record.stagedForwards, sequence)
		record = reduceRecordCommittedForward(record, staged, retentionLimit)
	}
	delete(record.expectedCommitSources, sequence)
	delete(record.pendingWrites, sequence)
	record = reduceRemoveDirtyEntry(record, operation.Key, sequence)
	return record
}

func reduceStageForward(record replicaRecord, req ForwardWriteRequest) replicaRecord {
	record = ensureProtocolReplicaState(record)
	delete(record.bufferedForwards, req.Operation.Sequence)
	if _, ok := record.stagedForwards[req.Operation.Sequence]; ok {
		return record
	}
	record.stagedForwards[req.Operation.Sequence] = cloneForwardRequest(req)
	record.expectedCommitSources[req.Operation.Sequence] = expectedCommitSource{
		FromNodeID:   record.assignment.Peers.SuccessorNodeID,
		ChainVersion: record.assignment.ChainVersion,
	}
	record = reduceAddDirtyEntry(record, req.Operation)
	record.nextSequence++
	return record
}

func reduceRecordCommitApplied(
	record replicaRecord,
	req CommitWriteRequest,
	retentionLimit int,
) replicaRecord {
	record = ensureProtocolReplicaState(record)
	delete(record.bufferedCommits, req.Sequence)
	record = reduceRecordCommittedCommit(record, req, retentionLimit)
	if pending, ok := record.pendingWrites[req.Sequence]; ok {
		pending.completed = true
		record.pendingWrites[req.Sequence] = pending
	}
	return record
}

func ensureProtocolReplicaState(record replicaRecord) replicaRecord {
	if record.pendingWrites == nil {
		record.pendingWrites = map[uint64]pendingWrite{}
	}
	if record.expectedCommitSources == nil {
		record.expectedCommitSources = map[uint64]expectedCommitSource{}
	}
	if record.stagedForwards == nil {
		record.stagedForwards = map[uint64]ForwardWriteRequest{}
	}
	if record.bufferedForwards == nil {
		record.bufferedForwards = map[uint64]ForwardWriteRequest{}
	}
	if record.bufferedCommits == nil {
		record.bufferedCommits = map[uint64]CommitWriteRequest{}
	}
	if record.recentCommittedForwards == nil {
		record.recentCommittedForwards = map[uint64]ForwardWriteRequest{}
	}
	if record.recentCommittedCommits == nil {
		record.recentCommittedCommits = map[uint64]CommitWriteRequest{}
	}
	if record.dirtyByKey == nil {
		record.dirtyByKey = map[string][]dirtyReadEntry{}
	}
	return record
}

func reduceHandlePastForward(record replicaRecord, req ForwardWriteRequest) error {
	record = ensureProtocolReplicaState(record)
	if staged, ok := record.stagedForwards[req.Operation.Sequence]; ok {
		if sameForwardRequest(staged, req) {
			return nil
		}
		return fmt.Errorf("%w: slot %d sequence %d forward payload conflict", ErrProtocolConflict, req.Operation.Slot, req.Operation.Sequence)
	}
	if committed, ok := record.recentCommittedForwards[req.Operation.Sequence]; ok {
		if sameForwardRequest(committed, req) {
			return nil
		}
		return fmt.Errorf("%w: slot %d sequence %d committed forward conflict", ErrProtocolConflict, req.Operation.Slot, req.Operation.Sequence)
	}
	return fmt.Errorf("%w: slot %d sequence %d is outside retained forward history", ErrSequenceMismatch, req.Operation.Slot, req.Operation.Sequence)
}

func reduceHandlePastCommit(record replicaRecord, req CommitWriteRequest) error {
	record = ensureProtocolReplicaState(record)
	if committed, ok := record.recentCommittedCommits[req.Sequence]; ok {
		if sameCommitRequest(committed, req) {
			return nil
		}
		return fmt.Errorf("%w: slot %d sequence %d committed ack conflict", ErrProtocolConflict, req.Slot, req.Sequence)
	}
	return fmt.Errorf("%w: slot %d sequence %d is outside retained commit history", ErrSequenceMismatch, req.Slot, req.Sequence)
}

func reduceBufferFutureForward(
	record replicaRecord,
	req ForwardWriteRequest,
	limits slotProtocolBufferLimits,
) (replicaRecord, error) {
	record = ensureProtocolReplicaState(record)
	if existing, ok := record.bufferedForwards[req.Operation.Sequence]; ok {
		if sameForwardRequest(existing, req) {
			return record, nil
		}
		return record, fmt.Errorf("%w: slot %d sequence %d buffered forward conflict", ErrProtocolConflict, req.Operation.Slot, req.Operation.Sequence)
	}
	if reduceTotalBufferedMessages(record) >= limits.perSlotLimit {
		return record, newReplicaBackpressureError(req.Operation.Slot, reduceTotalBufferedMessages(record), limits.perSlotLimit)
	}
	if limits.perNodeLimit > 0 && limits.nodeBufferedNow >= limits.perNodeLimit {
		return record, newReplicaBackpressureError(req.Operation.Slot, limits.nodeBufferedNow, limits.perNodeLimit)
	}
	record.bufferedForwards[req.Operation.Sequence] = cloneForwardRequest(req)
	return record, nil
}

func reduceBufferFutureCommit(
	record replicaRecord,
	req CommitWriteRequest,
	limits slotProtocolBufferLimits,
) (replicaRecord, error) {
	record = ensureProtocolReplicaState(record)
	if existing, ok := record.bufferedCommits[req.Sequence]; ok {
		if sameCommitRequest(existing, req) {
			return record, nil
		}
		return record, fmt.Errorf("%w: slot %d sequence %d buffered commit conflict", ErrProtocolConflict, req.Slot, req.Sequence)
	}
	if reduceTotalBufferedMessages(record) >= limits.perSlotLimit {
		return record, newReplicaBackpressureError(req.Slot, reduceTotalBufferedMessages(record), limits.perSlotLimit)
	}
	if limits.perNodeLimit > 0 && limits.nodeBufferedNow >= limits.perNodeLimit {
		return record, newReplicaBackpressureError(req.Slot, limits.nodeBufferedNow, limits.perNodeLimit)
	}
	record.bufferedCommits[req.Sequence] = cloneCommitRequest(req)
	return record, nil
}

func reduceTotalBufferedMessages(record replicaRecord) int {
	return len(record.bufferedForwards) + len(record.bufferedCommits)
}

func reduceAddDirtyEntry(record replicaRecord, operation WriteOperation) replicaRecord {
	record = ensureProtocolReplicaState(record)
	entries := append([]dirtyReadEntry(nil), record.dirtyByKey[operation.Key]...)
	for i, entry := range entries {
		if entry.Sequence == operation.Sequence {
			entries[i].Operation = cloneWriteOperation(operation)
			record.dirtyByKey[operation.Key] = entries
			return record
		}
	}
	entries = append(entries, dirtyReadEntry{
		Sequence:  operation.Sequence,
		Operation: cloneWriteOperation(operation),
	})
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Sequence < entries[j].Sequence
	})
	record.dirtyByKey[operation.Key] = entries
	return record
}

func reduceRemoveDirtyEntry(record replicaRecord, key string, sequence uint64) replicaRecord {
	record = ensureProtocolReplicaState(record)
	entries, ok := record.dirtyByKey[key]
	if !ok {
		return record
	}
	filtered := entries[:0]
	for _, entry := range entries {
		if entry.Sequence == sequence {
			continue
		}
		filtered = append(filtered, entry)
	}
	if len(filtered) == 0 {
		delete(record.dirtyByKey, key)
		return record
	}
	record.dirtyByKey[key] = filtered
	return record
}

func reduceHasStagedForward(record replicaRecord, sequence uint64) bool {
	record = ensureProtocolReplicaState(record)
	_, ok := record.stagedForwards[sequence]
	return ok
}

func reduceHasCommittableSequence(record replicaRecord, sequence uint64) bool {
	if reduceHasStagedForward(record, sequence) {
		return true
	}
	record = ensureProtocolReplicaState(record)
	_, ok := record.pendingWrites[sequence]
	return ok
}

func reduceCommittableOperation(record replicaRecord, sequence uint64) (WriteOperation, error) {
	record = ensureProtocolReplicaState(record)
	if pending, ok := record.pendingWrites[sequence]; ok && pending.operation != nil {
		return cloneWriteOperation(*pending.operation), nil
	}
	if staged, ok := record.stagedForwards[sequence]; ok {
		return cloneWriteOperation(staged.Operation), nil
	}
	return WriteOperation{}, fmt.Errorf("%w: slot %d sequence %d is not committable", ErrSequenceMismatch, record.assignment.Slot, sequence)
}

func reduceRecordCommittedForward(record replicaRecord, req ForwardWriteRequest, retentionLimit int) replicaRecord {
	record = ensureProtocolReplicaState(record)
	record.recentCommittedForwards[req.Operation.Sequence] = cloneForwardRequest(req)
	record.recentForwardOrder = append(record.recentForwardOrder, req.Operation.Sequence)
	for len(record.recentForwardOrder) > retentionLimit {
		evicted := record.recentForwardOrder[0]
		record.recentForwardOrder = record.recentForwardOrder[1:]
		delete(record.recentCommittedForwards, evicted)
	}
	return record
}

func reduceRecordCommittedCommit(record replicaRecord, req CommitWriteRequest, retentionLimit int) replicaRecord {
	record = ensureProtocolReplicaState(record)
	record.recentCommittedCommits[req.Sequence] = cloneCommitRequest(req)
	record.recentCommitOrder = append(record.recentCommitOrder, req.Sequence)
	for len(record.recentCommitOrder) > retentionLimit {
		evicted := record.recentCommitOrder[0]
		record.recentCommitOrder = record.recentCommitOrder[1:]
		delete(record.recentCommittedCommits, evicted)
	}
	return record
}
