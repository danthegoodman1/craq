package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

func dirtyKeyCount(record replicaRecord) int {
	record = ensureProtocolReplicaState(record)
	return len(record.dirtyByKey)
}

func cloneReplicaRecord(record replicaRecord) replicaRecord {
	cloned := record
	if record.pendingWrites != nil {
		cloned.pendingWrites = make(map[uint64]pendingWrite, len(record.pendingWrites))
		for sequence, write := range record.pendingWrites {
			cloned.pendingWrites[sequence] = clonePendingWrite(write)
		}
	}
	if record.preparedEntries != nil {
		cloned.preparedEntries = make(map[uint64]WriteOperation, len(record.preparedEntries))
		for sequence, operation := range record.preparedEntries {
			cloned.preparedEntries[sequence] = cloneWriteOperation(operation)
		}
	}
	if record.stagedForwards != nil {
		cloned.stagedForwards = make(map[uint64]ForwardWriteRequest, len(record.stagedForwards))
		for sequence, req := range record.stagedForwards {
			cloned.stagedForwards[sequence] = cloneForwardRequest(req)
		}
	}
	if record.expectedCommitSources != nil {
		cloned.expectedCommitSources = make(map[uint64]expectedCommitSource, len(record.expectedCommitSources))
		for sequence, source := range record.expectedCommitSources {
			cloned.expectedCommitSources[sequence] = source
		}
	}
	if record.bufferedForwards != nil {
		cloned.bufferedForwards = make(map[uint64]ForwardWriteRequest, len(record.bufferedForwards))
		for sequence, req := range record.bufferedForwards {
			cloned.bufferedForwards[sequence] = cloneForwardRequest(req)
		}
	}
	if record.bufferedCommits != nil {
		cloned.bufferedCommits = make(map[uint64]CommitWriteRequest, len(record.bufferedCommits))
		for sequence, req := range record.bufferedCommits {
			cloned.bufferedCommits[sequence] = cloneCommitRequest(req)
		}
	}
	if record.recentCommittedForwards != nil {
		cloned.recentCommittedForwards = make(map[uint64]ForwardWriteRequest, len(record.recentCommittedForwards))
		for sequence, req := range record.recentCommittedForwards {
			cloned.recentCommittedForwards[sequence] = cloneForwardRequest(req)
		}
	}
	if record.recentCommittedCommits != nil {
		cloned.recentCommittedCommits = make(map[uint64]CommitWriteRequest, len(record.recentCommittedCommits))
		for sequence, req := range record.recentCommittedCommits {
			cloned.recentCommittedCommits[sequence] = cloneCommitRequest(req)
		}
	}
	cloned.recentForwardOrder = append([]uint64(nil), record.recentForwardOrder...)
	cloned.recentCommitOrder = append([]uint64(nil), record.recentCommitOrder...)
	if record.dirtyByKey != nil {
		cloned.dirtyByKey = make(map[string][]dirtyReadEntry, len(record.dirtyByKey))
		for key, entries := range record.dirtyByKey {
			clonedEntries := make([]dirtyReadEntry, 0, len(entries))
			for _, entry := range entries {
				clonedEntries = append(clonedEntries, dirtyReadEntry{
					Sequence:  entry.Sequence,
					Operation: cloneWriteOperation(entry.Operation),
				})
			}
			cloned.dirtyByKey[key] = clonedEntries
		}
	}
	if record.committedOverlay != nil {
		cloned.committedOverlay = make(map[string]dirtyReadEntry, len(record.committedOverlay))
		for key, entry := range record.committedOverlay {
			cloned.committedOverlay[key] = dirtyReadEntry{
				Sequence:  entry.Sequence,
				Operation: cloneWriteOperation(entry.Operation),
			}
		}
	}
	return cloned
}

func clonePendingWrite(write pendingWrite) pendingWrite {
	cloned := write
	cloned.result.Metadata = cloneObjectMetadataPtr(write.result.Metadata)
	if write.operation != nil {
		op := cloneWriteOperation(*write.operation)
		cloned.operation = &op
	}
	cloned.waiter = nil
	return cloned
}

func (n *Node) persistReplica(ctx context.Context, record replicaRecord) error {
	persisted := persistedReplica(record)
	if err := n.local.UpsertReplica(ctx, n.nodeID, persisted); err != nil {
		return fmt.Errorf("err in n.local.UpsertReplica: %w", err)
	}
	return nil
}

func persistedReplica(record replicaRecord) PersistedReplica {
	return PersistedReplica{
		Assignment:                       cloneAssignment(record.assignment),
		LastKnownState:                   record.lastKnownState,
		HighestCommittedSequence:         record.highestCommittedSequence,
		HighestUpstreamConfirmedSequence: normalizeUpstreamConfirmedSequence(record),
		HasCommittedData:                 record.localDataPresent,
	}
}

func recordWithCommittedOverlay(record replicaRecord, operation WriteOperation) replicaRecord {
	record = ensureProtocolReplicaState(record)
	record.committedOverlay[operation.Key] = dirtyReadEntry{
		Sequence:  operation.Sequence,
		Operation: cloneWriteOperation(operation),
	}
	return record
}

func recordPruneMaterializedOverlay(record replicaRecord, highestMaterialized uint64) replicaRecord {
	record = ensureProtocolReplicaState(record)
	if highestMaterialized > record.materializedCommittedSequence {
		record.materializedCommittedSequence = highestMaterialized
	}
	for sequence := range record.preparedEntries {
		if sequence <= highestMaterialized {
			delete(record.preparedEntries, sequence)
		}
	}
	for key, entry := range record.committedOverlay {
		if entry.Sequence <= highestMaterialized {
			delete(record.committedOverlay, key)
		}
	}
	if record.materializedCommittedSequence > record.highestCommittedSequence {
		record.materializedCommittedSequence = record.highestCommittedSequence
	}
	return record
}

func recordCommittedOverlayObject(record replicaRecord, key string) (CommittedObject, bool, bool) {
	record = ensureProtocolReplicaState(record)
	entry, ok := record.committedOverlay[key]
	if !ok {
		return CommittedObject{}, false, false
	}
	switch entry.Operation.Kind {
	case OperationKindPut:
		return CommittedObject{
			Value:    entry.Operation.Value,
			Metadata: cloneObjectMetadata(entry.Operation.Metadata),
		}, true, true
	case OperationKindDelete:
		return CommittedObject{}, false, true
	default:
		return CommittedObject{}, false, false
	}
}

func recordMergedCommittedSnapshot(record replicaRecord, base Snapshot) Snapshot {
	record = ensureProtocolReplicaState(record)
	merged := cloneSnapshot(base)
	for key, entry := range record.committedOverlay {
		switch entry.Operation.Kind {
		case OperationKindPut:
			merged[key] = CommittedObject{
				Value:    entry.Operation.Value,
				Metadata: cloneObjectMetadata(entry.Operation.Metadata),
			}
		case OperationKindDelete:
			delete(merged, key)
		}
	}
	return merged
}

func normalizeUpstreamConfirmedSequence(record replicaRecord) uint64 {
	if record.assignment.Role == ReplicaRoleHead || record.assignment.Role == ReplicaRoleSingle || record.assignment.Peers.PredecessorNodeID == "" {
		return record.highestCommittedSequence
	}
	if record.highestUpstreamConfirmedSequence > record.highestCommittedSequence {
		return record.highestCommittedSequence
	}
	return record.highestUpstreamConfirmedSequence
}

func upstreamConfirmedSequenceForLocalCommit(record replicaRecord, sequence uint64) uint64 {
	if record.assignment.Role == ReplicaRoleHead || record.assignment.Role == ReplicaRoleSingle || record.assignment.Peers.PredecessorNodeID == "" {
		return sequence
	}
	confirmed := normalizeUpstreamConfirmedSequence(record)
	if confirmed > sequence {
		return sequence
	}
	return confirmed
}

func (n *Node) submitPreparedOperation(
	ctx context.Context,
	owner *slotOwner,
	prepare DurableCommit,
	onComplete journalCompletionHandler,
) error {
	if n.commitJournal == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	if err := n.commitJournal.submitPrepare(ctx, owner, prepare, onComplete); err != nil {
		return fmt.Errorf("err in n.commitJournal.submitPrepare: %w", err)
	}
	return nil
}

func (n *Node) submitCommittedOperation(
	ctx context.Context,
	owner *slotOwner,
	commit DurableCommit,
	onComplete journalCompletionHandler,
) error {
	return n.submitPreparedOperation(ctx, owner, commit, onComplete)
}

func (n *Node) submitCommitWatermark(
	ctx context.Context,
	owner *slotOwner,
	assignment ReplicaAssignment,
	sequence uint64,
	onComplete journalCompletionHandler,
) error {
	if n.commitJournal == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	if err := n.commitJournal.submitCommitWatermark(ctx, owner, assignment, sequence, onComplete); err != nil {
		return fmt.Errorf("err in n.commitJournal.submitCommitWatermark: %w", err)
	}
	return nil
}

func (n *Node) submitHeadCommitRange(
	ctx context.Context,
	owner *slotOwner,
	assignment ReplicaAssignment,
	sequence uint64,
	onComplete journalCompletionHandler,
) error {
	if n.commitJournal == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	if err := n.commitJournal.submitHeadCommitRange(ctx, owner, assignment, sequence, onComplete); err != nil {
		return fmt.Errorf("err in n.commitJournal.submitHeadCommitRange: %w", err)
	}
	return nil
}

func (n *Node) submitUpstreamConfirmedSequence(
	ctx context.Context,
	owner *slotOwner,
	assignment ReplicaAssignment,
	sequence uint64,
	onComplete journalCompletionHandler,
) error {
	if n.commitJournal == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	if err := n.commitJournal.submitUpstreamConfirm(ctx, owner, assignment, sequence, onComplete); err != nil {
		return fmt.Errorf("err in n.commitJournal.submitUpstreamConfirm: %w", err)
	}
	return nil
}

func (n *Node) writeActuallyCommitted(slot int, sequence uint64) bool {
	if n.durableCommittedSequence(slot) >= sequence {
		return true
	}
	return false
}

func (n *Node) durablePreparedSequence(slot int) uint64 {
	if n.commitJournal != nil {
		return n.commitJournal.preparedSequence(slot)
	}
	return n.durableCommittedSequence(slot)
}

func (n *Node) durableCommittedSequence(slot int) uint64 {
	if n.commitJournal != nil {
		return n.commitJournal.committedSequence(slot)
	}
	highestCommitted, err := n.backend.HighestCommittedSequence(slot)
	if err != nil {
		return 0
	}
	return highestCommitted
}

func (n *Node) shouldMaterializeCommit(commit DurableCommit) bool {
	record, ok := n.publishedReplicaSnapshot(commit.Operation.Slot)
	if !ok {
		return false
	}
	return record.assignment.ChainVersion == commit.Persisted.Assignment.ChainVersion
}

func (n *Node) existingSlotOwner(slot int) *slotOwner {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.slotOwners[slot]
}

func (n *Node) notifyMaterialized(slot int, highest uint64) {
	owner := n.existingSlotOwner(slot)
	if owner == nil {
		return
	}
	_ = owner.enqueue(func(runtime *slotRuntime) {
		runtime.markMaterialized(highest)
	})
}

func (n *Node) ensureBackendReplica(slot int) error {
	if _, err := n.backend.HighestCommittedSequence(slot); err == nil {
		return nil
	} else if !errors.Is(err, ErrUnknownReplica) {
		return fmt.Errorf("err in n.backend.HighestCommittedSequence: %w", err)
	}
	if err := n.backend.CreateReplica(slot); err != nil && !errors.Is(err, ErrReplicaExists) {
		return fmt.Errorf("err in n.backend.CreateReplica: %w", err)
	}
	return nil
}

func (n *Node) waitForReplicaCreationReplay(ctx context.Context, assignment ReplicaAssignment) bool {
	for attempt := 0; attempt < 64; attempt++ {
		if err := ctx.Err(); err != nil {
			return false
		}
		existing, exists := n.publishedReplicaSnapshot(assignment.Slot)
		if exists && reflect.DeepEqual(existing.assignment, assignment) && existing.state != ReplicaStateRemoved {
			return true
		}
		persisted, err := n.local.LoadNode(ctx, n.nodeID)
		if err == nil && persistedReplicaMatchesAssignment(persisted, assignment) {
			return true
		}
		time.Sleep(50 * time.Microsecond)
	}
	return false
}

func autoActivationReadyContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent != nil {
		if _, ok := parent.Deadline(); !ok {
			return parent, func() {}
		}
	}
	return context.WithTimeout(context.Background(), defaultAutoActivationReadyTimeout)
}

func persistedReplicaMatchesAssignment(state PersistedNodeState, assignment ReplicaAssignment) bool {
	for _, replica := range state.Replicas {
		if replica.Assignment.Slot != assignment.Slot {
			continue
		}
		if reflect.DeepEqual(replica.Assignment, assignment) && replica.LastKnownState != ReplicaStateRemoved {
			return true
		}
		return false
	}
	return false
}
