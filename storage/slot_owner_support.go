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

func (n *Node) applyCommittedOperation(ctx context.Context, commit DurableCommit) (bool, error) {
	if commit.UpstreamConfirmedSequence > commit.Operation.Sequence {
		commit.UpstreamConfirmedSequence = commit.Operation.Sequence
	}
	if commit.UpstreamConfirmedSequence == 0 && (commit.Persisted.Assignment.Role == ReplicaRoleHead || commit.Persisted.Assignment.Role == ReplicaRoleSingle || commit.Persisted.Assignment.Peers.PredecessorNodeID == "") {
		commit.UpstreamConfirmedSequence = commit.Operation.Sequence
	}
	committed, applyErr := n.commitEngine.submit(ctx, commit)
	if applyErr != nil {
		highestCommitted, err := n.backend.HighestCommittedSequence(commit.Operation.Slot)
		if err != nil || highestCommitted != commit.Operation.Sequence {
			return committed, fmt.Errorf("err in n.commitEngine.submit: %w", applyErr)
		}
		return true, fmt.Errorf("err in n.commitEngine.submit: %w", applyErr)
	}
	return committed, nil
}

func (n *Node) writeActuallyCommitted(slot int, sequence uint64) bool {
	highestCommitted, err := n.backend.HighestCommittedSequence(slot)
	if err != nil {
		return false
	}
	return highestCommitted >= sequence
}

func (n *Node) recordUpstreamCommitConfirmed(slot int, sequence uint64) error {
	backend, ok := n.backend.(upstreamConfirmationBackend)
	if !ok {
		return nil
	}
	if err := backend.SetHighestUpstreamConfirmedSequence(slot, sequence); err != nil && !errors.Is(err, ErrUnknownReplica) {
		return fmt.Errorf("err in backend.SetHighestUpstreamConfirmedSequence: %w", err)
	}
	return nil
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
