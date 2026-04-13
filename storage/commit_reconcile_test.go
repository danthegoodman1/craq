package storage

import (
	"context"
	"testing"
	"time"
)

func TestAcceptCommitWriteDuplicateRetryCoalescesAndReconciles(t *testing.T) {
	ctx := context.Background()
	node := mustNewNode(t, ctx, Config{NodeID: "head"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers: ChainPeers{
			SuccessorNodeID: "mid",
		},
	})

	preparePendingCommitState(t, node, 7, []uint64{1}, "mid", 1)
	mustWithSlotRuntime(t, node, 7, func(runtime *slotRuntime) {
		runtime.commitEffectInFlight = true
		runtime.commitEffectSequence = 1
		runtime.acceptedCommitEntry(1, CommitWriteRequest{
			Slot:         7,
			Sequence:     1,
			FromNodeID:   "mid",
			ChainVersion: 1,
		}).stage = acceptedCommitDurableInFlight
		record := ensureProtocolReplicaState(runtime.record)
		record.highestCommitTokenReceived = 1
		runtime.setRecord(record)
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- node.AcceptCommitWrite(ctx, CommitWriteRequest{
			Slot:         7,
			Sequence:     1,
			FromNodeID:   "mid",
			ChainVersion: 1,
		})
	}()

	duplicateFinished := false
	var duplicateErr error
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case duplicateErr = <-errCh:
			duplicateFinished = true
		default:
		}
		if duplicateFinished {
			break
		}
		parked := false
		mustWithSlotRuntime(t, node, 7, func(runtime *slotRuntime) {
			entry := runtime.acceptedCommit(1)
			parked = entry != nil && len(entry.waiters) >= 1
		})
		if parked {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	record := mustSlotRecord(t, node, 7)
	if _, ok := record.bufferedCommits[1]; ok {
		t.Fatal("duplicate accepted commit was buffered instead of parked")
	}

	mustJournalDurablyCommitSequence(t, node, 7, 1)
	mustWithSlotRuntime(t, node, 7, func(runtime *slotRuntime) {
		if duplicateFinished {
			return
		}
		if !runtime.reconcileDurableCommitProgress(runtime.backgroundContext()) {
			t.Fatal("reconcileDurableCommitProgress unexpectedly reported no progress")
		}
	})

	if !duplicateFinished {
		select {
		case duplicateErr = <-errCh:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for duplicate accepted commit to finish")
		}
	}
	if duplicateErr != nil {
		t.Fatalf("AcceptCommitWrite returned error: %v", duplicateErr)
	}

	record = mustSlotRecord(t, node, 7)
	if got, want := record.highestCommittedSequence, uint64(1); got != want {
		t.Fatalf("highest committed sequence = %d, want %d", got, want)
	}
	if len(record.bufferedCommits) != 0 {
		t.Fatalf("buffered commits = %v, want empty", record.bufferedCommits)
	}
}

func TestFutureAcceptedCommitReconcilesMissedLocalApply(t *testing.T) {
	ctx := context.Background()
	node := mustNewNode(t, ctx, Config{NodeID: "head"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 9, ReplicaAssignment{
		Slot:         9,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers: ChainPeers{
			SuccessorNodeID: "mid",
		},
	})

	preparePendingCommitState(t, node, 9, []uint64{1, 2}, "mid", 1)
	waiter := make(chan error, 1)
	mustWithSlotRuntime(t, node, 9, func(runtime *slotRuntime) {
		runtime.commitEffectInFlight = true
		runtime.commitEffectSequence = 1
		runtime.acceptedCommitEntry(1, CommitWriteRequest{
			Slot:         9,
			Sequence:     1,
			FromNodeID:   "mid",
			ChainVersion: 1,
		}).stage = acceptedCommitDurableInFlight
		runtime.parkAcceptedCommitWaiter(1, waiter, context.Background())
		record := ensureProtocolReplicaState(runtime.record)
		record.highestCommitTokenReceived = 1
		runtime.setRecord(record)
	})

	mustJournalDurablyCommitSequence(t, node, 9, 1)
	if err := node.AcceptCommitWrite(ctx, CommitWriteRequest{
		Slot:         9,
		Sequence:     2,
		FromNodeID:   "mid",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("AcceptCommitWrite(seq=2) returned error: %v", err)
	}

	select {
	case err := <-waiter:
		if err != nil {
			t.Fatalf("parked waiter returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for parked waiter to finish")
	}

	waitForSlotCondition(t, node, 9, func(runtime *slotRuntime) bool {
		record := ensureProtocolReplicaState(runtime.record)
		return record.highestCommittedSequence >= 2
	})
	record := mustSlotRecord(t, node, 9)
	if len(record.bufferedCommits) != 0 {
		t.Fatalf("buffered commits = %v, want empty", record.bufferedCommits)
	}
}

func TestFutureAcceptedCommitDoesNotAckBeforePriorSequenceCommits(t *testing.T) {
	ctx := context.Background()
	node := mustNewNode(t, ctx, Config{NodeID: "head"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 11, ReplicaAssignment{
		Slot:         11,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers: ChainPeers{
			SuccessorNodeID: "mid",
		},
	})

	preparePendingCommitState(t, node, 11, []uint64{1, 2}, "mid", 1)
	mustWithSlotRuntime(t, node, 11, func(runtime *slotRuntime) {
		runtime.commitEffectInFlight = true
		runtime.commitEffectSequence = 1
		runtime.acceptedCommitEntry(1, CommitWriteRequest{
			Slot:         11,
			Sequence:     1,
			FromNodeID:   "mid",
			ChainVersion: 1,
		}).stage = acceptedCommitDurableInFlight
		record := ensureProtocolReplicaState(runtime.record)
		record.highestCommitTokenReceived = 1
		runtime.setRecord(record)
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- node.AcceptCommitWrite(ctx, CommitWriteRequest{
			Slot:         11,
			Sequence:     2,
			FromNodeID:   "mid",
			ChainVersion: 1,
		})
	}()

	acceptedFinished := false
	var acceptedErr error
	deadline := time.Now().Add(20 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case acceptedErr = <-errCh:
			acceptedFinished = true
		default:
		}
		if acceptedFinished {
			break
		}
		parked := false
		mustWithSlotRuntime(t, node, 11, func(runtime *slotRuntime) {
			entry := runtime.acceptedCommit(2)
			parked = entry != nil && len(entry.waiters) >= 1
		})
		if parked {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if acceptedFinished {
		if acceptedErr != nil {
			t.Fatalf("AcceptCommitWrite(seq=2) returned error: %v", acceptedErr)
		}
		record := mustSlotRecord(t, node, 11)
		if got := record.highestCommittedSequence; got < 2 {
			t.Fatalf("AcceptCommitWrite(seq=2) returned before slot committed through seq=2; highest committed = %d", got)
		}
		return
	}

	mustJournalDurablyCommitSequence(t, node, 11, 1)
	mustWithSlotRuntime(t, node, 11, func(runtime *slotRuntime) {
		if !runtime.reconcileDurableCommitProgress(runtime.backgroundContext()) {
			t.Fatal("reconcileDurableCommitProgress unexpectedly reported no progress")
		}
	})

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("AcceptCommitWrite(seq=2) returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for seq=2 commit acceptance to finish")
	}
}

func TestOutOfOrderAcceptedCommitDoesNotTriggerProgressionGap(t *testing.T) {
	ctx := context.Background()
	node := mustNewNode(t, ctx, Config{NodeID: "head"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 13, ReplicaAssignment{
		Slot:         13,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers: ChainPeers{
			SuccessorNodeID: "mid",
		},
	})

	preparePendingCommitState(t, node, 13, []uint64{1, 2, 3}, "mid", 1)
	mustWithSlotRuntime(t, node, 13, func(runtime *slotRuntime) {
		runtime.commitEffectInFlight = true
		runtime.commitEffectSequence = 1
		runtime.acceptedCommitEntry(1, CommitWriteRequest{
			Slot:         13,
			Sequence:     1,
			FromNodeID:   "mid",
			ChainVersion: 1,
		}).stage = acceptedCommitDurableInFlight
		record := ensureProtocolReplicaState(runtime.record)
		record.highestCommitTokenReceived = 1
		runtime.setRecord(record)
	})
	errCh := make(chan error, 1)
	go func() {
		errCh <- node.AcceptCommitWrite(ctx, CommitWriteRequest{
			Slot:         13,
			Sequence:     3,
			FromNodeID:   "mid",
			ChainVersion: 1,
		})
	}()

	mustWithSlotRuntime(t, node, 13, func(runtime *slotRuntime) {
		if runtime.progressionGap {
			t.Fatal("unexpected progression gap before out-of-order accept")
		}
	})

	waitForSlotCondition(t, node, 13, func(runtime *slotRuntime) bool {
		entry := runtime.acceptedCommit(3)
		return entry != nil && len(entry.waiters) == 1
	})
	mustWithSlotRuntime(t, node, 13, func(runtime *slotRuntime) {
		if runtime.progressionGap {
			t.Fatal("out-of-order accepted commit incorrectly triggered a progression gap")
		}
	})
	if got := node.ReplicationSlotCredit(13); got == 0 {
		t.Fatal("ReplicationSlotCredit dropped to zero for an out-of-order accepted commit")
	}

	mustJournalDurablyCommitSequence(t, node, 13, 1)
	mustWithSlotRuntime(t, node, 13, func(runtime *slotRuntime) {
		_ = runtime.reconcileDurableCommitProgress(runtime.backgroundContext())
	})
	waitForSlotCondition(t, node, 13, func(runtime *slotRuntime) bool {
		record := ensureProtocolReplicaState(runtime.record)
		return record.highestCommittedSequence >= 1
	})
	if err := node.AcceptCommitWrite(ctx, CommitWriteRequest{
		Slot:         13,
		Sequence:     2,
		FromNodeID:   "mid",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("AcceptCommitWrite(seq=2) returned error: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("AcceptCommitWrite(seq=3) returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for out-of-order accepted commit to finish")
	}
}

func preparePendingCommitState(t *testing.T, node *Node, slot int, sequences []uint64, sourceNodeID string, chainVersion uint64) {
	t.Helper()
	mustMutateSlotRecord(t, node, slot, func(record replicaRecord) replicaRecord {
		record = ensureProtocolReplicaState(record)
		for _, sequence := range sequences {
			seq := sequence
			op := WriteOperation{
				Slot:     slot,
				Sequence: seq,
				Kind:     OperationKindPut,
				Key:      "key",
				Value:    "value",
				Metadata: ObjectMetadata{Version: seq},
			}
			record.pendingWrites[seq] = pendingWrite{
				operation: &op,
			}
			record.preparedEntries[seq] = op
			record.expectedCommitSources[seq] = expectedCommitSource{
				FromNodeID:   sourceNodeID,
				ChainVersion: chainVersion,
			}
		}
		if len(sequences) > 0 {
			record.highestPreparedDurable = sequences[len(sequences)-1]
		}
		record.nextSequence = uint64(len(sequences)) + 1
		return record
	})
}

func mustJournalDurablyCommitSequence(t *testing.T, node *Node, slot int, sequence uint64) {
	t.Helper()
	owner := node.ensureSlotOwner(slot)
	record := mustSlotRecord(t, node, slot)
	done := make(chan error, 1)
	if err := node.submitCommitWatermark(context.Background(), owner, record.assignment, sequence, func(_ *slotRuntime, err error, _ time.Time) {
		done <- err
	}); err != nil {
		t.Fatalf("submitCommitWatermark returned error: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("durable commit completion returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for durable commit journal append")
	}
}

func waitForSlotCondition(t *testing.T, node *Node, slot int, cond func(*slotRuntime) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		matched := false
		mustWithSlotRuntime(t, node, slot, func(runtime *slotRuntime) {
			matched = cond(runtime)
		})
		if matched {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for slot %d condition", slot)
}
