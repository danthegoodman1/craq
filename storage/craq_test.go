package storage

import (
	"context"
	"errors"
	"testing"
)

func TestCRAQHeadLinearizableReadUsesTailCommittedWriteAfterDroppedCommitAck(t *testing.T) {
	ctx := context.Background()
	nodes, _, transport := setupActiveChainWithQueuedTransport(t, 12, []string{"head", "tail"})

	if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], transport, 12, "k", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}

	var dropped bool
	transport.SetBeforeDeliver(func(msg QueuedReplicationMessage) {
		if dropped || msg.Forward == nil || msg.ToNodeID != "tail" {
			return
		}
		dropped = true
		transport.DropNext()
	})

	if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], transport, 12, "k", "v2"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else if !errors.Is(err, ErrWriteTimeout) {
		t.Fatalf("SubmitPut error = %v, want ErrWriteTimeout", err)
	}

	linearizable, err := nodes["head"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 12,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("linearizable HandleClientGet returned error: %v", err)
	}
	assertCRAQValue(t, linearizable, "v2", 2)

	relaxed, err := nodes["head"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 12,
		Key:                  "k",
		ExpectedChainVersion: 1,
		Consistency:          ReadConsistencyLocalCommitted,
	})
	if err != nil {
		t.Fatalf("local committed HandleClientGet returned error: %v", err)
	}
	assertCRAQValue(t, relaxed, "v1", 1)
}

func TestCRAQMiddleReplicaDirtyReadsTrackLatestCommittedOperationForKey(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name      string
		ops       []WriteOperation
		wantFound bool
		wantValue string
		wantVer   uint64
	}{
		{
			name: "put then put",
			ops: []WriteOperation{
				{Kind: OperationKindPut, Value: "v2", Metadata: testObjectMetadata(2)},
				{Kind: OperationKindPut, Value: "v3", Metadata: testObjectMetadata(3)},
			},
			wantFound: true,
			wantValue: "v3",
			wantVer:   3,
		},
		{
			name: "put then delete",
			ops: []WriteOperation{
				{Kind: OperationKindPut, Value: "v2", Metadata: testObjectMetadata(2)},
				{Kind: OperationKindDelete, Metadata: testObjectMetadata(3)},
			},
			wantFound: false,
		},
		{
			name: "delete then recreate",
			ops: []WriteOperation{
				{Kind: OperationKindDelete, Metadata: testObjectMetadata(2)},
				{Kind: OperationKindPut, Value: "v3", Metadata: testObjectMetadata(3)},
			},
			wantFound: true,
			wantValue: "v3",
			wantVer:   3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			nodes, _, repl := setupActiveChainWithQueuedTransport(t, 13, []string{"head", "mid", "tail"})
			if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], repl, 13, "k", "v1"); err != nil {
				t.Fatalf("SubmitPut returned error: %v", err)
			}

			stageTailCommittedDirtyOps(t, ctx, nodes["mid"], repl, 13, "k", tc.ops)
			if got, want := dirtyEntryCountForKey(nodes["mid"], 13, "k"), len(tc.ops); got != want {
				t.Fatalf("dirty entries = %d, want %d", got, want)
			}

			linearizable, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
				Slot:                 13,
				Key:                  "k",
				ExpectedChainVersion: 1,
			})
			if err != nil {
				t.Fatalf("linearizable HandleClientGet returned error: %v", err)
			}
			assertCRAQReadResult(t, linearizable, tc.wantFound, tc.wantValue, tc.wantVer)

			relaxed, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
				Slot:                 13,
				Key:                  "k",
				ExpectedChainVersion: 1,
				Consistency:          ReadConsistencyLocalCommitted,
			})
			if err != nil {
				t.Fatalf("local committed HandleClientGet returned error: %v", err)
			}
			assertCRAQValue(t, relaxed, "v1", 1)

			if err := repl.DeliverAll(ctx); err != nil {
				t.Fatalf("DeliverAll returned error: %v", err)
			}
			committed, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
				Slot:                 13,
				Key:                  "k",
				ExpectedChainVersion: 1,
			})
			if err != nil {
				t.Fatalf("committed HandleClientGet returned error: %v", err)
			}
			assertCRAQReadResult(t, committed, tc.wantFound, tc.wantValue, tc.wantVer)
		})
	}
}

func TestCRAQDirtyReadsIgnoreUnrelatedDirtyKeys(t *testing.T) {
	ctx := context.Background()
	nodes, _, repl := setupActiveChainWithQueuedTransport(t, 14, []string{"head", "mid", "tail"})

	if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], repl, 14, "clean", "v1"); err != nil {
		t.Fatalf("SubmitPut(clean) returned error: %v", err)
	}
	stageTailCommittedDirtyOps(t, ctx, nodes["mid"], repl, 14, "dirty", []WriteOperation{{
		Kind:     OperationKindPut,
		Value:    "v2",
		Metadata: testObjectMetadata(2),
	}})

	fetches := 0
	repl.SetBeforeFetchCommittedSequence(func(fromNodeID string, slot int) {
		fetches++
	})

	clean, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 14,
		Key:                  "clean",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(clean) returned error: %v", err)
	}
	assertCRAQValue(t, clean, "v1", 1)
	if fetches != 0 {
		t.Fatalf("clean-key fetch count = %d, want 0", fetches)
	}

	dirty, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 14,
		Key:                  "dirty",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(dirty) returned error: %v", err)
	}
	assertCRAQValue(t, dirty, "v2", 2)
	if fetches != 1 {
		t.Fatalf("dirty-key fetch count = %d, want 1", fetches)
	}
}

func TestCRAQDuplicateForwardAndCommitDoNotCorruptDirtyIndex(t *testing.T) {
	ctx := context.Background()
	nodes, _, repl := setupActiveChainWithQueuedTransport(t, 15, []string{"head", "mid", "tail"})

	if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], repl, 15, "k", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}

	req := ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     15,
			Sequence: 2,
			Kind:     OperationKindPut,
			Key:      "k",
			Value:    "v2",
			Metadata: testObjectMetadata(2),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	}
	if err := nodes["mid"].HandleForwardWrite(ctx, req); err != nil {
		t.Fatalf("first HandleForwardWrite returned error: %v", err)
	}
	if got, want := dirtyEntryCountForKey(nodes["mid"], 15, "k"), 1; got != want {
		t.Fatalf("dirty entries after first forward = %d, want %d", got, want)
	}
	if err := nodes["mid"].HandleForwardWrite(ctx, req); err != nil {
		t.Fatalf("duplicate HandleForwardWrite returned error: %v", err)
	}
	if got, want := dirtyEntryCountForKey(nodes["mid"], 15, "k"), 1; got != want {
		t.Fatalf("dirty entries after duplicate forward = %d, want %d", got, want)
	}
	if err := repl.DeliverNext(ctx); err != nil {
		t.Fatalf("DeliverNext(to tail) returned error: %v", err)
	}

	read, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 15,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("linearizable HandleClientGet returned error: %v", err)
	}
	assertCRAQValue(t, read, "v2", 2)

	if err := repl.DeliverNext(ctx); err != nil {
		t.Fatalf("DeliverNext(commit to mid) returned error: %v", err)
	}
	if got := dirtyEntryCountForKey(nodes["mid"], 15, "k"); got != 0 {
		t.Fatalf("dirty entries after commit = %d, want 0", got)
	}
	if err := nodes["mid"].HandleCommitWrite(ctx, CommitWriteRequest{
		Slot:         15,
		Sequence:     2,
		FromNodeID:   "tail",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("duplicate HandleCommitWrite returned error: %v", err)
	}
	if got := dirtyEntryCountForKey(nodes["mid"], 15, "k"); got != 0 {
		t.Fatalf("dirty entries after duplicate commit = %d, want 0", got)
	}
	if err := repl.DeliverAll(ctx); err != nil {
		t.Fatalf("DeliverAll returned error: %v", err)
	}
}

func TestCRAQBufferedFutureForwardIsNotReadVisibleUntilContiguousAndTailCommitted(t *testing.T) {
	ctx := context.Background()
	nodes, _, repl := setupActiveChainWithQueuedTransport(t, 16, []string{"head", "mid", "tail"})

	if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], repl, 16, "k", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}

	if err := nodes["mid"].HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     16,
			Sequence: 3,
			Kind:     OperationKindPut,
			Key:      "k",
			Value:    "v3",
			Metadata: testObjectMetadata(3),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("future HandleForwardWrite returned error: %v", err)
	}
	initial, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 16,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(initial) returned error: %v", err)
	}
	assertCRAQValue(t, initial, "v1", 1)

	if err := nodes["mid"].HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     16,
			Sequence: 2,
			Kind:     OperationKindPut,
			Key:      "k",
			Value:    "v2",
			Metadata: testObjectMetadata(2),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("contiguous HandleForwardWrite returned error: %v", err)
	}
	if err := repl.DeliverNext(ctx); err != nil {
		t.Fatalf("DeliverNext(seq2 to tail) returned error: %v", err)
	}

	afterSeq2, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 16,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(after seq2 tail commit) returned error: %v", err)
	}
	assertCRAQValue(t, afterSeq2, "v2", 2)

	if err := deliverQueuedCommit(t, ctx, repl, "mid", 16, 2); err != nil {
		t.Fatalf("DeliverNext(commit seq2 to mid) returned error: %v", err)
	}
	relaxed, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 16,
		Key:                  "k",
		ExpectedChainVersion: 1,
		Consistency:          ReadConsistencyLocalCommitted,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(local committed) returned error: %v", err)
	}
	assertCRAQValue(t, relaxed, "v2", 2)

	if err := deliverQueuedForward(t, ctx, repl, "tail", 16, 3); err != nil {
		t.Fatalf("DeliverNext(seq3 to tail) returned error: %v", err)
	}
	afterSeq3, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 16,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(after seq3 tail commit) returned error: %v", err)
	}
	assertCRAQValue(t, afterSeq3, "v3", 3)

	if err := repl.DeliverAll(ctx); err != nil {
		t.Fatalf("DeliverAll returned error: %v", err)
	}
	final, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 16,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(final) returned error: %v", err)
	}
	assertCRAQValue(t, final, "v3", 3)
}

func TestCRAQReadDependencyUnavailableOnDirtyRead(t *testing.T) {
	ctx := context.Background()
	nodes, _, repl := setupActiveChainWithQueuedTransport(t, 17, []string{"head", "mid", "tail"})

	if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], repl, 17, "k", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	stageTailCommittedDirtyOps(t, ctx, nodes["mid"], repl, 17, "k", []WriteOperation{{
		Kind:     OperationKindPut,
		Value:    "v2",
		Metadata: testObjectMetadata(2),
	}})
	misconfigureReplicaTail(t, nodes["mid"], 17, "missing-tail")

	_, err := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 17,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err == nil {
		t.Fatal("HandleClientGet unexpectedly succeeded")
	}
	var dependency *ReadDependencyError
	if !errors.As(err, &dependency) {
		t.Fatalf("HandleClientGet error = %v, want ReadDependencyError", err)
	}
	if !errors.Is(err, ErrReadDependencyUnavailable) {
		t.Fatalf("HandleClientGet error = %v, want ErrReadDependencyUnavailable", err)
	}

	relaxed, relaxedErr := nodes["mid"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 17,
		Key:                  "k",
		ExpectedChainVersion: 1,
		Consistency:          ReadConsistencyLocalCommitted,
	})
	if relaxedErr != nil {
		t.Fatalf("local committed HandleClientGet returned error: %v", relaxedErr)
	}
	assertCRAQValue(t, relaxed, "v1", 1)
}

func stageTailCommittedDirtyOps(
	t *testing.T,
	ctx context.Context,
	mid *Node,
	repl *QueuedInMemoryReplicationTransport,
	slot int,
	key string,
	ops []WriteOperation,
) {
	t.Helper()
	for i, op := range ops {
		op.Slot = slot
		op.Sequence = uint64(i + 2)
		op.Key = key
		req := ForwardWriteRequest{Operation: op, FromNodeID: "head", ChainVersion: 1}
		if err := mid.HandleForwardWrite(ctx, req); err != nil {
			t.Fatalf("HandleForwardWrite(%d) returned error: %v", op.Sequence, err)
		}
		pending := repl.Pending()
		if pending == 0 {
			t.Fatalf("replication queue empty after staging sequence %d", op.Sequence)
		}
		if pending > 1 {
			if err := repl.MoveToFront(pending - 1); err != nil {
				t.Fatalf("MoveToFront(%d) returned error: %v", pending-1, err)
			}
		}
		if err := repl.DeliverNext(ctx); err != nil {
			t.Fatalf("DeliverNext(sequence=%d) returned error: %v", op.Sequence, err)
		}
	}
}

func assertCRAQValue(t *testing.T, result ReadResult, wantValue string, wantVersion uint64) {
	t.Helper()
	assertCRAQReadResult(t, result, true, wantValue, wantVersion)
}

func assertCRAQReadResult(t *testing.T, result ReadResult, wantFound bool, wantValue string, wantVersion uint64) {
	t.Helper()
	if result.Found != wantFound {
		t.Fatalf("read found = %t, want %t (%#v)", result.Found, wantFound, result)
	}
	if !wantFound {
		if result.Metadata != nil {
			t.Fatalf("read metadata = %#v, want nil for not found", result.Metadata)
		}
		if result.Value != "" {
			t.Fatalf("read value = %q, want empty for not found", result.Value)
		}
		return
	}
	if result.Value != wantValue {
		t.Fatalf("read value = %q, want %q", result.Value, wantValue)
	}
	if result.Metadata == nil || result.Metadata.Version != wantVersion {
		t.Fatalf("read metadata = %#v, want version %d", result.Metadata, wantVersion)
	}
}

func dirtyEntryCountForKey(node *Node, slot int, key string) int {
	owner := node.ensureSlotOwner(slot)
	done := make(chan struct{}, 1)
	record := replicaRecord{}
	if err := owner.dispatch(context.Background(), func(runtime *slotRuntime) {
		record = cloneReplicaRecord(runtime.record)
		done <- struct{}{}
	}); err != nil {
		panic(err)
	}
	<-done
	record = ensureProtocolReplicaState(record)
	return len(record.dirtyByKey[key])
}

func deliverQueuedForward(
	t *testing.T,
	ctx context.Context,
	repl *QueuedInMemoryReplicationTransport,
	toNodeID string,
	slot int,
	sequence uint64,
) error {
	t.Helper()
	return deliverQueuedMessage(t, ctx, repl, func(msg QueuedReplicationMessage) bool {
		return msg.ToNodeID == toNodeID &&
			msg.Forward != nil &&
			msg.Forward.Operation.Slot == slot &&
			msg.Forward.Operation.Sequence == sequence
	})
}

func deliverQueuedCommit(
	t *testing.T,
	ctx context.Context,
	repl *QueuedInMemoryReplicationTransport,
	toNodeID string,
	slot int,
	sequence uint64,
) error {
	t.Helper()
	return deliverQueuedMessage(t, ctx, repl, func(msg QueuedReplicationMessage) bool {
		return msg.ToNodeID == toNodeID &&
			msg.Commit != nil &&
			msg.Commit.Slot == slot &&
			msg.Commit.Sequence == sequence
	})
}

func deliverQueuedMessage(
	t *testing.T,
	ctx context.Context,
	repl *QueuedInMemoryReplicationTransport,
	match func(QueuedReplicationMessage) bool,
) error {
	t.Helper()
	for index, msg := range repl.PendingMessages() {
		if !match(msg) {
			continue
		}
		if index != 0 {
			if err := repl.MoveToFront(index); err != nil {
				t.Fatalf("MoveToFront(%d) returned error: %v", index, err)
			}
		}
		return repl.DeliverNext(ctx)
	}
	t.Fatalf("queued replication message not found")
	return nil
}

func misconfigureReplicaTail(t *testing.T, node *Node, slot int, tailNodeID string) {
	t.Helper()
	record, ok := node.State().Replicas[slot]
	if !ok {
		t.Fatalf("slot %d missing from node %q", slot, node.nodeID)
	}
	assignment := cloneAssignment(record.Assignment)
	assignment.Peers.TailNodeID = tailNodeID
	assignment.Peers.TailTarget = tailNodeID
	if err := node.UpdateChainPeers(context.Background(), UpdateChainPeersCommand{Assignment: assignment}); err != nil {
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}
}
