package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestFaultInjectedDroppedCommitAckLeavesHeadUncommitted(t *testing.T) {
	ctx := context.Background()
	nodes, _, transport := setupActiveChainWithQueuedTransport(t, 6, []string{"head", "tail"})

	var dropped bool
	transport.SetBeforeDeliver(func(msg QueuedReplicationMessage) {
		if dropped || msg.Forward == nil || msg.ToNodeID != "tail" {
			return
		}
		dropped = true
		transport.DropNext()
	})

	if _, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], transport, 6, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else if !errors.Is(err, ErrWriteTimeout) {
		t.Fatalf("error = %v, want write timeout", err)
	}

	if got, want := mustNodeCommittedSnapshot(t, nodes["head"], 6), map[string]string{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("head committed snapshot = %v, want %v", got, want)
	}
	if got, want := mustNodeStagedSequences(t, nodes["head"], 6), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("head staged sequences = %v, want %v", got, want)
	}
	if got, want := mustHighestCommitted(t, nodes["head"], 6), uint64(0); got != want {
		t.Fatalf("head highest committed = %d, want %d", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, nodes["tail"], 6), map[string]string{"k": "v"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail committed snapshot = %v, want %v", got, want)
	}
}

func TestFaultInjectedQueuedReplicaHistoryIsDeterministic(t *testing.T) {
	left := runQueuedReplicaFaultHistory(t)
	right := runQueuedReplicaFaultHistory(t)

	if !reflect.DeepEqual(left.finalStates, right.finalStates) {
		t.Fatalf("final states mismatch\nleft=%v\nright=%v", left.finalStates, right.finalStates)
	}
	if !reflect.DeepEqual(left.highestCommitted, right.highestCommitted) {
		t.Fatalf("highest committed mismatch\nleft=%v\nright=%v", left.highestCommitted, right.highestCommitted)
	}
	if !reflect.DeepEqual(left.staged, right.staged) {
		t.Fatalf("staged sequences mismatch\nleft=%v\nright=%v", left.staged, right.staged)
	}
	if !reflect.DeepEqual(left.bufferedForwards, right.bufferedForwards) {
		t.Fatalf("buffered forwards mismatch\nleft=%v\nright=%v", left.bufferedForwards, right.bufferedForwards)
	}
	if !reflect.DeepEqual(left.bufferedCommits, right.bufferedCommits) {
		t.Fatalf("buffered commits mismatch\nleft=%v\nright=%v", left.bufferedCommits, right.bufferedCommits)
	}
}

type queuedReplicaFaultHistory struct {
	finalStates      map[string]map[string]string
	highestCommitted map[string]uint64
	staged           map[string][]uint64
	bufferedForwards map[string][]uint64
	bufferedCommits  map[string][]uint64
}

func runQueuedReplicaFaultHistory(t *testing.T) queuedReplicaFaultHistory {
	t.Helper()
	ctx := context.Background()
	nodes, _, transport := setupActiveChainWithQueuedTransport(t, 9, []string{"head", "mid", "tail"})

	var duplicatedForward bool
	var duplicatedCommit bool
	transport.SetBeforeDeliver(func(msg QueuedReplicationMessage) {
		switch {
		case msg.Forward != nil && !duplicatedForward:
			duplicatedForward = true
			dup := cloneQueuedReplicationMessage(msg)
			transport.queue = append([]QueuedReplicationMessage{dup}, transport.queue...)
		case msg.Commit != nil && msg.ToNodeID == "head" && !duplicatedCommit:
			duplicatedCommit = true
			dup := cloneQueuedReplicationMessage(msg)
			transport.queue = append(transport.queue, dup)
		}
	})

	if result, err := submitPutWithQueuedDelivery(t, ctx, nodes["head"], transport, 9, "k", "v"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	} else {
		assertAppliedCommitResult(t, result, 9, 1)
	}
	if err := transport.DeliverAll(ctx); err != nil {
		t.Fatalf("DeliverAll returned error: %v", err)
	}

	history := queuedReplicaFaultHistory{
		finalStates:      map[string]map[string]string{},
		highestCommitted: map[string]uint64{},
		staged:           map[string][]uint64{},
		bufferedForwards: map[string][]uint64{},
		bufferedCommits:  map[string][]uint64{},
	}
	for _, nodeID := range []string{"head", "mid", "tail"} {
		history.finalStates[nodeID] = mustNodeCommittedSnapshot(t, nodes[nodeID], 9)
		history.highestCommitted[nodeID] = mustHighestCommitted(t, nodes[nodeID], 9)
		history.staged[nodeID] = mustNodeStagedSequences(t, nodes[nodeID], 9)
		history.bufferedForwards[nodeID] = mustBufferedForwardSequences(t, nodes[nodeID], 9)
		history.bufferedCommits[nodeID] = mustBufferedCommitSequences(t, nodes[nodeID], 9)
	}
	assertCommittedStateEqual(t, nodes, 9, map[string]string{"k": "v"}, 1)
	return history
}
