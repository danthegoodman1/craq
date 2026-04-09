package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSubmitPutTimesOutAndPreservesInFlightState(t *testing.T) {
	ctx := context.Background()
	transport := &blockingWriteTransport{}
	node := mustNewNode(t, ctx, Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Nanosecond,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})

	if _, err := node.SubmitPut(ctx, 7, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want write timeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	}

	if got, want := mustNodeStagedSequences(t, node, 7), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences = %v, want %v", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, node, 7), map[string]string{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
	if got, want := mustHighestCommitted(t, node, 7), uint64(0); got != want {
		t.Fatalf("highest committed = %d, want %d", got, want)
	}
	record := node.replicas[7]
	if _, ok := record.pendingWrites[1]; !ok {
		t.Fatal("pending write missing after timeout")
	}
}

func TestSubmitPutRespectsCallerCancellationBeforeDefaultTimeout(t *testing.T) {
	transport := &blockingWriteTransport{}
	node := mustNewNode(t, context.Background(), Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Hour,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.SubmitPut(ctx, 7, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want write timeout", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want canceled", err)
		}
	}
}

func TestSubmitPutUsesTighterCallerDeadline(t *testing.T) {
	transport := &blockingWriteTransport{}
	node := mustNewNode(t, context.Background(), Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Hour,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	if _, err := node.SubmitPut(ctx, 7, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want write timeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	}
}

func TestQueuedAwaitWriteCommitHonorsCanceledContext(t *testing.T) {
	transport := NewQueuedInMemoryReplicationTransport()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := transport.AwaitWriteCommit(ctx, func() bool { return false }); err == nil {
		t.Fatal("AwaitWriteCommit unexpectedly succeeded")
	} else if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want canceled", err)
	}
}

func TestSubmitPutTimesOutWithoutAwaitWriteCommitTransport(t *testing.T) {
	ctx := context.Background()
	transport := newAsyncWriteTransport()
	head := mustNewNode(t, ctx, Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Nanosecond,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	tail := mustNewNode(t, ctx, Config{
		NodeID: "tail",
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	transport.RegisterNode("head", head)
	transport.RegisterNode("tail", tail)
	mustActivateReplica(t, head, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})
	mustActivateReplica(t, tail, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "head"},
	})

	// This transport returns from ForwardWrite before the tail processes the
	// write and does not implement AwaitWriteCommit, matching the transport
	// shape that previously fell through to ErrStateMismatch.
	if _, err := head.SubmitPut(ctx, 7, "k", "v"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want write timeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	}

	if got, want := mustNodeStagedSequences(t, head, 7), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences = %v, want %v", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, head, 7), map[string]string{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
	if got, want := mustHighestCommitted(t, head, 7), uint64(0); got != want {
		t.Fatalf("highest committed = %d, want %d", got, want)
	}
}

func TestSubmitPutWaitsForDelayedCommitWithoutAwaitWriteCommitTransport(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	transport := newAsyncWriteTransport()
	head := mustNewNode(t, ctx, Config{
		NodeID:             "head",
		WriteCommitTimeout: 50 * time.Millisecond,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	tail := mustNewNode(t, ctx, Config{
		NodeID: "tail",
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	transport.RegisterNode("head", head)
	transport.RegisterNode("tail", tail)
	mustActivateReplica(t, head, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})
	mustActivateReplica(t, tail, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "head"},
	})

	deliverErrCh := make(chan error, 1)
	go func() {
		deliverErrCh <- transport.DeliverNextForward(ctx)
	}()

	// The fallback poll should observe the delayed local commit and return a
	// normal success once the tail forwards the commit back to the head.
	result, err := head.SubmitPut(ctx, 7, "k", "v")
	if err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	if err := <-deliverErrCh; err != nil {
		t.Fatalf("DeliverNextForward returned error: %v", err)
	}
	assertAppliedCommitResult(t, result, 7, 1)
	assertCommittedStateEqual(t, map[string]*Node{
		"head": head,
		"tail": tail,
	}, 7, map[string]string{"k": "v"}, 1)
}

type blockingWriteTransport struct{}

func (t *blockingWriteTransport) FetchSnapshot(context.Context, string, int) (Snapshot, uint64, error) {
	return Snapshot{}, 0, nil
}

func (t *blockingWriteTransport) FetchCommittedSequence(context.Context, string, int) (uint64, error) {
	return 0, nil
}

func (t *blockingWriteTransport) ForwardWrite(context.Context, string, ForwardWriteRequest) error {
	return nil
}

func (t *blockingWriteTransport) CommitWrite(context.Context, string, CommitWriteRequest) error {
	return nil
}

func (t *blockingWriteTransport) AwaitWriteCommit(ctx context.Context, check func() bool) error {
	if check() {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

type asyncWriteTransport struct {
	nodes    map[string]replicationHandler
	forwards chan asyncForward
}

type asyncForward struct {
	toNodeID string
	req      ForwardWriteRequest
}

func newAsyncWriteTransport() *asyncWriteTransport {
	return &asyncWriteTransport{
		nodes:    map[string]replicationHandler{},
		forwards: make(chan asyncForward, 16),
	}
}

func (t *asyncWriteTransport) RegisterNode(nodeID string, node replicationHandler) {
	t.nodes[nodeID] = node
}

func (t *asyncWriteTransport) FetchSnapshot(context.Context, string, int) (Snapshot, uint64, error) {
	return Snapshot{}, 0, nil
}

func (t *asyncWriteTransport) FetchCommittedSequence(context.Context, string, int) (uint64, error) {
	return 0, nil
}

func (t *asyncWriteTransport) ForwardWrite(ctx context.Context, toNodeID string, req ForwardWriteRequest) error {
	if _, ok := t.nodes[toNodeID]; !ok {
		return ErrSnapshotSourceUnavailable
	}
	cloned := req
	cloned.Operation = cloneWriteOperation(req.Operation)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case t.forwards <- asyncForward{toNodeID: toNodeID, req: cloned}:
		return nil
	}
}

func (t *asyncWriteTransport) CommitWrite(ctx context.Context, toNodeID string, req CommitWriteRequest) error {
	node, ok := t.nodes[toNodeID]
	if !ok {
		return ErrSnapshotSourceUnavailable
	}
	return node.HandleCommitWrite(ctx, req)
}

func (t *asyncWriteTransport) DeliverNextForward(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case msg := <-t.forwards:
		node, ok := t.nodes[msg.toNodeID]
		if !ok {
			return ErrSnapshotSourceUnavailable
		}
		return node.HandleForwardWrite(ctx, msg.req)
	}
}
