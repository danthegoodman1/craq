package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestOpenNodeHonorsCanceledContextDuringLoad(t *testing.T) {
	repl := NewInMemoryReplicationTransport()
	local := &blockingLocalStateStore{
		inner:     NewInMemoryLocalStateStore(),
		blockLoad: true,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := OpenNode(ctx, Config{NodeID: "node-a"}, NewInMemoryBackend(), local, NewInMemoryCoordinatorClient(), repl)
	if err == nil {
		t.Fatal("OpenNode unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
}

func TestMarkReplicaLeavingHonorsContextDuringPersist(t *testing.T) {
	ctx := context.Background()
	local := &blockingLocalStateStore{inner: NewInMemoryLocalStateStore()}
	node, err := OpenNode(ctx, Config{NodeID: "node-a"}, NewInMemoryBackend(), local, NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	mustActivateReplica(t, node, 1, ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle})

	local.blockUpsert = true
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = node.MarkReplicaLeaving(cancelCtx, MarkReplicaLeavingCommand{Slot: 1})
	if err == nil {
		t.Fatal("MarkReplicaLeaving unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if got, want := node.State().Replicas[1].State, ReplicaStateLeaving; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
}

func TestUpdateChainPeersHonorsContextDuringPersist(t *testing.T) {
	ctx := context.Background()
	local := &blockingLocalStateStore{inner: NewInMemoryLocalStateStore()}
	node, err := OpenNode(ctx, Config{NodeID: "node-a"}, NewInMemoryBackend(), local, NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	mustActivateReplica(t, node, 1, ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle})

	local.blockUpsert = true
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = node.UpdateChainPeers(cancelCtx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 2,
			Role:         ReplicaRoleSingle,
		},
	})
	if err == nil {
		t.Fatal("UpdateChainPeers unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if got, want := node.State().Replicas[1].Assignment.ChainVersion, uint64(2); got != want {
		t.Fatalf("chain version = %d, want %d", got, want)
	}
}

func TestHandleClientPutSingleReplicaReturnsAmbiguousOnPersistCancellation(t *testing.T) {
	ctx := context.Background()
	local := &blockingLocalStateStore{inner: NewInMemoryLocalStateStore()}
	node, err := OpenNode(ctx, Config{NodeID: "single"}, NewInMemoryBackend(), local, NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	mustActivateReplica(t, node, 4, ReplicaAssignment{Slot: 4, ChainVersion: 1, Role: ReplicaRoleSingle})

	local.blockUpsert = true
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = node.HandleClientPut(cancelCtx, ClientPutRequest{
		Slot:                 4,
		Key:                  "k",
		Value:                "v",
		ExpectedChainVersion: 1,
	})
	if err == nil {
		t.Fatal("HandleClientPut unexpectedly succeeded")
	}
	var ambiguous *AmbiguousWriteError
	if !errors.As(err, &ambiguous) {
		t.Fatalf("error = %v, want ambiguous write", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if !errors.Is(err, ErrAmbiguousWrite) {
		t.Fatalf("error = %v, want ErrAmbiguousWrite", err)
	}
	snapshot, snapErr := node.CommittedSnapshot(4)
	if snapErr != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", snapErr)
	}
	if got, want := snapshotValues(snapshot), map[string]string{"k": "v"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
}

func TestTailHandleForwardWriteHonorsContextDuringCommitPersist(t *testing.T) {
	ctx := context.Background()
	local := &blockingLocalStateStore{inner: NewInMemoryLocalStateStore()}
	repl := NewInMemoryReplicationTransport()
	node, err := OpenNode(ctx, Config{NodeID: "tail"}, NewInMemoryBackend(), local, NewInMemoryCoordinatorClient(), repl)
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	repl.RegisterNode("tail", node)
	mustActivateReplica(t, node, 6, ReplicaAssignment{
		Slot:         6,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
	})
	record := node.replicas[6]
	record.assignment.Role = ReplicaRoleTail
	record.assignment.Peers.PredecessorNodeID = "head"
	node.replicas[6] = record
	local.blockUpsert = true
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = node.HandleForwardWrite(cancelCtx, ForwardWriteRequest{
		Operation:    WriteOperation{Slot: 6, Sequence: 1, Kind: OperationKindPut, Key: "k", Value: "v", Metadata: testObjectMetadata(1)},
		FromNodeID:   "head",
		ChainVersion: 1,
	})
	if err == nil {
		t.Fatal("HandleForwardWrite unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	snapshot, snapErr := node.CommittedSnapshot(6)
	if snapErr != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", snapErr)
	}
	if got, want := snapshotValues(snapshot), map[string]string{"k": "v"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
}

func TestHandleCommitWriteHonorsContextDuringCommitPersist(t *testing.T) {
	ctx := context.Background()
	local := &blockingLocalStateStore{inner: NewInMemoryLocalStateStore()}
	node, err := OpenNode(ctx, Config{NodeID: "head"}, NewInMemoryBackend(), local, NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	mustActivateReplica(t, node, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})
	record := node.replicas[7]
	record = node.ensureProtocolState(record)
	op := WriteOperation{Slot: 7, Sequence: 1, Kind: OperationKindPut, Key: "k", Value: "v", Metadata: testObjectMetadata(1)}
	if err := node.stageOperation(op); err != nil {
		t.Fatalf("stageOperation returned error: %v", err)
	}
	record.stagedForwards[1] = ForwardWriteRequest{Operation: op, FromNodeID: "client", ChainVersion: 1}
	record.pendingWrites[1] = pendingWrite{}
	record.nextSequence = 2
	node.replicas[7] = record

	local.blockUpsert = true
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()

	err = node.HandleCommitWrite(cancelCtx, CommitWriteRequest{
		Slot:         7,
		Sequence:     1,
		FromNodeID:   "tail",
		ChainVersion: 1,
	})
	if err == nil {
		t.Fatal("HandleCommitWrite unexpectedly succeeded")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	snapshot, snapErr := node.CommittedSnapshot(7)
	if snapErr != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", snapErr)
	}
	if got, want := snapshotValues(snapshot), map[string]string{"k": "v"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed snapshot = %v, want %v", got, want)
	}
}

type blockingLocalStateStore struct {
	inner       *InMemoryLocalStateStore
	blockLoad   bool
	blockUpsert bool
	blockDelete bool
}

func (s *blockingLocalStateStore) LoadNode(ctx context.Context, nodeID string) (PersistedNodeState, error) {
	if s.blockLoad {
		<-ctx.Done()
		return PersistedNodeState{}, ctx.Err()
	}
	return s.inner.LoadNode(ctx, nodeID)
}

func (s *blockingLocalStateStore) UpsertReplica(ctx context.Context, nodeID string, replica PersistedReplica) error {
	if s.blockUpsert {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.inner.UpsertReplica(ctx, nodeID, replica)
}

func (s *blockingLocalStateStore) DeleteReplica(ctx context.Context, nodeID string, slot int) error {
	if s.blockDelete {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.inner.DeleteReplica(ctx, nodeID, slot)
}

func (s *blockingLocalStateStore) SetHighestAcceptedCoordinatorEpoch(ctx context.Context, nodeID string, epoch uint64) error {
	if s.blockUpsert {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.inner.SetHighestAcceptedCoordinatorEpoch(ctx, nodeID, epoch)
}

func (s *blockingLocalStateStore) Close() error {
	return s.inner.Close()
}
