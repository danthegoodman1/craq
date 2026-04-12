package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestWriteBackpressureRejectsConcurrentWriteOnSameSlot(t *testing.T) {
	ctx := context.Background()
	nodes, _, transport := newConfiguredQueuedNodes(t, map[string]Config{
		"head": {
			NodeID:                         "head",
			MaxInFlightClientWritesPerNode: 2,
			MaxInFlightClientWritesPerSlot: 1,
		},
		"tail": {NodeID: "tail"},
	})
	setupChainOnExistingNodes(t, nodes, 1, []string{"head", "tail"})

	var nestedErr error
	injected := false
	transport.SetBeforeDeliver(func(msg QueuedReplicationMessage) {
		if injected || msg.Forward == nil || msg.Forward.Operation.Slot != 1 || msg.ToNodeID != "tail" {
			return
		}
		injected = true
		_, nestedErr = nodes["head"].HandleClientPut(ctx, ClientPutRequest{
			Slot:                 1,
			Key:                  "k2",
			Value:                "v2",
			ExpectedChainVersion: 1,
		})
	})

	if _, err := handleClientPutWithQueuedDelivery(t, ctx, nodes["head"], transport, ClientPutRequest{
		Slot:                 1,
		Key:                  "k1",
		Value:                "v1",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("outer HandleClientPut returned error: %v", err)
	}
	assertWriteBackpressure(t, nestedErr, 1, 1)
	if got, want := nodes["head"].InFlightClientWrites(), 0; got != want {
		t.Fatalf("in-flight client writes = %d, want %d", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, nodes["tail"], 1), map[string]string{"k1": "v1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail committed snapshot = %v, want %v", got, want)
	}
}

func TestWriteBackpressureRejectsOtherSlotWhenNodeBudgetIsFull(t *testing.T) {
	ctx := context.Background()
	nodes, _, transport := newConfiguredQueuedNodes(t, map[string]Config{
		"head": {
			NodeID:                         "head",
			MaxInFlightClientWritesPerNode: 1,
			MaxInFlightClientWritesPerSlot: 1,
		},
		"tail-1": {NodeID: "tail-1"},
		"tail-2": {NodeID: "tail-2"},
	})
	setupChainOnExistingNodes(t, nodes, 1, []string{"head", "tail-1"})
	setupChainOnExistingNodes(t, nodes, 2, []string{"head", "tail-2"})

	var nestedErr error
	nestedErrCh := make(chan error, 1)
	injected := false
	transport.SetBeforeDeliver(func(msg QueuedReplicationMessage) {
		if injected || msg.Forward == nil || msg.Forward.Operation.Slot != 1 {
			return
		}
		injected = true
		go func() {
			_, err := runQueuedCommitResultWithDelivery(ctx, transport, func() (CommitResult, error) {
				return nodes["head"].HandleClientPut(ctx, ClientPutRequest{
					Slot:                 2,
					Key:                  "k2",
					Value:                "v2",
					ExpectedChainVersion: 1,
				})
			})
			nestedErrCh <- err
		}()
	})

	if _, err := handleClientPutWithQueuedDelivery(t, ctx, nodes["head"], transport, ClientPutRequest{
		Slot:                 1,
		Key:                  "k1",
		Value:                "v1",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("outer HandleClientPut returned error: %v", err)
	}
	nestedErr = <-nestedErrCh
	assertWriteBackpressure(t, nestedErr, 2, 1)
	if got, want := mustNodeCommittedSnapshot(t, nodes["tail-2"], 2), map[string]string{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slot 2 committed snapshot = %v, want %v", got, want)
	}
}

func TestWriteBackpressurePerSlotCapDoesNotStarveOtherSlots(t *testing.T) {
	ctx := context.Background()
	nodes, _, transport := newConfiguredQueuedNodes(t, map[string]Config{
		"head": {
			NodeID:                         "head",
			MaxInFlightClientWritesPerNode: 2,
			MaxInFlightClientWritesPerSlot: 1,
		},
		"tail-1": {NodeID: "tail-1"},
		"tail-2": {NodeID: "tail-2"},
	})
	setupChainOnExistingNodes(t, nodes, 1, []string{"head", "tail-1"})
	setupChainOnExistingNodes(t, nodes, 2, []string{"head", "tail-2"})

	var nestedErr error
	nestedErrCh := make(chan error, 1)
	injected := false
	transport.SetBeforeDeliver(func(msg QueuedReplicationMessage) {
		if injected || msg.Forward == nil || msg.Forward.Operation.Slot != 1 {
			return
		}
		injected = true
		go func() {
			_, err := runQueuedCommitResultWithDelivery(ctx, transport, func() (CommitResult, error) {
				return nodes["head"].HandleClientPut(ctx, ClientPutRequest{
					Slot:                 2,
					Key:                  "k2",
					Value:                "v2",
					ExpectedChainVersion: 1,
				})
			})
			nestedErrCh <- err
		}()
	})

	if _, err := handleClientPutWithQueuedDelivery(t, ctx, nodes["head"], transport, ClientPutRequest{
		Slot:                 1,
		Key:                  "k1",
		Value:                "v1",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("outer HandleClientPut returned error: %v", err)
	}
	nestedErr = <-nestedErrCh
	if nestedErr != nil {
		t.Fatalf("nested HandleClientPut returned error: %v", nestedErr)
	}
	if got, want := mustNodeCommittedSnapshot(t, nodes["tail-1"], 1), map[string]string{"k1": "v1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slot 1 committed snapshot = %v, want %v", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, nodes["tail-2"], 2), map[string]string{"k2": "v2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slot 2 committed snapshot = %v, want %v", got, want)
	}
	if got, want := nodes["head"].InFlightClientWrites(), 0; got != want {
		t.Fatalf("in-flight client writes = %d, want %d", got, want)
	}
}

func TestWriteAdmissionReleasesAfterAmbiguousTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	nodes, _, transport := newConfiguredQueuedNodes(t, map[string]Config{
		"head": {
			NodeID:                         "head",
			MaxInFlightClientWritesPerNode: 1,
			MaxInFlightClientWritesPerSlot: 1,
		},
		"tail": {NodeID: "tail"},
	})
	setupChainOnExistingNodes(t, nodes, 7, []string{"head", "tail"})

	canceled := false
	transport.SetBeforeDeliver(func(msg QueuedReplicationMessage) {
		if canceled || msg.Forward == nil || msg.Forward.Operation.Slot != 7 {
			return
		}
		canceled = true
		cancel()
	})

	_, err := handleClientPutWithQueuedDelivery(t, ctx, nodes["head"], transport, ClientPutRequest{
		Slot:                 7,
		Key:                  "k",
		Value:                "v",
		ExpectedChainVersion: 1,
	})
	if err == nil {
		t.Fatal("HandleClientPut unexpectedly succeeded")
	}
	if !errors.Is(err, ErrAmbiguousWrite) {
		t.Fatalf("error = %v, want ErrAmbiguousWrite", err)
	}
	if got, want := nodes["head"].InFlightClientWrites(), 0; got != want {
		t.Fatalf("in-flight client writes = %d, want %d", got, want)
	}
	if got, want := mustNodeStagedSequences(t, nodes["head"], 7), []uint64{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("staged sequences = %v, want %v", got, want)
	}
	if got, want := mustNodeCommittedSnapshot(t, nodes["tail"], 7), map[string]string{"k": "v"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail committed snapshot = %v, want %v", got, want)
	}
}

func TestReplicaBackpressureEnforcesNodeWideBufferedLimit(t *testing.T) {
	ctx := context.Background()
	transport := &scriptedTransport{}
	node := mustNewNode(t, ctx, Config{
		NodeID:                            "tail",
		MaxBufferedReplicaMessagesPerNode: 2,
		MaxBufferedReplicaMessagesPerSlot: 4,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 1, ReplicaAssignment{
		Slot:         1,
		ChainVersion: 1,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "pred-1"},
	})
	mustActivateReplica(t, node, 2, ReplicaAssignment{
		Slot:         2,
		ChainVersion: 1,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "pred-2"},
	})

	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:    WriteOperation{Slot: 1, Sequence: 2, Kind: OperationKindPut, Key: "k1", Value: "v1"},
		FromNodeID:   "pred-1",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("HandleForwardWrite(seq=2) returned error: %v", err)
	}
	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:    WriteOperation{Slot: 1, Sequence: 2, Kind: OperationKindPut, Key: "k1", Value: "v1"},
		FromNodeID:   "pred-1",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("duplicate HandleForwardWrite(seq=2) returned error: %v", err)
	}
	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:    WriteOperation{Slot: 1, Sequence: 3, Kind: OperationKindPut, Key: "k2", Value: "v2"},
		FromNodeID:   "pred-1",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("HandleForwardWrite(seq=3) returned error: %v", err)
	}
	if got, want := node.BufferedReplicaMessages(), 2; got != want {
		t.Fatalf("buffered replica messages = %d, want %d", got, want)
	}

	err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:    WriteOperation{Slot: 2, Sequence: 2, Kind: OperationKindPut, Key: "k3", Value: "v3"},
		FromNodeID:   "pred-2",
		ChainVersion: 1,
	})
	assertReplicaBackpressure(t, err, 2, 2)

	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:    WriteOperation{Slot: 1, Sequence: 1, Kind: OperationKindPut, Key: "k0", Value: "v0"},
		FromNodeID:   "pred-1",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("HandleForwardWrite(seq=1) returned error: %v", err)
	}
	if got, want := node.BufferedReplicaMessages(), 0; got != want {
		t.Fatalf("buffered replica messages after drain = %d, want %d", got, want)
	}
	if err := node.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation:    WriteOperation{Slot: 2, Sequence: 2, Kind: OperationKindPut, Key: "k3", Value: "v3"},
		FromNodeID:   "pred-2",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("HandleForwardWrite(slot=2, seq=2) returned error after drain: %v", err)
	}
}

func TestCatchupBackpressureRejectsConcurrentAddReplicaAsTail(t *testing.T) {
	ctx := context.Background()
	nodes, backends, transport := newConfiguredQueuedNodes(t, map[string]Config{
		"source": {NodeID: "source"},
		"target": {NodeID: "target", MaxConcurrentCatchups: 1},
	})
	mustActivateReplica(t, nodes["source"], 1, ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle})
	mustActivateReplica(t, nodes["source"], 2, ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle})
	if err := backends["source"].Put(1, "k1", "v1", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put(slot=1) returned error: %v", err)
	}
	if err := backends["source"].Put(2, "k2", "v2", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put(slot=2) returned error: %v", err)
	}

	var nestedErr error
	injected := false
	transport.SetBeforeFetchSnapshot(func(fromNodeID string, slot int) {
		if injected || fromNodeID != "source" || slot != 1 {
			return
		}
		injected = true
		nestedErr = nodes["target"].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{
				Slot:         2,
				ChainVersion: 1,
				Role:         ReplicaRoleTail,
				Peers:        ChainPeers{PredecessorNodeID: "source"},
			},
		})
	})

	if err := nodes["target"].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("outer AddReplicaAsTail returned error: %v", err)
	}
	assertCatchupBackpressure(t, nestedErr, 1)
	if got, want := nodes["target"].CatchupCount(), 0; got != want {
		t.Fatalf("catchup count = %d, want %d", got, want)
	}
	if _, exists := nodes["target"].State().Replicas[2]; exists {
		t.Fatal("slot 2 unexpectedly present after rejected add")
	}
	if err := nodes["target"].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         2,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("follow-up AddReplicaAsTail returned error: %v", err)
	}
}

func TestCatchupBackpressureSharedWithRecoverReplicaAndReleasesOnFailure(t *testing.T) {
	ctx := context.Background()
	nodes, backends, transport := newConfiguredQueuedNodes(t, map[string]Config{
		"source": {NodeID: "source"},
		"target": {NodeID: "target", MaxConcurrentCatchups: 1},
	})
	mustActivateReplica(t, nodes["source"], 1, ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle})
	mustActivateReplica(t, nodes["source"], 2, ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle})
	if err := backends["source"].Put(1, "k1", "v1", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put(slot=1) returned error: %v", err)
	}
	if err := backends["source"].Put(2, "k2", "v2", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put(slot=2) returned error: %v", err)
	}

	var nestedErr error
	injected := false
	transport.SetBeforeFetchSnapshot(func(fromNodeID string, slot int) {
		if injected || fromNodeID != "source" || slot != 1 {
			return
		}
		injected = true
		nestedErr = nodes["target"].RecoverReplica(ctx, RecoverReplicaCommand{
			Assignment:   ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle},
			SourceNodeID: "source",
		})
	})

	if err := nodes["target"].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("outer AddReplicaAsTail returned error: %v", err)
	}
	assertCatchupBackpressure(t, nestedErr, 1)
	if err := nodes["target"].RecoverReplica(ctx, RecoverReplicaCommand{
		Assignment:   ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle},
		SourceNodeID: "missing",
	}); err == nil {
		t.Fatal("RecoverReplica unexpectedly succeeded with missing source")
	} else if !errors.Is(err, ErrSnapshotSourceUnavailable) {
		t.Fatalf("RecoverReplica error = %v, want snapshot source unavailable", err)
	}
	if got, want := nodes["target"].CatchupCount(), 0; got != want {
		t.Fatalf("catchup count after failed recover = %d, want %d", got, want)
	}
	if err := nodes["target"].RecoverReplica(ctx, RecoverReplicaCommand{
		Assignment:   ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle},
		SourceNodeID: "source",
	}); err != nil {
		t.Fatalf("follow-up RecoverReplica returned error: %v", err)
	}
}

func newConfiguredQueuedNodes(
	t *testing.T,
	configs map[string]Config,
) (map[string]*Node, map[string]*InMemoryBackend, *QueuedInMemoryReplicationTransport) {
	t.Helper()
	ctx := context.Background()
	transport := NewQueuedInMemoryReplicationTransport()
	nodes := make(map[string]*Node, len(configs))
	backends := make(map[string]*InMemoryBackend, len(configs))
	for nodeID, cfg := range configs {
		if cfg.NodeID == "" {
			cfg.NodeID = nodeID
		}
		backend := NewInMemoryBackend()
		backends[nodeID] = backend
		transport.Register(nodeID, backend)
		node := mustNewNode(t, ctx, cfg, backend, NewInMemoryCoordinatorClient(), transport)
		nodes[nodeID] = node
		transport.RegisterNode(nodeID, node)
	}
	return nodes, backends, transport
}

func assertWriteBackpressure(t *testing.T, err error, wantSlot int, wantLimit int) {
	t.Helper()
	if err == nil {
		t.Fatal("expected write backpressure error")
	}
	if !errors.Is(err, ErrWriteBackpressure) {
		t.Fatalf("error = %v, want ErrWriteBackpressure", err)
	}
	var pressure *BackpressureError
	if !errors.As(err, &pressure) {
		t.Fatalf("error = %v, want BackpressureError", err)
	}
	if got, want := pressure.Resource, BackpressureResourceClientWrite; got != want {
		t.Fatalf("resource = %q, want %q", got, want)
	}
	if got, want := pressure.Slot, wantSlot; got != want {
		t.Fatalf("slot = %d, want %d", got, want)
	}
	if got, want := pressure.Limit, wantLimit; got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}

func assertReplicaBackpressure(t *testing.T, err error, wantSlot int, wantLimit int) {
	t.Helper()
	if err == nil {
		t.Fatal("expected replica backpressure error")
	}
	if !errors.Is(err, ErrReplicaBackpressure) {
		t.Fatalf("error = %v, want ErrReplicaBackpressure", err)
	}
	if !errors.Is(err, ErrBufferedMessageLimit) {
		t.Fatalf("error = %v, want ErrBufferedMessageLimit alias", err)
	}
	var pressure *BackpressureError
	if !errors.As(err, &pressure) {
		t.Fatalf("error = %v, want BackpressureError", err)
	}
	if got, want := pressure.Resource, BackpressureResourceReplicaBuffer; got != want {
		t.Fatalf("resource = %q, want %q", got, want)
	}
	if got, want := pressure.Slot, wantSlot; got != want {
		t.Fatalf("slot = %d, want %d", got, want)
	}
	if got, want := pressure.Limit, wantLimit; got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}

func assertCatchupBackpressure(t *testing.T, err error, wantLimit int) {
	t.Helper()
	if err == nil {
		t.Fatal("expected catchup backpressure error")
	}
	if !errors.Is(err, ErrCatchupBackpressure) {
		t.Fatalf("error = %v, want ErrCatchupBackpressure", err)
	}
	var pressure *BackpressureError
	if !errors.As(err, &pressure) {
		t.Fatalf("error = %v, want BackpressureError", err)
	}
	if got, want := pressure.Resource, BackpressureResourceCatchup; got != want {
		t.Fatalf("resource = %q, want %q", got, want)
	}
	if got, want := pressure.Limit, wantLimit; got != want {
		t.Fatalf("limit = %d, want %d", got, want)
	}
}
