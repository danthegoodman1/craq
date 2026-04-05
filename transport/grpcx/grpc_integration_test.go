package grpcx_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/client"
	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

func TestClientTransportPutGetDeleteOverGRPC(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	node := mustSingleReplicaNode(t, ctx, "node-a", storage.Config{NodeID: "node-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 7, 1)
	server, address := mustStartStorageServer(t, node)
	defer func() { _ = server.Close() }()

	transport := grpcx.NewClientTransport(pool)
	if _, err := transport.Put(ctx, address, storage.ClientPutRequest{
		Slot:                 7,
		Key:                  "alpha",
		Value:                "one",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	result, err := transport.Get(ctx, address, storage.ClientGetRequest{
		Slot:                 7,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !result.Found || result.Value != "one" {
		t.Fatalf("Get result = %#v, want found value", result)
	}
	if _, err := transport.Delete(ctx, address, storage.ClientDeleteRequest{
		Slot:                 7,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	result, err = transport.Get(ctx, address, storage.ClientGetRequest{
		Slot:                 7,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("Get after delete returned error: %v", err)
	}
	if result.Found {
		t.Fatalf("Get after delete = %#v, want not found", result)
	}
}

func TestClientTransportReturnsTypedErrorsOverGRPC(t *testing.T) {
	t.Run("routing mismatch", func(t *testing.T) {
		ctx := context.Background()
		pool := grpcx.NewConnPool()
		t.Cleanup(func() { _ = pool.Close() })

		node := mustSingleReplicaNode(t, ctx, "node-a", storage.Config{NodeID: "node-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 3, 2)
		server, address := mustStartStorageServer(t, node)
		defer func() { _ = server.Close() }()

		transport := grpcx.NewClientTransport(pool)
		_, err := transport.Put(ctx, address, storage.ClientPutRequest{
			Slot:                 3,
			Key:                  "alpha",
			Value:                "one",
			ExpectedChainVersion: 1,
		})
		var mismatch *storage.RoutingMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("Put error = %v, want routing mismatch", err)
		}
		if mismatch.CurrentChainVersion != 2 {
			t.Fatalf("routing mismatch = %#v, want current version 2", mismatch)
		}
	})

	t.Run("ambiguous write", func(t *testing.T) {
		ctx := context.Background()
		pool := grpcx.NewConnPool()
		t.Cleanup(func() { _ = pool.Close() })

		repl := &blockingTransport{}
		head := mustOpenNode(t, ctx, storage.Config{NodeID: "head", WriteCommitTimeout: 2 * time.Millisecond}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), repl)
		mustActivateReplica(t, head, storage.ReplicaAssignment{
			Slot:         4,
			ChainVersion: 1,
			Role:         storage.ReplicaRoleHead,
			Peers:        storage.ChainPeers{SuccessorNodeID: "tail"},
		})
		server, address := mustStartStorageServer(t, head)
		defer func() { _ = server.Close() }()

		transport := grpcx.NewClientTransport(pool)
		_, err := transport.Put(ctx, address, storage.ClientPutRequest{
			Slot:                 4,
			Key:                  "alpha",
			Value:                "one",
			ExpectedChainVersion: 1,
		})
		var ambiguous *storage.AmbiguousWriteError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("Put error = %v, want ambiguous write", err)
		}
		if !errors.Is(err, storage.ErrAmbiguousWrite) {
			t.Fatalf("Put error = %v, want ErrAmbiguousWrite", err)
		}
	})

	t.Run("backpressure", func(t *testing.T) {
		ctx := context.Background()
		pool := grpcx.NewConnPool()
		t.Cleanup(func() { _ = pool.Close() })

		repl := &blockingTransport{}
		head := mustOpenNode(t, ctx, storage.Config{
			NodeID:                         "head",
			MaxInFlightClientWritesPerNode: 1,
			WriteCommitTimeout:             50 * time.Millisecond,
		}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), repl)
		mustActivateReplica(t, head, storage.ReplicaAssignment{
			Slot:         5,
			ChainVersion: 1,
			Role:         storage.ReplicaRoleHead,
			Peers:        storage.ChainPeers{SuccessorNodeID: "tail"},
		})
		server, address := mustStartStorageServer(t, head)
		defer func() { _ = server.Close() }()

		transport := grpcx.NewClientTransport(pool)
		firstDone := make(chan error, 1)
		go func() {
			_, err := transport.Put(ctx, address, storage.ClientPutRequest{
				Slot:                 5,
				Key:                  "alpha",
				Value:                "one",
				ExpectedChainVersion: 1,
			})
			firstDone <- err
		}()
		waitFor(t, func() bool { return head.InFlightClientWrites() == 1 })
		_, err := transport.Put(ctx, address, storage.ClientPutRequest{
			Slot:                 5,
			Key:                  "beta",
			Value:                "two",
			ExpectedChainVersion: 1,
		})
		var pressure *storage.BackpressureError
		if !errors.As(err, &pressure) {
			t.Fatalf("second Put error = %v, want backpressure", err)
		}
		if pressure.Resource != storage.BackpressureResourceClientWrite {
			t.Fatalf("backpressure = %#v, want client write resource", pressure)
		}
		if err := <-firstDone; err == nil {
			t.Fatal("first Put unexpectedly succeeded")
		}
	})

	t.Run("condition failed", func(t *testing.T) {
		ctx := context.Background()
		pool := grpcx.NewConnPool()
		t.Cleanup(func() { _ = pool.Close() })

		node := mustSingleReplicaNode(t, ctx, "node-a", storage.Config{NodeID: "node-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 6, 1)
		server, address := mustStartStorageServer(t, node)
		defer func() { _ = server.Close() }()

		transport := grpcx.NewClientTransport(pool)
		if _, err := transport.Put(ctx, address, storage.ClientPutRequest{
			Slot:                 6,
			Key:                  "alpha",
			Value:                "one",
			ExpectedChainVersion: 1,
		}); err != nil {
			t.Fatalf("initial Put returned error: %v", err)
		}
		existsFalse := false
		_, err := transport.Put(ctx, address, storage.ClientPutRequest{
			Slot:                 6,
			Key:                  "alpha",
			Value:                "two",
			ExpectedChainVersion: 1,
			Conditions: storage.WriteConditions{
				Exists: &existsFalse,
			},
		})
		var failed *storage.ConditionFailedError
		if !errors.As(err, &failed) {
			t.Fatalf("second Put error = %v, want condition failed", err)
		}
		if !failed.CurrentExists || failed.CurrentMetadata == nil || failed.CurrentMetadata.Version != 1 {
			t.Fatalf("condition failure = %#v, want current version 1", failed)
		}
	})
}

func TestDynamicAutoJoinIdentityAndTombstoneSafetyOverFactoryBackedGRPC(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	coordSvc, coordAddr, coordImpl := mustStartCoordinator(t, ctx, coordruntime.NewInMemoryStore(), pool)
	defer func() { _ = coordSvc.Close() }()

	admin := grpcx.NewCoordinatorAdminClient(coordAddr, pool)
	reporterN1 := grpcx.NewCoordinatorReporterClient("n1", coordAddr, pool)
	reporterN2 := grpcx.NewCoordinatorReporterClient("n2", coordAddr, pool)

	if _, err := admin.Bootstrap(ctx, coordruntime.Command{
		ID:              "bootstrap",
		ExpectedVersion: 0,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
			Nodes: []coordinator.Node{{
				ID:         "seed",
				RPCAddress: "127.0.0.1:7400",
			}},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	regN1 := storage.NodeRegistration{
		NodeID:         "n1",
		RPCAddress:     "127.0.0.1:7411",
		FailureDomains: map[string]string{"host": "h1"},
	}
	if err := reporterN1.RegisterNode(ctx, regN1); err != nil {
		t.Fatalf("RegisterNode(n1) returned error: %v", err)
	}
	if err := reporterN1.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat(n1) returned error: %v", err)
	}
	if !coordImpl.Current().Cluster.ReadyNodeIDs["n1"] {
		t.Fatalf("n1 not marked ready after heartbeat: %#v", coordImpl.Current().Cluster.ReadyNodeIDs)
	}

	if err := reporterN1.RegisterNode(ctx, storage.NodeRegistration{
		NodeID:         "n1",
		RPCAddress:     "127.0.0.1:7999",
		FailureDomains: map[string]string{"host": "h1"},
	}); err == nil {
		t.Fatal("conflicting RegisterNode(n1) unexpectedly succeeded")
	}

	current := coordImpl.Current()
	if _, err := admin.MarkNodeDead(ctx, coordruntime.Command{
		ID:              "dead-n1",
		ExpectedVersion: current.Version,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: []coordinator.Event{{
				Kind:   coordinator.EventKindMarkNodeDead,
				NodeID: "n1",
			}},
		},
	}); err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if err := reporterN1.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 1}); err == nil {
		t.Fatal("ReportNodeHeartbeat(n1) unexpectedly succeeded after tombstone")
	}

	if err := reporterN2.RegisterNode(ctx, storage.NodeRegistration{
		NodeID:         "n2",
		RPCAddress:     "127.0.0.1:7412",
		FailureDomains: map[string]string{"host": "h2"},
	}); err != nil {
		t.Fatalf("RegisterNode(n2) returned error: %v", err)
	}
	if err := reporterN2.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n2", ReplicaCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat(n2) returned error: %v", err)
	}
	if !coordImpl.Current().Cluster.ReadyNodeIDs["n2"] {
		t.Fatalf("n2 not marked ready after replacement join: %#v", coordImpl.Current().Cluster.ReadyNodeIDs)
	}
}

func TestDuplicateSameIdentityRegistrationOverFactoryBackedGRPCIsNoOp(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	coordSvc, coordAddr, coordImpl := mustStartCoordinator(t, ctx, coordruntime.NewInMemoryStore(), pool)
	defer func() { _ = coordSvc.Close() }()

	admin := grpcx.NewCoordinatorAdminClient(coordAddr, pool)
	reporter := grpcx.NewCoordinatorReporterClient("n1", coordAddr, pool)

	if _, err := admin.Bootstrap(ctx, coordruntime.Command{
		ID:              "bootstrap",
		ExpectedVersion: 0,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	reg := storage.NodeRegistration{
		NodeID:         "n1",
		RPCAddress:     "127.0.0.1:7411",
		FailureDomains: map[string]string{"host": "h1"},
	}
	if err := reporter.RegisterNode(ctx, reg); err != nil {
		t.Fatalf("RegisterNode(n1) returned error: %v", err)
	}
	if err := reporter.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat(n1) returned error: %v", err)
	}

	before := coordImpl.Current()
	beforePending := coordImpl.Pending()
	beforeRouting, err := admin.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot before duplicate RegisterNode returned error: %v", err)
	}
	if err := reporter.RegisterNode(ctx, reg); err != nil {
		t.Fatalf("duplicate RegisterNode(n1) returned error: %v", err)
	}
	afterRouting, err := admin.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after duplicate RegisterNode returned error: %v", err)
	}
	if got := coordImpl.Current(); !reflect.DeepEqual(got, before) {
		t.Fatalf("coordinator state changed on duplicate RegisterNode over gRPC\ngot=%#v\nwant=%#v", got, before)
	}
	if got := coordImpl.Pending(); !reflect.DeepEqual(got, beforePending) {
		t.Fatalf("pending changed on duplicate RegisterNode over gRPC\ngot=%#v\nwant=%#v", got, beforePending)
	}
	if !reflect.DeepEqual(afterRouting, beforeRouting) {
		t.Fatalf("routing changed on duplicate RegisterNode over gRPC\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
	}
	if err := reporter.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 1}); err != nil {
		t.Fatalf("second ReportNodeHeartbeat(n1) returned error: %v", err)
	}
	if got, want := len(coordImpl.Current().Cluster.NodesByID), 1; got != want {
		t.Fatalf("membership size after duplicate RegisterNode = %d, want %d", got, want)
	}
	if !coordImpl.Current().Cluster.ReadyNodeIDs["n1"] {
		t.Fatalf("n1 not marked ready after duplicate RegisterNode and heartbeat: %#v", coordImpl.Current().Cluster.ReadyNodeIDs)
	}
	if got := coordImpl.Current().Cluster.NodesByID["n1"]; !reflect.DeepEqual(got, before.Cluster.NodesByID["n1"]) {
		t.Fatalf("node identity changed after duplicate RegisterNode over gRPC\ngot=%#v\nwant=%#v", got, before.Cluster.NodesByID["n1"])
	}
}

func TestReplicationTransportOverGRPC(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	repl := grpcx.NewReplicationTransport(pool)

	source := mustSingleReplicaNode(t, ctx, "source", storage.Config{NodeID: "source"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 9, 1)
	if _, err := source.HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 9,
		Key:                  "alpha",
		Value:                "one",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("source Put returned error: %v", err)
	}
	sourceServer, sourceAddr := mustStartStorageServer(t, source)
	defer func() { _ = sourceServer.Close() }()

	target := mustOpenNode(t, ctx, storage.Config{NodeID: "target"}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), repl)
	targetServer, targetAddr := mustStartStorageServer(t, target)
	defer func() { _ = targetServer.Close() }()

	if err := target.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{
			Slot:         9,
			ChainVersion: 2,
			Role:         storage.ReplicaRoleTail,
			Peers:        storage.ChainPeers{PredecessorNodeID: "source", PredecessorTarget: sourceAddr},
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := target.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	snapshot, err := target.CommittedSnapshot(9)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	if got := snapshot["alpha"]; got.Value != "one" {
		t.Fatalf("target snapshot alpha = %q, want one", got.Value)
	}

	head := mustOpenNode(t, ctx, storage.Config{NodeID: "head"}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), repl)
	mustActivateReplica(t, head, storage.ReplicaAssignment{
		Slot:         11,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleHead,
		Peers:        storage.ChainPeers{SuccessorNodeID: "target", SuccessorTarget: targetAddr},
	})
	headServer, headAddr := mustStartStorageServer(t, head)
	defer func() { _ = headServer.Close() }()
	mustActivateReplica(t, target, storage.ReplicaAssignment{
		Slot:         11,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleTail,
		Peers:        storage.ChainPeers{PredecessorNodeID: "head", PredecessorTarget: headAddr},
	})

	if err := target.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{Assignment: storage.ReplicaAssignment{
		Slot:         11,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleTail,
		Peers:        storage.ChainPeers{PredecessorNodeID: "head", PredecessorTarget: headAddr},
	}}); err != nil {
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}
	if _, err := head.HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 11,
		Key:                  "beta",
		Value:                "two",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("head Put returned error: %v", err)
	}
	snapshot, err = target.CommittedSnapshot(11)
	if err != nil {
		t.Fatalf("target CommittedSnapshot returned error: %v", err)
	}
	if got := snapshot["beta"]; got.Value != "two" {
		t.Fatalf("target snapshot beta = %q, want two", got.Value)
	}
}

func TestClientTransportCRAQReadsOverGRPCFromNonTailReplica(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	chain := mustStartQueuedCRAQGRPCChain(t, ctx, 18)
	transport := grpcx.NewClientTransport(pool)

	if _, err := chain.nodes["head"].SubmitPut(ctx, 18, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	queueDirtyCommittedMiddleWrite(t, ctx, chain, 18, "alpha", "v2", 2)

	linearizable, err := transport.Get(ctx, chain.addresses["mid"], storage.ClientGetRequest{
		Slot:                 18,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("linearizable Get returned error: %v", err)
	}
	if !linearizable.Found || linearizable.Value != "v2" || linearizable.Metadata == nil || linearizable.Metadata.Version != 2 {
		t.Fatalf("linearizable Get result = %#v, want value v2 version 2", linearizable)
	}

	relaxed, err := transport.Get(ctx, chain.addresses["mid"], storage.ClientGetRequest{
		Slot:                 18,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
		Consistency:          storage.ReadConsistencyLocalCommitted,
	})
	if err != nil {
		t.Fatalf("local committed Get returned error: %v", err)
	}
	if !relaxed.Found || relaxed.Value != "v1" || relaxed.Metadata == nil || relaxed.Metadata.Version != 1 {
		t.Fatalf("local committed Get result = %#v, want value v1 version 1", relaxed)
	}
}

func TestClientTransportReturnsReadDependencyErrorOverGRPCForDirtyNonTailRead(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	chain := mustStartQueuedCRAQGRPCChain(t, ctx, 19)
	transport := grpcx.NewClientTransport(pool)

	if _, err := chain.nodes["head"].SubmitPut(ctx, 19, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	queueDirtyCommittedMiddleWrite(t, ctx, chain, 19, "alpha", "v2", 2)
	misconfigureCRAQReadDependency(t, chain.nodes["mid"], 19, "missing-tail")

	_, err := transport.Get(ctx, chain.addresses["mid"], storage.ClientGetRequest{
		Slot:                 19,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	})
	if err == nil {
		t.Fatal("Get unexpectedly succeeded")
	}
	var dependency *storage.ReadDependencyError
	if !errors.As(err, &dependency) {
		t.Fatalf("Get error = %v, want ReadDependencyError", err)
	}
	if !errors.Is(err, storage.ErrReadDependencyUnavailable) {
		t.Fatalf("Get error = %v, want ErrReadDependencyUnavailable", err)
	}
	if got, want := dependency.TailNodeID, "missing-tail"; got != want {
		t.Fatalf("TailNodeID = %q, want %q", got, want)
	}
}

func TestCoordinatorDispatchAndRoutingOverGRPC(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	coordStore := coordruntime.NewInMemoryStore()
	coordSvc, coordAddr, coordImpl := mustStartCoordinator(t, ctx, coordStore, pool)
	defer func() { _ = coordSvc.Close() }()
	admin := grpcx.NewCoordinatorAdminClient(coordAddr, pool)

	nodeA := coordinator.Node{ID: "a", RPCAddress: mustReserveAddress(t), FailureDomains: map[string]string{"rack": "r1", "az": "az1"}}
	nodeC := coordinator.Node{ID: "c", RPCAddress: mustReserveAddress(t), FailureDomains: map[string]string{"rack": "r3", "az": "az3"}}

	a := mustSingleReplicaNode(t, ctx, "a", storage.Config{NodeID: "a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 0, 1)
	aServer := mustStartStorageServerAt(t, a, nodeA.RPCAddress)
	defer func() { _ = aServer.Close() }()

	c := mustOpenNode(t, ctx, storage.Config{NodeID: "c"}, storage.NewInMemoryBackend(), grpcx.NewCoordinatorReporterClient("c", coordAddr, pool), grpcx.NewReplicationTransport(pool))
	cServer := mustStartStorageServerAt(t, c, nodeC.RPCAddress)
	defer func() { _ = cServer.Close() }()

	if _, err := admin.Bootstrap(ctx, coordruntime.Command{
		ID:   "bootstrap",
		Kind: coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
			Nodes:  []coordinator.Node{nodeA, nodeC},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	if _, err := admin.BeginDrainNode(ctx, coordruntime.Command{
		ID:              "drain-a",
		ExpectedVersion: 1,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: []coordinator.Event{{
				Kind:   coordinator.EventKindBeginDrainNode,
				NodeID: "a",
			}},
		},
	}); err != nil {
		t.Fatalf("BeginDrainNode returned error: %v", err)
	}

	state := c.State()
	replica, ok := state.Replicas[0]
	if !ok {
		t.Fatalf("node c state = %#v, want slot 0 replica", state)
	}
	if replica.State != storage.ReplicaStateCatchingUp {
		t.Fatalf("node c replica state = %q, want catching_up", replica.State)
	}

	cClient := grpcx.NewStorageNodeClient(nodeC.RPCAddress, pool)
	if err := cClient.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	snapshot, err := admin.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if snapshot.Slots[0].HeadEndpoint == "" || snapshot.Slots[0].TailEndpoint == "" {
		t.Fatalf("routing snapshot = %#v, want embedded endpoints", snapshot)
	}
	if snapshot.Slots[0].HeadEndpoint != nodeA.RPCAddress && snapshot.Slots[0].HeadEndpoint != nodeC.RPCAddress {
		t.Fatalf("head endpoint = %q, want one of %q or %q", snapshot.Slots[0].HeadEndpoint, nodeA.RPCAddress, nodeC.RPCAddress)
	}
	if coordImpl.Current().Cluster.NodesByID["a"].RPCAddress != nodeA.RPCAddress {
		t.Fatalf("coordinator node a address = %q, want %q", coordImpl.Current().Cluster.NodesByID["a"].RPCAddress, nodeA.RPCAddress)
	}
	if coordImpl.Current().Cluster.NodesByID["c"].RPCAddress != nodeC.RPCAddress {
		t.Fatalf("coordinator node c address = %q, want %q", coordImpl.Current().Cluster.NodesByID["c"].RPCAddress, nodeC.RPCAddress)
	}

	router, err := client.NewRouter(admin, grpcx.NewClientTransport(pool))
	if err != nil {
		t.Fatalf("NewRouter returned error: %v", err)
	}
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
}

type queuedCRAQGRPCChain struct {
	nodes     map[string]*storage.Node
	addresses map[string]string
	repl      *storage.QueuedInMemoryReplicationTransport
}

func mustStartQueuedCRAQGRPCChain(t *testing.T, ctx context.Context, slot int) queuedCRAQGRPCChain {
	t.Helper()
	repl := storage.NewQueuedInMemoryReplicationTransport()
	nodes := map[string]*storage.Node{}
	addresses := map[string]string{}
	for _, nodeID := range []string{"head", "mid", "tail"} {
		backend := storage.NewInMemoryBackend()
		node := mustOpenNode(t, ctx, storage.Config{NodeID: nodeID}, backend, storage.NewInMemoryCoordinatorClient(), repl)
		repl.Register(nodeID, backend)
		repl.RegisterNode(nodeID, node)
		address := mustReserveAddress(t)
		server := mustStartStorageServerAt(t, node, address)
		t.Cleanup(func() { _ = server.Close() })
		nodes[nodeID] = node
		addresses[nodeID] = address
	}

	assignments := map[string]storage.ReplicaAssignment{
		"head": {
			Slot:         slot,
			ChainVersion: 1,
			Role:         storage.ReplicaRoleHead,
			Peers: storage.ChainPeers{
				SuccessorNodeID: "mid",
				SuccessorTarget: "mid",
				TailNodeID:      "tail",
				TailTarget:      "tail",
			},
		},
		"mid": {
			Slot:         slot,
			ChainVersion: 1,
			Role:         storage.ReplicaRoleMiddle,
			Peers: storage.ChainPeers{
				PredecessorNodeID: "head",
				PredecessorTarget: "head",
				SuccessorNodeID:   "tail",
				SuccessorTarget:   "tail",
				TailNodeID:        "tail",
				TailTarget:        "tail",
			},
		},
		"tail": {
			Slot:         slot,
			ChainVersion: 1,
			Role:         storage.ReplicaRoleTail,
			Peers: storage.ChainPeers{
				PredecessorNodeID: "mid",
				PredecessorTarget: "mid",
				TailNodeID:        "tail",
				TailTarget:        "tail",
			},
		},
	}
	for _, nodeID := range []string{"head", "mid", "tail"} {
		if err := nodes[nodeID].AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignments[nodeID]}); err != nil {
			t.Fatalf("AddReplicaAsTail(%q) returned error: %v", nodeID, err)
		}
		if err := nodes[nodeID].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(%q) returned error: %v", nodeID, err)
		}
		if err := nodes[nodeID].UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{Assignment: assignments[nodeID]}); err != nil {
			t.Fatalf("UpdateChainPeers(%q) returned error: %v", nodeID, err)
		}
	}

	return queuedCRAQGRPCChain{
		nodes:     nodes,
		addresses: addresses,
		repl:      repl,
	}
}

func queueDirtyCommittedMiddleWrite(
	t *testing.T,
	ctx context.Context,
	chain queuedCRAQGRPCChain,
	slot int,
	key string,
	value string,
	sequence uint64,
) {
	t.Helper()
	ts := time.Unix(int64(sequence), 0).UTC()
	if err := chain.nodes["mid"].HandleForwardWrite(ctx, storage.ForwardWriteRequest{
		Operation: storage.WriteOperation{
			Slot:     slot,
			Sequence: sequence,
			Kind:     storage.OperationKindPut,
			Key:      key,
			Value:    value,
			Metadata: storage.ObjectMetadata{
				Version:   sequence,
				CreatedAt: ts,
				UpdatedAt: ts,
			},
		},
		FromNodeID: "head",
	}); err != nil {
		t.Fatalf("HandleForwardWrite returned error: %v", err)
	}
	if err := chain.repl.DeliverNext(ctx); err != nil {
		t.Fatalf("DeliverNext returned error: %v", err)
	}
}

func misconfigureCRAQReadDependency(t *testing.T, node *storage.Node, slot int, tailNodeID string) {
	t.Helper()
	state := node.State()
	replica, ok := state.Replicas[slot]
	if !ok {
		t.Fatalf("slot %d missing from node %q", slot, state.NodeID)
	}
	assignment := replica.Assignment
	assignment.Peers.TailNodeID = tailNodeID
	assignment.Peers.TailTarget = tailNodeID
	if err := node.UpdateChainPeers(context.Background(), storage.UpdateChainPeersCommand{Assignment: assignment}); err != nil {
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}
}

func mustSingleReplicaNode(
	t *testing.T,
	ctx context.Context,
	nodeID string,
	cfg storage.Config,
	coord storage.CoordinatorClient,
	repl storage.ReplicationTransport,
	slot int,
	version uint64,
) *storage.Node {
	t.Helper()
	node := mustOpenNode(t, ctx, cfg, storage.NewInMemoryBackend(), coord, repl)
	mustActivateReplica(t, node, storage.ReplicaAssignment{
		Slot:         slot,
		ChainVersion: version,
		Role:         storage.ReplicaRoleSingle,
	})
	return node
}

func mustOpenNode(
	t *testing.T,
	ctx context.Context,
	cfg storage.Config,
	backend storage.Backend,
	coord storage.CoordinatorClient,
	repl storage.ReplicationTransport,
) *storage.Node {
	t.Helper()
	node, err := storage.OpenNode(ctx, cfg, backend, storage.NewInMemoryLocalStateStore(), coord, repl)
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	return node
}

func mustActivateReplica(t *testing.T, node *storage.Node, assignment storage.ReplicaAssignment) {
	t.Helper()
	ctx := context.Background()
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: assignment.Slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
}

func mustStartStorageServer(t *testing.T, node *storage.Node) (*grpcx.StorageGRPCServer, string) {
	t.Helper()
	address := mustReserveAddress(t)
	return mustStartStorageServerAt(t, node, address), address
}

func mustStartStorageServerAt(t *testing.T, node *storage.Node, address string) *grpcx.StorageGRPCServer {
	t.Helper()
	lis, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	server := grpcx.NewStorageGRPCServer(node)
	go func() {
		if err := server.Serve(lis); err != nil && !errors.Is(err, net.ErrClosed) {
			panic(err)
		}
	}()
	return server
}

func mustStartCoordinator(
	t *testing.T,
	ctx context.Context,
	store coordruntime.Store,
	pool *grpcx.ConnPool,
) (*grpcx.CoordinatorGRPCServer, string, *coordserver.Server) {
	t.Helper()
	server, err := coordserver.OpenWithConfig(ctx, store, nil, coordserver.ServerConfig{
		NodeClientFactory: grpcx.NewDynamicNodeClientFactory(pool),
	})
	if err != nil {
		t.Fatalf("OpenWithConfig returned error: %v", err)
	}
	address := mustReserveAddress(t)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	service := grpcx.NewCoordinatorGRPCServer(server)
	go func() {
		if err := service.Serve(lis); err != nil && !errors.Is(err, net.ErrClosed) {
			panic(err)
		}
	}()
	return service, address, server
}

func mustReserveAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	address := lis.Addr().String()
	_ = lis.Close()
	return address
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("condition was not met before deadline")
}

type blockingTransport struct{}

func (t *blockingTransport) FetchSnapshot(context.Context, string, int) (storage.Snapshot, error) {
	return storage.Snapshot{}, nil
}

func (t *blockingTransport) FetchCommittedSequence(context.Context, string, int) (uint64, error) {
	return 0, nil
}

func (t *blockingTransport) ForwardWrite(context.Context, string, storage.ForwardWriteRequest) error {
	return nil
}

func (t *blockingTransport) CommitWrite(context.Context, string, storage.CommitWriteRequest) error {
	return nil
}

func (t *blockingTransport) AwaitWriteCommit(ctx context.Context, check func() bool) error {
	if check() {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}
