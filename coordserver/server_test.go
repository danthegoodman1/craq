package coordserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func TestBootstrapCreatesCoordinatorStateAndDispatchesNothing(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
	}
	server := mustOpenServer(t, mapToClient(nodes))

	state, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if got, want := state.Cluster.SlotCount, 8; got != want {
		t.Fatalf("slot count = %d, want %d", got, want)
	}
	for nodeID, node := range nodes {
		if len(node.calls) != 0 {
			t.Fatalf("node %q received commands during bootstrap: %v", nodeID, node.calls)
		}
	}
}

func TestOpenWithConfigSeedsReconfigurationPolicy(t *testing.T) {
	server, err := OpenWithConfig(context.Background(), coordruntime.NewInMemoryStore(), nil, ServerConfig{
		Clock: &fakeClock{now: time.Unix(1, 0)},
		ReconfigurationPolicy: coordinator.ReconfigurationPolicy{
			MaxChangedChains: 32,
		},
	})
	if err != nil {
		t.Fatalf("OpenWithConfig returned error: %v", err)
	}
	defer func() { _ = server.Close() }()

	if got, want := server.lastPolicy.MaxChangedChains, 32; got != want {
		t.Fatalf("server.lastPolicy.MaxChangedChains = %d, want %d", got, want)
	}
}

func TestBootstrapFailsCleanlyOnInvalidConfig(t *testing.T) {
	server := mustOpenServer(t, nil)
	_, err := server.Bootstrap(context.Background(), coordruntime.Command{
		ID:              "bootstrap-bad",
		ExpectedVersion: 0,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 0, ReplicationFactor: 1},
			Nodes:  uniqueNodes("a"),
		},
	})
	if err == nil {
		t.Fatal("Bootstrap unexpectedly succeeded")
	}
	if !errors.Is(err, coordinator.ErrInvalidConfig) {
		t.Fatalf("error = %v, want invalid config", err)
	}
}

func TestAddNodeDispatchesAddReplicaAsTailToExpectedNode(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

	state, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if got, want := state.Version, uint64(2); got != want {
		t.Fatalf("version = %d, want %d", got, want)
	}
	if got, want := server.Pending()[1], (PendingWork{
		Slot:        1,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: state.SlotVersions[1],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending[1] = %#v, want %#v", got, want)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls = %v, want %v", got, want)
	}
	if got, want := nodes["b"].calls, []string{"update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node b calls = %v, want %v", got, want)
	}
	if got, want := nodes["a"].calls, []string{"update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node a calls = %v, want %v", got, want)
	}
	if got, want := nodes["c"].calls, []string{"update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node c calls = %v, want %v", got, want)
	}
}

func TestBeginDrainDispatchesReplacementTailBeforeRemoval(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 1, 3, "a", "b", "c", "d")

	_, err := server.BeginDrainNode(ctx, reconfigureCommand("drain-b", 1, coordinator.Event{
		Kind:   coordinator.EventKindBeginDrainNode,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{}))
	if err != nil {
		t.Fatalf("BeginDrainNode returned error: %v", err)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls = %v, want %v", got, want)
	}
	if containsCall(nodes["b"].calls, "mark_leaving:0") {
		t.Fatalf("node b should not be marked leaving before replacement is ready: %v", nodes["b"].calls)
	}
}

func TestMarkNodeDeadDispatchesMandatoryRepairEvenWithZeroBudget(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 1, 3, "a", "b", "c", "d")

	_, err := server.MarkNodeDead(ctx, reconfigureCommand("dead-b", 1, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 0}))
	if err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls = %v, want %v", got, want)
	}
}

func TestValidReadyAndRemovedProgressAdvanceStateAndDispatchNextStep(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	if _, err := server.ReportReplicaReady(ctx, "d", 1, 0, "ready-1"); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	if got, want := nodes["d"].calls, []string{"add_tail:1", "update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node d calls after ready = %v, want %v", got, want)
	}
	if got, want := nodes["b"].calls, []string{"update_peers:1", "update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node b calls after ready = %v, want %v", got, want)
	}
	if got, want := nodes["a"].calls, []string{"update_peers:1", "update_peers:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node a calls after ready = %v, want %v", got, want)
	}
	if got, want := nodes["c"].calls, []string{"update_peers:1", "mark_leaving:1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("node c calls after ready = %v, want %v", got, want)
	}
	if got, want := server.Pending()[1], (PendingWork{
		Slot:        1,
		NodeID:      "c",
		Kind:        pendingKindRemoved,
		SlotVersion: server.Current().SlotVersions[1],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending[1] after ready = %#v, want %#v", got, want)
	}

	if _, err := server.ReportReplicaRemoved(ctx, "c", 1, 0, "removed-1"); err != nil {
		t.Fatalf("ReportReplicaRemoved returned error: %v", err)
	}
	if _, exists := server.Pending()[1]; exists {
		t.Fatal("pending work still present after removal")
	}
}

func TestSettledSlotProgressIdempotence(t *testing.T) {
	t.Run("unexpected_and_duplicate_live", func(t *testing.T) {
		ctx := context.Background()
		nodes := map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
			"d": newRecordingNodeClient("d"),
		}
		server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

		if _, err := server.ReportReplicaReady(ctx, "d", 1, 0, "ready-1"); err == nil {
			t.Fatal("ReportReplicaReady unexpectedly succeeded")
		} else if !errors.Is(err, ErrUnexpectedProgress) {
			t.Fatalf("error = %v, want unexpected progress", err)
		}

		slot, leavingNodeID := mustStartAndSettleAddNodeRepair(t, ctx, server, "ready-1", "removed-1")
		beforeState, beforePending, beforeRouting := captureSettledSlotState(t, ctx, server)

		if _, err := server.ReportReplicaReady(ctx, "d", slot, 0, "ready-1"); err != nil {
			t.Fatalf("duplicate ReportReplicaReady returned error: %v", err)
		}
		if _, err := server.ReportReplicaRemoved(ctx, leavingNodeID, slot, 0, "removed-1"); err != nil {
			t.Fatalf("duplicate ReportReplicaRemoved returned error: %v", err)
		}
		if _, err := server.ReportReplicaReady(ctx, leavingNodeID, slot, 0, "removed-1"); err == nil {
			t.Fatal("mismatched progress unexpectedly succeeded")
		} else if !errors.Is(err, ErrUnexpectedProgress) {
			t.Fatalf("error = %v, want unexpected progress", err)
		}

		assertSettledSlotStateUnchanged(t, ctx, server, slot, beforeState, beforePending, beforeRouting)
	})

	t.Run("duplicate_after_reopen", func(t *testing.T) {
		ctx := context.Background()
		store := coordruntime.NewInMemoryStore()
		nodes := map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
			"d": newRecordingNodeClient("d"),
		}
		server := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{})
		slot, leavingNodeID := mustBootstrapAndSettleAddNodeRepair(t, ctx, server, "", "")

		reopened := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{})
		beforeState, beforePending, beforeRouting := captureSettledSlotState(t, ctx, reopened)
		if _, err := reopened.ReportReplicaReady(ctx, "d", slot, 0, ""); err != nil {
			t.Fatalf("duplicate ReportReplicaReady after reopen returned error: %v", err)
		}
		if _, err := reopened.ReportReplicaRemoved(ctx, leavingNodeID, slot, 0, ""); err != nil {
			t.Fatalf("duplicate ReportReplicaRemoved after reopen returned error: %v", err)
		}
		assertSettledSlotStateUnchanged(t, ctx, reopened, slot, beforeState, beforePending, beforeRouting)
	})

	t.Run("out_of_order_after_reopen", func(t *testing.T) {
		ctx := context.Background()
		store := coordruntime.NewInMemoryStore()
		nodes := map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
			"d": newRecordingNodeClient("d"),
		}
		server := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{})
		slot, leavingNodeID := mustBootstrapAndSettleAddNodeRepair(t, ctx, server, "", "")

		reopened := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{})
		beforeState, beforePending, beforeRouting := captureSettledSlotState(t, ctx, reopened)
		if _, err := reopened.ReportReplicaRemoved(ctx, leavingNodeID, slot, 0, ""); err != nil {
			t.Fatalf("out-of-order duplicate ReportReplicaRemoved returned error: %v", err)
		}
		if _, err := reopened.ReportReplicaReady(ctx, "d", slot, 0, ""); err != nil {
			t.Fatalf("out-of-order duplicate ReportReplicaReady returned error: %v", err)
		}
		assertSettledSlotStateUnchanged(t, ctx, reopened, slot, beforeState, beforePending, beforeRouting)
	})

	t.Run("duplicate_after_checkpoint_reopen", func(t *testing.T) {
		ctx := context.Background()
		store := coordruntime.NewInMemoryStore()
		nodes := map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
			"d": newRecordingNodeClient("d"),
		}
		server := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{})
		slot, leavingNodeID := mustBootstrapAndSettleAddNodeRepair(t, ctx, server, "", "")
		if err := server.rt.Checkpoint(ctx); err != nil {
			t.Fatalf("Checkpoint returned error: %v", err)
		}

		reopened := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{})
		beforeState, beforePending, beforeRouting := captureSettledSlotState(t, ctx, reopened)
		if _, err := reopened.ReportReplicaReady(ctx, "d", slot, 0, ""); err != nil {
			t.Fatalf("duplicate ReportReplicaReady after checkpoint returned error: %v", err)
		}
		if _, err := reopened.ReportReplicaRemoved(ctx, leavingNodeID, slot, 0, ""); err != nil {
			t.Fatalf("duplicate ReportReplicaRemoved after checkpoint returned error: %v", err)
		}
		assertSettledSlotStateUnchanged(t, ctx, reopened, slot, beforeState, beforePending, beforeRouting)
		if _, exists := reopened.Pending()[slot]; exists {
			t.Fatalf("pending for completed slot after checkpoint reopen duplicate progress = %#v, want none", reopened.Pending()[slot])
		}
	})
}

func TestDuplicateAutoJoinRegistrationUsesSingleMembershipRecord(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"d"}, ServerConfig{})
	server := h.server
	h.adapters["d"].BindServer(server)

	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	reg := storage.NodeRegistration{
		NodeID:         "d",
		FailureDomains: uniqueNode("d").FailureDomains,
	}
	if _, err := server.RegisterNode(ctx, reg); err != nil {
		t.Fatalf("first RegisterNode returned error: %v", err)
	}
	beforeState := server.Current()
	beforePending := server.Pending()
	beforeRouting, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot before duplicate register returned error: %v", err)
	}
	stateAfterDuplicate, err := server.RegisterNode(ctx, reg)
	if err != nil {
		t.Fatalf("duplicate RegisterNode returned error: %v", err)
	}
	afterRouting, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after duplicate register returned error: %v", err)
	}
	if !reflect.DeepEqual(stateAfterDuplicate, beforeState) {
		t.Fatalf("state returned from duplicate RegisterNode changed\ngot=%#v\nwant=%#v", stateAfterDuplicate, beforeState)
	}
	if got := server.Current(); !reflect.DeepEqual(got, beforeState) {
		t.Fatalf("current state changed on duplicate RegisterNode\ngot=%#v\nwant=%#v", got, beforeState)
	}
	if got := server.Pending(); !reflect.DeepEqual(got, beforePending) {
		t.Fatalf("pending changed on duplicate RegisterNode\ngot=%#v\nwant=%#v", got, beforePending)
	}
	if !reflect.DeepEqual(afterRouting, beforeRouting) {
		t.Fatalf("routing changed on duplicate RegisterNode\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
	}
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}

	if got, want := len(server.Current().Cluster.NodesByID), 1; got != want {
		t.Fatalf("membership size = %d, want %d", got, want)
	}
	if !server.Current().Cluster.ReadyNodeIDs["d"] {
		t.Fatal("node d is not ready after concurrent auto-join heartbeats")
	}
	if got, want := server.Pending()[0], (PendingWork{
		Slot:        0,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[0],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending after concurrent auto-join heartbeats = %#v, want %#v", got, want)
	}
}

func TestAutoJoinHeartbeatRemainsStableAcrossServerReopen(t *testing.T) {
	ctx := context.Background()
	store := coordruntime.NewInMemoryStore()
	repl := storage.NewInMemoryReplicationTransport()
	backend := storage.NewInMemoryBackend()
	repl.Register("d", backend)
	adapter, err := NewInMemoryNodeAdapter(ctx, "d", backend, repl)
	if err != nil {
		t.Fatalf("NewInMemoryNodeAdapter returned error: %v", err)
	}
	repl.RegisterNode("d", adapter.Node())

	server := mustOpenServerWithConfig(t, store, map[string]StorageNodeClient{"d": adapter}, ServerConfig{})
	adapter.BindServer(server)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := adapter.Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("initial ReportHeartbeat returned error: %v", err)
	}
	if got, want := len(server.Current().Cluster.NodesByID), 1; got != want {
		t.Fatalf("membership size before reopen = %d, want %d", got, want)
	}

	reopened := mustOpenServerWithConfig(t, store, map[string]StorageNodeClient{"d": adapter}, ServerConfig{})
	adapter.BindServer(reopened)
	if err := adapter.Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("post-reopen ReportHeartbeat returned error: %v", err)
	}
	if got, want := len(reopened.Current().Cluster.NodesByID), 1; got != want {
		t.Fatalf("membership size after reopen = %d, want %d", got, want)
	}
	if !reopened.Current().Cluster.ReadyNodeIDs["d"] {
		t.Fatal("node d is not ready after reopen heartbeat")
	}
	if got, want := reopened.Pending()[0], (PendingWork{
		Slot:        0,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: reopened.Current().SlotVersions[0],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending after reopen heartbeat = %#v, want %#v", got, want)
	}
}

func TestFactoryCachedClientsDoNotBypassIdentityAndTombstoneSafety(t *testing.T) {
	ctx := context.Background()
	h := newFactoryOnlyHarness(t, []string{"a", "b", "d"})
	server := h.server

	if _, err := server.Bootstrap(ctx, coordruntime.Command{
		ID:              "bootstrap-1",
		ExpectedVersion: 0,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 2},
			Nodes: []coordinator.Node{
				{
					ID:             "a",
					RPCAddress:     "a",
					FailureDomains: cloneFailureDomains(uniqueNode("a").FailureDomains),
				},
				{
					ID:             "b",
					RPCAddress:     "b",
					FailureDomains: cloneFailureDomains(uniqueNode("b").FailureDomains),
				},
			},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	seedServerBootstrap(t, server, map[string]*InMemoryNodeAdapter{"a": h.adapters["a"], "b": h.adapters["b"]}, 1, 2, []string{"a", "b"})

	firstClient, err := server.clientForNodeID("b")
	if err != nil {
		t.Fatalf("clientForNodeID(b) returned error: %v", err)
	}
	secondClient, err := server.clientForNodeID("b")
	if err != nil {
		t.Fatalf("second clientForNodeID(b) returned error: %v", err)
	}
	if firstClient != secondClient {
		t.Fatal("cached clientForNodeID(b) returned different client instance")
	}
	if got, want := h.factory.callCount("b"), 1; got != want {
		t.Fatalf("factory call count for b = %d, want %d", got, want)
	}
	if got, want := len(server.nodes), 1; got != want {
		t.Fatalf("cached server nodes = %d, want %d", got, want)
	}

	if _, err := server.RegisterNode(ctx, storage.NodeRegistration{
		NodeID:         "d",
		FailureDomains: cloneFailureDomains(uniqueNode("d").FailureDomains),
	}); err != nil {
		t.Fatalf("RegisterNode(d) returned error: %v", err)
	}
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}

	if _, err := server.RegisterNode(ctx, storage.NodeRegistration{
		NodeID:         "b",
		RPCAddress:     "b-conflict",
		FailureDomains: cloneFailureDomains(uniqueNode("b").FailureDomains),
	}); err == nil {
		t.Fatal("conflicting RegisterNode(b) unexpectedly succeeded after caching factory client")
	}
	if got := server.Current().Cluster.NodesByID["b"].RPCAddress; got != "b" {
		t.Fatalf("node b RPC address changed after conflicting register = %q, want %q", got, "b")
	}

	current := server.Current()
	if _, err := server.MarkNodeDead(ctx, reconfigureCommand("dead-b", current.Version, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if got, want := h.factory.callCount("d"), 1; got != want {
		t.Fatalf("factory call count for d after repair dispatch = %d, want %d", got, want)
	}
	if got, ok := server.nodes["b"]; !ok || got != firstClient {
		t.Fatalf("cached client for b changed after tombstone\ngot=%#v\nwant=%#v", got, firstClient)
	}
	if err := h.adapters["b"].Node().ReportHeartbeat(ctx); err == nil {
		t.Fatal("ReportHeartbeat(b) unexpectedly succeeded after tombstone with cached client present")
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if _, err := server.ReportReplicaReady(ctx, "d", 0, 0, "ready-1"); err != nil {
		t.Fatalf("ReportReplicaReady(d) returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(server.Current().Cluster.Chains[0], coordinator.ReplicaStateLeaving)
	if leavingNodeID != "" {
		if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("RemoveReplica(%s) returned error: %v", leavingNodeID, err)
		}
		if _, err := server.ReportReplicaRemoved(ctx, leavingNodeID, 0, 0, "removed-1"); err != nil {
			t.Fatalf("ReportReplicaRemoved(%s) returned error: %v", leavingNodeID, err)
		}
	}
	assertSlotRoundTrip(t, ctx, server, h.adapters, 0, "factory-cache", "v1")
}

func TestNoOpReconcileDoesNotChurnSettledState(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

	before := server.Current()
	beforePending := server.Pending()
	beforeRouting, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if err := server.reconcileAndDispatch(ctx); err != nil {
		t.Fatalf("first reconcileAndDispatch returned error: %v", err)
	}
	if err := server.reconcileAndDispatch(ctx); err != nil {
		t.Fatalf("second reconcileAndDispatch returned error: %v", err)
	}
	afterRouting, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after reconcile returned error: %v", err)
	}

	if got := server.Current(); !reflect.DeepEqual(got, before) {
		t.Fatalf("current state changed on no-op reconcile\ngot=%#v\nwant=%#v", got, before)
	}
	if got := server.Pending(); !reflect.DeepEqual(got, beforePending) {
		t.Fatalf("pending changed on no-op reconcile\ngot=%#v\nwant=%#v", got, beforePending)
	}
	if !reflect.DeepEqual(afterRouting, beforeRouting) {
		t.Fatalf("routing changed on no-op reconcile\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
	}
}

func TestHealthyEvaluateLivenessDoesNotChurnState(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nil, ServerConfig{
		Clock:          clock,
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
	})
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if _, err := server.RegisterNode(ctx, storage.NodeRegistration{NodeID: "a"}); err != nil {
		t.Fatalf("RegisterNode(a) returned error: %v", err)
	}
	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "a"}); err != nil {
		t.Fatalf("ReportNodeHeartbeat(a) returned error: %v", err)
	}

	before := server.Current()
	beforePending := server.Pending()
	beforeRouting, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("first EvaluateLiveness returned error: %v", err)
	}
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("second EvaluateLiveness returned error: %v", err)
	}
	afterRouting, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after liveness returned error: %v", err)
	}

	if got := server.Current(); !reflect.DeepEqual(got, before) {
		t.Fatalf("current state changed on healthy EvaluateLiveness\ngot=%#v\nwant=%#v", got, before)
	}
	if got := server.Pending(); !reflect.DeepEqual(got, beforePending) {
		t.Fatalf("pending changed on healthy EvaluateLiveness\ngot=%#v\nwant=%#v", got, beforePending)
	}
	if !reflect.DeepEqual(afterRouting, beforeRouting) {
		t.Fatalf("routing changed on healthy EvaluateLiveness\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
	}
}

func TestDelayedProgressFromQueuedAdaptersCompletesPendingWork(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	for _, adapter := range h.adapters {
		adapter.EnableQueuedProgress()
	}
	server := h.server

	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := h.adapters["d"].PendingProgress(), 1; got != want {
		t.Fatalf("pending queued progress = %d, want %d", got, want)
	}
	if got, want := server.Pending()[1], (PendingWork{
		Slot:        1,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[1],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending ready = %#v, want %#v", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress returned error: %v", err)
	}
	if got, want := server.Pending()[1], (PendingWork{
		Slot:        1,
		NodeID:      "c",
		Kind:        pendingKindRemoved,
		SlotVersion: server.Current().SlotVersions[1],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending after delayed ready = %#v, want %#v", got, want)
	}

	if err := h.adapters["c"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if got, want := h.adapters["c"].PendingProgress(), 1; got != want {
		t.Fatalf("pending queued removed progress = %d, want %d", got, want)
	}
	if err := h.adapters["c"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress removed returned error: %v", err)
	}
	if _, exists := server.Pending()[1]; exists {
		t.Fatal("pending work still present after delayed removal delivery")
	}
}

func TestDuplicateQueuedProgressIsIdempotent(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	for _, adapter := range h.adapters {
		adapter.EnableQueuedProgress()
	}
	server := h.server

	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["d"].DuplicateProgressAt(0); err != nil {
		t.Fatalf("DuplicateProgressAt returned error: %v", err)
	}
	if got, want := h.adapters["d"].PendingProgress(), 2; got != want {
		t.Fatalf("pending queued progress = %d, want %d", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress returned error: %v", err)
	}
	if got, want := server.Pending()[1].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after first ready = %q, want %q", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("duplicate DeliverNextProgress returned error: %v", err)
	}
	if got, want := server.Pending()[1].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after duplicate ready = %q, want %q", got, want)
	}
}

func TestUnknownTargetNodeAndCommandFailuresAreSurfaced(t *testing.T) {
	ctx := context.Background()
	server := mustBootstrappedServer(t, ctx, mapToClient(map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
	}), 8, 3, "a", "b", "c")

	_, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrUnknownNode) {
		t.Fatalf("error = %v, want unknown node", err)
	}

	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	nodes["d"].addTailErr = errors.New("boom")
	server = mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")
	_, err = server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("error = %v, want dispatch failed", err)
	}
	if got, want := server.Pending()[1], (PendingWork{
		Slot:        1,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[1],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending work after failed AddReplicaAsTail dispatch = %#v, want %#v", got, want)
	}
}

func TestHeartbeatIsRecordedButDoesNotTriggerReconfiguration(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
	}
	server := mustOpenServer(t, mapToClient(nodes))
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if _, err := server.RegisterNode(ctx, storage.NodeRegistration{NodeID: "a"}); err != nil {
		t.Fatalf("RegisterNode returned error: %v", err)
	}

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{
		NodeID:          "a",
		ReplicaCount:    2,
		ActiveCount:     1,
		CatchingUpCount: 1,
	}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	if got, want := server.Heartbeats()["a"], (storage.NodeStatus{
		NodeID:          "a",
		ReplicaCount:    2,
		ActiveCount:     1,
		CatchingUpCount: 1,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("heartbeat = %#v, want %#v", got, want)
	}
}

func TestRoutingSnapshotExposesActiveHeadAndTail(t *testing.T) {
	ctx := context.Background()
	server := mustBootstrappedServer(t, ctx, mapToClient(map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
	}), 4, 3, "a", "b", "c")

	snapshot, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if got, want := snapshot.SlotCount, 4; got != want {
		t.Fatalf("slot count = %d, want %d", got, want)
	}
	for _, route := range snapshot.Slots {
		if !route.Readable || !route.Writable {
			t.Fatalf("route %#v should be readable and writable", route)
		}
		if route.HeadNodeID == "" || route.TailNodeID == "" {
			t.Fatalf("route %#v missing endpoints", route)
		}
		if route.ChainVersion != 1 {
			t.Fatalf("route %#v chain version = %d, want 1", route, route.ChainVersion)
		}
	}
}

func TestRoutingSnapshotExcludesJoiningAndLeavingReplicas(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	snapshot, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	route := snapshot.Slots[1]
	if got, want := route.HeadNodeID, "b"; got != want {
		t.Fatalf("head during join = %q, want %q", got, want)
	}
	if got, want := route.TailNodeID, "a"; got != want {
		t.Fatalf("tail during join = %q, want %q", got, want)
	}
	if route.Writable || !route.Readable {
		t.Fatalf("route during join = %#v, want readable but not writable until join is ready", route)
	}
	if got, want := route.ReadReplicas, []ReadReplicaRoute{
		{NodeID: "b", Role: storage.ReplicaRoleHead},
		{NodeID: "c", Role: storage.ReplicaRoleMiddle},
		{NodeID: "a", Role: storage.ReplicaRoleTail},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("read replicas during join = %#v, want %#v", got, want)
	}
	if got, want := route.ChainVersion, uint64(2); got != want {
		t.Fatalf("chain version during join = %d, want %d", got, want)
	}

	if _, err := server.ReportReplicaReady(ctx, "d", 1, 0, "ready-1"); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	snapshot, err = server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after ready returned error: %v", err)
	}
	route = snapshot.Slots[1]
	if got, want := route.HeadNodeID, "d"; got != want {
		t.Fatalf("head after ready = %q, want %q", got, want)
	}
	if got, want := route.TailNodeID, "a"; got != want {
		t.Fatalf("tail after ready = %q, want %q", got, want)
	}
	if route.Writable || route.Readable {
		t.Fatalf("route after ready = %#v, want blocked until leaving replica removal and peer refresh complete", route)
	}
	if got, want := route.ReadReplicas, []ReadReplicaRoute{
		{NodeID: "d", Role: storage.ReplicaRoleHead},
		{NodeID: "b", Role: storage.ReplicaRoleMiddle},
		{NodeID: "a", Role: storage.ReplicaRoleTail},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("read replicas after ready = %#v, want %#v", got, want)
	}
	if got, want := route.ChainVersion, server.Current().SlotVersions[1]; got != want {
		t.Fatalf("chain version after ready = %d, want %d", got, want)
	}
}

func TestRoutingSnapshotReturnsDefensiveCopyAndStableRepeatedReads(t *testing.T) {
	ctx := context.Background()
	server := mustBootstrappedServer(t, ctx, mapToClient(map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
	}), 4, 3, "a", "b", "c")

	first, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	second, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot second read returned error: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("repeated snapshots differ\nfirst=%#v\nsecond=%#v", first, second)
	}

	first.Slots[0].HeadNodeID = "mutated-head"
	first.Slots[0].TailNodeID = "mutated-tail"
	first.Slots[0].Readable = false
	first.Slots[0].Writable = false

	third, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot third read returned error: %v", err)
	}
	if got, want := third.Slots[0].HeadNodeID, second.Slots[0].HeadNodeID; got != want {
		t.Fatalf("head node after external mutation = %q, want %q", got, want)
	}
	if got, want := third.Slots[0].TailNodeID, second.Slots[0].TailNodeID; got != want {
		t.Fatalf("tail node after external mutation = %q, want %q", got, want)
	}
	if third.Slots[0].Readable != second.Slots[0].Readable || third.Slots[0].Writable != second.Slots[0].Writable {
		t.Fatalf("route flags after external mutation = %#v, want %#v", third.Slots[0], second.Slots[0])
	}
}

func TestEndToEndAddNodeFlowWithInMemoryNodes(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	server := h.server

	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("adapter ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["c"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("adapter RemoveReplica returned error: %v", err)
	}

	final := server.Current()
	if got, want := replicaNodeStates(final.Cluster.Chains[1]), []string{"d:active", "b:active", "a:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain = %v, want %v", got, want)
	}
	data, err := h.backends["d"].ReplicaData(1)
	if err != nil {
		t.Fatalf("ReplicaData returned error: %v", err)
	}
	if !reflect.DeepEqual(storageSnapshotValues(data), map[string]string{"seed-1": "value-1"}) {
		t.Fatalf("replica data = %v, want seed copy", data)
	}
}

func TestEndToEndDrainAndDeadFlowsWithInMemoryNodes(t *testing.T) {
	ctx := context.Background()
	t.Run("drain", func(t *testing.T) {
		h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
		server := h.server
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})

		if _, err := server.BeginDrainNode(ctx, reconfigureCommand("drain-b", 1, coordinator.Event{
			Kind:   coordinator.EventKindBeginDrainNode,
			NodeID: "b",
		}, coordinator.ReconfigurationPolicy{})); err != nil {
			t.Fatalf("BeginDrainNode returned error: %v", err)
		}
		if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("ActivateReplica returned error: %v", err)
		}
		if err := h.adapters["b"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("RemoveReplica returned error: %v", err)
		}
		if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("final chain = %v, want %v", got, want)
		}
	})

	t.Run("dead", func(t *testing.T) {
		h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
		server := h.server
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})

		if _, err := server.MarkNodeDead(ctx, reconfigureCommand("dead-b", 1, coordinator.Event{
			Kind:   coordinator.EventKindMarkNodeDead,
			NodeID: "b",
		}, coordinator.ReconfigurationPolicy{})); err != nil {
			t.Fatalf("MarkNodeDead returned error: %v", err)
		}
		if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("ActivateReplica returned error: %v", err)
		}
		if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("final chain = %v, want %v", got, want)
		}
	})
}

func TestDeterministicRepeatedHistory(t *testing.T) {
	left := runServerHistory(t)
	right := runServerHistory(t)
	if !reflect.DeepEqual(left.finalState, right.finalState) {
		t.Fatalf("final state mismatch\nleft=%#v\nright=%#v", left.finalState, right.finalState)
	}
	if !reflect.DeepEqual(left.heartbeats, right.heartbeats) {
		t.Fatalf("heartbeat mismatch\nleft=%#v\nright=%#v", left.heartbeats, right.heartbeats)
	}
	if !reflect.DeepEqual(left.pending, right.pending) {
		t.Fatalf("pending mismatch\nleft=%#v\nright=%#v", left.pending, right.pending)
	}
}

func mustBootstrapAndSettleAddNodeRepair(
	t *testing.T,
	ctx context.Context,
	server *Server,
	readyCommandID string,
	removedCommandID string,
) (int, string) {
	t.Helper()
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	return mustStartAndSettleAddNodeRepair(t, ctx, server, readyCommandID, removedCommandID)
}

func mustStartAndSettleAddNodeRepair(
	t *testing.T,
	ctx context.Context,
	server *Server,
	readyCommandID string,
	removedCommandID string,
) (int, string) {
	t.Helper()
	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	if _, err := server.ReportReplicaReady(ctx, "d", slot, 0, readyCommandID); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(server.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node before settle")
	}
	if _, err := server.ReportReplicaRemoved(ctx, leavingNodeID, slot, 0, removedCommandID); err != nil {
		t.Fatalf("ReportReplicaRemoved returned error: %v", err)
	}
	return slot, leavingNodeID
}

func captureSettledSlotState(
	t *testing.T,
	ctx context.Context,
	server *Server,
) (coordruntime.State, map[int]PendingWork, RoutingSnapshot) {
	t.Helper()
	routing, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	return server.Current(), server.Pending(), routing
}

func assertSettledSlotStateUnchanged(
	t *testing.T,
	ctx context.Context,
	server *Server,
	slot int,
	wantState coordruntime.State,
	wantPending map[int]PendingWork,
	wantRouting RoutingSnapshot,
) {
	t.Helper()
	afterRouting, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after duplicates returned error: %v", err)
	}
	if got := server.Current(); !reflect.DeepEqual(got, wantState) {
		t.Fatalf("state changed on settled-slot idempotence check\ngot=%#v\nwant=%#v", got, wantState)
	}
	if got := server.Pending(); !reflect.DeepEqual(got, wantPending) {
		t.Fatalf("pending changed on settled-slot idempotence check\ngot=%#v\nwant=%#v", got, wantPending)
	}
	if !reflect.DeepEqual(afterRouting, wantRouting) {
		t.Fatalf("routing changed on settled-slot idempotence check\nafter=%#v\nbefore=%#v", afterRouting, wantRouting)
	}
	if runtimeOutboxHasSlot(server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox unexpectedly recreated for settled slot %d: %#v", slot, server.Current().Outbox)
	}
}

type recordingNodeClient struct {
	nodeID           string
	calls            []string
	addTailErr       error
	updatePeersErr   error
	markLeavingErr   error
	removeReplicaErr error
	resumeErr        error
	recoverErr       error
	dropErr          error
}

func newRecordingNodeClient(nodeID string) *recordingNodeClient {
	return &recordingNodeClient{nodeID: nodeID}
}

type trackingNodeClientFactory struct {
	clients map[string]StorageNodeClient
	calls   map[string]int
}

func newTrackingNodeClientFactory(clients map[string]StorageNodeClient) *trackingNodeClientFactory {
	return &trackingNodeClientFactory{
		clients: clients,
		calls:   make(map[string]int, len(clients)),
	}
}

func (f *trackingNodeClientFactory) ClientForNode(node coordinator.Node) (StorageNodeClient, error) {
	f.calls[node.ID]++
	client, ok := f.clients[node.ID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, node.ID)
	}
	return client, nil
}

func (f *trackingNodeClientFactory) callCount(nodeID string) int {
	return f.calls[nodeID]
}

func (r *recordingNodeClient) AddReplicaAsTail(_ context.Context, cmd storage.AddReplicaAsTailCommand) error {
	if r.addTailErr != nil {
		return r.addTailErr
	}
	r.calls = append(r.calls, fmt.Sprintf("add_tail:%d", cmd.Assignment.Slot))
	return nil
}

func (r *recordingNodeClient) ActivateReplica(_ context.Context, cmd storage.ActivateReplicaCommand) error {
	r.calls = append(r.calls, fmt.Sprintf("activate:%d", cmd.Slot))
	return nil
}

func (r *recordingNodeClient) MarkReplicaLeaving(_ context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	if r.markLeavingErr != nil {
		return r.markLeavingErr
	}
	r.calls = append(r.calls, fmt.Sprintf("mark_leaving:%d", cmd.Slot))
	return nil
}

func (r *recordingNodeClient) RemoveReplica(_ context.Context, cmd storage.RemoveReplicaCommand) error {
	if r.removeReplicaErr != nil {
		return r.removeReplicaErr
	}
	r.calls = append(r.calls, fmt.Sprintf("remove:%d", cmd.Slot))
	return nil
}

func (r *recordingNodeClient) UpdateChainPeers(_ context.Context, cmd storage.UpdateChainPeersCommand) error {
	if r.updatePeersErr != nil {
		return r.updatePeersErr
	}
	r.calls = append(r.calls, fmt.Sprintf("update_peers:%d", cmd.Assignment.Slot))
	return nil
}

func (r *recordingNodeClient) ResumeRecoveredReplica(_ context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	if r.resumeErr != nil {
		return r.resumeErr
	}
	r.calls = append(r.calls, fmt.Sprintf("resume:%d", cmd.Assignment.Slot))
	return nil
}

func (r *recordingNodeClient) RecoverReplica(_ context.Context, cmd storage.RecoverReplicaCommand) error {
	if r.recoverErr != nil {
		return r.recoverErr
	}
	r.calls = append(r.calls, fmt.Sprintf("recover:%d:%s", cmd.Assignment.Slot, cmd.SourceNodeID))
	return nil
}

func (r *recordingNodeClient) DropRecoveredReplica(_ context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	if r.dropErr != nil {
		return r.dropErr
	}
	r.calls = append(r.calls, fmt.Sprintf("drop:%d", cmd.Slot))
	return nil
}

type inMemoryHarness struct {
	server   *Server
	adapters map[string]*InMemoryNodeAdapter
	backends map[string]*storage.InMemoryBackend
}

type factoryOnlyHarness struct {
	server   *Server
	adapters map[string]*InMemoryNodeAdapter
	backends map[string]*storage.InMemoryBackend
	factory  *trackingNodeClientFactory
}

func newInMemoryHarness(t *testing.T, nodeIDs []string) *inMemoryHarness {
	t.Helper()
	repl := storage.NewInMemoryReplicationTransport()
	adapters := make(map[string]*InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
	}
	server := mustOpenServer(t, nodeClients)
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	for _, adapter := range adapters {
		adapter.BindServer(nil)
	}
	return &inMemoryHarness{
		server:   server,
		adapters: adapters,
		backends: backends,
	}
}

func newFactoryOnlyHarness(t *testing.T, nodeIDs []string) *factoryOnlyHarness {
	t.Helper()
	repl := storage.NewInMemoryReplicationTransport()
	adapters := make(map[string]*InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
	}
	factory := newTrackingNodeClientFactory(nodeClients)
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nil, ServerConfig{
		NodeClientFactory: factory,
	})
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	return &factoryOnlyHarness{
		server:   server,
		adapters: adapters,
		backends: backends,
		factory:  factory,
	}
}

func (h *inMemoryHarness) seedBootstrap(t *testing.T, slotCount int, replicationFactor int, nodeIDs []string) {
	t.Helper()
	state, err := coordinator.BuildInitialPlacement(coordinator.Config{
		SlotCount:         slotCount,
		ReplicationFactor: replicationFactor,
	}, uniqueNodes(nodeIDs...))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(nil)
	}
	for _, chain := range state.Chains {
		for _, replica := range chain.Replicas {
			assignment, err := assignmentForNode(chain, state.NodesByID, replica.NodeID, 1)
			if err != nil {
				t.Fatalf("assignmentForNode returned error: %v", err)
			}
			adapter := h.adapters[replica.NodeID]
			if err := adapter.Node().AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				t.Fatalf("seed AddReplicaAsTail returned error: %v", err)
			}
			if err := adapter.Node().ActivateReplica(context.Background(), storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				t.Fatalf("seed ActivateReplica returned error: %v", err)
			}
			if err := h.backends[replica.NodeID].Put(chain.Slot, fmt.Sprintf("seed-%d", chain.Slot), fmt.Sprintf("value-%d", chain.Slot), storage.ObjectMetadata{Version: 1}); err != nil {
				t.Fatalf("seed Put returned error: %v", err)
			}
		}
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(h.server)
	}
}

func storageSnapshotValues(snapshot storage.Snapshot) map[string]string {
	values := make(map[string]string, len(snapshot))
	for key, object := range snapshot {
		values[key] = object.Value
	}
	return values
}

type historyResult struct {
	finalState coordruntime.State
	heartbeats map[string]storage.NodeStatus
	pending    map[int]PendingWork
}

func runServerHistory(t *testing.T) historyResult {
	t.Helper()
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})
	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["c"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}
	return historyResult{
		finalState: server.Current(),
		heartbeats: server.Heartbeats(),
		pending:    server.Pending(),
	}
}

func mustOpenServer(t *testing.T, nodes map[string]StorageNodeClient) *Server {
	t.Helper()
	server, err := OpenWithConfig(context.Background(), coordruntime.NewInMemoryStore(), nodes, ServerConfig{
		Clock: &fakeClock{now: time.Unix(1, 0)},
	})
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	return server
}

func mustBootstrappedServer(
	t *testing.T,
	ctx context.Context,
	nodes map[string]StorageNodeClient,
	slotCount int,
	replicationFactor int,
	nodeIDs ...string,
) *Server {
	t.Helper()
	server := mustOpenServer(t, nodes)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor, nodeIDs...)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	return server
}

func bootstrapCommand(id string, expected uint64, slotCount int, replicationFactor int, nodeIDs ...string) coordruntime.Command {
	return coordruntime.Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         slotCount,
				ReplicationFactor: replicationFactor,
			},
			Nodes: uniqueNodes(nodeIDs...),
		},
	}
}

func reconfigureCommand(
	id string,
	expected uint64,
	event coordinator.Event,
	policy coordinator.ReconfigurationPolicy,
) coordruntime.Command {
	return coordruntime.Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: []coordinator.Event{event},
			Policy: policy,
		},
	}
}

func uniqueNodes(ids ...string) []coordinator.Node {
	nodes := make([]coordinator.Node, len(ids))
	for i, id := range ids {
		nodes[i] = uniqueNode(id)
	}
	return nodes
}

func uniqueNode(id string) coordinator.Node {
	return coordinator.Node{
		ID: id,
		FailureDomains: map[string]string{
			"host": "host-" + id,
			"rack": "rack-" + id,
			"az":   "az-" + id,
		},
	}
}

func replicaNodeStates(chain coordinator.Chain) []string {
	states := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		states = append(states, fmt.Sprintf("%s:%s", replica.NodeID, replica.State))
	}
	return states
}

func plannedLeavingNodeAfterReady(t *testing.T, server *Server, slot int, joiningNodeID string) string {
	t.Helper()
	current := server.Current()
	preview, err := coordinator.ApplyProgress(current.Cluster, coordinator.Event{
		Kind:   coordinator.EventKindReplicaBecameActive,
		NodeID: joiningNodeID,
		Slot:   slot,
	})
	if err != nil {
		t.Fatalf("ApplyProgress preview returned error: %v", err)
	}
	plan, err := coordinator.PlanReconfiguration(*preview, nil, server.reconfigurationPolicy())
	if err != nil {
		t.Fatalf("PlanReconfiguration preview returned error: %v", err)
	}
	for _, slotPlan := range plan.ChangedSlots {
		if slotPlan.Slot != slot {
			continue
		}
		for _, step := range slotPlan.Steps {
			if step.Kind == coordinator.StepKindMarkLeaving {
				return step.NodeID
			}
		}
	}
	return ""
}

func mapToClient(nodes map[string]*recordingNodeClient) map[string]StorageNodeClient {
	clients := make(map[string]StorageNodeClient, len(nodes))
	for nodeID, node := range nodes {
		clients[nodeID] = node
	}
	return clients
}

func containsCall(calls []string, want string) bool {
	for _, call := range calls {
		if call == want {
			return true
		}
	}
	return false
}

func TestDispatchOrderIsDeterministic(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustBootstrappedServer(t, ctx, mapToClient(nodes), 8, 3, "a", "b", "c")
	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	got := make([]string, 0, len(nodes))
	for _, nodeID := range []string{"a", "b", "c", "d"} {
		for _, call := range nodes[nodeID].calls {
			got = append(got, fmt.Sprintf("%s:%s", nodeID, call))
		}
	}
	want := []string{"a:update_peers:1", "b:update_peers:1", "c:update_peers:1", "d:add_tail:1"}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dispatched calls = %v, want %v", got, want)
	}
}
