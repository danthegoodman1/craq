package coordserver

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func TestAddNodeDispatchTimeoutReturnsBoundedErrorAndNoPendingWork(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
		"d": newBlockingNodeClient("d"),
	}
	nodes["d"].blockAddTail = true
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{DispatchTimeout: time.Nanosecond}, 8, 3, "a", "b", "c")

	_, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}
	if got, want := server.Pending()[1], (PendingWork{
		Slot:        1,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[1],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending work = %#v, want %#v", got, want)
	}
}

func TestReportNodeHeartbeatDoesNotInlineDispatchLargeOutbox(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 64, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 64, 3, []string{"a", "b", "c"})

	timeoutWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	timeoutWrapper.addTailTimeouts = 1
	h.server.nodes["d"] = timeoutWrapper

	_, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 16}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}

	for _, nodeID := range []string{"a", "b", "c", "d"} {
		h.server.nodes[nodeID] = &slowNodeClient{
			delegate: h.adapters[nodeID],
			delay:    25 * time.Millisecond,
		}
	}

	hbCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if err := h.server.ReportNodeHeartbeat(hbCtx, storage.NodeStatus{
		NodeID:          "a",
		ReplicaCount:    64,
		ActiveCount:     64,
		CatchingUpCount: 0,
	}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error under large pending outbox: %v", err)
	}
}

func TestReportReplicaReadyDoesNotInlineDispatchLargeOutbox(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 64, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 64, 3, []string{"a", "b", "c"})

	timeoutWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	timeoutWrapper.addTailTimeouts = 1
	h.server.nodes["d"] = timeoutWrapper

	_, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 16}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}

	slot := mustPendingSlotForNode(t, h.server.Pending(), "d", pendingKindReady)
	for _, nodeID := range []string{"a", "b", "c", "d"} {
		h.server.nodes[nodeID] = &slowNodeClient{
			delegate: h.adapters[nodeID],
			delay:    25 * time.Millisecond,
		}
	}

	readyCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := h.server.ReportReplicaReady(readyCtx, "d", slot, 0, ""); err != nil {
		t.Fatalf("ReportReplicaReady returned error under large pending outbox: %v", err)
	}
}

func TestStartupScaleProgressAndHeartbeatStayBoundedUnderLargeOutbox(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 256, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 256, 3, []string{"a", "b", "c"})

	timeoutWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	timeoutWrapper.addTailTimeouts = 1
	h.server.nodes["d"] = timeoutWrapper

	_, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 32}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}

	slot := mustPendingSlotForNode(t, h.server.Pending(), "d", pendingKindReady)
	for _, nodeID := range []string{"a", "b", "c", "d"} {
		h.server.nodes[nodeID] = &slowNodeClient{
			delegate: h.adapters[nodeID],
			delay:    10 * time.Millisecond,
		}
	}

	heartbeatCtx, heartbeatCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer heartbeatCancel()
	if err := h.server.ReportNodeHeartbeat(heartbeatCtx, storage.NodeStatus{
		NodeID:          "a",
		ReplicaCount:    256,
		ActiveCount:     256,
		CatchingUpCount: 0,
	}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error at startup scale: %v", err)
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer readyCancel()
	if _, err := h.server.ReportReplicaReady(readyCtx, "d", slot, 0, ""); err != nil {
		t.Fatalf("ReportReplicaReady returned error at startup scale: %v", err)
	}
}

func TestCloudShapeStartupScaleProgressAndHeartbeatStayBoundedUnderLargeOutbox(t *testing.T) {
	requireBenchmarkCloudShapeSoak(t)

	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1024, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1024, 3, []string{"a", "b", "c"})

	timeoutWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	timeoutWrapper.addTailTimeouts = 1
	h.server.nodes["d"] = timeoutWrapper

	_, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 32}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}

	slot := mustPendingSlotForNode(t, h.server.Pending(), "d", pendingKindReady)
	for _, nodeID := range []string{"a", "b", "c", "d"} {
		h.server.nodes[nodeID] = &slowNodeClient{
			delegate: h.adapters[nodeID],
			delay:    10 * time.Millisecond,
		}
	}

	heartbeatCtx, heartbeatCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer heartbeatCancel()
	if err := h.server.ReportNodeHeartbeat(heartbeatCtx, storage.NodeStatus{
		NodeID:          "a",
		ReplicaCount:    1024,
		ActiveCount:     1024,
		CatchingUpCount: 0,
	}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error at cloud startup scale: %v", err)
	}

	readyCtx, readyCancel := context.WithTimeout(ctx, 150*time.Millisecond)
	defer readyCancel()
	if _, err := h.server.ReportReplicaReady(readyCtx, "d", slot, 0, ""); err != nil {
		t.Fatalf("ReportReplicaReady returned error at cloud startup scale: %v", err)
	}
}

func requireBenchmarkCloudShapeSoak(t *testing.T) {
	t.Helper()
	if os.Getenv("CRAQ_RUN_BENCHMARK_SOAK_LOCAL") == "" {
		t.Skip("set CRAQ_RUN_BENCHMARK_SOAK_LOCAL=1 to run the local cloud-shape benchmark soak")
	}
}

func TestBeginDrainUpdatePeersTimeoutDoesNotFabricateCoordinatorAdvancement(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
		"d": newBlockingNodeClient("d"),
	}
	nodes["a"].blockUpdatePeers = true
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{DispatchTimeout: time.Nanosecond}, 1, 3, "a", "b", "c", "d")

	_, err := server.BeginDrainNode(ctx, reconfigureCommand("drain-b", 1, coordinator.Event{
		Kind:   coordinator.EventKindBeginDrainNode,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{}))
	if err == nil {
		t.Fatal("BeginDrainNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	if got, want := server.Pending()[0], (PendingWork{
		Slot:        0,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[0],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending work = %#v, want %#v", got, want)
	}
	for _, replica := range server.Current().Cluster.Chains[0].Replicas {
		if replica.State == coordinator.ReplicaStateLeaving {
			t.Fatalf("coordinator state unexpectedly advanced replica to leaving: %v", replicaNodeStates(server.Current().Cluster.Chains[0]))
		}
	}
}

func TestRecoveryCommandTimeoutLeavesReplicaUnavailable(t *testing.T) {
	ctx := context.Background()
	store := coordruntime.NewInMemoryStore()
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
	}
	server := mustOpenServerWithConfig(t, store, mapBlockingToClient(nodes), ServerConfig{
		RecoveryCommandTimeout: time.Nanosecond,
	})
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	current := server.Current()
	assignment, ok := currentAssignmentForNode(current, current.Cluster.Chains[0].Replicas[0].NodeID, 0)
	if !ok {
		t.Fatal("failed to find current assignment for recovered timeout test")
	}
	recoveringNodeID := current.Cluster.Chains[0].Replicas[0].NodeID
	if replicaRoleCanResumeRecovered(assignment.Role) {
		nodes[recoveringNodeID].blockResume = true
	} else {
		nodes[recoveringNodeID].blockRecover = true
	}

	err := server.ReportNodeRecovered(ctx, storage.NodeRecoveryReport{
		NodeID: recoveringNodeID,
		Replicas: []storage.RecoveredReplica{{
			Assignment:               assignment,
			LastKnownState:           storage.ReplicaStateActive,
			HighestCommittedSequence: 0,
			HasCommittedData:         true,
		}},
	})
	if err == nil {
		t.Fatal("ReportNodeRecovered unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	snapshot, snapErr := server.RoutingSnapshot(ctx)
	if snapErr != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", snapErr)
	}
	if snapshot.Slots[0].Writable || snapshot.Slots[0].Readable {
		t.Fatalf("routing slot = %#v, want unavailable while recovery timed out", snapshot.Slots[0])
	}
}

func TestLivenessTriggeredDeadRepairRespectsDispatchTimeout(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
		"d": newBlockingNodeClient("d"),
	}
	nodes["d"].blockAddTail = true
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{
		Clock:           clock,
		DispatchTimeout: time.Nanosecond,
		LivenessPolicy:  LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
	}, 1, 3, "a", "b", "c", "d")

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err == nil {
		t.Fatal("EvaluateLiveness unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrDispatchTimeout) {
			t.Fatalf("error = %v, want dispatch timeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want deadline exceeded", err)
		}
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("liveness state = %q, want %q", got, want)
	}
	if got, want := server.Pending()[0], (PendingWork{
		Slot:        0,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[0],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending work = %#v, want %#v", got, want)
	}
}

func TestAddNodeDispatchTimeoutThenRetryCompletesRepairAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		DispatchTimeout:       time.Nanosecond,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	wrapper := newFaultInjectingNodeClient(h.adapters["d"])
	wrapper.addTailTimeouts = 1
	h.server.nodes["d"] = wrapper

	_, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	slot := mustPendingSlotForNode(t, h.server.Pending(), "d", pendingKindReady)
	if got, want := wrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after timeout = %d, want %d", got, want)
	}
	if !runtimeOutboxHasSlot(h.server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox missing repaired slot %d after timeout", slot)
	}

	if err := h.server.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if got, want := wrapper.addTailCallCount(), 2; got != want {
		t.Fatalf("add-tail calls after retry = %d, want %d", got, want)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(h.server.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after retry activation")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}

	assertActiveReplicaSet(t, h.server.Current().Cluster.Chains[slot], "a", "b", "d")
	if runtimeOutboxHasSlot(h.server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, h.server.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, h.server, h.adapters, slot, "timeout-add-tail", "v1")
}

func TestAddNodePartialOutboxSuccessThenRetryDoesNotRedispatchCompletedPeerUpdates(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		DispatchTimeout:       time.Nanosecond,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	preview, err := coordinator.PlanReconfiguration(h.server.Current().Cluster, []coordinator.Event{{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})
	if err != nil {
		t.Fatalf("PlanReconfiguration preview returned error: %v", err)
	}
	slot := preview.ChangedSlots[0].Slot
	updateNodes := activeAfterNodeIDs(activeServingChain(preview.ChangedSlots[0].After), map[string]bool{"d": true})
	if len(updateNodes) < 2 {
		t.Fatalf("update node count = %d, want at least 2 for partial-success replay", len(updateNodes))
	}

	addTailWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	h.server.nodes["d"] = addTailWrapper
	updateWrappers := make(map[string]*faultInjectingNodeClient, len(updateNodes))
	for _, nodeID := range updateNodes {
		wrapper := newFaultInjectingNodeClient(h.adapters[nodeID])
		updateWrappers[nodeID] = wrapper
		h.server.nodes[nodeID] = wrapper
	}
	updateWrappers[updateNodes[len(updateNodes)-1]].updatePeersTimeouts = 1

	_, err = h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	if got, want := addTailWrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after partial success = %d, want %d", got, want)
	}
	for _, nodeID := range updateNodes[:len(updateNodes)-1] {
		if got, want := updateWrappers[nodeID].updatePeersCallCount(), 1; got != want {
			t.Fatalf("update-peers calls for %q after partial success = %d, want %d", nodeID, got, want)
		}
	}
	if got, want := updateWrappers[updateNodes[len(updateNodes)-1]].updatePeersCallCount(), 1; got != want {
		t.Fatalf("update-peers calls for timed-out node after partial success = %d, want %d", got, want)
	}
	if got, want := h.server.Pending()[slot].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after partial success = %q, want %q", got, want)
	}

	if err := h.server.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if got, want := addTailWrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after retry = %d, want no redispatch", got)
	}
	for _, nodeID := range updateNodes[:len(updateNodes)-1] {
		if got, want := updateWrappers[nodeID].updatePeersCallCount(), 1; got != want {
			t.Fatalf("update-peers calls for %q after retry = %d, want no duplicate", nodeID, got)
		}
	}
	if got, want := updateWrappers[updateNodes[len(updateNodes)-1]].updatePeersCallCount(), 2; got != want {
		t.Fatalf("update-peers calls for retried node after retry = %d, want %d", got, want)
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(h.server.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after partial-success retry activation")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	assertSlotRoundTrip(t, ctx, h.server, h.adapters, slot, "partial-add-tail", "v-partial-1")
}

func TestMarkLeavingDispatchTimeoutThenRetryCompletesRepairAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		DispatchTimeout:       time.Nanosecond,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})
	h.adapters["d"].EnableQueuedProgress()

	if _, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.server.Pending(), "d", pendingKindReady)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}

	leavingNodeID := lastActiveReplicaNode(h.server.Current().Cluster.Chains[slot])
	if leavingNodeID == "" {
		t.Fatal("failed to determine leaving node before retry timeout")
	}
	wrapper := newFaultInjectingNodeClient(h.adapters[leavingNodeID])
	wrapper.markLeavingTimeouts = 1
	h.server.nodes[leavingNodeID] = wrapper

	if err := h.adapters["d"].DeliverNextProgress(ctx); err == nil {
		t.Fatal("DeliverNextProgress unexpectedly succeeded")
	} else if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("DeliverNextProgress error = %v, want dispatch timeout", err)
	}
	if got, want := wrapper.markLeavingCallCount(), 1; got != want {
		t.Fatalf("mark-leaving calls after timeout = %d, want %d", got, want)
	}
	if got, want := h.server.Pending()[slot].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after ready progress timeout = %q, want %q", got, want)
	}
	if !runtimeOutboxHasSlot(h.server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox missing repaired slot %d after mark-leaving timeout", slot)
	}

	if err := h.server.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if got, want := wrapper.markLeavingCallCount(), 2; got != want {
		t.Fatalf("mark-leaving calls after retry = %d, want %d", got, want)
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}

	assertActiveReplicaSet(t, h.server.Current().Cluster.Chains[slot], "a", "b", "d")
	if runtimeOutboxHasSlot(h.server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, h.server.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, h.server, h.adapters, slot, "timeout-mark-leaving", "v2")
}

func TestMarkLeavingPartialOutboxSuccessThenRetryDoesNotRedispatchCompletedPeerUpdates(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		DispatchTimeout:       time.Nanosecond,
		DispatchRetryInterval: time.Hour,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})
	h.adapters["d"].EnableQueuedProgress()

	if _, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.server.Pending(), "d", pendingKindReady)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}

	leavingNodeID := lastActiveReplicaNode(h.server.Current().Cluster.Chains[slot])
	if leavingNodeID == "" {
		t.Fatal("failed to determine leaving node before partial mark-leaving timeout")
	}
	updateNodes := activeAfterNodeIDs(activeServingChain(h.server.Current().Cluster.Chains[slot]), map[string]bool{leavingNodeID: true})
	if len(updateNodes) == 0 {
		t.Fatal("no peer-update nodes found before mark-leaving timeout")
	}
	updateWrapper := newFaultInjectingNodeClient(h.adapters[updateNodes[0]])
	h.server.nodes[updateNodes[0]] = updateWrapper
	leavingWrapper := newFaultInjectingNodeClient(h.adapters[leavingNodeID])
	leavingWrapper.markLeavingTimeouts = 1
	h.server.nodes[leavingNodeID] = leavingWrapper

	if err := h.adapters["d"].DeliverNextProgress(ctx); err == nil {
		t.Fatal("DeliverNextProgress unexpectedly succeeded")
	} else if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("DeliverNextProgress error = %v, want dispatch timeout", err)
	}
	if got, want := updateWrapper.updatePeersCallCount(), 1; got != want {
		t.Fatalf("update-peers calls after partial mark-leaving success = %d, want %d", got, want)
	}
	if got, want := leavingWrapper.markLeavingCallCount(), 1; got != want {
		t.Fatalf("mark-leaving calls after timeout = %d, want %d", got, want)
	}

	if err := h.server.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if got, want := updateWrapper.updatePeersCallCount(), 1; got != want {
		t.Fatalf("update-peers calls after retry = %d, want no duplicate", got)
	}
	if got, want := leavingWrapper.markLeavingCallCount(), 2; got != want {
		t.Fatalf("mark-leaving calls after retry = %d, want %d", got, want)
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	assertSlotRoundTrip(t, ctx, h.server, h.adapters, slot, "partial-mark-leaving", "v-partial-2")
}

func TestLivenessTriggeredDeadRepairTimeoutThenRetryCompletesRepairAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		Clock:                 clock,
		DispatchTimeout:       time.Nanosecond,
		DispatchRetryInterval: time.Hour,
		LivenessPolicy:        LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}

	wrapper := newFaultInjectingNodeClient(h.adapters["d"])
	wrapper.addTailTimeouts = 1
	h.server.nodes["d"] = wrapper

	if err := h.server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	clock.Advance(11 * time.Second)
	if err := h.server.EvaluateLiveness(ctx); err == nil {
		t.Fatal("EvaluateLiveness unexpectedly succeeded")
	} else if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("EvaluateLiveness error = %v, want dispatch timeout", err)
	}
	if got, want := wrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after liveness timeout = %d, want %d", got, want)
	}
	slot := mustPendingSlotForNode(t, h.server.Pending(), "d", pendingKindReady)
	if got, want := h.server.Liveness()["b"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("liveness state = %q, want %q", got, want)
	}

	if err := h.server.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if got, want := wrapper.addTailCallCount(), 2; got != want {
		t.Fatalf("add-tail calls after retry = %d, want %d", got, want)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}

	assertActiveReplicaSet(t, h.server.Current().Cluster.Chains[slot], "a", "c", "d")
	if runtimeOutboxHasSlot(h.server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, h.server.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, h.server, h.adapters, slot, "timeout-liveness-retry", "v3")
}

type blockingNodeClient struct {
	nodeID           string
	blockAddTail     bool
	blockUpdatePeers bool
	blockMarkLeaving bool
	blockResume      bool
	blockRecover     bool
	blockDrop        bool
}

func newBlockingNodeClient(nodeID string) *blockingNodeClient {
	return &blockingNodeClient{nodeID: nodeID}
}

func (c *blockingNodeClient) AddReplicaAsTail(ctx context.Context, _ storage.AddReplicaAsTailCommand) error {
	if c.blockAddTail {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) ActivateReplica(context.Context, storage.ActivateReplicaCommand) error {
	return nil
}

func (c *blockingNodeClient) MarkReplicaLeaving(ctx context.Context, _ storage.MarkReplicaLeavingCommand) error {
	if c.blockMarkLeaving {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) RemoveReplica(context.Context, storage.RemoveReplicaCommand) error {
	return nil
}

func (c *blockingNodeClient) UpdateChainPeers(ctx context.Context, _ storage.UpdateChainPeersCommand) error {
	if c.blockUpdatePeers {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) ResumeRecoveredReplica(ctx context.Context, _ storage.ResumeRecoveredReplicaCommand) error {
	if c.blockResume {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) RecoverReplica(ctx context.Context, _ storage.RecoverReplicaCommand) error {
	if c.blockRecover {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (c *blockingNodeClient) DropRecoveredReplica(ctx context.Context, _ storage.DropRecoveredReplicaCommand) error {
	if c.blockDrop {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func mapBlockingToClient(nodes map[string]*blockingNodeClient) map[string]StorageNodeClient {
	cloned := make(map[string]StorageNodeClient, len(nodes))
	for nodeID, node := range nodes {
		cloned[nodeID] = node
	}
	return cloned
}

func mustBootstrappedBlockingServer(
	t *testing.T,
	ctx context.Context,
	nodes map[string]*blockingNodeClient,
	cfg ServerConfig,
	slotCount int,
	replicationFactor int,
	nodeIDs ...string,
) *Server {
	t.Helper()
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), mapBlockingToClient(nodes), cfg)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor, nodeIDs...)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	return server
}
