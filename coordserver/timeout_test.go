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

func TestStartupMaxChangedChainsExpandsInitialEmptyClusterWave(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
		ReconfigurationPolicy: coordinator.ReconfigurationPolicy{
			MaxChangedChains: 32,
		},
		StartupMaxChangedChains: 64,
	})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 64, 3)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	for _, nodeID := range []string{"a", "b", "c"} {
		node := uniqueNode(nodeID)
		if _, err := h.server.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         nodeID,
			RPCAddress:     node.RPCAddress,
			FailureDomains: node.FailureDomains,
		}); err != nil {
			t.Fatalf("RegisterNode(%q) returned error: %v", nodeID, err)
		}
		if err := h.server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: nodeID}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(%q) returned error: %v", nodeID, err)
		}
	}
	if got, want := len(h.server.Pending()), 64; got != want {
		t.Fatalf("len(Pending()) = %d, want %d", got, want)
	}
}

func TestStartupMaxChangedChainsDisablesAfterSettledBootstrap(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
		ReconfigurationPolicy: coordinator.ReconfigurationPolicy{
			MaxChangedChains: 32,
		},
		StartupMaxChangedChains: 64,
	})
	cmd := bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")
	cmd.Bootstrap.Policy = coordinator.ReconfigurationPolicy{MaxChangedChains: 32}
	if _, err := h.server.Bootstrap(ctx, cmd); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})
	if got, want := h.server.reconfigurationPolicy().MaxChangedChains, 32; got != want {
		t.Fatalf("reconfigurationPolicy().MaxChangedChains = %d, want %d", got, want)
	}
}

func TestCloudShapeSecondWaveDispatchDrains(t *testing.T) {
	if os.Getenv("CRAQ_RUN_BENCHMARK_SOAK_LOCAL") == "" {
		t.Skip("set CRAQ_RUN_BENCHMARK_SOAK_LOCAL=1 to run the cloud-shape dispatch drain test")
	}

	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
		ReconfigurationPolicy: coordinator.ReconfigurationPolicy{
			MaxChangedChains: 32,
		},
		StartupMaxChangedChains: 1024,
	})
	for _, adapter := range h.adapters {
		adapter.BindServer(h.server)
	}

	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1024, 3)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	for _, nodeID := range []string{"a", "b", "c"} {
		node := uniqueNode(nodeID)
		if _, err := h.server.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         nodeID,
			RPCAddress:     node.RPCAddress,
			FailureDomains: node.FailureDomains,
		}); err != nil {
			t.Fatalf("RegisterNode(%q) returned error: %v", nodeID, err)
		}
		if err := h.server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: nodeID}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(%q) returned error: %v", nodeID, err)
		}
	}

	waitForCondition(t, 30*time.Second, func() bool {
		h.server.runBackgroundDispatchOnce()
		return len(h.server.Pending()) == 1024
	}, "first-wave pending work to be scheduled")

	firstWavePending := h.server.Pending()
	for slot, pending := range firstWavePending {
		if err := h.adapters[pending.NodeID].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(node=%q, slot=%d) returned error: %v", pending.NodeID, slot, err)
		}
	}

	waitForCondition(t, 30*time.Second, func() bool {
		current := h.server.Current()
		return len(current.PendingBySlot) == 0 && len(current.Outbox) == 0
	}, "first-wave work to drain")

	if err := h.server.reconcileState(ctx); err != nil {
		t.Fatalf("reconcileState returned error: %v", err)
	}
	current := h.server.Current()
	if got, want := len(current.PendingBySlot), 1024; got != want {
		t.Fatalf("len(current.PendingBySlot) after second-wave reconcile = %d, want %d", got, want)
	}
	if got, want := len(current.Outbox), 1024; got != want {
		t.Fatalf("len(current.Outbox) after second-wave reconcile = %d, want %d", got, want)
	}

	dispatchCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	if err := h.server.dispatchRuntimeOutbox(dispatchCtx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	current = h.server.Current()
	if got := len(current.Outbox); got != 0 {
		t.Fatalf("len(current.Outbox) after second-wave dispatch = %d, want 0", got)
	}
}

func TestAsyncStartupLeavesFirstWaveAndMakesForwardProgress(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
		ReconfigurationPolicy: coordinator.ReconfigurationPolicy{
			MaxChangedChains: 3,
		},
	})
	for _, adapter := range h.adapters {
		adapter.BindServer(h.server)
	}

	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 12, 3)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	updateWrappers := map[string]*faultInjectingNodeClient{}
	for _, nodeID := range []string{"a", "b", "c"} {
		wrapper := newFaultInjectingNodeClient(h.adapters[nodeID])
		h.server.nodes[nodeID] = wrapper
		updateWrappers[nodeID] = wrapper
		node := uniqueNode(nodeID)
		if _, err := h.server.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         nodeID,
			RPCAddress:     node.RPCAddress,
			FailureDomains: node.FailureDomains,
		}); err != nil {
			t.Fatalf("RegisterNode(%q) returned error: %v", nodeID, err)
		}
		if err := h.server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: nodeID}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(%q) returned error: %v", nodeID, err)
		}
	}

	waitForCondition(t, 2*time.Second, func() bool {
		h.server.runBackgroundDispatchOnce()
		return len(h.server.Pending()) > 0
	}, "first-wave pending work to be scheduled")

	firstWavePending := h.server.Pending()
	for slot, pending := range firstWavePending {
		if pending.Kind != pendingKindReady {
			t.Fatalf("pending[%d] kind = %q, want %q", slot, pending.Kind, pendingKindReady)
		}
		if err := h.adapters[pending.NodeID].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(node=%q, slot=%d) returned error: %v", pending.NodeID, slot, err)
		}
	}

	stuckVersion := h.server.Current().Version
	waitForCondition(t, 2*time.Second, func() bool {
		h.server.runBackgroundDispatchOnce()
		current := h.server.Current()
		snapshot, err := h.server.RoutingSnapshot(ctx)
		if err != nil {
			t.Fatalf("RoutingSnapshot returned error: %v", err)
		}
		if current.Version > stuckVersion {
			return true
		}
		if len(current.Outbox) > 0 || len(current.PendingBySlot) > 0 {
			return true
		}
		readable := 0
		for _, route := range snapshot.Slots {
			if route.Readable {
				readable++
			}
		}
		return readable > 0
	}, "post-first-wave coordinator progress")

	current := h.server.Current()
	snapshot, err := h.server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after progress returned error: %v", err)
	}
	readable := 0
	for _, route := range snapshot.Slots {
		if route.Readable {
			readable++
		}
	}
	if current.Version == stuckVersion && len(current.Outbox) == 0 && len(current.PendingBySlot) == 0 && readable == 0 {
		t.Fatalf(
			"coordinator stalled after first wave: version=%d outbox=%d pending=%d active_peer_refresh=%d readable=%d",
			current.Version,
			len(current.Outbox),
			len(current.PendingBySlot),
			len(h.server.snapshotActivePeerRefreshSlots()),
			readable,
		)
	}

	updateCalls := 0
	for _, wrapper := range updateWrappers {
		updateCalls += wrapper.updatePeersCallCount()
	}
	if updateCalls == 0 && readable == 0 && len(current.PendingBySlot) == 0 && len(current.Outbox) == 0 {
		t.Fatalf("expected peer refresh or new repair work after first wave, but no UpdateChainPeers calls were attempted")
	}
}

func TestAsyncRemovedProgressKeepsRoutingBlockedUntilPeerRefreshRetrySucceeds(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
		DispatchTimeout:       5 * time.Millisecond,
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
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(ready) returned error: %v", err)
	}
	h.server.runBackgroundDispatchOnce()

	leavingNodeID := replicaNodeWithState(h.server.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after ready progress")
	}
	h.adapters[leavingNodeID].EnableQueuedProgress()
	updateNodes := activeAfterNodeIDs(activeServingChain(h.server.Current().Cluster.Chains[slot]), map[string]bool{leavingNodeID: true})
	if len(updateNodes) == 0 {
		t.Fatal("no active peer-update targets found after ready progress")
	}
	updateWrapper := newFaultInjectingNodeClient(h.adapters[updateNodes[0]])
	updateWrapper.updatePeersTimeouts = 1
	h.server.nodes[updateNodes[0]] = updateWrapper

	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	if err := h.adapters[leavingNodeID].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(removed) returned error: %v", err)
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for updateWrapper.updatePeersCallCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := updateWrapper.updatePeersCallCount(); got == 0 {
		t.Fatal("expected async peer refresh dispatch to attempt UpdateChainPeers")
	}

	snapshot, err := h.server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot while peer refresh is queued returned error: %v", err)
	}
	if snapshot.Slots[slot].Writable || snapshot.Slots[slot].Readable {
		t.Fatalf("route while peer refresh retry is outstanding = %#v, want blocked", snapshot.Slots[slot])
	}

	h.server.nodes[updateNodes[0]] = h.adapters[updateNodes[0]]
	h.server.runBackgroundDispatchOnce()

	snapshot, err = h.server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after peer refresh retry returned error: %v", err)
	}
	if !snapshot.Slots[slot].Writable || !snapshot.Slots[slot].Readable {
		t.Fatalf("route after peer refresh retry = %#v, want readable+writable", snapshot.Slots[slot])
	}
	assertSlotRoundTrip(t, ctx, h.server, h.adapters, slot, "queued-peer-refresh", "v1")
}

func TestAsyncStartupReadyKeepsRoutingWritableViaPreviousServingChainWhilePeerRefreshRetries(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchRetryInterval: time.Hour,
		DispatchTimeout:       5 * time.Millisecond,
	})
	nodesByID := map[string]coordinator.Node{
		"a": uniqueNode("a"),
		"b": uniqueNode("b"),
		"c": uniqueNode("c"),
	}
	stableChain := coordinator.Chain{
		Slot: 0,
		Replicas: []coordinator.Replica{
			{NodeID: "a", State: coordinator.ReplicaStateActive},
			{NodeID: "b", State: coordinator.ReplicaStateActive},
		},
	}
	joiningChain := coordinator.Chain{
		Slot: 0,
		Replicas: []coordinator.Replica{
			{NodeID: "a", State: coordinator.ReplicaStateActive},
			{NodeID: "b", State: coordinator.ReplicaStateActive},
			{NodeID: "c", State: coordinator.ReplicaStateJoining},
		},
	}
	for _, nodeID := range []string{"a", "b"} {
		assignment, err := assignmentForNode(stableChain, nodesByID, nodeID, 2)
		if err != nil {
			t.Fatalf("assignmentForNode(stable, %s) returned error: %v", nodeID, err)
		}
		if err := h.adapters[nodeID].Node().AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
			t.Fatalf("AddReplicaAsTail(%s, stable) returned error: %v", nodeID, err)
		}
		if err := h.adapters[nodeID].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("ActivateReplica(%s, stable) returned error: %v", nodeID, err)
		}
	}
	joiningAssignment, err := assignmentForNode(joiningChain, nodesByID, "c", 2)
	if err != nil {
		t.Fatalf("assignmentForNode(joining, c) returned error: %v", err)
	}
	if err := h.adapters["c"].Node().AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: joiningAssignment}); err != nil {
		t.Fatalf("AddReplicaAsTail(c, joining) returned error: %v", err)
	}

	h.server.replaceRuntime(coordruntime.OpenInMemoryFromState(coordruntime.State{
		Version:      2,
		LastLogIndex: 2,
		Cluster: coordinator.ClusterState{
			Chains:            []coordinator.Chain{joiningChain},
			NodesByID:         nodesByID,
			NodeHealthByID:    map[string]coordinator.NodeHealth{"a": coordinator.NodeHealthAlive, "b": coordinator.NodeHealthAlive, "c": coordinator.NodeHealthAlive},
			ReadyNodeIDs:      map[string]bool{"a": true, "b": true, "c": true},
			DrainingNodeIDs:   map[string]bool{},
			NodeOrder:         []string{"a", "b", "c"},
			SlotCount:         1,
			ReplicationFactor: 3,
		},
		SlotVersions: map[int]uint64{0: 2},
		NodeLivenessByID: map[string]coordruntime.NodeLivenessRecord{
			"a": {State: coordruntime.NodeLivenessStateHealthy, LastStatus: storage.NodeStatus{NodeID: "a", ReplicaCount: 1, ActiveCount: 1}},
			"b": {State: coordruntime.NodeLivenessStateHealthy, LastStatus: storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}},
			"c": {State: coordruntime.NodeLivenessStateHealthy, LastStatus: storage.NodeStatus{NodeID: "c", ReplicaCount: 1, CatchingUpCount: 1}},
		},
		PendingBySlot: map[int]coordruntime.PendingWork{
			0: {Slot: 0, NodeID: "c", Kind: coordruntime.PendingKindReady, SlotVersion: 2},
		},
		CompletedProgressBySlot: map[int][]coordruntime.CompletedProgressRecord{},
		Outbox:                  []coordruntime.OutboxEntry{},
		AppliedCommands:         map[string]coordruntime.AppliedCommand{},
		LastPolicy:              coordinator.ReconfigurationPolicy{MaxChangedChains: 32},
	}))
	h.server.syncViewsFromRuntime()
	h.server.rebuildRoutingSnapshot()

	before, err := h.server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot before ready returned error: %v", err)
	}
	if before.Slots[0].Writable || !before.Slots[0].Readable {
		t.Fatalf("route before ready = %#v, want readable but not writable while the tail is still joining", before.Slots[0])
	}

	updateWrapper := newFaultInjectingNodeClient(h.adapters["a"])
	updateWrapper.updatePeersTimeouts = 1
	h.server.nodes["a"] = updateWrapper

	if _, err := h.server.ReportReplicaReady(ctx, "c", 0, 0, "ready-c"); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	waitForCondition(t, 250*time.Millisecond, func() bool {
		return updateWrapper.updatePeersCallCount() > 0
	}, "startup peer refresh dispatch after ready progress")

	current := h.server.Current()
	if got, want := replicaNodeStates(current.Cluster.Chains[0]), []string{"a:active", "b:active", "c:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("current chain after ready = %v, want %v", got, want)
	}

	after, err := h.server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot during peer refresh retry returned error: %v", err)
	}
	if !after.Slots[0].Writable || !after.Slots[0].Readable {
		t.Fatalf("route during startup peer refresh retry = %#v, want readable+writable via previous serving chain", after.Slots[0])
	}
	if got, want := after.Slots[0].ReadReplicas, before.Slots[0].ReadReplicas; !reflect.DeepEqual(got, want) {
		t.Fatalf("route during startup peer refresh retry = %#v, want previous serving chain %#v", got, want)
	}
	if got, want := after.Slots[0].ChainVersion, h.server.Current().SlotVersions[0]; got != want {
		t.Fatalf("route chain version during startup peer refresh retry = %d, want %d", got, want)
	}
}

func requireBenchmarkCloudShapeSoak(t *testing.T) {
	t.Helper()
	if os.Getenv("CRAQ_RUN_BENCHMARK_SOAK_LOCAL") == "" {
		t.Skip("set CRAQ_RUN_BENCHMARK_SOAK_LOCAL=1 to run the local cloud-shape benchmark soak")
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
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
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{
		AsyncHotPathDispatch:  true,
		DispatchTimeout:       time.Nanosecond,
		DispatchRetryInterval: time.Hour,
	}, 1, 3, "a", "b", "c", "d")

	_, err := server.BeginDrainNode(ctx, reconfigureCommand("drain-b", 1, coordinator.Event{
		Kind:   coordinator.EventKindBeginDrainNode,
		NodeID: "b",
	}, coordinator.ReconfigurationPolicy{}))
	if err != nil {
		t.Fatalf("BeginDrainNode returned error: %v", err)
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
		AsyncHotPathDispatch:  true,
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
	h.server.runBackgroundDispatchOnce()
	leavingNodeID := replicaNodeWithState(h.server.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after retry activation")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	h.server.runBackgroundDispatchOnce()

	assertActiveReplicaSet(t, h.server.Current().Cluster.Chains[slot], "a", "b", "d")
	if runtimeOutboxHasSlot(h.server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, h.server.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, h.server, h.adapters, slot, "timeout-add-tail", "v1")
}

func TestAddNodePartialOutboxSuccessThenRetryDoesNotRedispatchCompletedPeerUpdates(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		AsyncHotPathDispatch:  true,
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
	addTailWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	h.server.nodes["d"] = addTailWrapper
	updateWrappers := map[string]*faultInjectingNodeClient{
		"a": newFaultInjectingNodeClient(h.adapters["a"]),
		"b": newFaultInjectingNodeClient(h.adapters["b"]),
		"c": newFaultInjectingNodeClient(h.adapters["c"]),
	}
	for nodeID, wrapper := range updateWrappers {
		h.server.nodes[nodeID] = wrapper
	}

	_, err = h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if got, want := addTailWrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after partial success = %d, want %d", got, want)
	}
	predictedCluster := h.server.Current().Cluster
	predictedCluster.Chains = append([]coordinator.Chain(nil), predictedCluster.Chains...)
	predictedChain := cloneCoordinatorChain(predictedCluster.Chains[slot])
	for i := range predictedChain.Replicas {
		if predictedChain.Replicas[i].NodeID == "d" {
			predictedChain.Replicas[i].State = coordinator.ReplicaStateActive
		}
	}
	predictedCluster.Chains[slot] = predictedChain
	readyPlan, err := coordinator.PlanReconfiguration(predictedCluster, nil, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})
	if err != nil {
		t.Fatalf("PlanReconfiguration after ready returned error: %v", err)
	}
	updateNodes := activeAfterNodeIDs(activeServingChain(readyPlan.ChangedSlots[0].After), map[string]bool{})
	if len(updateNodes) < 2 {
		t.Fatalf("update node count after ready preview = %d, want at least 2 for partial-success replay", len(updateNodes))
	}
	preReadyNodes := activeAfterNodeIDs(activeServingChain(h.server.Current().Cluster.Chains[slot]), map[string]bool{})
	waitDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(waitDeadline) {
		ready := true
		for _, nodeID := range preReadyNodes {
			if nodeID == "d" {
				continue
			}
			if updateWrappers[nodeID].updatePeersCallCount() == 0 {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got, want := h.server.Pending()[slot].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after partial success = %q, want %q", got, want)
	}
	retryNodeID := updateNodes[len(updateNodes)-1]
	switch retryNodeID {
	case "d":
		addTailWrapper.updatePeersTimeouts = 1
	default:
		updateWrappers[retryNodeID].updatePeersTimeouts = 1
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	peerUpdateCalls := func(nodeID string) int {
		if nodeID == "d" {
			return addTailWrapper.updatePeersCallCount()
		}
		return updateWrappers[nodeID].updatePeersCallCount()
	}
	baselineCalls := make(map[string]int, len(updateNodes))
	for _, nodeID := range updateNodes {
		baselineCalls[nodeID] = peerUpdateCalls(nodeID)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		chain := h.server.Current().Cluster.Chains[slot]
		if replicaNodeWithState(chain, coordinator.ReplicaStateLeaving) == "" {
			return false
		}
		for _, nodeID := range updateNodes {
			if peerUpdateCalls(nodeID) < baselineCalls[nodeID]+1 {
				return false
			}
		}
		return true
	}, "ready progress plus first peer refresh wave")
	h.server.runBackgroundDispatchOnce()
	h.server.runBackgroundDispatchOnce()
	chain := h.server.Current().Cluster.Chains[slot]
	leavingNodeID := replicaNodeWithState(chain, coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after partial-success retry activation")
	}
	activeAfterReady := activeAfterNodeIDs(activeServingChain(chain), map[string]bool{})
	if !reflect.DeepEqual(activeAfterReady, updateNodes) {
		t.Fatalf("active nodes after ready = %v, want preview update nodes %v", activeAfterReady, updateNodes)
	}
	for _, nodeID := range updateNodes[:len(updateNodes)-1] {
		if got, want := peerUpdateCalls(nodeID), baselineCalls[nodeID]+1; got != want {
			t.Fatalf("update-peers calls for %q after settled retry = %d, want %d", nodeID, got, want)
		}
	}
	retryCalls := peerUpdateCalls(retryNodeID)
	if got, want := retryCalls, baselineCalls[retryNodeID]+2; got != want {
		t.Fatalf("update-peers calls for retry target after settled retry = %d, want %d", got, want)
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	h.server.runBackgroundDispatchOnce()
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

	leavingNodeID := plannedLeavingNodeAfterReady(t, h.server, slot, "d")
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

	leavingNodeID := plannedLeavingNodeAfterReady(t, h.server, slot, "d")
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
