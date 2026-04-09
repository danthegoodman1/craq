package coordserver

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func TestHAFailoverRepairHistoryIsDeterministic(t *testing.T) {
	left := runHAFailoverRepairHistory(t)
	right := runHAFailoverRepairHistory(t)

	if !reflect.DeepEqual(left.finalState, right.finalState) {
		t.Fatalf("final state mismatch\nleft=%#v\nright=%#v", left.finalState, right.finalState)
	}
	if !reflect.DeepEqual(left.pending, right.pending) {
		t.Fatalf("pending mismatch\nleft=%#v\nright=%#v", left.pending, right.pending)
	}
	if !reflect.DeepEqual(left.routing, right.routing) {
		t.Fatalf("routing mismatch\nleft=%#v\nright=%#v", left.routing, right.routing)
	}
	if !reflect.DeepEqual(left.slotSnapshot, right.slotSnapshot) {
		t.Fatalf("slot snapshot mismatch\nleft=%#v\nright=%#v", left.slotSnapshot, right.slotSnapshot)
	}
}

func TestHAFailoverAcceptsInFlightProgressFromPriorEpoch(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["d"].EnableQueuedProgress()

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)
	h.mustStepLeader(t)

	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := h.adapters["d"].PendingProgress(), 1; got != want {
		t.Fatalf("queued ready progress = %d, want %d", got, want)
	}
	pendingBefore := h.leader.Pending()[slot]
	if pendingBefore.Epoch == 0 {
		t.Fatal("pending epoch before failover = 0, want active epoch")
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)

	pendingAfter := h.standby.Pending()[slot]
	if got, want := pendingAfter.Epoch, pendingBefore.Epoch; got != want {
		t.Fatalf("pending epoch after failover = %d, want %d", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress returned error: %v", err)
	}
	if got, want := h.standby.Pending()[slot].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after delivered ready = %q, want %q", got, want)
	}
	h.mustStepStandby(t)
	leavingNodeID := replicaNodeWithState(h.standby.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after ready-progress failover")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	assertActiveReplicaSet(t, h.standby.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := h.standby.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending after delivered ready completion = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(h.standby.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, h.standby.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, slot, "ha-ready-failover", "v1")
}

func TestHADuplicateAddTailDispatchAfterFailoverIsSafe(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)

	entry := h.mustFindOutbox(t, OutboxCommandAddReplicaAsTail, "d", slot)
	if err := h.leader.dispatchOutboxEntry(ctx, entry); err != nil {
		t.Fatalf("manual dispatchOutboxEntry(add tail) returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	h.mustStepStandby(t)

	replica := h.adapters["d"].Node().State().Replicas[slot]
	if got, want := replica.State, storage.ReplicaStateCatchingUp; got != want {
		t.Fatalf("replica state after replayed add tail = %q, want %q", got, want)
	}
	if got, want := h.standby.Pending()[slot].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after replayed add tail = %q, want %q", got, want)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	h.mustStepStandby(t)
	leavingNodeID := replicaNodeWithState(h.standby.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after replayed add-tail activation")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	assertActiveReplicaSet(t, h.standby.Current().Cluster.Chains[slot], "a", "b", "d")
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, slot, "ha-dispatch-before-ack", "v2")
}

func TestHADuplicateMarkLeavingDispatchAfterFailoverIsSafe(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)
	h.mustStepLeader(t)
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	leavingNodeID := replicaNodeWithState(h.leader.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node before mark-leaving replay")
	}
	entry := h.mustFindOutbox(t, OutboxCommandMarkReplicaLeaving, leavingNodeID, slot)
	if err := h.leader.dispatchOutboxEntry(ctx, entry); err != nil {
		t.Fatalf("manual dispatchOutboxEntry(mark leaving) returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	h.mustStepStandby(t)

	replica := h.adapters[leavingNodeID].Node().State().Replicas[slot]
	if got, want := replica.State, storage.ReplicaStateLeaving; got != want {
		t.Fatalf("replica state after replayed mark leaving = %q, want %q", got, want)
	}
	if got, want := h.standby.Pending()[slot].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after replayed mark leaving = %q, want %q", got, want)
	}
}

func TestHAFlapHistorySurvivesFailoverAndEvictsNode(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    10 * time.Second,
			FlapThreshold: 2,
		},
	})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
	}

	h.clock.Advance(6 * time.Second)
	if err := h.leader.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("leader EvaluateLiveness returned error: %v", err)
	}
	if got, want := h.leader.Liveness()["c"].State, coordruntime.NodeLivenessStateSuspect; got != want {
		t.Fatalf("leader state after first suspect = %q, want %q", got, want)
	}
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(c) recovery returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)

	h.clock.Advance(6 * time.Second)
	if err := h.standby.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("standby EvaluateLiveness returned error: %v", err)
	}
	if got, want := h.standby.Liveness()["c"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("standby state after flap eviction = %q, want %q", got, want)
	}
	if !nodeMarkedDead(h.standby.Current().Cluster, "c") {
		t.Fatal("node c was not tombstoned after HA flap eviction")
	}
	if got, want := h.standby.Pending()[0].NodeID, "d"; got != want {
		t.Fatalf("pending replacement node after HA flap eviction = %q, want %q", got, want)
	}
}

func TestHARestartResumesUndispatchedOutboxWork(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)

	if err := h.leader.Close(); err != nil {
		t.Fatalf("leader.Close returned error: %v", err)
	}
	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	h.mustStepStandby(t)

	if got, want := h.standby.Pending()[slot].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after standby resume = %q, want %q", got, want)
	}
	replica := h.adapters["d"].Node().State().Replicas[slot]
	if got, want := replica.State, storage.ReplicaStateCatchingUp; got != want {
		t.Fatalf("replica state after standby resume = %q, want %q", got, want)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	h.mustStepStandby(t)
	leavingNodeID := replicaNodeWithState(h.standby.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after standby resumed outbox work")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	assertActiveReplicaSet(t, h.standby.Current().Cluster.Chains[slot], "a", "b", "d")
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, slot, "ha-before-dispatch", "v3")
}

func TestHAFailoverAcceptsInFlightRemovedProgressAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["c"].EnableQueuedProgress()

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)
	h.mustStepLeader(t)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	h.mustStepLeader(t)
	leavingNodeID := replicaNodeWithState(h.leader.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node before removed-progress failover")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if got, want := h.adapters[leavingNodeID].PendingProgress(), 1; got != want {
		t.Fatalf("queued removed progress = %d, want %d", got, want)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)

	if err := h.adapters[leavingNodeID].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(removed) returned error: %v", err)
	}
	assertActiveReplicaSet(t, h.standby.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := h.standby.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending after removed failover = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(h.standby.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, h.standby.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, slot, "ha-removed-failover", "v4")
}

func TestHAFailoverAfterAddTailAckBeforeReadyProgressDoesNotRedispatchAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})
	wrapper := newFaultInjectingNodeClient(h.adapters["d"])

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	h.leader.nodes["d"] = wrapper
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	h.mustStepLeader(t)
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)
	if got, want := wrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls before failover = %d, want %d", got, want)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	h.standby.nodes["d"] = wrapper
	if got, want := wrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after failover = %d, want no redispatch", got)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	h.mustStepStandby(t)
	leavingNodeID := replicaNodeWithState(h.standby.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after HA post-ack failover")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if got, want := wrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after completion = %d, want no redispatch", got)
	}
	assertActiveReplicaSet(t, h.standby.Current().Cluster.Chains[slot], "a", "b", "d")
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, slot, "ha-after-add-ack", "v5")
}

func TestHAFailoverAfterPartialAddTailOutboxSuccessRetriesRemainingPeerUpdatesOnly(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})

	preview, err := coordinator.PlanReconfiguration(h.leader.Current().Cluster, []coordinator.Event{uniqueAddNodeEvent("d")}, noBudgetPolicy())
	if err != nil {
		t.Fatalf("PlanReconfiguration preview returned error: %v", err)
	}
	slot := preview.ChangedSlots[0].Slot

	addTailWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	h.leader.nodes["d"] = addTailWrapper
	updateWrappers := map[string]*faultInjectingNodeClient{
		"a": newFaultInjectingNodeClient(h.adapters["a"]),
		"b": newFaultInjectingNodeClient(h.adapters["b"]),
	}
	for nodeID, wrapper := range updateWrappers {
		h.leader.nodes[nodeID] = wrapper
	}

	if _, err = h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	snapshot, err := h.store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot returned error: %v", err)
	}
	addTailEntry := h.mustFindOutbox(t, OutboxCommandAddReplicaAsTail, "d", slot)
	if err := h.leader.dispatchOutboxEntry(ctx, addTailEntry); err != nil {
		t.Fatalf("dispatchOutboxEntry(add tail) returned error: %v", err)
	}
	snapshot.Outbox = removeOutboxEntry(snapshot.Outbox, addTailEntry.ID)
	if err := h.leader.saveHASnapshot(ctx, snapshot.SnapshotVersion, snapshot); err != nil {
		t.Fatalf("saveHASnapshot(after add tail) returned error: %v", err)
	}
	if got, want := addTailWrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after manual dispatch = %d, want %d", got, want)
	}
	snapshot, err = h.store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot after add tail returned error: %v", err)
	}
	for _, entry := range append([]OutboxEntry(nil), snapshot.Outbox...) {
		if entry.Kind != OutboxCommandUpdateChainPeers || entry.Slot != slot {
			continue
		}
		if err := h.leader.dispatchOutboxEntry(ctx, entry); err != nil {
			t.Fatalf("dispatchOutboxEntry(pre-ready update peers %q) returned error: %v", entry.NodeID, err)
		}
		snapshot, err = h.store.LoadSnapshot(ctx)
		if err != nil {
			t.Fatalf("LoadSnapshot after pre-ready update peers %q returned error: %v", entry.NodeID, err)
		}
		snapshot.Outbox = removeOutboxEntry(snapshot.Outbox, entry.ID)
		if err := h.leader.saveHASnapshot(ctx, snapshot.SnapshotVersion, snapshot); err != nil {
			t.Fatalf("saveHASnapshot(after pre-ready update peers %q) returned error: %v", entry.NodeID, err)
		}
	}
	baselineCalls := map[string]int{
		"a": updateWrappers["a"].updatePeersCallCount(),
		"b": updateWrappers["b"].updatePeersCallCount(),
		"d": addTailWrapper.updatePeersCallCount(),
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	chain := h.leader.Current().Cluster.Chains[slot]
	leavingNodeID := replicaNodeWithState(chain, coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after ready progress")
	}
	updateNodes := activeAfterNodeIDs(activeServingChain(chain), map[string]bool{leavingNodeID: true})
	if len(updateNodes) < 2 {
		t.Fatalf("update node count after ready = %d, want at least 2 for HA partial-success replay", len(updateNodes))
	}
	retriedNodeID := updateNodes[len(updateNodes)-1]
	switch retriedNodeID {
	case "d":
		addTailWrapper.updatePeersTimeouts = 1
	default:
		updateWrappers[retriedNodeID].updatePeersTimeouts = 1
	}

	for _, nodeID := range updateNodes[:len(updateNodes)-1] {
		entry := h.mustFindOutbox(t, OutboxCommandUpdateChainPeers, nodeID, slot)
		if err := h.leader.dispatchOutboxEntry(ctx, entry); err != nil {
			t.Fatalf("dispatchOutboxEntry(update peers %q) returned error: %v", nodeID, err)
		}
		snapshot, err = h.store.LoadSnapshot(ctx)
		if err != nil {
			t.Fatalf("LoadSnapshot after update peers %q returned error: %v", nodeID, err)
		}
		snapshot.Outbox = removeOutboxEntry(snapshot.Outbox, entry.ID)
		if err := h.leader.saveHASnapshot(ctx, snapshot.SnapshotVersion, snapshot); err != nil {
			t.Fatalf("saveHASnapshot(after update peers %q) returned error: %v", nodeID, err)
		}
		var got int
		if nodeID == "d" {
			got = addTailWrapper.updatePeersCallCount()
		} else {
			got = updateWrappers[nodeID].updatePeersCallCount()
		}
		want := baselineCalls[nodeID] + 1
		if got != want {
			t.Fatalf("update-peers calls for %q after manual dispatch = %d, want %d", nodeID, got, want)
		}
	}

	entry := h.mustFindOutbox(t, OutboxCommandUpdateChainPeers, retriedNodeID, slot)
	if err := h.leader.dispatchOutboxEntry(ctx, entry); err == nil {
		t.Fatal("dispatchOutboxEntry(update peers retry target) unexpectedly succeeded")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dispatchOutboxEntry(update peers retry target) error = %v, want deadline exceeded", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustBind(t, h.standby)
	h.standby.nodes["d"] = addTailWrapper
	for nodeID, wrapper := range updateWrappers {
		h.standby.nodes[nodeID] = wrapper
	}
	h.mustStepStandby(t)
	if err := h.standby.dispatchOutbox(ctx); err != nil {
		t.Fatalf("standby dispatchOutbox returned error: %v", err)
	}
	if got, want := addTailWrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after failover = %d, want no redispatch", got)
	}
	for _, nodeID := range updateNodes[:len(updateNodes)-1] {
		var got int
		if nodeID == "d" {
			got = addTailWrapper.updatePeersCallCount()
		} else {
			got = updateWrappers[nodeID].updatePeersCallCount()
		}
		want := baselineCalls[nodeID] + 1
		if got != want {
			t.Fatalf("update-peers calls for %q after failover = %d, want %d", nodeID, got, want)
		}
	}
	var retriedCalls int
	if retriedNodeID == "d" {
		retriedCalls = addTailWrapper.updatePeersCallCount()
	} else {
		retriedCalls = updateWrappers[retriedNodeID].updatePeersCallCount()
	}
	if got, want := retriedCalls, baselineCalls[retriedNodeID]+2; got != want {
		t.Fatalf("retry-target update-peers calls after failover = %d, want %d", got, want)
	}
	snapshot, err = h.store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot after failover returned error: %v", err)
	}
	for _, entry := range snapshot.Outbox {
		if entry.Kind == OutboxCommandUpdateChainPeers && entry.Slot == slot && entry.NodeID == retriedNodeID {
			t.Fatalf("retry-target update-peers entry still present after failover dispatch: %#v", entry)
		}
	}
	h.mustStepStandby(t)
	leavingNodeID = replicaNodeWithState(h.standby.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after HA partial-success replay")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, slot, "ha-partial-add-tail", "v6")
}

func TestHAFailoverAfterPartialRecoveryOutboxSuccessRetriesRemainingCommandOnlyAndDuplicateReportIsNoOp(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 1, 2, []string{"a", "b"})
	if _, err := h.adapters["a"].Node().SubmitPut(ctx, 0, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	h.adapters["b"].BindServer(nil)
	if err := h.adapters["b"].Node().AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 9, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail(extra slot) returned error: %v", err)
	}
	if err := h.adapters["b"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("ActivateReplica(extra slot) returned error: %v", err)
	}
	if err := h.adapters["b"].Node().Close(); err != nil {
		t.Fatalf("adapter b Close returned error: %v", err)
	}

	recoveredB, err := OpenInMemoryNodeAdapter(ctx, "b", h.backends["b"], h.adapters["b"].local, h.repl)
	if err != nil {
		t.Fatalf("OpenInMemoryNodeAdapter(recovered b) returned error: %v", err)
	}
	h.adapters["b"] = recoveredB
	h.repl.RegisterNode("b", recoveredB.Node())
	h.mustBind(t, h.leader)
	wrapper := newFaultInjectingNodeClient(recoveredB)
	h.leader.nodes["b"] = wrapper

	report := storage.NodeRecoveryReport{
		NodeID: "b",
		Replicas: []storage.RecoveredReplica{
			{
				Assignment: storage.ReplicaAssignment{
					Slot:         0,
					ChainVersion: 0,
					Role:         storage.ReplicaRoleTail,
					Peers:        storage.ChainPeers{PredecessorNodeID: "a"},
				},
				LastKnownState:           storage.ReplicaStateActive,
				HighestCommittedSequence: 0,
				HasCommittedData:         false,
			},
			{
				Assignment: storage.ReplicaAssignment{
					Slot:         9,
					ChainVersion: 1,
					Role:         storage.ReplicaRoleSingle,
				},
				LastKnownState:           storage.ReplicaStateActive,
				HighestCommittedSequence: 0,
				HasCommittedData:         true,
			},
		},
	}
	if err := h.leader.ReportNodeRecovered(ctx, report); err != nil {
		t.Fatalf("ReportNodeRecovered returned error: %v", err)
	}

	recoverEntry := h.mustFindOutbox(t, OutboxCommandRecoverReplica, "b", 0)
	if err := h.leader.dispatchOutboxEntry(ctx, recoverEntry); err != nil {
		t.Fatalf("dispatchOutboxEntry(recover) returned error: %v", err)
	}
	snapshot, err := h.store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot returned error: %v", err)
	}
	snapshot.Outbox = removeOutboxEntry(snapshot.Outbox, recoverEntry.ID)
	if slots := snapshot.UnavailableReplicas["b"]; slots != nil {
		delete(slots, 0)
		if len(slots) == 0 {
			delete(snapshot.UnavailableReplicas, "b")
		}
	}
	if err := h.leader.saveHASnapshot(ctx, snapshot.SnapshotVersion, snapshot); err != nil {
		t.Fatalf("saveHASnapshot(after recover) returned error: %v", err)
	}
	if got, want := wrapper.recoverCallCount(), 1; got != want {
		t.Fatalf("recover calls after manual dispatch = %d, want %d", got, want)
	}

	wrapper.dropTimeouts = 1
	dropEntry := h.mustFindOutbox(t, OutboxCommandDropRecovered, "b", 9)
	if err := h.leader.dispatchOutboxEntry(ctx, dropEntry); err == nil {
		t.Fatal("dispatchOutboxEntry(drop recovered) unexpectedly succeeded")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("dispatchOutboxEntry(drop recovered) error = %v, want deadline exceeded", err)
	}

	h.clock.Advance(3 * time.Second)
	h.standby.nodes["b"] = wrapper
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	if err := h.standby.dispatchOutbox(ctx); err != nil {
		t.Fatalf("standby dispatchOutbox returned error: %v", err)
	}
	if got, want := wrapper.recoverCallCount(), 1; got != want {
		t.Fatalf("recover calls after failover = %d, want no redispatch", got)
	}
	if got, want := wrapper.dropCallCount(), 2; got != want {
		t.Fatalf("drop calls after failover retry = %d, want %d", got, want)
	}
	if h.standby.nodeHasUnavailableSlots("b") {
		t.Fatal("node b still has unavailable slots after HA recovery replay")
	}
	snapshot, err = h.store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot after failover returned error: %v", err)
	}
	for _, entry := range snapshot.Outbox {
		if entry.NodeID == "b" && isRecoveryOutboxKind(entry.Kind) {
			t.Fatalf("recovery outbox entry still present after failover replay: %#v", entry)
		}
	}
	if _, exists := recoveredB.Node().State().Replicas[9]; exists {
		t.Fatal("stale recovered slot 9 still present after HA recovery replay")
	}
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, 0, "ha-partial-recovery", "v1")

	before := h.standby.Current()
	beforePending := h.standby.Pending()
	beforeRouting, err := h.standby.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot before duplicate report returned error: %v", err)
	}
	recoverCalls := wrapper.recoverCallCount()
	dropCalls := wrapper.dropCallCount()
	if err := h.standby.ReportNodeRecovered(ctx, report); err != nil {
		t.Fatalf("duplicate ReportNodeRecovered after failover returned error: %v", err)
	}
	afterRouting, err := h.standby.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after duplicate report returned error: %v", err)
	}
	if got := h.standby.Current(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state changed on duplicate HA recovery report\ngot=%#v\nwant=%#v", got, before)
	}
	if got := h.standby.Pending(); !reflect.DeepEqual(got, beforePending) {
		t.Fatalf("pending changed on duplicate HA recovery report\ngot=%#v\nwant=%#v", got, beforePending)
	}
	if !reflect.DeepEqual(afterRouting, beforeRouting) {
		t.Fatalf("routing changed on duplicate HA recovery report\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
	}
	if got, want := wrapper.recoverCallCount(), recoverCalls; got != want {
		t.Fatalf("recover calls after duplicate HA report = %d, want %d", got, want)
	}
	if got, want := wrapper.dropCallCount(), dropCalls; got != want {
		t.Fatalf("drop calls after duplicate HA report = %d, want %d", got, want)
	}
}

func TestHAFailoverAfterRecoveryAckBeforeNextStepDoesNotRedispatchRecoveredCommand(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 1, 2, []string{"a", "b"})
	if _, err := h.adapters["a"].Node().SubmitPut(ctx, 0, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	h.adapters["b"].BindServer(nil)
	if err := h.adapters["b"].Node().AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 9, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail(extra slot) returned error: %v", err)
	}
	if err := h.adapters["b"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("ActivateReplica(extra slot) returned error: %v", err)
	}
	if err := h.adapters["b"].Node().Close(); err != nil {
		t.Fatalf("adapter b Close returned error: %v", err)
	}

	recoveredB, err := OpenInMemoryNodeAdapter(ctx, "b", h.backends["b"], h.adapters["b"].local, h.repl)
	if err != nil {
		t.Fatalf("OpenInMemoryNodeAdapter(recovered b) returned error: %v", err)
	}
	h.adapters["b"] = recoveredB
	h.repl.RegisterNode("b", recoveredB.Node())
	h.mustBind(t, h.leader)
	wrapper := newFaultInjectingNodeClient(recoveredB)
	h.leader.nodes["b"] = wrapper

	report := storage.NodeRecoveryReport{
		NodeID: "b",
		Replicas: []storage.RecoveredReplica{
			{
				Assignment: storage.ReplicaAssignment{
					Slot:         0,
					ChainVersion: 0,
					Role:         storage.ReplicaRoleTail,
					Peers:        storage.ChainPeers{PredecessorNodeID: "a"},
				},
				LastKnownState:           storage.ReplicaStateActive,
				HighestCommittedSequence: 0,
				HasCommittedData:         false,
			},
			{
				Assignment: storage.ReplicaAssignment{
					Slot:         9,
					ChainVersion: 1,
					Role:         storage.ReplicaRoleSingle,
				},
				LastKnownState:           storage.ReplicaStateActive,
				HighestCommittedSequence: 0,
				HasCommittedData:         true,
			},
		},
	}
	if err := h.leader.ReportNodeRecovered(ctx, report); err != nil {
		t.Fatalf("ReportNodeRecovered returned error: %v", err)
	}

	recoverEntry := h.mustFindOutbox(t, OutboxCommandRecoverReplica, "b", 0)
	if err := h.leader.dispatchOutboxEntry(ctx, recoverEntry); err != nil {
		t.Fatalf("dispatchOutboxEntry(recover) returned error: %v", err)
	}
	snapshot, err := h.store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot returned error: %v", err)
	}
	snapshot.Outbox = removeOutboxEntry(snapshot.Outbox, recoverEntry.ID)
	if slots := snapshot.UnavailableReplicas["b"]; slots != nil {
		delete(slots, 0)
		if len(slots) == 0 {
			delete(snapshot.UnavailableReplicas, "b")
		}
	}
	if err := h.leader.saveHASnapshot(ctx, snapshot.SnapshotVersion, snapshot); err != nil {
		t.Fatalf("saveHASnapshot(after recover ack) returned error: %v", err)
	}
	if got, want := wrapper.recoverCallCount(), 1; got != want {
		t.Fatalf("recover calls after manual ack = %d, want %d", got, want)
	}

	h.clock.Advance(3 * time.Second)
	h.standby.nodes["b"] = wrapper
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	if err := h.standby.dispatchOutbox(ctx); err != nil {
		t.Fatalf("standby dispatchOutbox returned error: %v", err)
	}
	if got, want := wrapper.recoverCallCount(), 1; got != want {
		t.Fatalf("recover calls after post-ack failover = %d, want no redispatch", got)
	}
	if got, want := wrapper.dropCallCount(), 1; got != want {
		t.Fatalf("drop calls after post-ack failover = %d, want %d", got, want)
	}
	if h.standby.nodeHasUnavailableSlots("b") {
		t.Fatal("node b still has unavailable slots after post-ack HA recovery replay")
	}
	snapshot, err = h.store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot after failover returned error: %v", err)
	}
	for _, entry := range snapshot.Outbox {
		if entry.NodeID == "b" && isRecoveryOutboxKind(entry.Kind) {
			t.Fatalf("recovery outbox entry still present after post-ack failover: %#v", entry)
		}
	}
	if _, exists := recoveredB.Node().State().Replicas[9]; exists {
		t.Fatal("stale recovered slot 9 still present after post-ack HA recovery replay")
	}
	assertSlotRoundTrip(t, ctx, h.standby, h.adapters, 0, "ha-recovery-post-ack", "v1")

	stateBeforeDuplicate := h.standby.Current()
	pendingBeforeDuplicate := h.standby.Pending()
	routingBeforeDuplicate, err := h.standby.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot before duplicate report returned error: %v", err)
	}
	dropCalls := wrapper.dropCallCount()
	if err := h.standby.ReportNodeRecovered(ctx, report); err != nil {
		t.Fatalf("duplicate ReportNodeRecovered after post-ack failover returned error: %v", err)
	}
	routingAfterDuplicate, err := h.standby.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after duplicate report returned error: %v", err)
	}
	if got := h.standby.Current(); !reflect.DeepEqual(got, stateBeforeDuplicate) {
		t.Fatalf("state changed on duplicate HA recovery report after post-ack failover\ngot=%#v\nwant=%#v", got, stateBeforeDuplicate)
	}
	if got := h.standby.Pending(); !reflect.DeepEqual(got, pendingBeforeDuplicate) {
		t.Fatalf("pending changed on duplicate HA recovery report after post-ack failover\ngot=%#v\nwant=%#v", got, pendingBeforeDuplicate)
	}
	if !reflect.DeepEqual(routingAfterDuplicate, routingBeforeDuplicate) {
		t.Fatalf("routing changed on duplicate HA recovery report after post-ack failover\nafter=%#v\nbefore=%#v", routingAfterDuplicate, routingBeforeDuplicate)
	}
	if got, want := wrapper.recoverCallCount(), 1; got != want {
		t.Fatalf("recover calls after duplicate post-ack HA report = %d, want %d", got, want)
	}
	if got, want := wrapper.dropCallCount(), dropCalls; got != want {
		t.Fatalf("drop calls after duplicate post-ack HA report = %d, want %d", got, want)
	}
}

func TestHAMembershipContinuityAfterFailover(t *testing.T) {
	t.Run("register_and_heartbeat_after_failover", func(t *testing.T) {
		ctx := context.Background()
		h := newHAInMemoryHarness(t, []string{"d"})

		h.mustStepLeader(t)
		h.mustBind(t, h.leader)
		if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(d) on leader returned error: %v", err)
		}
		if got, want := len(h.leader.Current().Cluster.NodesByID), 1; got != want {
			t.Fatalf("leader membership size = %d, want %d", got, want)
		}

		h.clock.Advance(3 * time.Second)
		h.mustStepStandby(t)
		h.mustBind(t, h.standby)
		if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(d) on standby returned error: %v", err)
		}
		if got, want := len(h.standby.Current().Cluster.NodesByID), 1; got != want {
			t.Fatalf("standby membership size = %d, want %d", got, want)
		}
		if !h.standby.Current().Cluster.ReadyNodeIDs["d"] {
			t.Fatal("node d not ready after failover heartbeat")
		}
	})

	t.Run("identity_conflict_after_failover", func(t *testing.T) {
		ctx := context.Background()
		h := newHAInMemoryHarness(t, []string{"d"})

		h.mustStepLeader(t)
		h.mustBind(t, h.leader)
		if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		reg := storage.NodeRegistration{
			NodeID:         "d",
			RPCAddress:     "127.0.0.1:7414",
			FailureDomains: cloneFailureDomains(uniqueNode("d").FailureDomains),
		}
		if _, err := h.leader.RegisterNode(ctx, reg); err != nil {
			t.Fatalf("RegisterNode returned error: %v", err)
		}

		h.clock.Advance(3 * time.Second)
		h.mustStepStandby(t)
		h.mustBind(t, h.standby)
		if _, err := h.standby.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         "d",
			RPCAddress:     "127.0.0.1:7999",
			FailureDomains: cloneFailureDomains(reg.FailureDomains),
		}); err == nil {
			t.Fatal("conflicting HA RegisterNode unexpectedly succeeded")
		}
		if err := h.standby.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "d"}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(d) returned error: %v", err)
		}
	})

	t.Run("duplicate_same_identity_register_is_no_op_after_failover", func(t *testing.T) {
		ctx := context.Background()
		h := newHAInMemoryHarness(t, []string{"d"})

		h.mustStepLeader(t)
		h.mustBind(t, h.leader)
		if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		reg := storage.NodeRegistration{
			NodeID:         "d",
			RPCAddress:     "127.0.0.1:7414",
			FailureDomains: cloneFailureDomains(uniqueNode("d").FailureDomains),
		}
		if _, err := h.leader.RegisterNode(ctx, reg); err != nil {
			t.Fatalf("RegisterNode returned error: %v", err)
		}

		h.clock.Advance(3 * time.Second)
		h.mustStepStandby(t)
		h.mustBind(t, h.standby)
		before := h.standby.Current()
		beforePending := h.standby.Pending()
		beforeRouting, err := h.standby.RoutingSnapshot(ctx)
		if err != nil {
			t.Fatalf("RoutingSnapshot before duplicate HA register returned error: %v", err)
		}
		stateAfterDuplicate, err := h.standby.RegisterNode(ctx, reg)
		if err != nil {
			t.Fatalf("duplicate RegisterNode returned error: %v", err)
		}
		afterRouting, err := h.standby.RoutingSnapshot(ctx)
		if err != nil {
			t.Fatalf("RoutingSnapshot after duplicate HA register returned error: %v", err)
		}
		if !reflect.DeepEqual(stateAfterDuplicate, before) {
			t.Fatalf("state returned from duplicate HA RegisterNode changed\ngot=%#v\nwant=%#v", stateAfterDuplicate, before)
		}
		if got := h.standby.Current(); !reflect.DeepEqual(got, before) {
			t.Fatalf("current state changed on duplicate HA RegisterNode\ngot=%#v\nwant=%#v", got, before)
		}
		if got := h.standby.Pending(); !reflect.DeepEqual(got, beforePending) {
			t.Fatalf("pending changed on duplicate HA RegisterNode\ngot=%#v\nwant=%#v", got, beforePending)
		}
		if !reflect.DeepEqual(afterRouting, beforeRouting) {
			t.Fatalf("routing changed on duplicate HA RegisterNode\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
		}
		if err := h.standby.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "d"}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(d) returned error: %v", err)
		}
		if got, want := len(h.standby.Current().Cluster.NodesByID), 1; got != want {
			t.Fatalf("standby membership size after duplicate register = %d, want %d", got, want)
		}
		if got := h.standby.Current().Cluster.NodesByID["d"]; !reflect.DeepEqual(got, before.Cluster.NodesByID["d"]) {
			t.Fatalf("node identity changed after duplicate HA register\ngot=%#v\nwant=%#v", got, before.Cluster.NodesByID["d"])
		}
		if !h.standby.Current().Cluster.ReadyNodeIDs["d"] {
			t.Fatal("node d not ready after duplicate HA register and heartbeat")
		}
	})

	t.Run("tombstone_and_replacement_after_failover", func(t *testing.T) {
		ctx := context.Background()
		h := newHAInMemoryHarness(t, []string{"a", "b", "d"})

		h.mustStepLeader(t)
		h.mustBind(t, h.leader)
		if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, h.leader, 1, 2, []string{"a", "b"})
		current := h.leader.Current()
		if _, err := h.leader.MarkNodeDead(ctx, reconfigureCommand("dead-a", current.Version, coordinator.Event{
			Kind:   coordinator.EventKindMarkNodeDead,
			NodeID: "a",
		}, coordinator.ReconfigurationPolicy{})); err != nil {
			t.Fatalf("MarkNodeDead returned error: %v", err)
		}

		h.clock.Advance(3 * time.Second)
		h.mustStepStandby(t)
		h.mustBind(t, h.standby)
		if _, err := h.standby.RegisterNode(ctx, storage.NodeRegistration{NodeID: "a"}); err == nil {
			t.Fatal("RegisterNode(a) unexpectedly succeeded after HA failover tombstone")
		}
		if err := h.standby.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "a"}); err == nil {
			t.Fatal("ReportNodeHeartbeat(a) unexpectedly succeeded after HA failover tombstone")
		}
		if _, err := h.standby.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         "d",
			RPCAddress:     "127.0.0.1:7414",
			FailureDomains: cloneFailureDomains(uniqueNode("d").FailureDomains),
		}); err != nil {
			t.Fatalf("RegisterNode(d) returned error: %v", err)
		}
		if err := h.standby.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "d"}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(d) returned error: %v", err)
		}
		if !h.standby.Current().Cluster.ReadyNodeIDs["d"] {
			t.Fatal("replacement node d not ready after HA failover")
		}
	})
}

func TestHANoopStepDoesNotChurnSettledState(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})

	before := h.leader.Current()
	beforePending := h.leader.Pending()
	beforeRouting, err := h.leader.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	h.mustStepLeader(t)
	afterRouting, err := h.leader.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after StepHA returned error: %v", err)
	}

	if got := h.leader.Current(); !reflect.DeepEqual(got, before) {
		t.Fatalf("current state changed on no-op StepHA\ngot=%#v\nwant=%#v", got, before)
	}
	if got := h.leader.Pending(); !reflect.DeepEqual(got, beforePending) {
		t.Fatalf("pending changed on no-op StepHA\ngot=%#v\nwant=%#v", got, beforePending)
	}
	if !reflect.DeepEqual(afterRouting, beforeRouting) {
		t.Fatalf("routing changed on no-op StepHA\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
	}
}

func TestHAConcurrentHeartbeatsAndStepHandoffsConvergeAfterFailover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["c"].EnableQueuedProgress()
	h.adapters["d"].EnableQueuedProgress()

	startTraffic := func(t *testing.T, server *Server) func() {
		t.Helper()
		done := make(chan struct{})
		errCh := make(chan error, 16)
		var wg sync.WaitGroup
		for _, nodeID := range []string{"a", "b"} {
			nodeID := nodeID
			wg.Add(1)
			go func() {
				defer wg.Done()
				ticker := time.NewTicker(250 * time.Microsecond)
				defer ticker.Stop()
				for {
					select {
					case <-done:
						return
					case <-ticker.C:
						if err := h.adapters[nodeID].Node().ReportHeartbeat(ctx); err != nil &&
							!errors.Is(err, context.Canceled) &&
							!errors.Is(err, context.DeadlineExceeded) &&
							!errors.Is(err, ErrHASnapshotConflict) &&
							!errors.Is(err, ErrNotLeader) {
							errCh <- err
							return
						}
					}
				}
			}()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(250 * time.Microsecond)
			defer ticker.Stop()
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					if _, err := server.RoutingSnapshot(ctx); err != nil &&
						!errors.Is(err, context.Canceled) &&
						!errors.Is(err, context.DeadlineExceeded) &&
						!errors.Is(err, ErrNotLeader) {
						errCh <- err
						return
					}
				}
			}
		}()
		return func() {
			close(done)
			wg.Wait()
			close(errCh)
			for err := range errCh {
				if err != nil {
					t.Fatalf("background HA traffic returned error: %v", err)
				}
			}
		}
	}

	deliverEventually := func(t *testing.T, nodeID string) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			err := h.adapters[nodeID].DeliverNextProgress(ctx)
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("DeliverNextProgress(%s) returned error: %v", nodeID, err)
			}
			if errors.Is(err, ErrHASnapshotConflict) || errors.Is(err, ErrNotLeader) {
				time.Sleep(250 * time.Microsecond)
				continue
			}
			t.Fatalf("DeliverNextProgress(%s) returned error: %v", nodeID, err)
		}
	}

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.standby.StepHA(ctx); err != nil {
		t.Fatalf("standby initial StepHA returned error: %v", err)
	}
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)

	stopLeaderTraffic := startTraffic(t, h.leader)
	h.mustStepLeader(t)
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		stopLeaderTraffic()
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for h.adapters["d"].PendingProgress() == 0 && time.Now().Before(deadline) {
		time.Sleep(250 * time.Microsecond)
	}
	stopLeaderTraffic()
	if got, want := h.adapters["d"].PendingProgress(), 1; got != want {
		t.Fatalf("queued ready progress = %d, want %d", got, want)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	stopStandbyTraffic := startTraffic(t, h.standby)
	deliverEventually(t, "d")
	h.mustStepStandby(t)
	stopStandbyTraffic()
	h.mustStepStandby(t)

	snapshot, err := h.standby.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	route := snapshot.Slots[slot]
	if !route.Readable {
		t.Fatalf("route after failover churn = %#v, want readable", route)
	}
	if got := h.adapters["d"].PendingProgress(); got != 0 {
		t.Fatalf("queued ready progress after standby handoff = %d, want 0", got)
	}
	if got := h.standby.Current().Version; got <= 1 {
		t.Fatalf("standby version after ready progress = %d, want > 1", got)
	}
}

func TestHAOutOfOrderDuplicateProgressAfterFailoverDoesNotChurnSettledSlot(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, h.leader.Pending(), "d", pendingKindReady)
	h.mustStepLeader(t)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	h.mustStepLeader(t)
	leavingNodeID := replicaNodeWithState(h.leader.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node before settle")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	before := h.standby.Current()
	beforePending, hadPending := h.standby.Pending()[slot]
	beforeRouting, err := h.standby.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if _, err := h.standby.ReportReplicaReady(ctx, "d", slot, 0, ""); err != nil {
		t.Fatalf("out-of-order duplicate ReportReplicaReady returned error: %v", err)
	}
	if _, err := h.standby.ReportReplicaRemoved(ctx, leavingNodeID, slot, 0, ""); err != nil {
		t.Fatalf("out-of-order duplicate ReportReplicaRemoved returned error: %v", err)
	}
	afterRouting, err := h.standby.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after duplicates returned error: %v", err)
	}
	if got := h.standby.Current(); !reflect.DeepEqual(got, before) {
		t.Fatalf("state changed on HA out-of-order duplicates\ngot=%#v\nwant=%#v", got, before)
	}
	afterPending, stillPending := h.standby.Pending()[slot]
	if hadPending != stillPending || (hadPending && !reflect.DeepEqual(afterPending, beforePending)) {
		t.Fatalf("settled-slot pending changed on HA out-of-order duplicates\ngot=%#v\nwant=%#v", afterPending, beforePending)
	}
	if !reflect.DeepEqual(afterRouting, beforeRouting) {
		t.Fatalf("routing changed on HA out-of-order duplicates\nafter=%#v\nbefore=%#v", afterRouting, beforeRouting)
	}
	if runtimeOutboxHasSlot(h.standby.Current().Outbox, slot) {
		t.Fatalf("runtime outbox unexpectedly recreated for settled HA slot %d: %#v", slot, h.standby.Current().Outbox)
	}
}

func TestHADynamicAutoJoinRepairsAfterFailover(t *testing.T) {
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 1, 3, []string{"a", "b", "c"})

	current := h.leader.Current()
	if _, err := h.leader.MarkNodeDead(ctx, reconfigureCommand("dead-c", current.Version, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "c",
	}, coordinator.ReconfigurationPolicy{})); err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if got, want := replicaNodeStates(h.leader.Current().Cluster.Chains[0]), []string{"a:active", "b:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("degraded chain after dead mark = %v, want %v", got, want)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)

	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	h.mustStepStandby(t)
	if got, want := h.standby.Pending()[0].Kind, pendingKindReady; got != want {
		t.Fatalf("pending kind after dynamic join = %q, want %q", got, want)
	}
	if got, want := h.standby.Pending()[0].NodeID, "d"; got != want {
		t.Fatalf("pending node after dynamic join = %q, want %q", got, want)
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := replicaNodeStates(h.standby.Current().Cluster.Chains[0]), []string{"a:active", "b:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final repaired chain after dynamic join = %v, want %v", got, want)
	}
}

func TestPostgresHAStoreConcurrentAcquireAndStaleSave(t *testing.T) {
	dsn := postgresTestDSN(t)
	if dsn == "" {
		t.Skip("CRAQ_TEST_POSTGRES_DSN is not set")
	}

	ctx := context.Background()
	storeA, err := OpenPostgresHAStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgresHAStore(storeA) returned error: %v", err)
	}
	defer func() { _ = storeA.Close() }()
	if err := storeA.Reset(ctx); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}
	storeB, err := OpenPostgresHAStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgresHAStore(storeB) returned error: %v", err)
	}
	defer func() { _ = storeB.Close() }()

	now := time.Unix(100, 0).UTC()
	type acquireResult struct {
		lease  LeaderLease
		leader bool
		err    error
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	var left, right acquireResult
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		left.lease, left.leader, left.err = storeA.AcquireOrRenew(ctx, "coord-a", "leader-a", now, 2*time.Second)
	}()
	go func() {
		defer wg.Done()
		<-start
		right.lease, right.leader, right.err = storeB.AcquireOrRenew(ctx, "coord-b", "leader-b", now, 2*time.Second)
	}()
	close(start)
	wg.Wait()

	if left.err != nil {
		t.Fatalf("storeA AcquireOrRenew returned error: %v", left.err)
	}
	if right.err != nil {
		t.Fatalf("storeB AcquireOrRenew returned error: %v", right.err)
	}
	leaderCount := 0
	if left.leader {
		leaderCount++
	}
	if right.leader {
		leaderCount++
	}
	if got, want := leaderCount, 1; got != want {
		t.Fatalf("leader count = %d, want %d", got, want)
	}

	leaderStore := storeA
	leaderLease := left.lease
	staleStore := storeB
	staleLease := right.lease
	if !left.leader {
		leaderStore, staleStore = storeB, storeA
		leaderLease, staleLease = right.lease, left.lease
	}

	version, err := leaderStore.SaveSnapshot(ctx, leaderLease, now.Add(100*time.Millisecond), 0, HASnapshot{
		Outbox: []OutboxEntry{{
			ID:        "outbox-1",
			Epoch:     leaderLease.Epoch,
			NodeID:    "n1",
			Slot:      1,
			CommandID: "cmd-1",
			Kind:      OutboxCommandAddReplicaAsTail,
		}},
	})
	if err != nil {
		t.Fatalf("leader SaveSnapshot returned error: %v", err)
	}
	if got, want := version, uint64(1); got != want {
		t.Fatalf("snapshot version = %d, want %d", got, want)
	}
	if _, err := staleStore.SaveSnapshot(ctx, staleLease, now.Add(200*time.Millisecond), version, HASnapshot{}); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("stale SaveSnapshot error = %v, want ErrNotLeader", err)
	}
}

type haHistoryResult struct {
	finalState   coordruntime.State
	pending      map[int]PendingWork
	routing      RoutingSnapshot
	slotSnapshot map[string]map[string]string
}

type haInMemoryHarness struct {
	clock    *fakeClock
	store    *InMemoryHAStore
	repl     *storage.InMemoryReplicationTransport
	adapters map[string]*InMemoryNodeAdapter
	backends map[string]*storage.InMemoryBackend
	leader   *Server
	standby  *Server
}

func newHAInMemoryHarness(t *testing.T, nodeIDs []string) *haInMemoryHarness {
	return newHAInMemoryHarnessWithConfig(t, nodeIDs, ServerConfig{})
}

func newHAInMemoryHarnessWithConfig(t *testing.T, nodeIDs []string, cfg ServerConfig) *haInMemoryHarness {
	t.Helper()

	clock := &fakeClock{now: time.Unix(0, 0).UTC()}
	store := NewInMemoryHAStore()
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

	newServer := func(coordinatorID, advertise string) *Server {
		serverCfg := cfg
		serverCfg.Clock = clock
		if serverCfg.DispatchRetryInterval == 0 {
			serverCfg.DispatchRetryInterval = time.Hour
		}
		serverCfg.HA = &HAConfig{
			CoordinatorID:          coordinatorID,
			AdvertiseAddress:       advertise,
			Store:                  store,
			LeaseTTL:               2 * time.Second,
			RenewInterval:          time.Second,
			DisableBackgroundLoops: true,
		}
		return mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nodeClients, serverCfg)
	}
	h := &haInMemoryHarness{
		clock:    clock,
		store:    store,
		repl:     repl,
		adapters: adapters,
		backends: backends,
		leader:   newServer("coord-a", "coord-a"),
		standby:  newServer("coord-b", "coord-b"),
	}
	t.Cleanup(func() {
		_ = h.leader.Close()
		_ = h.standby.Close()
	})
	return h
}

func (h *haInMemoryHarness) mustStepLeader(t *testing.T) {
	t.Helper()
	isLeader, err := h.leader.StepHA(context.Background())
	if err != nil {
		t.Fatalf("leader StepHA returned error: %v", err)
	}
	if !isLeader {
		t.Fatal("leader did not hold lease")
	}
}

func (h *haInMemoryHarness) mustStepStandby(t *testing.T) {
	t.Helper()
	isLeader, err := h.standby.StepHA(context.Background())
	if err != nil {
		t.Fatalf("standby StepHA returned error: %v", err)
	}
	if !isLeader {
		t.Fatal("standby did not hold lease")
	}
}

func (h *haInMemoryHarness) mustBind(t *testing.T, server *Server) {
	t.Helper()
	for _, adapter := range h.adapters {
		adapter.BindServer(server)
	}
}

func (h *haInMemoryHarness) seedBootstrap(t *testing.T, server *Server, slotCount int, replicationFactor int, nodeIDs []string) {
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
			if err := h.backends[replica.NodeID].Put(chain.Slot, "seed", replica.NodeID, storage.ObjectMetadata{Version: 1}); err != nil {
				t.Fatalf("seed Put returned error: %v", err)
			}
		}
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(server)
	}
}

func (h *haInMemoryHarness) mustFindOutbox(t *testing.T, kind OutboxCommandKind, nodeID string, slot int) OutboxEntry {
	t.Helper()
	snapshot, err := h.store.LoadSnapshot(context.Background())
	if err != nil {
		t.Fatalf("LoadSnapshot returned error: %v", err)
	}
	for _, entry := range snapshot.Outbox {
		if entry.Kind == kind && entry.NodeID == nodeID && entry.Slot == slot {
			return entry
		}
	}
	t.Fatalf("outbox entry %q node=%s slot=%d not found", kind, nodeID, slot)
	return OutboxEntry{}
}

func runHAFailoverRepairHistory(t *testing.T) haHistoryResult {
	t.Helper()
	ctx := context.Background()
	h := newHAInMemoryHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["c"].EnableQueuedProgress()
	h.adapters["d"].EnableQueuedProgress()

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.standby.StepHA(ctx); err != nil {
		t.Fatalf("standby initial StepHA returned error: %v", err)
	}
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, uniqueAddNodeEvent("d"), noBudgetPolicy())); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	h.mustStepLeader(t)
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepStandby(t)
	h.mustBind(t, h.standby)
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(ready) returned error: %v", err)
	}
	h.mustStepStandby(t)
	if err := h.adapters["c"].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}

	h.clock.Advance(3 * time.Second)
	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if err := h.adapters["c"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(removed) returned error: %v", err)
	}

	routing, err := h.leader.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	snapshot, err := h.backends["d"].ReplicaData(1)
	if err != nil {
		t.Fatalf("ReplicaData(d,1) returned error: %v", err)
	}
	return haHistoryResult{
		finalState:   h.leader.Current(),
		pending:      h.leader.Pending(),
		routing:      routing,
		slotSnapshot: map[string]map[string]string{"d": storageSnapshotValues(snapshot)},
	}
}

func uniqueAddNodeEvent(nodeID string) coordinator.Event {
	return coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode(nodeID),
	}
}

func noBudgetPolicy() coordinator.ReconfigurationPolicy {
	return coordinator.ReconfigurationPolicy{MaxChangedChains: 1}
}

func postgresTestDSN(t *testing.T) string {
	t.Helper()
	return os.Getenv("CRAQ_TEST_POSTGRES_DSN")
}
