package client

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func TestFaultInjectedRouterHistoryIsDeterministic(t *testing.T) {
	left := runQueuedRouterFaultHistory(t)
	right := runQueuedRouterFaultHistory(t)

	if !reflect.DeepEqual(left.finalState, right.finalState) {
		t.Fatalf("final coordinator state mismatch\nleft=%#v\nright=%#v", left.finalState, right.finalState)
	}
	if !reflect.DeepEqual(snapshotValueSets(left.finalSnapshots), snapshotValueSets(right.finalSnapshots)) {
		t.Fatalf("final snapshots mismatch\nleft=%#v\nright=%#v", left.finalSnapshots, right.finalSnapshots)
	}
	if !sameReadResultValues(left.finalRoute, right.finalRoute) {
		t.Fatalf("final route mismatch\nleft=%#v\nright=%#v", left.finalRoute, right.finalRoute)
	}
}

func snapshotValueSets(snapshots map[string]storage.Snapshot) map[string]map[string]string {
	values := make(map[string]map[string]string, len(snapshots))
	for nodeID, snapshot := range snapshots {
		nodeValues := make(map[string]string, len(snapshot))
		for key, object := range snapshot {
			nodeValues[key] = object.Value
		}
		values[nodeID] = nodeValues
	}
	return values
}

func sameReadResultValues(left storage.ReadResult, right storage.ReadResult) bool {
	if left.Slot != right.Slot || left.ChainVersion != right.ChainVersion || left.Found != right.Found || left.Value != right.Value {
		return false
	}
	switch {
	case left.Metadata == nil && right.Metadata == nil:
		return true
	case left.Metadata == nil || right.Metadata == nil:
		return false
	default:
		return left.Metadata.Version == right.Metadata.Version
	}
}

type queuedRouterFaultHistory struct {
	finalState     coordruntime.State
	finalSnapshots map[string]storage.Snapshot
	finalRoute     storage.ReadResult
}

func runQueuedRouterFaultHistory(t *testing.T) queuedRouterFaultHistory {
	t.Helper()
	ctx := context.Background()
	h := newQueuedRouterHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["c"].EnableQueuedProgress()
	h.adapters["d"].EnableQueuedProgress()

	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	key := keyForSlot(t, 1, 8, "fault")
	if _, err := router.Put(ctx, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if err := h.repl.DeliverAll(ctx); err != nil {
		t.Fatalf("DeliverAll returned error: %v", err)
	}
	if got, want := mustQueuedChainValue(t, h, 1, key), "v1"; got != want {
		t.Fatalf("initial replicated value = %q, want %q", got, want)
	}

	if _, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	if _, err := router.Put(ctx, key, "v2"); err == nil {
		t.Fatal("Put during join unexpectedly succeeded")
	} else if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("Put during join error = %v, want no route", err)
	}
	if read, err := router.Get(ctx, key); err != nil {
		t.Fatalf("Get during join returned error: %v", err)
	} else if got, want := read.Value, "v1"; got != want {
		t.Fatalf("Get during join value = %q, want %q", got, want)
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["d"].DropProgressAt(0); err != nil {
		t.Fatalf("DropProgressAt returned error: %v", err)
	}
	if snapshot, err := h.server.RoutingSnapshot(ctx); err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	} else if snapshot.Slots[1].Writable {
		t.Fatalf("routing after dropped ready = %#v, want not writable", snapshot.Slots[1])
	}

	if err := h.adapters["d"].ReportReplicaReady(ctx, 1, 0); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	if err := h.adapters["d"].DuplicateProgressAt(0); err != nil {
		t.Fatalf("DuplicateProgressAt returned error: %v", err)
	}
	if err := h.adapters["d"].DeliverAllProgress(ctx); err != nil {
		t.Fatalf("DeliverAllProgress ready returned error: %v", err)
	}

	if err := h.adapters["c"].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if err := h.adapters["c"].DuplicateProgressAt(0); err != nil {
		t.Fatalf("DuplicateProgressAt removed returned error: %v", err)
	}
	if err := h.adapters["c"].DeliverAllProgress(ctx); err != nil {
		t.Fatalf("DeliverAllProgress removed returned error: %v", err)
	}
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after repair returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v2"); err != nil {
		t.Fatalf("Put after repair returned error: %v", err)
	}
	read, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after repair returned error: %v", err)
	}
	if got, want := read.Value, "v2"; got != want {
		t.Fatalf("Get after repair value = %q, want %q", got, want)
	}

	history := queuedRouterFaultHistory{
		finalState:     h.server.Current(),
		finalSnapshots: map[string]storage.Snapshot{},
		finalRoute:     read,
	}
	for nodeID, adapter := range h.adapters {
		state := adapter.Node().State()
		replica, ok := state.Replicas[1]
		if !ok || replica.State != storage.ReplicaStateActive {
			continue
		}
		snapshot, err := adapter.Node().CommittedSnapshot(1)
		if err != nil {
			t.Fatalf("CommittedSnapshot(%q) returned error: %v", nodeID, err)
		}
		history.finalSnapshots[nodeID] = snapshot
	}
	return history
}

func mustQueuedChainValue(t *testing.T, h *queuedRouterHarness, slot int, key string) string {
	t.Helper()
	var value string
	found := false
	for nodeID, adapter := range h.adapters {
		state := adapter.Node().State()
		replica, ok := state.Replicas[slot]
		if !ok || replica.State != storage.ReplicaStateActive {
			continue
		}
		snapshot, err := adapter.Node().CommittedSnapshot(slot)
		if err != nil {
			t.Fatalf("CommittedSnapshot(%q, %d) returned error: %v", nodeID, slot, err)
		}
		if got, ok := snapshot[key]; ok {
			if !found {
				value = got.Value
				found = true
				continue
			}
			if got.Value != value {
				t.Fatalf("inconsistent chain value: got %q and %q", value, got.Value)
			}
		}
	}
	return value
}
