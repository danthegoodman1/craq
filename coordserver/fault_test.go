package coordserver

import (
	"context"
	"reflect"
	"testing"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func TestFaultInjectedQueuedProgressHistoryIsDeterministic(t *testing.T) {
	left := runQueuedProgressFaultHistory(t)
	right := runQueuedProgressFaultHistory(t)

	if !reflect.DeepEqual(left.finalState, right.finalState) {
		t.Fatalf("final state mismatch\nleft=%#v\nright=%#v", left.finalState, right.finalState)
	}
	if !reflect.DeepEqual(left.pending, right.pending) {
		t.Fatalf("pending mismatch\nleft=%#v\nright=%#v", left.pending, right.pending)
	}
	if !reflect.DeepEqual(left.routing, right.routing) {
		t.Fatalf("routing mismatch\nleft=%#v\nright=%#v", left.routing, right.routing)
	}
}

type queuedProgressFaultHistory struct {
	finalState coordruntime.State
	pending    map[int]PendingWork
	routing    RoutingSnapshot
}

func runQueuedProgressFaultHistory(t *testing.T) queuedProgressFaultHistory {
	t.Helper()
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	h.adapters["c"].EnableQueuedProgress()
	h.adapters["d"].EnableQueuedProgress()
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
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := h.adapters["d"].PendingProgress(), 1; got != want {
		t.Fatalf("queued ready count = %d, want %d", got, want)
	}
	if err := h.adapters["d"].DropProgressAt(0); err != nil {
		t.Fatalf("DropProgressAt returned error: %v", err)
	}
	if got, want := server.Pending()[1], (PendingWork{
		Slot:        1,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[1],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending after dropped ready = %#v, want %#v", got, want)
	}
	routing, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if routing.Slots[1].Writable || !routing.Slots[1].Readable {
		t.Fatalf("routing during undelivered ready = %#v, want readable but not writable until the new replica is ready", routing.Slots[1])
	}

	if err := h.adapters["d"].ReportReplicaReady(ctx, 1, 0); err != nil {
		t.Fatalf("ReportReplicaReady returned error: %v", err)
	}
	if err := h.adapters["d"].DuplicateProgressAt(0); err != nil {
		t.Fatalf("DuplicateProgressAt returned error: %v", err)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress returned error: %v", err)
	}
	if got, want := server.Pending()[1].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after delivered ready = %q, want %q", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("duplicate DeliverNextProgress returned error: %v", err)
	}

	if err := h.adapters["c"].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if err := h.adapters["c"].DuplicateProgressAt(0); err != nil {
		t.Fatalf("DuplicateProgressAt(removed) returned error: %v", err)
	}
	if err := h.adapters["c"].DeliverAllProgress(ctx); err != nil {
		t.Fatalf("DeliverAllProgress returned error: %v", err)
	}
	if _, exists := server.Pending()[1]; exists {
		t.Fatal("pending work still present after removed progress delivery")
	}

	routing, err = server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot final returned error: %v", err)
	}
	if !routing.Slots[1].Writable || !routing.Slots[1].Readable {
		t.Fatalf("final routing = %#v, want readable and writable", routing.Slots[1])
	}

	return queuedProgressFaultHistory{
		finalState: server.Current(),
		pending:    server.Pending(),
		routing:    routing,
	}
}
