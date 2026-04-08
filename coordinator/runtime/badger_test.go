package runtime

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
)

func TestBadgerStoreRecoversPendingAndOutboxState(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}
	defer func() { _ = store.Close() }()

	rt := mustOpenRuntime(t, store)
	bootstrapState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "add-d",
		ExpectedVersion: bootstrapState.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}
	if len(plan.ChangedSlots) == 0 {
		t.Fatal("Reconfigure produced no changed slots")
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopenedStore, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore(reopen) returned error: %v", err)
	}
	defer func() { _ = reopenedStore.Close() }()
	reopened := mustOpenRuntime(t, reopenedStore)

	want := state
	want.AppliedCommands = map[string]AppliedCommand{}
	if got := reopened.Current(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, want)
	}
	if got, wantPending := reopened.Current().PendingBySlot[1].NodeID, "d"; got != wantPending {
		t.Fatalf("pending slot 1 node = %q, want %q", got, wantPending)
	}
	if len(reopened.Current().Outbox) == 0 {
		t.Fatal("reopened runtime lost outbox entries")
	}
}

func TestBadgerStoreRecoversFlapHistory(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}

	rt := mustOpenRuntime(t, store)
	windowNanos := int64((30 * time.Second).Nanoseconds())
	state, err := rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-a-1",
		ExpectedVersion: 0,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: int64((100 * time.Second).Nanoseconds()),
			FlapWindowNanos:    windowNanos,
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	state, err = rt.ApplyLiveness(ctx, Command{
		ID:              "liveness-a-suspect-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindLiveness,
		Liveness: &LivenessCommand{
			NodeID:              "a",
			State:               NodeLivenessStateSuspect,
			EvaluatedAtUnixNano: int64((110 * time.Second).Nanoseconds()),
			FlapWindowNanos:     windowNanos,
		},
	})
	if err != nil {
		t.Fatalf("ApplyLiveness(suspect-1) returned error: %v", err)
	}
	state, err = rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-a-2",
		ExpectedVersion: state.Version,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: int64((115 * time.Second).Nanoseconds()),
			FlapWindowNanos:    windowNanos,
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat(second) returned error: %v", err)
	}
	state, err = rt.ApplyLiveness(ctx, Command{
		ID:              "liveness-a-suspect-2",
		ExpectedVersion: state.Version,
		Kind:            CommandKindLiveness,
		Liveness: &LivenessCommand{
			NodeID:              "a",
			State:               NodeLivenessStateSuspect,
			EvaluatedAtUnixNano: int64((125 * time.Second).Nanoseconds()),
			FlapWindowNanos:     windowNanos,
		},
	})
	if err != nil {
		t.Fatalf("ApplyLiveness(suspect-2) returned error: %v", err)
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopenedStore, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore(reopen) returned error: %v", err)
	}
	defer func() { _ = reopenedStore.Close() }()
	reopened := mustOpenRuntime(t, reopenedStore)
	if got, want := reopened.Current().NodeLivenessByID["a"].SuspectTransitionsUnixNano, []int64{
		int64((110 * time.Second).Nanoseconds()),
		int64((125 * time.Second).Nanoseconds()),
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("suspect transitions after reopen = %v, want %v", got, want)
	}
}

func TestBadgerStoreInterleavedRepairCheckpointAndReplay(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}
	defer func() { _ = store.Close() }()

	rt := mustOpenRuntime(t, store)
	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	state, err = rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-a",
		ExpectedVersion: state.Version,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: int64((10 * time.Second).Nanoseconds()),
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat(a) returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "add-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 2},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(add-d) returned error: %v", err)
	}
	if got, want := len(plan.ChangedSlots), 2; got != want {
		t.Fatalf("changed slot count = %d, want %d", got, want)
	}

	firstSlot := plan.ChangedSlots[0].Slot
	secondSlot := plan.ChangedSlots[1].Slot
	for i, entry := range append([]OutboxEntry(nil), state.Outbox...) {
		if entry.Slot != firstSlot {
			continue
		}
		state, err = rt.AcknowledgeOutbox(ctx, Command{
			ID:              "ack-first-slot-" + string(rune('a'+i)),
			ExpectedVersion: state.Version,
			Kind:            CommandKindAcknowledgeOutbox,
			AcknowledgeOutbox: &AcknowledgeOutboxCommand{
				EntryID: entry.ID,
			},
		})
		if err != nil {
			t.Fatalf("AcknowledgeOutbox(%s) returned error: %v", entry.ID, err)
		}
	}

	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-ready-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   firstSlot,
				NodeID: plan.ChangedSlots[0].Steps[0].NodeID,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress(ready) returned error: %v", err)
	}
	_, state, err = rt.Reconfigure(ctx, Command{
		ID:              "reconcile-after-ready",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: nil,
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 2},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(after ready) returned error: %v", err)
	}
	state, err = rt.ApplyLiveness(ctx, Command{
		ID:              "liveness-b-suspect",
		ExpectedVersion: state.Version,
		Kind:            CommandKindLiveness,
		Liveness: &LivenessCommand{
			NodeID:              "b",
			State:               NodeLivenessStateSuspect,
			EvaluatedAtUnixNano: int64((13 * time.Second).Nanoseconds()),
			FlapWindowNanos:     int64((30 * time.Second).Nanoseconds()),
		},
	})
	if err != nil {
		t.Fatalf("ApplyLiveness returned error: %v", err)
	}

	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopenedStore, err := OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore(reopen) returned error: %v", err)
	}
	defer func() { _ = reopenedStore.Close() }()
	reopened := mustOpenRuntime(t, reopenedStore)

	want := state
	want.AppliedCommands = map[string]AppliedCommand{}
	if got := reopened.Current(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, want)
	}
	if got, want := reopened.Current().PendingBySlot[firstSlot].Kind, PendingKindRemoved; got != want {
		t.Fatalf("first slot pending kind after reopen = %q, want %q", got, want)
	}
	if got, want := reopened.Current().PendingBySlot[secondSlot].Kind, PendingKindReady; got != want {
		t.Fatalf("second slot pending kind after reopen = %q, want %q", got, want)
	}
	if got := countOutboxEntriesForSlot(reopened.Current().Outbox, secondSlot); got == 0 {
		t.Fatal("second slot lost its pending outbox work after reopen")
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}
