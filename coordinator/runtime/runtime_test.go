package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/storage"
)

func TestOpenEmptyStore(t *testing.T) {
	rt, err := Open(context.Background(), NewInMemoryStore())
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}

	if got := rt.Current(); got.Version != 0 || got.LastLogIndex != 0 || got.Cluster.SlotCount != 0 {
		t.Fatalf("unexpected empty runtime state: %#v", got)
	}
}

func TestHeartbeatPersistsAndRecovers(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-a-1",
		ExpectedVersion: 0,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: 101,
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	record := state.NodeLivenessByID["a"]
	if got, want := record.State, NodeLivenessStateHealthy; got != want {
		t.Fatalf("liveness state = %q, want %q", got, want)
	}
	if got, want := record.LastHeartbeatUnixNano, int64(101); got != want {
		t.Fatalf("last heartbeat = %d, want %d", got, want)
	}
	if got, want := len(store.wal), 1; got != want {
		t.Fatalf("wal length = %d, want %d", got, want)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, state) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, state)
	}
}

func TestHeartbeatDuplicateCommandIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	first, err := rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-a-1",
		ExpectedVersion: 0,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: 101,
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	second, err := rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-a-1",
		ExpectedVersion: 0,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: 101,
		},
	})
	if err != nil {
		t.Fatalf("duplicate Heartbeat returned error: %v", err)
	}
	if got, want := len(store.wal), 1; got != want {
		t.Fatalf("wal length = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("duplicate heartbeat state mismatch\ngot=%#v\nwant=%#v", second, first)
	}
}

func TestNodeReadyPersistsAndRecovers(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)
	if _, err := rt.Bootstrap(ctx, Command{
		ID:              "bootstrap-1",
		ExpectedVersion: 0,
		Kind:            CommandKindBootstrap,
		Bootstrap: &BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
			Nodes:  []coordinator.Node{uniqueNode("a")},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	state, err := rt.MarkNodeReady(ctx, Command{
		ID:              "node-ready-a-1",
		ExpectedVersion: 1,
		Kind:            CommandKindNodeReady,
		NodeReady: &NodeReadyCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: 101,
		},
	})
	if err != nil {
		t.Fatalf("MarkNodeReady returned error: %v", err)
	}
	if !state.Cluster.ReadyNodeIDs["a"] {
		t.Fatal("node a was not marked ready")
	}
	record := state.NodeLivenessByID["a"]
	if got, want := record.State, NodeLivenessStateHealthy; got != want {
		t.Fatalf("liveness state = %q, want %q", got, want)
	}
	if got, want := record.LastHeartbeatUnixNano, int64(101); got != want {
		t.Fatalf("last heartbeat = %d, want %d", got, want)
	}
	if got, want := record.UpdatedAtUnixNano, int64(101); got != want {
		t.Fatalf("updated at = %d, want %d", got, want)
	}
	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, state) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, state)
	}
}

func TestCurrentViewReturnsHotStateWithoutAppliedCommandHistory(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)
	if _, err := rt.Bootstrap(ctx, Command{
		ID:              "bootstrap-1",
		ExpectedVersion: 0,
		Kind:            CommandKindBootstrap,
		Bootstrap: &BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
			Nodes:  []coordinator.Node{uniqueNode("a")},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	state, err := rt.MarkNodeReady(ctx, Command{
		ID:              "node-ready-a-1",
		ExpectedVersion: 1,
		Kind:            CommandKindNodeReady,
		NodeReady: &NodeReadyCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: 101,
		},
	})
	if err != nil {
		t.Fatalf("MarkNodeReady returned error: %v", err)
	}
	view := rt.CurrentView()
	if got, want := view.Version, state.Version; got != want {
		t.Fatalf("view version = %d, want %d", got, want)
	}
	if got, want := view.LastLogIndex, state.LastLogIndex; got != want {
		t.Fatalf("view last log index = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(view.Cluster, state.Cluster) {
		t.Fatalf("view cluster mismatch\ngot=%#v\nwant=%#v", view.Cluster, state.Cluster)
	}
	view.Cluster.ReadyNodeIDs["a"] = false
	view.NodeLivenessByID["a"] = NodeLivenessRecord{}
	if !rt.Current().Cluster.ReadyNodeIDs["a"] {
		t.Fatal("mutating view changed runtime cluster state")
	}
	if got := rt.Current().NodeLivenessByID["a"].State; got != NodeLivenessStateHealthy {
		t.Fatalf("runtime liveness after mutating view = %q, want healthy", got)
	}
}

func TestLivenessTransitionPersistsAndCheckpointRecovers(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-a-1",
		ExpectedVersion: 0,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: 101,
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	state, err = rt.ApplyLiveness(ctx, Command{
		ID:              "liveness-a-suspect",
		ExpectedVersion: state.Version,
		Kind:            CommandKindLiveness,
		Liveness: &LivenessCommand{
			NodeID:              "a",
			State:               NodeLivenessStateSuspect,
			EvaluatedAtUnixNano: 201,
			DeadActionFired:     false,
		},
	})
	if err != nil {
		t.Fatalf("ApplyLiveness returned error: %v", err)
	}
	state, err = rt.ApplyLiveness(ctx, Command{
		ID:              "liveness-a-dead",
		ExpectedVersion: state.Version,
		Kind:            CommandKindLiveness,
		Liveness: &LivenessCommand{
			NodeID:              "a",
			State:               NodeLivenessStateDead,
			EvaluatedAtUnixNano: 301,
			DeadActionFired:     true,
		},
	})
	if err != nil {
		t.Fatalf("ApplyLiveness(dead) returned error: %v", err)
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	reopened := mustOpenRuntime(t, store)
	want := state
	want.AppliedCommands = map[string]AppliedCommand{}
	if got := reopened.Current(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, want)
	}
	if got, want := reopened.Current().NodeLivenessByID["a"].State, NodeLivenessStateDead; got != want {
		t.Fatalf("liveness state after reopen = %q, want %q", got, want)
	}
}

func TestFlapHistoryPersistsAndPrunesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
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
	if got, want := state.NodeLivenessByID["a"].SuspectTransitionsUnixNano, []int64{
		int64((110 * time.Second).Nanoseconds()),
		int64((125 * time.Second).Nanoseconds()),
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("suspect transitions before reopen = %v, want %v", got, want)
	}

	reopened := mustOpenRuntime(t, store)
	if got, want := reopened.Current().NodeLivenessByID["a"].SuspectTransitionsUnixNano, []int64{
		int64((110 * time.Second).Nanoseconds()),
		int64((125 * time.Second).Nanoseconds()),
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("suspect transitions after reopen = %v, want %v", got, want)
	}

	state, err = reopened.Heartbeat(ctx, Command{
		ID:              "heartbeat-a-3",
		ExpectedVersion: reopened.Current().Version,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("a"),
			ObservedAtUnixNano: int64((150 * time.Second).Nanoseconds()),
			FlapWindowNanos:    windowNanos,
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat(third) returned error: %v", err)
	}
	if got, want := state.NodeLivenessByID["a"].SuspectTransitionsUnixNano, []int64{
		int64((125 * time.Second).Nanoseconds()),
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("suspect transitions after prune = %v, want %v", got, want)
	}
}

func TestBootstrapPersistsAndRecovers(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()

	rt := mustOpenRuntime(t, store)
	first, err := rt.Bootstrap(ctx, Command{
		ID:              "bootstrap-1",
		ExpectedVersion: 0,
		Kind:            CommandKindBootstrap,
		Bootstrap: &BootstrapCommand{
			Config: coordinator.Config{SlotCount: 8, ReplicationFactor: 3},
			Nodes:  uniqueNodes("a", "b", "c"),
		},
	})
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if got, want := len(store.wal), 1; got != want {
		t.Fatalf("wal length = %d, want %d", got, want)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, first) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, first)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func TestRecoverFromCheckpointAndWALTail(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()

	rt := mustOpenRuntime(t, store)
	bootstrapState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}

	plan, stateAfterReconfigure, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
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

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, stateAfterReconfigure) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, stateAfterReconfigure)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func TestOpenFailsOnInvalidWALOrder(t *testing.T) {
	store := NewInMemoryStore()
	store.wal = []LogRecord{
		{Index: 2, Command: bootstrapCommand("bootstrap-1", 0, 1, 1, "a")},
	}

	_, err := Open(context.Background(), store)
	if err == nil {
		t.Fatal("Open unexpectedly succeeded")
	}
	if !errors.Is(err, ErrRecovery) {
		t.Fatalf("error = %v, want recovery failure", err)
	}
}

func TestOpenFailsWhenReplayCommandCannotApply(t *testing.T) {
	store := NewInMemoryStore()
	store.wal = []LogRecord{
		{
			Index: 1,
			Command: Command{
				ID:              "reconfigure-1",
				ExpectedVersion: 0,
				Kind:            CommandKindReconfigure,
				Reconfigure: &ReconfigureCommand{
					Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
					Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
				},
			},
		},
	}

	_, err := Open(context.Background(), store)
	if err == nil {
		t.Fatal("Open unexpectedly succeeded")
	}
	if !errors.Is(err, ErrRecovery) {
		t.Fatalf("error = %v, want recovery failure", err)
	}
}

func TestDuplicateCommandIdenticalPayloadIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	firstState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	secondState, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("duplicate Bootstrap returned error: %v", err)
	}

	if got, want := len(store.wal), 1; got != want {
		t.Fatalf("wal length = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(secondState, firstState) {
		t.Fatalf("duplicate state mismatch\ngot=%#v\nwant=%#v", secondState, firstState)
	}
}

func TestDuplicateCommandDifferentPayloadFails(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	_, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 2, "a", "b"))
	if err == nil {
		t.Fatal("Bootstrap unexpectedly succeeded")
	}
	if !errors.Is(err, ErrCommandConflict) {
		t.Fatalf("error = %v, want command conflict", err)
	}
}

func TestStaleExpectedVersionFails(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	_, _, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: 0,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err == nil {
		t.Fatal("Reconfigure unexpectedly succeeded")
	}
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("error = %v, want version mismatch", err)
	}
}

func TestWrongLifecycleVersionFails(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	if _, _, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: 0,
		Kind:            CommandKindReconfigure,
		Reconfigure:     &ReconfigureCommand{},
	}); err == nil {
		t.Fatal("Reconfigure unexpectedly succeeded before bootstrap")
	} else if !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("error = %v, want not initialized", err)
	}

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	if _, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-2", 1, 8, 3, "a", "b", "c")); err == nil {
		t.Fatal("second Bootstrap unexpectedly succeeded")
	} else if !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("error = %v, want already initialized", err)
	}
}

func TestReconfigureAndApplyProgressPersistAndRecover(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	slot := plan.ChangedSlots[0].Slot
	joiningNode := plan.ChangedSlots[0].Steps[0].NodeID
	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   slot,
				NodeID: joiningNode,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress returned error: %v", err)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, state) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, state)
	}
	records := reopened.Current().CompletedProgressBySlot[slot]
	if got, want := len(records), 1; got != want {
		t.Fatalf("completed progress record count = %d, want %d", got, want)
	}
	if got, want := records[0], (CompletedProgressRecord{
		NodeID:      joiningNode,
		Kind:        CompletedProgressKindReady,
		SlotVersion: reopened.Current().SlotVersions[slot],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("completed progress record = %#v, want %#v", got, want)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func TestReconcileAfterRemovedProgressMovesPendingOffCompletedSlot(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "add-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(add-d) returned error: %v", err)
	}
	slot := plan.ChangedSlots[0].Slot
	if len(state.Outbox) == 0 {
		t.Fatal("reconfigure(add-d) produced no outbox entries")
	}
	for i, entry := range state.Outbox {
		state, err = rt.AcknowledgeOutbox(ctx, Command{
			ID:              fmt.Sprintf("ack-add-tail-%d", i),
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
				Slot:   slot,
				NodeID: "d",
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress(ready) returned error: %v", err)
	}
	plan, state, err = rt.Reconfigure(ctx, Command{
		ID:              "reconcile-after-ready",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: nil,
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(after ready) returned error: %v", err)
	}
	if got, want := state.PendingBySlot[slot].Kind, PendingKindRemoved; got != want {
		t.Fatalf("pending kind after ready reconcile = %q, want %q", got, want)
	}
	leavingNodeID := ""
	for _, replica := range state.Cluster.Chains[slot].Replicas {
		if replica.State == coordinator.ReplicaStateLeaving {
			leavingNodeID = replica.NodeID
			break
		}
	}
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after ready reconcile")
	}
	if got, want := plan.ChangedSlots[0].Slot, slot; got != want {
		t.Fatalf("ready reconcile changed slot = %d, want %d", got, want)
	}

	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-removed-old",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaRemoved,
				Slot:   slot,
				NodeID: leavingNodeID,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress(removed) returned error: %v", err)
	}
	if _, exists := state.PendingBySlot[slot]; exists {
		t.Fatalf("pending for repaired slot after removed progress = %#v, want none", state.PendingBySlot[slot])
	}

	plan, state, err = rt.Reconfigure(ctx, Command{
		ID:              "reconcile-after-removed",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: nil,
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(after removed) returned error: %v", err)
	}
	if _, exists := state.PendingBySlot[slot]; exists {
		t.Fatalf("pending for completed slot after removed reconcile = %#v, want none", state.PendingBySlot[slot])
	}
	if len(plan.ChangedSlots) == 0 {
		t.Fatal("reconcile after removed produced no follow-up plan")
	}
}

func TestCheckpointPreservesCompletedProgressWhileAnotherSlotRemainsPending(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
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
	for i, entry := range state.Outbox {
		state, err = rt.AcknowledgeOutbox(ctx, Command{
			ID:              fmt.Sprintf("ack-%d", i),
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

	firstSlot := plan.ChangedSlots[0].Slot
	secondSlot := plan.ChangedSlots[1].Slot
	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-ready-d-first",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   firstSlot,
				NodeID: "d",
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress(first ready) returned error: %v", err)
	}
	if got, want := state.PendingBySlot[secondSlot].Kind, PendingKindReady; got != want {
		t.Fatalf("second slot pending kind before checkpoint = %q, want %q", got, want)
	}
	if _, exists := state.PendingBySlot[firstSlot]; exists {
		t.Fatalf("first slot pending before checkpoint = %#v, want none", state.PendingBySlot[firstSlot])
	}
	if got, want := len(state.CompletedProgressBySlot[firstSlot]), 1; got != want {
		t.Fatalf("completed progress record count for first slot before checkpoint = %d, want %d", got, want)
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}

	reopened := mustOpenRuntime(t, store)
	want := state
	want.AppliedCommands = map[string]AppliedCommand{}
	if got := reopened.Current(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, want)
	}
	if got, want := reopened.Current().PendingBySlot[secondSlot].Kind, PendingKindReady; got != want {
		t.Fatalf("second slot pending kind after checkpoint reopen = %q, want %q", got, want)
	}
	if _, exists := reopened.Current().PendingBySlot[firstSlot]; exists {
		t.Fatalf("first slot pending after checkpoint reopen = %#v, want none", reopened.Current().PendingBySlot[firstSlot])
	}
	if got, want := len(reopened.Current().CompletedProgressBySlot[firstSlot]), 1; got != want {
		t.Fatalf("completed progress record count after checkpoint reopen = %d, want %d", got, want)
	}
}

func TestInterleavedRepairHeartbeatLivenessAndCheckpointReplay(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
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
	state, err = rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-b",
		ExpectedVersion: state.Version,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("b"),
			ObservedAtUnixNano: int64((11 * time.Second).Nanoseconds()),
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat(b) returned error: %v", err)
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
	joiningNodeID := plan.ChangedSlots[0].Steps[0].NodeID
	ackedFirstSlot := false
	for i, entry := range append([]OutboxEntry(nil), state.Outbox...) {
		if entry.Slot != firstSlot {
			continue
		}
		ackedFirstSlot = true
		state, err = rt.AcknowledgeOutbox(ctx, Command{
			ID:              fmt.Sprintf("ack-first-slot-%d", i),
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
	if !ackedFirstSlot {
		t.Fatal("failed to acknowledge any first-slot outbox entry")
	}

	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-ready-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   firstSlot,
				NodeID: joiningNodeID,
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
	if got, want := state.PendingBySlot[firstSlot].Kind, PendingKindRemoved; got != want {
		t.Fatalf("first slot pending kind = %q, want %q", got, want)
	}
	if got, want := state.PendingBySlot[secondSlot].Kind, PendingKindReady; got != want {
		t.Fatalf("second slot pending kind = %q, want %q", got, want)
	}
	if got := countOutboxEntriesForSlot(state.Outbox, secondSlot); got == 0 {
		t.Fatal("second slot unexpectedly lost its outbox work")
	}

	state, err = rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status:             uniqueNodeStatus("d"),
			ObservedAtUnixNano: int64((12 * time.Second).Nanoseconds()),
			FlapWindowNanos:    int64((30 * time.Second).Nanoseconds()),
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat(d) returned error: %v", err)
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
	reopened := mustOpenRuntime(t, store)

	want := state
	want.AppliedCommands = map[string]AppliedCommand{}
	if got := reopened.Current(); !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, want)
	}
	if got, want := reopened.Current().NodeLivenessByID["b"].State, NodeLivenessStateSuspect; got != want {
		t.Fatalf("reopened liveness state = %q, want %q", got, want)
	}
	if got, want := len(reopened.Current().CompletedProgressBySlot[firstSlot]), 1; got != want {
		t.Fatalf("completed progress record count after reopen = %d, want %d", got, want)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func TestDuplicateOutboxAcknowledgeIsIdempotentBeforeAndAfterReopen(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	_, state, err = rt.Reconfigure(ctx, Command{
		ID:              "add-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(add-d) returned error: %v", err)
	}
	if len(state.Outbox) == 0 {
		t.Fatal("outbox unexpectedly empty")
	}

	ack := Command{
		ID:              "ack-outbox-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindAcknowledgeOutbox,
		AcknowledgeOutbox: &AcknowledgeOutboxCommand{
			EntryID: state.Outbox[0].ID,
		},
	}
	acked, err := rt.AcknowledgeOutbox(ctx, ack)
	if err != nil {
		t.Fatalf("AcknowledgeOutbox returned error: %v", err)
	}
	duplicate, err := rt.AcknowledgeOutbox(ctx, ack)
	if err != nil {
		t.Fatalf("duplicate AcknowledgeOutbox returned error: %v", err)
	}
	if !reflect.DeepEqual(duplicate, acked) {
		t.Fatalf("duplicate ack snapshot mismatch\nduplicate=%#v\nacked=%#v", duplicate, acked)
	}

	reopened := mustOpenRuntime(t, store)
	reopenedDuplicate, err := reopened.AcknowledgeOutbox(ctx, ack)
	if err != nil {
		t.Fatalf("reopened duplicate AcknowledgeOutbox returned error: %v", err)
	}
	if !reflect.DeepEqual(reopenedDuplicate, acked) {
		t.Fatalf("reopened duplicate ack snapshot mismatch\nreopened=%#v\nacked=%#v", reopenedDuplicate, acked)
	}
}

func TestMultiSlotReconcilePreservesUntouchedSlotPendingOutboxAndVersion(t *testing.T) {
	ctx := context.Background()
	rt := mustOpenRuntime(t, NewInMemoryStore())

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
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
	secondVersionBefore := state.SlotVersions[secondSlot]
	secondOutboxBefore := countOutboxEntriesForSlot(state.Outbox, secondSlot)

	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-ready-d-first",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   firstSlot,
				NodeID: "d",
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress(first ready) returned error: %v", err)
	}
	_, state, err = rt.Reconfigure(ctx, Command{
		ID:              "reconcile-after-first-ready",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: nil,
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 2},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(after first ready) returned error: %v", err)
	}

	if got, want := state.PendingBySlot[secondSlot].Kind, PendingKindReady; got != want {
		t.Fatalf("second slot pending kind = %q, want %q", got, want)
	}
	if got, want := state.SlotVersions[secondSlot], secondVersionBefore; got != want {
		t.Fatalf("second slot version = %d, want %d", got, want)
	}
	if got, want := countOutboxEntriesForSlot(state.Outbox, secondSlot), secondOutboxBefore; got != want {
		t.Fatalf("second slot outbox count = %d, want %d", got, want)
	}
}

func TestAcknowledgeOutboxMissingEntryDoesNotAdvanceVersion(t *testing.T) {
	ctx := context.Background()
	rt := mustOpenRuntime(t, NewInMemoryStore())

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	_, state, err = rt.Reconfigure(ctx, Command{
		ID:              "add-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(add-d) returned error: %v", err)
	}

	before := rt.Current()
	missing, err := rt.AcknowledgeOutbox(ctx, Command{
		ID:              "ack-outbox-missing",
		ExpectedVersion: before.Version,
		Kind:            CommandKindAcknowledgeOutbox,
		AcknowledgeOutbox: &AcknowledgeOutboxCommand{
			EntryID: "does-not-exist",
		},
	})
	if err != nil {
		t.Fatalf("AcknowledgeOutbox(missing) returned error: %v", err)
	}
	after := rt.Current()
	if got, want := after.Version, before.Version; got != want {
		t.Fatalf("runtime version after missing ack = %d, want %d", got, want)
	}
	if !reflect.DeepEqual(missing, before) {
		t.Fatalf("missing ack snapshot mismatch\nmissing=%#v\nbefore=%#v", missing, before)
	}
}

func TestCompletedProgressHistoryPrunesToBoundedWindow(t *testing.T) {
	current := map[int][]CompletedProgressRecord{}
	slotVersions := map[int]uint64{7: 0}
	for i := 1; i <= completedProgressHistoryLimit+2; i++ {
		slotVersions[7] = uint64(i)
		current = nextCompletedProgress(current, slotVersions, Command{
			Kind: CommandKindProgress,
			Progress: &ProgressCommand{
				Event: coordinator.Event{
					Kind:   coordinator.EventKindReplicaBecameActive,
					Slot:   7,
					NodeID: fmt.Sprintf("node-%d", i),
				},
			},
		})
	}

	records := current[7]
	if got, want := len(records), completedProgressHistoryLimit; got != want {
		t.Fatalf("completed progress history length = %d, want %d", got, want)
	}
	if got, want := records[0], (CompletedProgressRecord{
		NodeID:      "node-3",
		Kind:        CompletedProgressKindReady,
		SlotVersion: 3,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("oldest retained record = %#v, want %#v", got, want)
	}
	if got, want := records[len(records)-1], (CompletedProgressRecord{
		NodeID:      "node-10",
		Kind:        CompletedProgressKindReady,
		SlotVersion: 10,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("newest retained record = %#v, want %#v", got, want)
	}
}

func countOutboxEntriesForSlot(entries []OutboxEntry, slot int) int {
	count := 0
	for _, entry := range entries {
		if entry.Slot == slot {
			count++
		}
	}
	return count
}

func TestSlotVersionsAdvanceOnlyOnSlotChanges(t *testing.T) {
	ctx := context.Background()
	rt := mustOpenRuntime(t, NewInMemoryStore())

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if got, want := state.SlotVersions[0], uint64(1); got != want {
		t.Fatalf("slot version after bootstrap = %d, want %d", got, want)
	}

	for i, nodeID := range []string{"a", "b", "c"} {
		state, err = rt.Heartbeat(ctx, Command{
			ID:              fmt.Sprintf("heartbeat-%d", i+1),
			ExpectedVersion: state.Version,
			Kind:            CommandKindHeartbeat,
			Heartbeat: &HeartbeatCommand{
				Status: storage.NodeStatus{NodeID: nodeID},
			},
		})
		if err != nil {
			t.Fatalf("Heartbeat(%q) returned error: %v", nodeID, err)
		}
	}
	if got, want := state.SlotVersions[0], uint64(1); got != want {
		t.Fatalf("slot version after heartbeats = %d, want %d", got, want)
	}

	_, state, err = rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-add-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(AddNode) returned error: %v", err)
	}
	if got, want := state.SlotVersions[0], uint64(1); got != want {
		t.Fatalf("slot version after add-node without placement change = %d, want %d", got, want)
	}

	state, err = rt.Heartbeat(ctx, Command{
		ID:              "heartbeat-d-ready",
		ExpectedVersion: state.Version,
		Kind:            CommandKindHeartbeat,
		Heartbeat: &HeartbeatCommand{
			Status: storage.NodeStatus{NodeID: "d"},
		},
	})
	if err != nil {
		t.Fatalf("Heartbeat(d) returned error: %v", err)
	}

	for i := 0; i < 5; i++ {
		state, err = rt.Heartbeat(ctx, Command{
			ID:              fmt.Sprintf("heartbeat-extra-%d", i+1),
			ExpectedVersion: state.Version,
			Kind:            CommandKindHeartbeat,
			Heartbeat: &HeartbeatCommand{
				Status: storage.NodeStatus{NodeID: "a"},
			},
		})
		if err != nil {
			t.Fatalf("Heartbeat(extra %d) returned error: %v", i+1, err)
		}
	}
	if got, want := state.SlotVersions[0], uint64(1); got != want {
		t.Fatalf("slot version after extra heartbeats = %d, want %d", got, want)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-begin-drain-c",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindBeginDrainNode, NodeID: "c"}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure(BeginDrainNode) returned error: %v", err)
	}
	if got, want := state.SlotVersions[0], uint64(2); got != want {
		t.Fatalf("slot version after begin-drain reconfigure = %d, want %d", got, want)
	}

	slot := plan.ChangedSlots[0].Slot
	joiningNode := plan.ChangedSlots[0].Steps[0].NodeID
	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-ready-d",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   slot,
				NodeID: joiningNode,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress(ready) returned error: %v", err)
	}
	if got, want := state.SlotVersions[slot], uint64(2); got != want {
		t.Fatalf("slot version after ready progress = %d, want %d", got, want)
	}
}

func TestCheckpointPrunesAppliedCommandsAndPreservesRecovery(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	_, state, err = rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 0},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	expectedAfterCheckpoint := state
	expectedAfterCheckpoint.AppliedCommands = map[string]AppliedCommand{}

	if got := len(rt.Current().AppliedCommands); got != 0 {
		t.Fatalf("applied command count after checkpoint = %d, want 0", got)
	}
	if got := len(store.wal); got != 0 {
		t.Fatalf("wal length after checkpoint = %d, want 0", got)
	}
	if got := rt.Current(); !reflect.DeepEqual(got, expectedAfterCheckpoint) {
		t.Fatalf("state after checkpoint mismatch\ngot=%#v\nwant=%#v", got, expectedAfterCheckpoint)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, expectedAfterCheckpoint) {
		t.Fatalf("recovered state mismatch\nrecovered=%#v\nwant=%#v", got, expectedAfterCheckpoint)
	}
	if got := len(reopened.Current().AppliedCommands); got != 0 {
		t.Fatalf("recovered applied command count after checkpoint = %d, want 0", got)
	}

	_, err = reopened.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err == nil {
		t.Fatal("duplicate Bootstrap after checkpoint unexpectedly succeeded")
	}
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("error = %v, want version mismatch", err)
	}
}

func TestInMemoryStoreReadWrite(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()

	if _, ok, err := store.LoadLatestCheckpoint(ctx); err != nil {
		t.Fatalf("LoadLatestCheckpoint returned error: %v", err)
	} else if ok {
		t.Fatal("LoadLatestCheckpoint unexpectedly reported a checkpoint")
	}

	record := LogRecord{Index: 1, Command: bootstrapCommand("bootstrap-1", 0, 1, 1, "a")}
	if err := store.AppendWAL(ctx, record); err != nil {
		t.Fatalf("AppendWAL returned error: %v", err)
	}
	records, err := store.LoadWAL(ctx, 0)
	if err != nil {
		t.Fatalf("LoadWAL returned error: %v", err)
	}
	if got, want := records, []LogRecord{record}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wal records = %#v, want %#v", got, want)
	}

	checkpoint := Checkpoint{State: State{Version: 1, LastLogIndex: 1, AppliedCommands: map[string]AppliedCommand{}}}
	if err := store.SaveCheckpoint(ctx, checkpoint); err != nil {
		t.Fatalf("SaveCheckpoint returned error: %v", err)
	}
	gotCheckpoint, ok, err := store.LoadLatestCheckpoint(ctx)
	if err != nil {
		t.Fatalf("LoadLatestCheckpoint returned error: %v", err)
	}
	if !ok {
		t.Fatal("LoadLatestCheckpoint did not report checkpoint")
	}
	if gotCheckpoint.State.Version != checkpoint.State.Version {
		t.Fatalf("checkpoint version = %d, want %d", gotCheckpoint.State.Version, checkpoint.State.Version)
	}
	if gotCheckpoint.State.LastLogIndex != checkpoint.State.LastLogIndex {
		t.Fatalf(
			"checkpoint last log index = %d, want %d",
			gotCheckpoint.State.LastLogIndex,
			checkpoint.State.LastLogIndex,
		)
	}
	if !reflect.DeepEqual(gotCheckpoint.State.AppliedCommands, checkpoint.State.AppliedCommands) {
		t.Fatalf(
			"checkpoint applied commands = %#v, want %#v",
			gotCheckpoint.State.AppliedCommands,
			checkpoint.State.AppliedCommands,
		)
	}

	secondRecord := LogRecord{Index: 2, Command: bootstrapCommand("bootstrap-2", 1, 1, 1, "b")}
	if err := store.AppendWAL(ctx, secondRecord); err != nil {
		t.Fatalf("AppendWAL(second) returned error: %v", err)
	}
	if err := store.TruncateWAL(ctx, 1); err != nil {
		t.Fatalf("TruncateWAL returned error: %v", err)
	}
	records, err = store.LoadWAL(ctx, 0)
	if err != nil {
		t.Fatalf("LoadWAL after truncate returned error: %v", err)
	}
	if got, want := records, []LogRecord{secondRecord}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wal records after truncate = %#v, want %#v", got, want)
	}
}

func TestDuplicateCommandAfterCheckpointFailsStaleInsteadOfIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if _, _, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 0},
		},
	}); err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}

	_, err = rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err == nil {
		t.Fatal("duplicate Bootstrap after checkpoint unexpectedly succeeded")
	}
	if !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("error = %v, want version mismatch", err)
	}
}

func TestPostCheckpointCommandsRemainIdempotentUntilNextCheckpoint(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := rt.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}

	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}

	duplicatePlan, duplicateState, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: 1,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("duplicate Reconfigure returned error: %v", err)
	}
	if !reflect.DeepEqual(duplicatePlan, plan) {
		t.Fatalf("duplicate plan mismatch\ngot=%#v\nwant=%#v", duplicatePlan, plan)
	}
	if !reflect.DeepEqual(duplicateState, state) {
		t.Fatalf("duplicate state mismatch\ngot=%#v\nwant=%#v", duplicateState, state)
	}
}

func TestReplayMatchesLiveRuntime(t *testing.T) {
	ctx := context.Background()
	store := NewInMemoryStore()
	rt := mustOpenRuntime(t, store)

	state, err := rt.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c"))
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	plan, state, err := rt.Reconfigure(ctx, Command{
		ID:              "reconfigure-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindReconfigure,
		Reconfigure: &ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindAddNode, Node: uniqueNode("d")}},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 1},
		},
	})
	if err != nil {
		t.Fatalf("Reconfigure returned error: %v", err)
	}
	state, err = rt.ApplyProgress(ctx, Command{
		ID:              "progress-1",
		ExpectedVersion: state.Version,
		Kind:            CommandKindProgress,
		Progress: &ProgressCommand{
			Event: coordinator.Event{
				Kind:   coordinator.EventKindReplicaBecameActive,
				Slot:   plan.ChangedSlots[0].Slot,
				NodeID: plan.ChangedSlots[0].Steps[0].NodeID,
			},
		},
	})
	if err != nil {
		t.Fatalf("ApplyProgress returned error: %v", err)
	}

	reopened := mustOpenRuntime(t, store)
	if got := reopened.Current(); !reflect.DeepEqual(got, state) {
		t.Fatalf("replayed state mismatch\nreplayed=%#v\nwant=%#v", got, state)
	}
	assertCoordinatorStateValid(t, reopened.Current().Cluster)
}

func mustOpenRuntime(t *testing.T, store Store) *Runtime {
	t.Helper()
	rt, err := Open(context.Background(), store)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	return rt
}

func bootstrapCommand(id string, expected uint64, slotCount int, replicationFactor int, nodeIDs ...string) Command {
	return Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            CommandKindBootstrap,
		Bootstrap: &BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         slotCount,
				ReplicationFactor: replicationFactor,
			},
			Nodes: uniqueNodes(nodeIDs...),
		},
	}
}

func TestBootstrapPersistsReconfigurationPolicy(t *testing.T) {
	rt := mustOpenRuntime(t, NewInMemoryStore())
	state, err := rt.Bootstrap(context.Background(), Command{
		ID:              "bootstrap-policy",
		ExpectedVersion: 0,
		Kind:            CommandKindBootstrap,
		Bootstrap: &BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         8,
				ReplicationFactor: 3,
			},
			Policy: coordinator.ReconfigurationPolicy{MaxChangedChains: 32},
		},
	})
	if err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if got, want := state.LastPolicy.MaxChangedChains, 32; got != want {
		t.Fatalf("state.LastPolicy.MaxChangedChains = %d, want %d", got, want)
	}
	if got, want := rt.Current().LastPolicy.MaxChangedChains, 32; got != want {
		t.Fatalf("runtime current LastPolicy.MaxChangedChains = %d, want %d", got, want)
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

func uniqueNodeStatus(id string) storage.NodeStatus {
	return storage.NodeStatus{
		NodeID:          id,
		ReplicaCount:    3,
		ActiveCount:     2,
		CatchingUpCount: 1,
	}
}

func assertCoordinatorStateValid(t *testing.T, state coordinator.ClusterState) {
	t.Helper()
	if _, err := coordinator.PlanReconfiguration(state, nil, coordinator.ReconfigurationPolicy{}); err != nil {
		t.Fatalf("cluster state is not valid for coordinator replay: %v", err)
	}
}
