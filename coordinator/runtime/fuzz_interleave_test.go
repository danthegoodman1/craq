package runtime

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
)

func FuzzRuntimeInterleaveAndReplay(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 4, 1, 3, 2, 1, 3, 2},
		{0, 1, 3, 2, 6},
		{0, 5, 1, 4, 3, 2, 6},
		{0, 1, 1, 3, 2, 1, 3, 2, 6},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 48 {
			program = program[:48]
		}
		ctx := context.Background()
		store := NewInMemoryStore()
		rt := mustOpenRuntime(t, store)
		cmdID := 0
		nodePool := []string{"a", "b", "c", "d", "e", "f"}

		for step, raw := range program {
			state := rt.Current()
			switch raw % 7 {
			case 0:
				if state.Version == 0 {
					cmdID++
					if _, err := rt.Bootstrap(ctx, bootstrapCommand(commandID("bootstrap", cmdID), 0, 8, 3, "a", "b", "c")); err != nil {
						t.Fatalf("Bootstrap returned error: %v", err)
					}
				}
			case 1:
				if state.Version == 0 {
					break
				}
				events := runtimeFuzzReconfigureEvents(state, nodePool)
				if len(events) == 0 && len(state.PendingBySlot) == 0 && len(state.Outbox) == 0 {
					break
				}
				policy := coordinator.ReconfigurationPolicy{MaxChangedChains: 1}
				if _, err := coordinator.PlanReconfiguration(cloneClusterState(state.Cluster), cloneEvents(events), policy); err != nil {
					break
				}
				cmdID++
				if _, _, err := rt.Reconfigure(ctx, Command{
					ID:              commandID("reconfigure", cmdID),
					ExpectedVersion: state.Version,
					Kind:            CommandKindReconfigure,
					Reconfigure: &ReconfigureCommand{
						Events: events,
						Policy: policy,
					},
				}); err != nil {
					t.Fatalf("Reconfigure returned error: %v", err)
				}
			case 2:
				if state.Version == 0 || len(state.PendingBySlot) == 0 {
					break
				}
				slot, pending := firstPendingWork(state.PendingBySlot)
				cmdID++
				if _, err := rt.ApplyProgress(ctx, Command{
					ID:              commandID("progress", cmdID),
					ExpectedVersion: state.Version,
					Kind:            CommandKindProgress,
					Progress: &ProgressCommand{
						Event: runtimeFuzzProgressEvent(slot, pending),
					},
				}); err != nil {
					t.Fatalf("ApplyProgress returned error: %v", err)
				}
			case 3:
				if state.Version == 0 || len(state.Outbox) == 0 {
					break
				}
				cmdID++
				entryIDs := []string{state.Outbox[0].ID}
				if _, err := rt.AcknowledgeOutbox(ctx, Command{
					ID:              commandID("ack", cmdID),
					ExpectedVersion: state.Version,
					Kind:            CommandKindAcknowledgeOutbox,
					AcknowledgeOutbox: &AcknowledgeOutboxCommand{
						EntryIDs: entryIDs,
					},
				}); err != nil {
					t.Fatalf("AcknowledgeOutbox returned error: %v", err)
				}
			case 4:
				if state.Version == 0 || len(state.Cluster.NodeOrder) == 0 {
					break
				}
				nodeID := state.Cluster.NodeOrder[int(raw)%len(state.Cluster.NodeOrder)]
				cmdID++
				if _, err := rt.Heartbeat(ctx, Command{
					ID:              commandID("heartbeat", cmdID),
					ExpectedVersion: state.Version,
					Kind:            CommandKindHeartbeat,
					Heartbeat: &HeartbeatCommand{
						Status:             uniqueNodeStatus(nodeID),
						ObservedAtUnixNano: int64(step + 1),
						FlapWindowNanos:    int64(time.Minute),
					},
				}); err != nil {
					t.Fatalf("Heartbeat returned error: %v", err)
				}
			case 5:
				if state.Version == 0 || len(state.Cluster.NodeOrder) == 0 {
					break
				}
				nodeID := state.Cluster.NodeOrder[int(raw)%len(state.Cluster.NodeOrder)]
				targetState := NodeLivenessStateSuspect
				if step%2 == 1 {
					targetState = NodeLivenessStateHealthy
				}
				cmdID++
				if _, err := rt.ApplyLiveness(ctx, Command{
					ID:              commandID("liveness", cmdID),
					ExpectedVersion: state.Version,
					Kind:            CommandKindLiveness,
					Liveness: &LivenessCommand{
						NodeID:              nodeID,
						State:               targetState,
						EvaluatedAtUnixNano: int64(step + 1),
						FlapWindowNanos:     int64(time.Minute),
					},
				}); err != nil {
					t.Fatalf("ApplyLiveness returned error: %v", err)
				}
			case 6:
				if state.Version == 0 {
					break
				}
				if err := rt.Checkpoint(ctx); err != nil {
					t.Fatalf("Checkpoint returned error: %v", err)
				}
			}

			assertRuntimeFuzzInvariants(t, rt.Current())
			live := rt.Current()
			reopened := mustOpenRuntime(t, store)
			if got := reopened.Current(); !reflect.DeepEqual(got, live) {
				t.Fatalf("reopened runtime mismatch\ngot=%#v\nwant=%#v", got, live)
			}
			rt = reopened
		}
	})
}

func runtimeFuzzReconfigureEvents(state State, nodePool []string) []coordinator.Event {
	for _, nodeID := range nodePool {
		if _, ok := state.Cluster.NodesByID[nodeID]; ok {
			continue
		}
		return []coordinator.Event{{
			Kind: coordinator.EventKindAddNode,
			Node: uniqueNode(nodeID),
		}}
	}
	return nil
}

func firstPendingWork(current map[int]PendingWork) (int, PendingWork) {
	slots := make([]int, 0, len(current))
	for slot := range current {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	slot := slots[0]
	return slot, current[slot]
}

func runtimeFuzzProgressEvent(slot int, pending PendingWork) coordinator.Event {
	switch pending.Kind {
	case PendingKindReady:
		return coordinator.Event{
			Kind:   coordinator.EventKindReplicaBecameActive,
			NodeID: pending.NodeID,
			Slot:   slot,
		}
	case PendingKindRemoved:
		return coordinator.Event{
			Kind:   coordinator.EventKindReplicaRemoved,
			NodeID: pending.NodeID,
			Slot:   slot,
		}
	default:
		return coordinator.Event{}
	}
}

func assertRuntimeFuzzInvariants(t *testing.T, state State) {
	t.Helper()
	assertCoordinatorStateValid(t, state.Cluster)
	if state.Version != state.LastLogIndex {
		t.Fatalf("state version = %d, want last log index %d", state.Version, state.LastLogIndex)
	}
	for slot, pending := range state.PendingBySlot {
		if got := state.SlotVersions[slot]; got != pending.SlotVersion {
			t.Fatalf("pending slot %d version = %d, want %d", slot, pending.SlotVersion, got)
		}
		if !runtimeFuzzPendingMatchesCluster(state.Cluster, slot, pending) {
			t.Fatalf("pending work for slot %d does not match cluster state: %#v", slot, pending)
		}
	}
}

func runtimeFuzzPendingMatchesCluster(cluster coordinator.ClusterState, slot int, pending PendingWork) bool {
	if slot < 0 || slot >= len(cluster.Chains) {
		return false
	}
	chain := cluster.Chains[slot]
	for _, replica := range chain.Replicas {
		if replica.NodeID != pending.NodeID {
			continue
		}
		switch pending.Kind {
		case PendingKindReady:
			return replica.State == coordinator.ReplicaStateJoining
		case PendingKindRemoved:
			return replica.State == coordinator.ReplicaStateLeaving
		default:
			return false
		}
	}
	return false
}

func commandID(kind string, seq int) string {
	return kind + "-" + fmt.Sprint(seq)
}
