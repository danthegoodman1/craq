package coordserver

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func TestReduceReplicaReadyProgressUsesFallbackRouteAndGeneratesCommand(t *testing.T) {
	current := coordruntime.SlotProgressView{
		Version:           17,
		ReplicationFactor: 3,
		Chain: coordinator.Chain{
			Slot: 4,
			Replicas: []coordinator.Replica{
				{NodeID: "a", State: coordinator.ReplicaStateActive},
				{NodeID: "b", State: coordinator.ReplicaStateActive},
				{NodeID: "d", State: coordinator.ReplicaStateJoining},
			},
		},
		SlotVersion: 9,
		Pending: &coordruntime.PendingWork{
			Slot:        4,
			NodeID:      "d",
			Kind:        coordruntime.PendingKindReady,
			SlotVersion: 9,
		},
	}

	reduction, err := reduceReplicaReadyProgress(current, "d", "", 2)
	if err != nil {
		t.Fatalf("reduceReplicaReadyProgress returned error: %v", err)
	}
	if reduction.duplicateCompleted {
		t.Fatal("reduction unexpectedly marked progress as duplicate")
	}
	if !reduction.enqueuePeerRefresh {
		t.Fatal("reduction did not request active peer refresh")
	}
	if !reduction.peerRefreshState.useFallbackRoute {
		t.Fatal("reduction did not keep the fallback serving route active")
	}
	if got, want := reduction.progressCommandID, "server-progress-ready-d-4-r2-v17"; got != want {
		t.Fatalf("progressCommandID = %q, want %q", got, want)
	}
	if got, want := reduction.peerRefreshState.fallbackServingChain.Replicas, []coordinator.Replica{
		{NodeID: "a", State: coordinator.ReplicaStateActive},
		{NodeID: "b", State: coordinator.ReplicaStateActive},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fallbackServingChain replicas = %#v, want %#v", got, want)
	}
}

func TestReduceReplicaReadyProgressTreatsCompletedRecordAsDuplicate(t *testing.T) {
	current := coordruntime.SlotProgressView{
		Version:     11,
		SlotVersion: 7,
		Chain: coordinator.Chain{
			Slot: 2,
			Replicas: []coordinator.Replica{
				{NodeID: "a", State: coordinator.ReplicaStateActive},
				{NodeID: "d", State: coordinator.ReplicaStateActive},
			},
		},
		Completed: []coordruntime.CompletedProgressRecord{
			{NodeID: "d", Kind: coordruntime.CompletedProgressKindReady, SlotVersion: 7},
		},
	}

	reduction, err := reduceReplicaReadyProgress(current, "d", "ready-1", 0)
	if err != nil {
		t.Fatalf("reduceReplicaReadyProgress returned error: %v", err)
	}
	if !reduction.duplicateCompleted {
		t.Fatal("reduction did not treat completed ready progress as duplicate")
	}
	if reduction.progressCommandID != "" {
		t.Fatalf("progressCommandID = %q, want empty for duplicate", reduction.progressCommandID)
	}
}

func TestReduceReplicaRemovedProgressGeneratesCommand(t *testing.T) {
	current := coordruntime.SlotProgressView{
		Version:           23,
		ReplicationFactor: 3,
		Chain: coordinator.Chain{
			Slot: 5,
			Replicas: []coordinator.Replica{
				{NodeID: "a", State: coordinator.ReplicaStateActive},
				{NodeID: "b", State: coordinator.ReplicaStateActive},
				{NodeID: "c", State: coordinator.ReplicaStateLeaving},
			},
		},
		SlotVersion: 12,
		Pending: &coordruntime.PendingWork{
			Slot:        5,
			NodeID:      "c",
			Kind:        coordruntime.PendingKindRemoved,
			SlotVersion: 12,
		},
	}

	reduction, err := reduceReplicaRemovedProgress(current, "c", "", 1)
	if err != nil {
		t.Fatalf("reduceReplicaRemovedProgress returned error: %v", err)
	}
	if reduction.duplicateCompleted {
		t.Fatal("reduction unexpectedly marked remove progress as duplicate")
	}
	if !reduction.enqueuePeerRefresh {
		t.Fatal("reduction did not request a peer refresh after removal")
	}
	if got, want := reduction.progressCommandID, "server-progress-removed-c-5-r1-v23"; got != want {
		t.Fatalf("progressCommandID = %q, want %q", got, want)
	}
}

func TestReduceReplicaRemovedProgressRejectsWrongPendingKind(t *testing.T) {
	current := coordruntime.SlotProgressView{
		Version:     19,
		SlotVersion: 4,
		Chain: coordinator.Chain{
			Slot: 3,
			Replicas: []coordinator.Replica{
				{NodeID: "a", State: coordinator.ReplicaStateActive},
				{NodeID: "b", State: coordinator.ReplicaStateLeaving},
			},
		},
		Pending: &coordruntime.PendingWork{
			Slot:        3,
			NodeID:      "b",
			Kind:        coordruntime.PendingKindReady,
			SlotVersion: 4,
		},
	}

	_, err := reduceReplicaRemovedProgress(current, "b", "", 0)
	if err == nil {
		t.Fatal("reduceReplicaRemovedProgress unexpectedly succeeded")
	}
	if !errors.Is(err, ErrUnexpectedProgress) {
		t.Fatalf("error = %v, want ErrUnexpectedProgress", err)
	}
}

func TestReduceRoutingSnapshotHonorsQueuedActivePeerRefresh(t *testing.T) {
	state := coordruntime.View{
		Version: 31,
		Cluster: coordinator.ClusterState{
			SlotCount: 2,
			NodesByID: map[string]coordinator.Node{
				"a": {ID: "a", RPCAddress: "node-a"},
				"b": {ID: "b", RPCAddress: "node-b"},
				"c": {ID: "c", RPCAddress: "node-c"},
			},
			Chains: []coordinator.Chain{
				{
					Slot: 0,
					Replicas: []coordinator.Replica{
						{NodeID: "a", State: coordinator.ReplicaStateActive},
						{NodeID: "b", State: coordinator.ReplicaStateActive},
						{NodeID: "c", State: coordinator.ReplicaStateJoining},
					},
				},
				{
					Slot: 1,
					Replicas: []coordinator.Replica{
						{NodeID: "a", State: coordinator.ReplicaStateActive},
						{NodeID: "b", State: coordinator.ReplicaStateLeaving},
					},
				},
			},
		},
		SlotVersions: map[int]uint64{
			0: 7,
			1: 8,
		},
	}

	snapshot := reduceRoutingSnapshot(
		state,
		nil,
		map[int]activePeerRefreshState{
			0: {
				fallbackServingChain: coordinator.Chain{
					Slot: 0,
					Replicas: []coordinator.Replica{
						{NodeID: "a", State: coordinator.ReplicaStateActive},
						{NodeID: "b", State: coordinator.ReplicaStateActive},
					},
				},
				useFallbackRoute: true,
			},
			1: {},
		},
	)

	if got, want := len(snapshot.Slots), 2; got != want {
		t.Fatalf("slot count = %d, want %d", got, want)
	}
	if got, want := snapshot.Slots[0].Writable, true; got != want {
		t.Fatalf("slot 0 Writable = %t, want %t", got, want)
	}
	if got, want := snapshot.Slots[0].Readable, true; got != want {
		t.Fatalf("slot 0 Readable = %t, want %t", got, want)
	}
	if got, want := snapshot.Slots[0].ReadReplicas, []ReadReplicaRoute{
		{NodeID: "a", Endpoint: "node-a", Role: storage.ReplicaRoleHead},
		{NodeID: "b", Endpoint: "node-b", Role: storage.ReplicaRoleTail},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slot 0 ReadReplicas = %#v, want %#v", got, want)
	}
	if snapshot.Slots[1].Readable || snapshot.Slots[1].Writable {
		t.Fatalf("slot 1 unexpectedly remained readable+writable during active peer refresh: %#v", snapshot.Slots[1])
	}
}

func TestReduceNodeLivenessTarget(t *testing.T) {
	policy := LivenessPolicy{
		SuspectAfter: 2 * time.Second,
		DeadAfter:    5 * time.Second,
	}
	now := time.Unix(0, 10*time.Second.Nanoseconds()).UnixNano()

	tests := []struct {
		name   string
		record coordruntime.NodeLivenessRecord
		want   coordruntime.NodeLivenessState
	}{
		{
			name:   "healthy",
			record: coordruntime.NodeLivenessRecord{State: coordruntime.NodeLivenessStateHealthy, LastHeartbeatUnixNano: time.Unix(0, 9*time.Second.Nanoseconds()).UnixNano()},
			want:   coordruntime.NodeLivenessStateHealthy,
		},
		{
			name:   "suspect",
			record: coordruntime.NodeLivenessRecord{State: coordruntime.NodeLivenessStateHealthy, LastHeartbeatUnixNano: time.Unix(0, 7*time.Second.Nanoseconds()).UnixNano()},
			want:   coordruntime.NodeLivenessStateSuspect,
		},
		{
			name:   "dead",
			record: coordruntime.NodeLivenessRecord{State: coordruntime.NodeLivenessStateSuspect, LastHeartbeatUnixNano: time.Unix(0, 4*time.Second.Nanoseconds()).UnixNano()},
			want:   coordruntime.NodeLivenessStateDead,
		},
		{
			name:   "future heartbeat stays healthy",
			record: coordruntime.NodeLivenessRecord{State: coordruntime.NodeLivenessStateSuspect, LastHeartbeatUnixNano: time.Unix(0, 12*time.Second.Nanoseconds()).UnixNano()},
			want:   coordruntime.NodeLivenessStateHealthy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reduceNodeLivenessTarget(tt.record, now, policy); got != tt.want {
				t.Fatalf("reduceNodeLivenessTarget() = %q, want %q", got, tt.want)
			}
		})
	}
}
