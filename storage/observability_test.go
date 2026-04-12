package storage

import (
	"context"
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestPublishedResourceUsageAndNodeStatusStayAccurate(t *testing.T) {
	ctx := context.Background()
	node, err := NewNode(
		ctx,
		Config{NodeID: "node-a"},
		NewInMemoryBackend(),
		NewInMemoryCoordinatorClient(),
		NewInMemoryReplicationTransport(),
	)
	if err != nil {
		t.Fatalf("NewNode returned error: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	mustActivateReplicaForObservability(t, node, ReplicaAssignment{
		Slot:         1,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
		Peers: ChainPeers{
			TailNodeID: "node-a",
			TailTarget: "node-a",
		},
	})
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         2,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail(slot 2) returned error: %v", err)
	}
	mustActivateReplicaForObservability(t, node, ReplicaAssignment{
		Slot:         3,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
		Peers: ChainPeers{
			TailNodeID: "node-a",
			TailTarget: "node-a",
		},
	})
	if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 3}); err != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}

	mustMutateSlotRecord(t, node, 1, func(record replicaRecord) replicaRecord {
		record.inFlightClientWrites = 2
		record.bufferedForwards[11] = ForwardWriteRequest{
			Operation: WriteOperation{Slot: 1, Sequence: 11, Kind: OperationKindPut, Key: "a", Value: "1"},
		}
		record.bufferedCommits[11] = CommitWriteRequest{Slot: 1, Sequence: 11}
		record.dirtyByKey["a"] = []dirtyReadEntry{{
			Sequence: 11,
			Operation: WriteOperation{
				Slot:     1,
				Sequence: 11,
				Kind:     OperationKindPut,
				Key:      "a",
				Value:    "1",
			},
		}}
		return record
	})
	mustMutateSlotRecord(t, node, 2, func(record replicaRecord) replicaRecord {
		record.bufferedForwards[21] = ForwardWriteRequest{
			Operation: WriteOperation{Slot: 2, Sequence: 21, Kind: OperationKindPut, Key: "b", Value: "2"},
		}
		return record
	})
	node.mu.Lock()
	node.inFlightClientWrites = 2
	node.inFlightCatchups = 1
	node.mu.Unlock()
	node.refreshMetricGauges()

	usage := node.ResourceUsage()
	if got, want := usage.InFlightClientWritesPerNode, 2; got != want {
		t.Fatalf("InFlightClientWritesPerNode = %d, want %d", got, want)
	}
	if got, want := usage.BufferedReplicaMessagesPerNode, 3; got != want {
		t.Fatalf("BufferedReplicaMessagesPerNode = %d, want %d", got, want)
	}
	if got, want := usage.ActiveCatchups, 1; got != want {
		t.Fatalf("ActiveCatchups = %d, want %d", got, want)
	}
	if got, want := usage.InFlightClientWritesPerSlot[1], 2; got != want {
		t.Fatalf("InFlightClientWritesPerSlot[1] = %d, want %d", got, want)
	}
	if got, want := usage.BufferedReplicaMessagesPerSlot[1], 2; got != want {
		t.Fatalf("BufferedReplicaMessagesPerSlot[1] = %d, want %d", got, want)
	}
	if got, want := usage.BufferedReplicaMessagesPerSlot[2], 1; got != want {
		t.Fatalf("BufferedReplicaMessagesPerSlot[2] = %d, want %d", got, want)
	}
	if got, want := usage.DirtyKeysPerSlot[1], 1; got != want {
		t.Fatalf("DirtyKeysPerSlot[1] = %d, want %d", got, want)
	}
	if got, want := node.CatchingUpSlots(), []int{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CatchingUpSlots() = %v, want %v", got, want)
	}
	if got, want := node.LeavingSlots(), []int{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LeavingSlots() = %v, want %v", got, want)
	}

	status := node.snapshotNodeStatus()
	if got, want := status.ReplicaCount, 3; got != want {
		t.Fatalf("ReplicaCount = %d, want %d", got, want)
	}
	if got, want := status.ActiveCount, 1; got != want {
		t.Fatalf("ActiveCount = %d, want %d", got, want)
	}
	if got, want := status.CatchingUpCount, 1; got != want {
		t.Fatalf("CatchingUpCount = %d, want %d", got, want)
	}
	if got, want := status.LeavingCount, 1; got != want {
		t.Fatalf("LeavingCount = %d, want %d", got, want)
	}

	if got, want := testutil.ToFloat64(node.metrics.inFlightWrites), float64(2); got != want {
		t.Fatalf("inFlightWrites gauge = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(node.metrics.bufferedReplicaMsgs), float64(3); got != want {
		t.Fatalf("bufferedReplicaMsgs gauge = %v, want %v", got, want)
	}
	if got, want := testutil.ToFloat64(node.metrics.catchups), float64(1); got != want {
		t.Fatalf("catchups gauge = %v, want %v", got, want)
	}
}

func mustActivateReplicaForObservability(t *testing.T, node *Node, assignment ReplicaAssignment) {
	t.Helper()
	ctx := context.Background()
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: assignment.Slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
}
