package storage

import (
	"context"
	"testing"
)

func BenchmarkNodeRefreshMetricGauges_1024Slots(b *testing.B) {
	node := benchmarkObservedNode(b, 1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		node.refreshMetricGauges()
	}
}

func BenchmarkNodeResourceUsage_1024Slots(b *testing.B) {
	node := benchmarkObservedNode(b, 1024)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = node.ResourceUsage()
	}
}

func benchmarkObservedNode(b *testing.B, slotCount int) *Node {
	b.Helper()

	ctx := context.Background()
	node, err := NewNode(
		ctx,
		Config{NodeID: "node-a"},
		NewInMemoryBackend(),
		NewInMemoryCoordinatorClient(),
		NewInMemoryReplicationTransport(),
	)
	if err != nil {
		b.Fatalf("NewNode returned error: %v", err)
	}
	b.Cleanup(func() { _ = node.Close() })

	for slot := 0; slot < slotCount; slot++ {
		assignment := ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 1,
			Role:         ReplicaRoleSingle,
			Peers: ChainPeers{
				TailNodeID: "node-a",
				TailTarget: "node-a",
			},
		}
		if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
			b.Fatalf("AddReplicaAsTail(%d) returned error: %v", slot, err)
		}
		if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
			b.Fatalf("ActivateReplica(%d) returned error: %v", slot, err)
		}
		mustMutateSlotRecordBenchmark(b, node, slot, func(record replicaRecord) replicaRecord {
			record.bufferedForwards[uint64(slot+1)] = ForwardWriteRequest{
				Operation: WriteOperation{
					Slot:     slot,
					Sequence: uint64(slot + 1),
					Kind:     OperationKindPut,
					Key:      "k",
					Value:    "v",
				},
			}
			record.inFlightClientWrites = 1
			record.dirtyByKey["k"] = []dirtyReadEntry{{
				Sequence: uint64(slot + 1),
				Operation: WriteOperation{
					Slot:     slot,
					Sequence: uint64(slot + 1),
					Kind:     OperationKindPut,
					Key:      "k",
					Value:    "v",
				},
			}}
			return record
		})
	}
	node.mu.Lock()
	node.inFlightClientWrites = slotCount
	node.mu.Unlock()
	node.refreshMetricGauges()
	return node
}

func mustMutateSlotRecordBenchmark(b *testing.B, node *Node, slot int, mutate func(replicaRecord) replicaRecord) {
	b.Helper()
	owner := node.ensureSlotOwner(slot)
	done := make(chan struct{}, 1)
	if err := owner.dispatch(context.Background(), func(runtime *slotRuntime) {
		record := ensureProtocolReplicaState(runtime.record)
		runtime.setRecord(mutate(record))
		done <- struct{}{}
	}); err != nil {
		b.Fatalf("slot owner dispatch returned error: %v", err)
	}
	select {
	case <-node.done:
		b.Fatal("node shut down while waiting on slot owner")
	case <-done:
	}
}
