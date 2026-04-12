package client

import (
	"context"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
)

const benchmarkRouterSlotCount = 1024

func BenchmarkRouterGet_1024Slots(b *testing.B) {
	router, key := benchmarkRouterWithSingleReplica(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := router.Get(context.Background(), key); err != nil {
			b.Fatalf("Get returned error: %v", err)
		}
	}
}

func BenchmarkRouterPut_1024Slots(b *testing.B) {
	router, key := benchmarkRouterWithSingleReplica(b)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := router.Put(context.Background(), key, "value"); err != nil {
			b.Fatalf("Put returned error: %v", err)
		}
	}
}

func benchmarkRouterWithSingleReplica(b *testing.B) (*Router, string) {
	b.Helper()

	ctx := context.Background()
	repl := storage.NewInMemoryReplicationTransport()
	backend := storage.NewInMemoryBackend()
	node := mustNewStorageNodeForRouterBenchmark(b, ctx, "node-a", backend, repl)
	b.Cleanup(func() { _ = node.Close() })

	slot := 17
	key := benchmarkKeyForSlot(b, slot, benchmarkRouterSlotCount, "bench")
	assignment := storage.ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleSingle,
		Peers: storage.ChainPeers{
			TailNodeID: "node-a",
			TailTarget: "node-a",
		},
	}
	mustActivateStorageAssignmentBenchmark(b, node, assignment)
	if _, err := node.HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 slot,
		Key:                  key,
		Value:                "seed",
		ExpectedChainVersion: assignment.ChainVersion,
	}); err != nil {
		b.Fatalf("HandleClientPut seed returned error: %v", err)
	}

	transport := NewInMemoryTransport()
	transport.RegisterNode("node-a", node)
	router := mustNewRouterForBenchmark(b, &scriptedSnapshotSource{
		snapshots: []coordserver.RoutingSnapshot{benchmarkRoutingSnapshot()},
	}, transport)
	if err := router.Refresh(ctx); err != nil {
		b.Fatalf("Refresh returned error: %v", err)
	}

	return router, key
}

func benchmarkRoutingSnapshot() coordserver.RoutingSnapshot {
	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: benchmarkRouterSlotCount,
		Slots:     make([]coordserver.SlotRoute, benchmarkRouterSlotCount),
	}
	for slot := 0; slot < benchmarkRouterSlotCount; slot++ {
		snapshot.Slots[slot] = coordserver.SlotRoute{
			Slot:         slot,
			ChainVersion: 1,
			HeadNodeID:   "node-a",
			TailNodeID:   "node-a",
			Writable:     true,
			Readable:     true,
			ReadReplicas: []coordserver.ReadReplicaRoute{{
				NodeID: "node-a",
				Role:   storage.ReplicaRoleSingle,
			}},
		}
	}
	return snapshot
}

func mustNewRouterForBenchmark(b *testing.B, source SnapshotSource, transport Transport) *Router {
	b.Helper()
	router, err := NewRouter(source, transport)
	if err != nil {
		b.Fatalf("NewRouter returned error: %v", err)
	}
	return router
}

func mustNewStorageNodeForRouterBenchmark(b *testing.B, ctx context.Context, nodeID string, backend storage.Backend, repl *storage.InMemoryReplicationTransport) *storage.Node {
	b.Helper()
	node, err := storage.OpenNode(
		ctx,
		storage.Config{NodeID: nodeID},
		backend,
		storage.NewInMemoryLocalStateStore(),
		storage.NewInMemoryCoordinatorClient(),
		repl,
	)
	if err != nil {
		b.Fatalf("OpenNode(%q) returned error: %v", nodeID, err)
	}
	repl.Register(nodeID, backend)
	repl.RegisterNode(nodeID, node)
	return node
}

func mustActivateStorageAssignmentBenchmark(b *testing.B, node *storage.Node, assignment storage.ReplicaAssignment) {
	b.Helper()
	ctx := context.Background()
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		b.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: assignment.Slot}); err != nil {
		b.Fatalf("ActivateReplica returned error: %v", err)
	}
}

func benchmarkKeyForSlot(b *testing.B, slot int, slotCount int, prefix string) string {
	b.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if int(crc32.ChecksumIEEE([]byte(key))%uint32(slotCount)) == slot {
			return key
		}
	}
	b.Fatalf("unable to find key for slot %d", slot)
	return ""
}
