package coordserver

import (
	"context"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func BenchmarkServerEvaluateLiveness_Healthy1024Slots(b *testing.B) {
	server := benchmarkHealthyServer(b, 1024, 3, []string{"a", "b", "c"})

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := server.EvaluateLiveness(context.Background()); err != nil {
			b.Fatalf("EvaluateLiveness returned error: %v", err)
		}
	}
}

func benchmarkHealthyServer(b *testing.B, slotCount int, replicationFactor int, nodeIDs []string) *Server {
	b.Helper()

	ctx := context.Background()
	clock := &benchmarkClock{now: time.Unix(1_000_000, 0).UTC()}
	repl := storage.NewInMemoryReplicationTransport()
	nodeClients := make(map[string]StorageNodeClient, len(nodeIDs))
	adapters := make([]*InMemoryNodeAdapter, 0, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(ctx, nodeID, backend, repl)
		if err != nil {
			b.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		repl.RegisterNode(nodeID, adapter.Node())
		nodeClients[nodeID] = adapter
		adapters = append(adapters, adapter)
		b.Cleanup(func() { _ = adapter.Node().Close() })
	}

	server, err := OpenWithConfig(ctx, coordruntime.NewInMemoryStore(), nodeClients, ServerConfig{
		Clock:                  clock,
		DisableBackgroundLoops: true,
		LivenessPolicy: LivenessPolicy{
			SuspectAfter: 30 * time.Second,
			DeadAfter:    time.Minute,
		},
	})
	if err != nil {
		b.Fatalf("OpenWithConfig returned error: %v", err)
	}
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	b.Cleanup(func() { _ = server.Close() })

	if _, err := server.Bootstrap(ctx, benchmarkBootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor, nodeIDs...)); err != nil {
		b.Fatalf("Bootstrap returned error: %v", err)
	}
	for _, nodeID := range nodeIDs {
		if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: nodeID}); err != nil {
			b.Fatalf("ReportNodeHeartbeat(%q) returned error: %v", nodeID, err)
		}
	}
	return server
}

func benchmarkBootstrapCommand(id string, expected uint64, slotCount int, replicationFactor int, nodeIDs ...string) coordruntime.Command {
	return coordruntime.Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         slotCount,
				ReplicationFactor: replicationFactor,
			},
			Nodes: benchmarkUniqueNodes(nodeIDs...),
		},
	}
}

type benchmarkClock struct {
	now time.Time
}

func (c *benchmarkClock) Now() time.Time {
	return c.now
}

func benchmarkUniqueNodes(ids ...string) []coordinator.Node {
	nodes := make([]coordinator.Node, 0, len(ids))
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		nodes = append(nodes, coordinator.Node{ID: id, RPCAddress: id})
	}
	return nodes
}
