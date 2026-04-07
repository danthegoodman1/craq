package benchmark

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/adminhttp"
	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/quickstart"
	"github.com/danthegoodman1/craq/storage"
	badgerstore "github.com/danthegoodman1/craq/storage/badger"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

func TestLocalRuntimeAndLoadGen(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	manifest := quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        reserveAddress(t),
			AdminAddress:      reserveAddress(t),
			SlotCount:         8,
			ReplicationFactor: 1,
		},
		Nodes: []quickstart.Node{
			{ID: "a", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "a", "rack": "a", "az": "a"}},
			{ID: "b", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "b", "rack": "b", "az": "b"}},
			{ID: "c", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "c", "rack": "c", "az": "c"}},
		},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest.Validate returned error: %v", err)
	}
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := SaveManifest(manifestPath, manifest); err != nil {
		t.Fatal(err)
	}

	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	coordStore, err := coordruntime.OpenBadgerStore(filepath.Join(tempDir, "coord"))
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}
	t.Cleanup(func() { _ = coordStore.Close() })

	server, err := coordserver.OpenWithConfig(ctx, coordStore, nil, coordserver.ServerConfig{
		NodeClientFactory: grpcx.NewDynamicNodeClientFactory(pool),
	})
	if err != nil {
		t.Fatalf("coordserver.OpenWithConfig returned error: %v", err)
	}
	t.Cleanup(func() { _ = server.Close() })

	coordLis, err := net.Listen("tcp", manifest.Coordinator.RPCAddress)
	if err != nil {
		t.Fatalf("Listen coordinator returned error: %v", err)
	}
	coordGRPC := grpcx.NewCoordinatorGRPCServer(server)
	go func() {
		if err := coordGRPC.Serve(coordLis); err != nil && !errors.Is(err, net.ErrClosed) {
			panic(err)
		}
	}()
	t.Cleanup(func() { _ = coordGRPC.Close() })

	coordAdmin := adminhttp.NewCoordinator(server, adminhttp.Config{
		Address:  manifest.Coordinator.AdminAddress,
		Gatherer: server.MetricsRegistry(),
	})
	go func() {
		if err := coordAdmin.ListenAndServe(); err != nil && !strings.Contains(err.Error(), "closed network connection") && !strings.Contains(err.Error(), "Server closed") {
			panic(err)
		}
	}()
	t.Cleanup(func() { _ = coordAdmin.Close(context.Background()) })

	repl := grpcx.NewReplicationTransport(pool)
	nodeClients := make(map[string]*grpcx.StorageNodeClient, len(manifest.Nodes))
	addressByID := map[string]string{}

	for _, nodeCfg := range manifest.Nodes {
		addressByID[nodeCfg.ID] = nodeCfg.RPCAddress
		store, err := badgerstore.Open(filepath.Join(tempDir, "storage-"+nodeCfg.ID))
		if err != nil {
			t.Fatalf("badgerstore.Open(%s) returned error: %v", nodeCfg.ID, err)
		}
		t.Cleanup(func() { _ = store.Close() })
		node, err := storage.OpenNode(ctx, storage.Config{
			NodeID:         nodeCfg.ID,
			RPCAddress:     nodeCfg.RPCAddress,
			FailureDomains: nodeCfg.FailureDomains,
		}, store.Backend(), store.LocalStateStore(), storage.NewInMemoryCoordinatorClient(), repl)
		if err != nil {
			t.Fatalf("storage.OpenNode(%s) returned error: %v", nodeCfg.ID, err)
		}
		t.Cleanup(func() { _ = node.Close() })

		lis, err := net.Listen("tcp", nodeCfg.RPCAddress)
		if err != nil {
			t.Fatalf("Listen storage %s returned error: %v", nodeCfg.ID, err)
		}
		grpcServer := grpcx.NewStorageGRPCServer(node)
		go func() {
			if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, net.ErrClosed) {
				panic(err)
			}
		}()
		t.Cleanup(func() { _ = grpcServer.Close() })

		adminServer := adminhttp.NewStorage(node, adminhttp.Config{
			Address:  nodeCfg.AdminAddress,
			Gatherer: node.MetricsRegistry(),
		})
		go func() {
			if err := adminServer.ListenAndServe(); err != nil && !strings.Contains(err.Error(), "closed network connection") && !strings.Contains(err.Error(), "Server closed") {
				panic(err)
			}
		}()
		t.Cleanup(func() { _ = adminServer.Close(context.Background()) })

		client := grpcx.NewStorageNodeClient(nodeCfg.RPCAddress, pool)
		nodeClients[nodeCfg.ID] = client
	}

	adminClient := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	clusterNodes := make([]coordinator.Node, 0, len(manifest.Nodes))
	for _, nodeCfg := range manifest.Nodes {
		clusterNodes = append(clusterNodes, coordinator.Node{
			ID:             nodeCfg.ID,
			RPCAddress:     nodeCfg.RPCAddress,
			FailureDomains: nodeCfg.FailureDomains,
		})
	}
	if _, err := adminClient.Bootstrap(ctx, coordruntime.Command{
		ID:   "runtime-test-bootstrap",
		Kind: coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         manifest.Coordinator.SlotCount,
				ReplicationFactor: manifest.Coordinator.ReplicationFactor,
			},
			Nodes: clusterNodes,
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	placement, err := coordinator.BuildInitialPlacement(coordinator.Config{
		SlotCount:         manifest.Coordinator.SlotCount,
		ReplicationFactor: manifest.Coordinator.ReplicationFactor,
	}, clusterNodes)
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	for _, chain := range placement.Chains {
		for idx, replica := range chain.Replicas {
			role := storage.ReplicaRoleMiddle
			switch {
			case len(chain.Replicas) == 1:
				role = storage.ReplicaRoleSingle
			case idx == 0:
				role = storage.ReplicaRoleHead
			case idx == len(chain.Replicas)-1:
				role = storage.ReplicaRoleTail
			}
			assignment := storage.ReplicaAssignment{
				Slot:         chain.Slot,
				ChainVersion: 1,
				Role:         role,
			}
			if idx > 0 {
				pred := chain.Replicas[idx-1].NodeID
				assignment.Peers.PredecessorNodeID = pred
				assignment.Peers.PredecessorTarget = addressByID[pred]
			}
			if idx+1 < len(chain.Replicas) {
				succ := chain.Replicas[idx+1].NodeID
				assignment.Peers.SuccessorNodeID = succ
				assignment.Peers.SuccessorTarget = addressByID[succ]
			}
			client := nodeClients[replica.NodeID]
			if err := client.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				t.Fatalf("AddReplicaAsTail(%s) returned error: %v", replica.NodeID, err)
			}
			if err := client.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				t.Fatalf("ActivateReplica(%s) returned error: %v", replica.NodeID, err)
			}
		}
	}

	outputDir := filepath.Join(tempDir, "client-out")
	report, err := RunLoadGen(context.Background(), LoadGenProcessConfig{
		RunID:        "local-runtime",
		ManifestPath: manifestPath,
		OutputDir:    outputDir,
		Workload: WorkloadProfile{
			Seed:             1,
			PreloadKeys:      10,
			ValueBytes:       16,
			RequestTimeout:   2 * time.Second,
			PerScenarioPause: 10 * time.Millisecond,
			Interval:         250 * time.Millisecond,
			Scenarios: []ScenarioProfile{
				{Name: "get", Kind: "get", Concurrency: 1, Warmup: 250 * time.Millisecond, Duration: 500 * time.Millisecond, ValueBytes: 16},
				{Name: "mixed", Kind: "mixed", Concurrency: 1, Warmup: 250 * time.Millisecond, Duration: 500 * time.Millisecond, ReadPercent: 80, ValueBytes: 16},
			},
		},
	})
	if err != nil {
		t.Fatalf("RunLoadGen returned error: %v", err)
	}
	if len(report.Scenarios) != 2 {
		t.Fatalf("len(report.Scenarios) = %d, want 2", len(report.Scenarios))
	}
	if report.Scenarios[0].TotalOps == 0 {
		t.Fatalf("first scenario TotalOps = 0, want > 0")
	}
	if _, err := os.Stat(filepath.Join(outputDir, "loadgen-report.json")); err != nil {
		t.Fatalf("loadgen-report.json stat error: %v", err)
	}
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	defer lis.Close()
	return lis.Addr().String()
}
