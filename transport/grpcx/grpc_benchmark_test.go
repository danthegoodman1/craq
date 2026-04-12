package grpcx_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/danthegoodman1/craq/client"
	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
	badgerstore "github.com/danthegoodman1/craq/storage/badger"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

func BenchmarkClientLatencyGRPC_Localhost(b *testing.B) {
	b.Run("single_replica_get", func(b *testing.B) {
		h := newLocalhostGRPCBenchmarkHarness(b, 1)
		readKey := h.mustSeedReadKey(b, "read-key", "value")

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			result, err := h.router.Get(context.Background(), readKey)
			if err != nil {
				b.Fatalf("Get returned error: %v", err)
			}
			if !result.Found || result.Value != "value" {
				b.Fatalf("Get result = %#v, want found value", result)
			}
		}
	})

	b.Run("single_replica_put", func(b *testing.B) {
		h := newLocalhostGRPCBenchmarkHarness(b, 1)
		h.mustWarmConnections(b)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("write-%d", i)
			if _, err := h.router.Put(context.Background(), key, "value"); err != nil {
				b.Fatalf("Put returned error: %v", err)
			}
		}
	})

	b.Run("three_replica_get", func(b *testing.B) {
		h := newLocalhostGRPCBenchmarkHarness(b, 3)
		readKey := h.mustSeedReadKey(b, "read-key", "value")

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			result, err := h.router.Get(context.Background(), readKey)
			if err != nil {
				b.Fatalf("Get returned error: %v", err)
			}
			if !result.Found || result.Value != "value" {
				b.Fatalf("Get result = %#v, want found value", result)
			}
		}
	})

	b.Run("three_replica_put", func(b *testing.B) {
		h := newLocalhostGRPCBenchmarkHarness(b, 3)
		h.mustWarmConnections(b)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("write-%d", i)
			if _, err := h.router.Put(context.Background(), key, "value"); err != nil {
				b.Fatalf("Put returned error: %v", err)
			}
		}
	})

	b.Run("five_replica_get", func(b *testing.B) {
		h := newLocalhostGRPCBenchmarkHarness(b, 5)
		readKey := h.mustSeedReadKey(b, "read-key", "value")

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			result, err := h.router.Get(context.Background(), readKey)
			if err != nil {
				b.Fatalf("Get returned error: %v", err)
			}
			if !result.Found || result.Value != "value" {
				b.Fatalf("Get result = %#v, want found value", result)
			}
		}
	})

	b.Run("five_replica_put", func(b *testing.B) {
		h := newLocalhostGRPCBenchmarkHarness(b, 5)
		h.mustWarmConnections(b)

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("write-%d", i)
			if _, err := h.router.Put(context.Background(), key, "value"); err != nil {
				b.Fatalf("Put returned error: %v", err)
			}
		}
	})
}

func BenchmarkClientPutThroughputGRPC_Localhost(b *testing.B) {
	for _, concurrency := range []int{32, 64, 128, 256} {
		b.Run(fmt.Sprintf("three_replica_put_c%d", concurrency), func(b *testing.B) {
			h := newLocalhostGRPCBenchmarkHarness(b, 3)
			h.mustWarmConnections(b)
			seedBenchmarkPutKeys(b, h.router, concurrency*8)
			benchmarkRouterPutThroughput(b, h.router, concurrency, concurrency*8)
		})
	}
}

type localhostGRPCBenchmarkHarness struct {
	router         *client.Router
	pool           *grpcx.ConnPool
	admin          *grpcx.CoordinatorAdminClient
	coordService   *grpcx.CoordinatorGRPCServer
	storageServers []*grpcx.StorageGRPCServer
	nodes          []*storage.Node
}

func newLocalhostGRPCBenchmarkHarness(tb testing.TB, replicationFactor int) *localhostGRPCBenchmarkHarness {
	tb.Helper()
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	tb.Cleanup(func() { _ = pool.Close() })

	coordAddr := mustReserveBenchmarkAddress(tb)
	coordListener, err := net.Listen("tcp", coordAddr)
	if err != nil {
		tb.Fatalf("Listen returned error: %v", err)
	}

	server, err := coordserver.Open(ctx, coordruntime.NewInMemoryStore(), nil)
	if err != nil {
		tb.Fatalf("coordserver.Open returned error: %v", err)
	}
	coordService := grpcx.NewCoordinatorGRPCServer(server)
	go func() {
		if err := coordService.Serve(coordListener); err != nil && !errors.Is(err, net.ErrClosed) {
			panic(err)
		}
	}()
	tb.Cleanup(func() { _ = coordService.Close() })

	repl := grpcx.NewReplicationTransport(pool)
	coordClient := storage.NewInMemoryCoordinatorClient()
	coordAdmin := grpcx.NewCoordinatorAdminClient(coordAddr, pool)

	clusterNodes := make([]coordinator.Node, 0, replicationFactor)
	nodeByID := make(map[string]*storage.Node, replicationFactor)
	addressByID := make(map[string]string, replicationFactor)
	h := &localhostGRPCBenchmarkHarness{
		pool:         pool,
		admin:        coordAdmin,
		coordService: coordService,
	}

	for i := 0; i < replicationFactor; i++ {
		nodeID := benchmarkNodeID(i)
		address := mustReserveBenchmarkAddress(tb)
		addressByID[nodeID] = address
		clusterNode := coordinator.Node{
			ID:         nodeID,
			RPCAddress: address,
			FailureDomains: map[string]string{
				"host": "host-" + nodeID,
				"rack": "rack-" + nodeID,
				"az":   "az-" + nodeID,
			},
		}
		clusterNodes = append(clusterNodes, clusterNode)

		storePath := filepath.Join(tb.TempDir(), nodeID)
		store, err := badgerstore.Open(storePath)
		if err != nil {
			tb.Fatalf("badger.Open(%q) returned error: %v", storePath, err)
		}
		node, err := storage.OpenNode(
			ctx,
			storage.Config{NodeID: nodeID},
			store.Backend(),
			store.LocalStateStore(),
			coordClient,
			repl,
		)
		if err != nil {
			_ = store.Close()
			tb.Fatalf("OpenNode(%q) returned error: %v", nodeID, err)
		}
		nodeByID[nodeID] = node
		h.nodes = append(h.nodes, node)
		tb.Cleanup(func() { _ = node.Close() })

		server := mustStartBenchmarkStorageServer(tb, node, address)
		h.storageServers = append(h.storageServers, server)
		tb.Cleanup(func() { _ = server.Close() })
	}

	if _, err := coordAdmin.Bootstrap(ctx, coordruntime.Command{
		ID:   fmt.Sprintf("bootstrap-rf-%d", replicationFactor),
		Kind: coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         1,
				ReplicationFactor: replicationFactor,
			},
			Nodes: clusterNodes,
		},
	}); err != nil {
		tb.Fatalf("Bootstrap returned error: %v", err)
	}

	placement, err := coordinator.BuildInitialPlacement(coordinator.Config{
		SlotCount:         1,
		ReplicationFactor: replicationFactor,
	}, clusterNodes)
	if err != nil {
		tb.Fatalf("BuildInitialPlacement returned error: %v", err)
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
			node := nodeByID[replica.NodeID]
			if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				tb.Fatalf("AddReplicaAsTail(%q) returned error: %v", replica.NodeID, err)
			}
			if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				tb.Fatalf("ActivateReplica(%q) returned error: %v", replica.NodeID, err)
			}
		}
	}

	router, err := client.NewRouter(coordAdmin, grpcx.NewClientTransport(pool))
	if err != nil {
		tb.Fatalf("NewRouter returned error: %v", err)
	}
	if err := router.Refresh(ctx); err != nil {
		tb.Fatalf("Refresh returned error: %v", err)
	}
	h.router = router

	return h
}

func (h *localhostGRPCBenchmarkHarness) mustSeedReadKey(tb testing.TB, key string, value string) string {
	tb.Helper()
	if _, err := h.router.Put(context.Background(), key, value); err != nil {
		tb.Fatalf("seed Put returned error: %v", err)
	}
	result, err := h.router.Get(context.Background(), key)
	if err != nil {
		tb.Fatalf("seed Get returned error: %v", err)
	}
	if !result.Found || result.Value != value {
		tb.Fatalf("seed Get result = %#v, want found value", result)
	}
	return key
}

func (h *localhostGRPCBenchmarkHarness) mustWarmConnections(tb testing.TB) {
	tb.Helper()
	if _, err := h.router.Put(context.Background(), "warmup", "value"); err != nil {
		tb.Fatalf("warmup Put returned error: %v", err)
	}
	if _, err := h.router.Get(context.Background(), "warmup"); err != nil {
		tb.Fatalf("warmup Get returned error: %v", err)
	}
}

func mustReserveBenchmarkAddress(tb testing.TB) string {
	tb.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("Listen returned error: %v", err)
	}
	address := lis.Addr().String()
	_ = lis.Close()
	return address
}

func mustStartBenchmarkStorageServer(tb testing.TB, node *storage.Node, address string) *grpcx.StorageGRPCServer {
	tb.Helper()
	lis, err := net.Listen("tcp", address)
	if err != nil {
		tb.Fatalf("Listen returned error: %v", err)
	}
	server := grpcx.NewStorageGRPCServer(node)
	go func() {
		if err := server.Serve(lis); err != nil && !errors.Is(err, net.ErrClosed) {
			panic(err)
		}
	}()
	return server
}

func benchmarkNodeID(index int) string {
	return fmt.Sprintf("n%d", index)
}

func seedBenchmarkPutKeys(tb testing.TB, router *client.Router, count int) {
	tb.Helper()
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("bench-put-%06d", i)
		if _, err := router.Put(context.Background(), key, "seed"); err != nil {
			tb.Fatalf("seed Put(%q) returned error: %v", key, err)
		}
	}
}

func benchmarkRouterPutThroughput(b *testing.B, router *client.Router, concurrency int, keyCount int) {
	if keyCount <= 0 {
		keyCount = concurrency * 8
	}
	var next atomic.Uint64
	start := make(chan struct{})
	var wg sync.WaitGroup
	var (
		errMu    sync.Mutex
		firstErr error
	)

	b.ReportAllocs()
	b.ResetTimer()

	for worker := 0; worker < concurrency; worker++ {
		workerID := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				index := int(next.Add(1) - 1)
				if index >= b.N {
					return
				}
				key := fmt.Sprintf("bench-put-%06d", index%keyCount)
				value := fmt.Sprintf("value-%d-%d", workerID, index)
				if _, err := router.Put(context.Background(), key, value); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	b.StopTimer()
	errMu.Lock()
	defer errMu.Unlock()
	if firstErr != nil {
		b.Fatalf("Put returned error: %v", firstErr)
	}
}
