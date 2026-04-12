package benchmark

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/adminhttp"
	"github.com/danthegoodman1/craq/client"
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
	for _, path := range []string{
		filepath.Join(outputDir, "metric-snapshots", "preload-end", "coordinator.prom"),
		filepath.Join(outputDir, "metric-snapshots", "preload-end", "storage-a.prom"),
		filepath.Join(outputDir, "metric-snapshots", "get-start", "coordinator.prom"),
		filepath.Join(outputDir, "metric-snapshots", "get-end", "storage-a.prom"),
		filepath.Join(outputDir, "metric-snapshots", "mixed-start", "storage-b.prom"),
		filepath.Join(outputDir, "metric-snapshots", "mixed-end", "storage-c.prom"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("metric snapshot %q stat error: %v", path, err)
		}
	}
	if putSummary, ok := scenarioOperationSummary(report.Scenarios[1], "put"); !ok || putSummary.TotalOps == 0 {
		t.Fatalf("mixed scenario put summary = %#v, want non-zero", putSummary)
	}
}

func TestBenchmarkStartupMaxChangedChainsUsesStartupFloor(t *testing.T) {
	if got, want := benchmarkStartupMaxChangedChains(1024, 32), 1024; got != want {
		t.Fatalf("benchmarkStartupMaxChangedChains(1024, 32) = %d, want %d", got, want)
	}
	if got, want := benchmarkStartupMaxChangedChains(64, 32), 64; got != want {
		t.Fatalf("benchmarkStartupMaxChangedChains(64, 32) = %d, want %d", got, want)
	}
	if got, want := benchmarkStartupMaxChangedChains(1024, 512), 1024; got != want {
		t.Fatalf("benchmarkStartupMaxChangedChains(1024, 512) = %d, want %d", got, want)
	}
}

func TestAdvanceReplicaLifecycleActivatesAndRemovesCurrentSlots(t *testing.T) {
	ctx := context.Background()
	transport := storage.NewInMemoryReplicationTransport()
	backend := storage.NewInMemoryBackend()
	coord := &slowReadyCoordinatorClient{
		inner:      storage.NewInMemoryCoordinatorClient(),
		readyDelay: 15 * time.Millisecond,
	}
	node, err := storage.NewNode(ctx, storage.Config{NodeID: "node-a"}, backend, coord, transport)
	if err != nil {
		t.Fatalf("storage.NewNode returned error: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	for _, slot := range []int{3, 1, 2} {
		if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
			Assignment: storage.ReplicaAssignment{Slot: slot, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail(slot=%d) returned error: %v", slot, err)
		}
	}

	advanceReplicaLifecycle(ctx, node, "node-a", 20*time.Millisecond)

	for _, slot := range []int{1, 2, 3} {
		if got, want := node.State().Replicas[slot].State, storage.ReplicaStateActive; got != want {
			t.Fatalf("slot %d state = %q, want %q", slot, got, want)
		}
	}
	if got, want := coord.inner.ReadySlots, []int{1, 2, 3}; !reflect.DeepEqual(got, want) {
		gotSorted := append([]int(nil), got...)
		sort.Ints(gotSorted)
		if !reflect.DeepEqual(gotSorted, want) {
			t.Fatalf("ready slots = %v, want %v", got, want)
		}
	}

	if err := node.MarkReplicaLeaving(ctx, storage.MarkReplicaLeavingCommand{Slot: 2}); err != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}

	advanceReplicaLifecycle(ctx, node, "node-a", 20*time.Millisecond)

	if _, exists := node.State().Replicas[2]; exists {
		t.Fatal("slot 2 replica still present after lifecycle removal")
	}
	if got, want := coord.inner.RemovedSlots, []int{2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("removed slots = %v, want %v", got, want)
	}
}

func TestRuntimeProcessesReachWritableRoutingUnderConcurrentAdminReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	manifest := quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        reserveAddress(t),
			AdminAddress:      reserveAddress(t),
			SlotCount:         8,
			ReplicationFactor: 3,
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
		t.Fatalf("SaveManifest returned error: %v", err)
	}

	processCtx, stopProcesses := context.WithCancel(ctx)
	defer stopProcesses()

	processErrs := make(chan error, len(manifest.Nodes)+1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processErrs <- RunCoordinatorProcess(processCtx, CoordinatorProcessConfig{
			ManifestPath:    manifestPath,
			DataDir:         filepath.Join(tempDir, "coordinator"),
			Reconfiguration: coordinator.ReconfigurationPolicy{MaxChangedChains: manifest.Coordinator.SlotCount},
			TickInterval:    25 * time.Millisecond,
			RPCDeadline:     250 * time.Millisecond,
		})
	}()
	for _, node := range manifest.Nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			processErrs <- RunStorageProcess(processCtx, StorageProcessConfig{
				ManifestPath:       manifestPath,
				NodeID:             node.ID,
				DataDir:            filepath.Join(tempDir, "storage-"+node.ID),
				HeartbeatInterval:  25 * time.Millisecond,
				ActivationInterval: 10 * time.Millisecond,
				RPCDeadline:        250 * time.Millisecond,
			})
		}()
	}

	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })
	admin := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	router, err := client.NewRouter(admin, grpcx.NewClientTransport(pool))
	if err != nil {
		t.Fatalf("client.NewRouter returned error: %v", err)
	}
	pollRouter, err := client.NewRouter(admin, grpcx.NewClientTransport(pool))
	if err != nil {
		t.Fatalf("client.NewRouter poller returned error: %v", err)
	}

	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	pollErrs := make(chan error, 2)
	var pollWG sync.WaitGroup
	startConcurrentAdminPoller(&pollWG, pollCtx, pollErrs, func(pollCtx context.Context) error {
		snapCtx, cancel := context.WithTimeout(pollCtx, 250*time.Millisecond)
		defer cancel()
		_, err := admin.RoutingSnapshot(snapCtx)
		return err
	})
	startConcurrentAdminPoller(&pollWG, pollCtx, pollErrs, func(pollCtx context.Context) error {
		refreshCtx, cancel := context.WithTimeout(pollCtx, 250*time.Millisecond)
		defer cancel()
		return pollRouter.Refresh(refreshCtx)
	})

	snapshot, err := waitForWritableRouting(ctx, admin)
	if err != nil {
		t.Fatalf("waitForWritableRouting returned error: %v", err)
	}
	for _, route := range snapshot.Slots {
		if !route.Readable || !route.Writable {
			t.Fatalf("route %#v is not fully readable+writable", route)
		}
	}
	stopPoll()
	pollWG.Wait()

	opCtx, opCancel := context.WithTimeout(ctx, 2*time.Second)
	defer opCancel()
	if err := router.Refresh(opCtx); err != nil {
		t.Fatalf("router.Refresh returned error: %v", err)
	}
	putResult, err := router.Put(opCtx, "alpha", "one")
	if err != nil {
		t.Fatalf("router.Put returned error: %v", err)
	}
	if got, want := putResult.Applied, true; got != want {
		t.Fatalf("putResult.Applied = %t, want %t", got, want)
	}
	readResult, err := router.Get(opCtx, "alpha")
	if err != nil {
		t.Fatalf("router.Get returned error: %v", err)
	}
	if !readResult.Found || readResult.Value != "one" {
		t.Fatalf("router.Get result = %#v, want found value", readResult)
	}

	stopProcesses()
	wg.Wait()
	close(processErrs)
	close(pollErrs)
	for err := range processErrs {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime process returned error: %v", err)
		}
	}
	for err := range pollErrs {
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("admin poller returned error: %v", err)
		}
	}
}

func TestRuntimeProcessesStayWritableAcrossSequentialAutoJoinRepairs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tempDir := t.TempDir()
	manifest := quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        reserveAddress(t),
			AdminAddress:      reserveAddress(t),
			SlotCount:         8,
			ReplicationFactor: 3,
		},
		Nodes: []quickstart.Node{
			{ID: "a", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "a", "rack": "a", "az": "a"}},
			{ID: "b", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "b", "rack": "b", "az": "b"}},
			{ID: "c", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "c", "rack": "c", "az": "c"}},
			{ID: "d", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "d", "rack": "d", "az": "d"}},
			{ID: "e", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "e", "rack": "e", "az": "e"}},
		},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest.Validate returned error: %v", err)
	}
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := SaveManifest(manifestPath, manifest); err != nil {
		t.Fatalf("SaveManifest returned error: %v", err)
	}

	processErrs := make(chan error, len(manifest.Nodes)+1)
	var wg sync.WaitGroup

	coordCtx, stopCoordinator := context.WithCancel(ctx)
	defer stopCoordinator()
	wg.Add(1)
	go func() {
		defer wg.Done()
		processErrs <- RunCoordinatorProcess(coordCtx, CoordinatorProcessConfig{
			ManifestPath: manifestPath,
			DataDir:      filepath.Join(tempDir, "coordinator"),
			Liveness: coordserver.LivenessPolicy{
				SuspectAfter:  100 * time.Millisecond,
				DeadAfter:     200 * time.Millisecond,
				FlapWindow:    time.Second,
				FlapThreshold: 8,
			},
			Reconfiguration: coordinator.ReconfigurationPolicy{MaxChangedChains: manifest.Coordinator.SlotCount},
			TickInterval:    25 * time.Millisecond,
			RPCDeadline:     250 * time.Millisecond,
		})
	}()

	nodeConfigByID := map[string]quickstart.Node{}
	for _, node := range manifest.Nodes {
		nodeConfigByID[node.ID] = node
	}
	nodeStops := map[string]context.CancelFunc{}
	startNode := func(nodeID string) {
		t.Helper()
		node, ok := nodeConfigByID[nodeID]
		if !ok {
			t.Fatalf("unknown node %q", nodeID)
		}
		if _, started := nodeStops[nodeID]; started {
			return
		}
		nodeCtx, stopNode := context.WithCancel(ctx)
		nodeStops[nodeID] = stopNode
		wg.Add(1)
		go func() {
			defer wg.Done()
			processErrs <- RunStorageProcess(nodeCtx, StorageProcessConfig{
				ManifestPath:       manifestPath,
				NodeID:             node.ID,
				DataDir:            filepath.Join(tempDir, "storage-"+node.ID),
				HeartbeatInterval:  25 * time.Millisecond,
				ActivationInterval: 10 * time.Millisecond,
				RPCDeadline:        250 * time.Millisecond,
			})
		}()
	}
	for _, nodeID := range []string{"a", "b", "c"} {
		startNode(nodeID)
	}

	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })
	admin := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	router, err := client.NewRouter(admin, grpcx.NewClientTransport(pool))
	if err != nil {
		t.Fatalf("client.NewRouter returned error: %v", err)
	}
	pollRouter, err := client.NewRouter(admin, grpcx.NewClientTransport(pool))
	if err != nil {
		t.Fatalf("client.NewRouter poller returned error: %v", err)
	}

	pollCtx, stopPoll := context.WithCancel(ctx)
	defer stopPoll()
	pollErrs := make(chan error, 2)
	var pollWG sync.WaitGroup
	startConcurrentAdminPoller(&pollWG, pollCtx, pollErrs, func(pollCtx context.Context) error {
		snapCtx, cancel := context.WithTimeout(pollCtx, 250*time.Millisecond)
		defer cancel()
		_, err := admin.RoutingSnapshot(snapCtx)
		return err
	})
	startConcurrentAdminPoller(&pollWG, pollCtx, pollErrs, func(pollCtx context.Context) error {
		refreshCtx, cancel := context.WithTimeout(pollCtx, 250*time.Millisecond)
		defer cancel()
		return pollRouter.Refresh(refreshCtx)
	})

	snapshot, err := waitForWritableRouting(ctx, admin)
	if err != nil {
		t.Fatalf("initial waitForWritableRouting returned error: %v", err)
	}
	for _, route := range snapshot.Slots {
		if !route.Readable || !route.Writable {
			t.Fatalf("initial route %#v is not fully readable+writable", route)
		}
	}

	verifyRouting := func(included ...string) {
		t.Helper()
		snapshot, err := waitForWritableRoutingIncluding(ctx, admin, included...)
		if err != nil {
			t.Fatalf("waitForWritableRoutingIncluding(%v) returned error: %v", included, err)
		}
		for _, route := range snapshot.Slots {
			if !route.Readable || !route.Writable {
				t.Fatalf("route %#v is not fully readable+writable", route)
			}
		}
		opCtx, opCancel := context.WithTimeout(ctx, 2*time.Second)
		defer opCancel()
		if err := router.Refresh(opCtx); err != nil {
			t.Fatalf("router.Refresh returned error: %v", err)
		}
		key := fmt.Sprintf("repair-cycle-%d", len(included))
		value := fmt.Sprintf("value-%d", len(included))
		if _, err := router.Put(opCtx, key, value); err != nil {
			t.Fatalf("router.Put(%q) returned error: %v", key, err)
		}
		read, err := router.Get(opCtx, key)
		if err != nil {
			t.Fatalf("router.Get(%q) returned error: %v", key, err)
		}
		if !read.Found || read.Value != value {
			t.Fatalf("router.Get(%q) = %#v, want value %q", key, read, value)
		}
	}

	verifyRouting()

	startNode("d")
	verifyRouting("d")

	startNode("e")
	verifyRouting()

	stopPoll()
	pollWG.Wait()
	stopCoordinator()
	for _, stopNode := range nodeStops {
		stopNode()
	}
	wg.Wait()
	close(processErrs)
	close(pollErrs)
	for err := range processErrs {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime process returned error: %v", err)
		}
	}
	for err := range pollErrs {
		if err != nil && !errors.Is(err, context.Canceled) && !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("admin poller returned error: %v", err)
		}
	}
}

func TestRuntimeProcessesBecomeWritableWithBudgetedBenchmarkStartup(t *testing.T) {
	testTimeout := 90 * time.Second
	slotCount := 64
	maxChangedChains := 32
	tickInterval := 250 * time.Millisecond
	heartbeatInterval := 250 * time.Millisecond
	activationInterval := 50 * time.Millisecond
	rpcDeadline := 5 * time.Second
	if benchmarkRaceEnabled {
		testTimeout = 75 * time.Second
		slotCount = 24
		maxChangedChains = 12
		tickInterval = 300 * time.Millisecond
		heartbeatInterval = 300 * time.Millisecond
		activationInterval = 50 * time.Millisecond
		rpcDeadline = 3 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	tempDir := t.TempDir()
	manifest := quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        reserveAddress(t),
			AdminAddress:      reserveAddress(t),
			SlotCount:         slotCount,
			ReplicationFactor: 3,
		},
		Nodes: []quickstart.Node{
			{ID: "a", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "a", "rack": "a"}},
			{ID: "b", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "b", "rack": "b"}},
			{ID: "c", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "c", "rack": "c"}},
		},
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest.Validate returned error: %v", err)
	}
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := SaveManifest(manifestPath, manifest); err != nil {
		t.Fatalf("SaveManifest returned error: %v", err)
	}

	processCtx, stopProcesses := context.WithCancel(ctx)
	defer stopProcesses()

	processErrs := make(chan error, len(manifest.Nodes)+1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		processErrs <- RunCoordinatorProcess(processCtx, CoordinatorProcessConfig{
			ManifestPath: manifestPath,
			DataDir:      filepath.Join(tempDir, "coordinator"),
			Liveness: coordserver.LivenessPolicy{
				SuspectAfter:  2 * time.Second,
				DeadAfter:     5 * time.Second,
				FlapWindow:    10 * time.Second,
				FlapThreshold: 8,
			},
			Reconfiguration: coordinator.ReconfigurationPolicy{MaxChangedChains: maxChangedChains},
			TickInterval:    tickInterval,
			RPCDeadline:     rpcDeadline,
		})
	}()
	if err := waitForHTTP200(ctx, "http://"+manifest.Coordinator.AdminAddress+"/livez", nil); err != nil {
		t.Fatalf("wait for coordinator admin server returned error: %v", err)
	}
	for _, node := range manifest.Nodes {
		node := node
		wg.Add(1)
		go func() {
			defer wg.Done()
			processErrs <- RunStorageProcess(processCtx, StorageProcessConfig{
				ManifestPath:       manifestPath,
				NodeID:             node.ID,
				DataDir:            filepath.Join(tempDir, "storage-"+node.ID),
				HeartbeatInterval:  heartbeatInterval,
				ActivationInterval: activationInterval,
				RPCDeadline:        rpcDeadline,
			})
		}()
	}

	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })
	admin := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	router, err := client.NewRouter(admin, grpcx.NewClientTransport(pool))
	if err != nil {
		t.Fatalf("client.NewRouter returned error: %v", err)
	}

	var runErr error
	progressLogPath := filepath.Join(tempDir, "routing-progress.jsonl")
	snapshotReadyAt, progress, err := waitForLocalRoutingReady(ctx, Profile{
		Cluster: ClusterProfile{
			SlotCount:          slotCount,
			ReplicationFactor:  3,
			Reconfiguration:    ReconfigProfile{MaxChangedChains: maxChangedChains},
			HeartbeatInterval:  heartbeatInterval,
			LivenessInterval:   tickInterval,
			ActivationInterval: activationInterval,
			RPCDeadline:        rpcDeadline,
		},
	}, manifest.Coordinator.AdminAddress, nil, progressLogPath)
	if err != nil {
		runErr = fmt.Errorf("waitForLocalRoutingReady returned error: %w", err)
	} else {
		_ = snapshotReadyAt
		_ = progress
		snapshot, snapshotErr := admin.RoutingSnapshot(ctx)
		if snapshotErr != nil {
			runErr = fmt.Errorf("admin.RoutingSnapshot returned error: %w", snapshotErr)
		} else {
			for _, route := range snapshot.Slots {
				if !route.Readable || !route.Writable {
					runErr = fmt.Errorf("route %#v is not fully readable+writable", route)
					break
				}
			}
		}
	}
	if runErr == nil {
		opCtx, opCancel := context.WithTimeout(ctx, 2*time.Second)
		defer opCancel()
		if err := router.Refresh(opCtx); err != nil {
			runErr = fmt.Errorf("router.Refresh returned error: %w", err)
		} else if _, err := router.Put(opCtx, "budgeted-startup", "ok"); err != nil {
			runErr = fmt.Errorf("router.Put returned error: %w", err)
		} else {
			readResult, err := router.Get(opCtx, "budgeted-startup")
			if err != nil {
				runErr = fmt.Errorf("router.Get returned error: %w", err)
			} else if !readResult.Found || readResult.Value != "ok" {
				runErr = fmt.Errorf("router.Get result = %#v, want found value", readResult)
			}
		}
	}

	stopProcesses()
	wg.Wait()
	close(processErrs)
	for err := range processErrs {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("runtime process returned error: %v", err)
		}
	}
	if runErr != nil {
		t.Fatal(runErr)
	}
}

type slowReadyCoordinatorClient struct {
	inner      *storage.InMemoryCoordinatorClient
	readyDelay time.Duration
}

func (c *slowReadyCoordinatorClient) RegisterNode(ctx context.Context, reg storage.NodeRegistration) error {
	return c.inner.RegisterNode(ctx, reg)
}

func (c *slowReadyCoordinatorClient) ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error {
	timer := time.NewTimer(c.readyDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return c.inner.ReportReplicaReady(ctx, slot, epoch)
}

func (c *slowReadyCoordinatorClient) ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error {
	return c.inner.ReportReplicaRemoved(ctx, slot, epoch)
}

func (c *slowReadyCoordinatorClient) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	return c.inner.ReportNodeRecovered(ctx, report)
}

func (c *slowReadyCoordinatorClient) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	return c.inner.ReportNodeHeartbeat(ctx, status)
}

func startConcurrentAdminPoller(wg *sync.WaitGroup, ctx context.Context, errCh chan<- error, poll func(context.Context) error) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				errCh <- nil
				return
			case <-ticker.C:
				if err := poll(ctx); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						continue
					}
					if strings.Contains(err.Error(), "connection refused") {
						continue
					}
					errCh <- err
					return
				}
			}
		}
	}()
}

func waitForWritableRouting(ctx context.Context, admin *grpcx.CoordinatorAdminClient) (coordserver.RoutingSnapshot, error) {
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var stable coordserver.RoutingSnapshot
	stableCount := 0
	for {
		select {
		case <-ctx.Done():
			return coordserver.RoutingSnapshot{}, ctx.Err()
		case <-ticker.C:
			snapCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			snapshot, err := admin.RoutingSnapshot(snapCtx)
			cancel()
			if err != nil {
				continue
			}
			if snapshot.SlotCount == 0 || len(snapshot.Slots) != snapshot.SlotCount {
				continue
			}
			writable := true
			for _, route := range snapshot.Slots {
				if !route.Readable || !route.Writable {
					writable = false
					break
				}
			}
			if writable {
				if stableCount > 0 && stable.Version == snapshot.Version {
					stableCount++
				} else {
					stable = snapshot
					stableCount = 1
				}
				if stableCount >= 3 {
					return snapshot, nil
				}
				continue
			}
			stableCount = 0
		}
	}
}

func waitForWritableRoutingIncluding(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	includedNodeIDs ...string,
) (coordserver.RoutingSnapshot, error) {
	included := make(map[string]struct{}, len(includedNodeIDs))
	for _, nodeID := range includedNodeIDs {
		included[nodeID] = struct{}{}
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	var stable coordserver.RoutingSnapshot
	stableCount := 0
	for {
		select {
		case <-ctx.Done():
			return coordserver.RoutingSnapshot{}, ctx.Err()
		case <-ticker.C:
			snapCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			snapshot, err := admin.RoutingSnapshot(snapCtx)
			cancel()
			if err != nil {
				continue
			}
			if snapshot.SlotCount == 0 || len(snapshot.Slots) != snapshot.SlotCount {
				continue
			}
			healthy := true
			seen := make(map[string]bool, len(included))
			for _, route := range snapshot.Slots {
				if !route.Readable || !route.Writable {
					healthy = false
					break
				}
				if _, tracked := included[route.HeadNodeID]; tracked {
					seen[route.HeadNodeID] = true
				}
				if _, tracked := included[route.TailNodeID]; tracked {
					seen[route.TailNodeID] = true
				}
				for _, replica := range route.ReadReplicas {
					if _, tracked := included[replica.NodeID]; tracked {
						seen[replica.NodeID] = true
					}
				}
			}
			if healthy {
				for nodeID := range included {
					if !seen[nodeID] {
						healthy = false
						break
					}
				}
			}
			if healthy {
				if stableCount > 0 && stable.Version == snapshot.Version {
					stableCount++
				} else {
					stable = snapshot
					stableCount = 1
				}
				if stableCount >= 3 {
					return snapshot, nil
				}
				continue
			}
			stableCount = 0
		}
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
