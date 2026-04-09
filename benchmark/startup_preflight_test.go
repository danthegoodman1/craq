package benchmark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/adminhttp"
	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/quickstart"
	"github.com/danthegoodman1/craq/storage"
	badgerstore "github.com/danthegoodman1/craq/storage/badger"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

type startupScenarioConfig struct {
	TestTimeout        time.Duration
	SlotCount          int
	MaxChangedChains   int
	LivenessInterval   time.Duration
	HeartbeatInterval  time.Duration
	ActivationInterval time.Duration
	RPCDeadline        time.Duration
	SuspectAfter       time.Duration
	DeadAfter          time.Duration
	ReporterDelay      time.Duration
	ReporterJitter     time.Duration
}

type startupScenarioHarness struct {
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	processErrs     chan error
	tempDir         string
	manifest        quickstart.Config
	progressLogPath string
	profile         Profile
	pool            *grpcx.ConnPool
	admin           *grpcx.CoordinatorAdminClient
}

func TestRuntimeLargeStartupConvergence(t *testing.T) {
	cfg := startupScenarioConfig{
		TestTimeout:        90 * time.Second,
		SlotCount:          64,
		MaxChangedChains:   32,
		LivenessInterval:   250 * time.Millisecond,
		HeartbeatInterval:  250 * time.Millisecond,
		ActivationInterval: 50 * time.Millisecond,
		RPCDeadline:        5 * time.Second,
	}
	if benchmarkRaceEnabled {
		cfg.TestTimeout = 90 * time.Second
		cfg.SlotCount = 16
		cfg.MaxChangedChains = 16
		cfg.LivenessInterval = 300 * time.Millisecond
		cfg.HeartbeatInterval = 300 * time.Millisecond
		cfg.ActivationInterval = 50 * time.Millisecond
		cfg.RPCDeadline = 3 * time.Second
	}

	h := startStartupScenario(t, cfg)
	defer h.Close(t)

	if _, _, err := waitForLocalRoutingReady(h.ctx, h.profile, h.manifest.Coordinator.AdminAddress, nil, h.progressLogPath); err != nil {
		t.Fatalf("waitForLocalRoutingReady returned error: %v", err)
	}
	if _, err := runLocalSmokeTraffic(h.ctx, h.admin, h.pool); err != nil {
		t.Fatalf("runLocalSmokeTraffic returned error: %v", err)
	}
}

func TestRuntimeStartupConvergenceWithInjectedControlPlaneLatency(t *testing.T) {
	cfg := startupScenarioConfig{
		TestTimeout:        90 * time.Second,
		SlotCount:          48,
		MaxChangedChains:   24,
		LivenessInterval:   250 * time.Millisecond,
		HeartbeatInterval:  250 * time.Millisecond,
		ActivationInterval: 50 * time.Millisecond,
		RPCDeadline:        5 * time.Second,
		ReporterDelay:      20 * time.Millisecond,
		ReporterJitter:     10 * time.Millisecond,
	}
	if benchmarkRaceEnabled {
		cfg.TestTimeout = 75 * time.Second
		cfg.SlotCount = 24
		cfg.MaxChangedChains = 12
		cfg.LivenessInterval = 300 * time.Millisecond
		cfg.HeartbeatInterval = 300 * time.Millisecond
		cfg.ActivationInterval = 50 * time.Millisecond
		cfg.RPCDeadline = 3 * time.Second
		cfg.ReporterDelay = 25 * time.Millisecond
		cfg.ReporterJitter = 15 * time.Millisecond
	}

	h := startStartupScenario(t, cfg)
	defer h.Close(t)

	if _, _, err := waitForLocalRoutingReady(h.ctx, h.profile, h.manifest.Coordinator.AdminAddress, nil, h.progressLogPath); err != nil {
		t.Fatalf("waitForLocalRoutingReady returned error under injected latency: %v", err)
	}
	if _, err := runLocalSmokeTraffic(h.ctx, h.admin, h.pool); err != nil {
		t.Fatalf("runLocalSmokeTraffic returned error under injected latency: %v", err)
	}
}

func TestRuntimeRoutingProgressDoesNotFlatlineOrCollapse(t *testing.T) {
	cfg := startupScenarioConfig{
		TestTimeout:        90 * time.Second,
		SlotCount:          64,
		MaxChangedChains:   32,
		LivenessInterval:   250 * time.Millisecond,
		HeartbeatInterval:  250 * time.Millisecond,
		ActivationInterval: 50 * time.Millisecond,
		RPCDeadline:        5 * time.Second,
	}
	if benchmarkRaceEnabled {
		cfg.TestTimeout = 90 * time.Second
		cfg.SlotCount = 16
		cfg.MaxChangedChains = 16
		cfg.LivenessInterval = 300 * time.Millisecond
		cfg.HeartbeatInterval = 300 * time.Millisecond
		cfg.ActivationInterval = 50 * time.Millisecond
		cfg.RPCDeadline = 3 * time.Second
	}

	h := startStartupScenario(t, cfg)
	defer h.Close(t)

	if _, _, err := waitForLocalRoutingReady(h.ctx, h.profile, h.manifest.Coordinator.AdminAddress, nil, h.progressLogPath); err != nil {
		t.Fatalf("waitForLocalRoutingReady returned error: %v", err)
	}
	records, err := loadRoutingProgressRecords(h.progressLogPath)
	if err != nil {
		t.Fatalf("loadRoutingProgressRecords returned error: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("routing progress log was empty")
	}
	if err := assertRoutingProgressHealthy(records, cfg.SlotCount, maxDuration(5*time.Second, cfg.RPCDeadline*4)); err != nil {
		t.Fatalf("routing progress health check failed: %v", err)
	}
}

func TestRuntimeAllNodesAppearInWritableRouting(t *testing.T) {
	cfg := startupScenarioConfig{
		TestTimeout:        90 * time.Second,
		SlotCount:          64,
		MaxChangedChains:   32,
		LivenessInterval:   250 * time.Millisecond,
		HeartbeatInterval:  250 * time.Millisecond,
		ActivationInterval: 50 * time.Millisecond,
		RPCDeadline:        5 * time.Second,
	}
	if benchmarkRaceEnabled {
		cfg.TestTimeout = 90 * time.Second
		cfg.SlotCount = 16
		cfg.MaxChangedChains = 16
		cfg.LivenessInterval = 300 * time.Millisecond
		cfg.HeartbeatInterval = 300 * time.Millisecond
		cfg.ActivationInterval = 50 * time.Millisecond
		cfg.RPCDeadline = 3 * time.Second
	}

	h := startStartupScenario(t, cfg)
	defer h.Close(t)

	verifyCtx, cancel := context.WithTimeout(h.ctx, cfg.TestTimeout)
	defer cancel()
	if _, _, err := waitForLocalRoutingReady(verifyCtx, h.profile, h.manifest.Coordinator.AdminAddress, nil, h.progressLogPath); err != nil {
		t.Fatalf("waitForLocalRoutingReady returned error: %v", err)
	}
	if _, err := waitForWritableRoutingIncluding(verifyCtx, h.admin, "a", "b", "c"); err != nil {
		t.Fatalf("waitForWritableRoutingIncluding returned error: %v", err)
	}
	if _, err := runLocalSmokeTraffic(h.ctx, h.admin, h.pool); err != nil {
		t.Fatalf("runLocalSmokeTraffic returned error: %v", err)
	}
}

func TestRuntimeCloudShapeStartupConvergence(t *testing.T) {
	requireBenchmarkCloudShapeSoak(t)

	cfg := startupScenarioConfig{
		TestTimeout:        15 * time.Minute,
		SlotCount:          1024,
		MaxChangedChains:   32,
		LivenessInterval:   time.Second,
		HeartbeatInterval:  time.Second,
		ActivationInterval: 250 * time.Millisecond,
		RPCDeadline:        5 * time.Second,
		SuspectAfter:       3 * time.Second,
		DeadAfter:          6 * time.Second,
	}

	h := startStartupScenario(t, cfg)
	defer h.Close(t)

	verifyCtx, cancel := context.WithTimeout(h.ctx, cfg.TestTimeout)
	defer cancel()
	if _, _, err := waitForLocalRoutingReady(verifyCtx, h.profile, h.manifest.Coordinator.AdminAddress, nil, h.progressLogPath); err != nil {
		t.Fatalf("waitForLocalRoutingReady returned error: %v", err)
	}
	if _, err := waitForWritableRoutingIncluding(verifyCtx, h.admin, "a", "b", "c"); err != nil {
		t.Fatalf("waitForWritableRoutingIncluding returned error: %v", err)
	}
	if _, err := runLocalSmokeTraffic(h.ctx, h.admin, h.pool); err != nil {
		t.Fatalf("runLocalSmokeTraffic returned error: %v", err)
	}
}

func TestRuntimeCloudShapeStartupConvergenceWithInjectedControlPlaneLatency(t *testing.T) {
	requireBenchmarkCloudShapeSoak(t)

	cfg := startupScenarioConfig{
		TestTimeout:        18 * time.Minute,
		SlotCount:          1024,
		MaxChangedChains:   32,
		LivenessInterval:   time.Second,
		HeartbeatInterval:  time.Second,
		ActivationInterval: 250 * time.Millisecond,
		RPCDeadline:        5 * time.Second,
		SuspectAfter:       3 * time.Second,
		DeadAfter:          6 * time.Second,
		ReporterDelay:      10 * time.Millisecond,
		ReporterJitter:     5 * time.Millisecond,
	}

	h := startStartupScenario(t, cfg)
	defer h.Close(t)

	verifyCtx, cancel := context.WithTimeout(h.ctx, cfg.TestTimeout)
	defer cancel()
	if _, _, err := waitForLocalRoutingReady(verifyCtx, h.profile, h.manifest.Coordinator.AdminAddress, nil, h.progressLogPath); err != nil {
		t.Fatalf("waitForLocalRoutingReady returned error under cloud-shape latency: %v", err)
	}
	if _, err := waitForWritableRoutingIncluding(verifyCtx, h.admin, "a", "b", "c"); err != nil {
		t.Fatalf("waitForWritableRoutingIncluding returned error under cloud-shape latency: %v", err)
	}
	if _, err := runLocalSmokeTraffic(h.ctx, h.admin, h.pool); err != nil {
		t.Fatalf("runLocalSmokeTraffic returned error under cloud-shape latency: %v", err)
	}
}

func TestRuntimeCloudShapeRoutingProgressDoesNotCollapse(t *testing.T) {
	requireBenchmarkCloudShapeSoak(t)

	cfg := startupScenarioConfig{
		TestTimeout:        15 * time.Minute,
		SlotCount:          1024,
		MaxChangedChains:   32,
		LivenessInterval:   time.Second,
		HeartbeatInterval:  time.Second,
		ActivationInterval: 250 * time.Millisecond,
		RPCDeadline:        5 * time.Second,
		SuspectAfter:       3 * time.Second,
		DeadAfter:          6 * time.Second,
	}

	h := startStartupScenario(t, cfg)
	defer h.Close(t)

	verifyCtx, cancel := context.WithTimeout(h.ctx, cfg.TestTimeout)
	defer cancel()
	if _, _, err := waitForLocalRoutingReady(verifyCtx, h.profile, h.manifest.Coordinator.AdminAddress, nil, h.progressLogPath); err != nil {
		t.Fatalf("waitForLocalRoutingReady returned error: %v", err)
	}
	records, err := loadRoutingProgressRecords(h.progressLogPath)
	if err != nil {
		t.Fatalf("loadRoutingProgressRecords returned error: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("routing progress log was empty")
	}
	if err := assertCloudShapeRoutingProgressHealthy(records, cfg.SlotCount, 90*time.Second); err != nil {
		t.Fatalf("cloud-shape routing progress health check failed: %v", err)
	}
	if _, err := waitForWritableRoutingIncluding(verifyCtx, h.admin, "a", "b", "c"); err != nil {
		t.Fatalf("waitForWritableRoutingIncluding returned error: %v", err)
	}
}

func startStartupScenario(t *testing.T, cfg startupScenarioConfig) *startupScenarioHarness {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.TestTimeout)
	suspectAfter := cfg.SuspectAfter
	if suspectAfter <= 0 {
		suspectAfter = 2 * time.Second
	}
	deadAfter := cfg.DeadAfter
	if deadAfter <= 0 {
		deadAfter = 5 * time.Second
	}
	tempDir := t.TempDir()
	manifest := quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        reserveAddress(t),
			AdminAddress:      reserveAddress(t),
			SlotCount:         cfg.SlotCount,
			ReplicationFactor: 3,
		},
		Nodes: []quickstart.Node{
			{ID: "a", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "a", "rack": "a"}},
			{ID: "b", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "b", "rack": "b"}},
			{ID: "c", RPCAddress: reserveAddress(t), AdminAddress: reserveAddress(t), FailureDomains: map[string]string{"host": "c", "rack": "c"}},
		},
	}
	if err := manifest.Validate(); err != nil {
		cancel()
		t.Fatalf("manifest.Validate returned error: %v", err)
	}
	manifestPath := filepath.Join(tempDir, "manifest.json")
	if err := SaveManifest(manifestPath, manifest); err != nil {
		cancel()
		t.Fatalf("SaveManifest returned error: %v", err)
	}

	h := &startupScenarioHarness{
		ctx:             ctx,
		cancel:          cancel,
		processErrs:     make(chan error, len(manifest.Nodes)+1),
		tempDir:         tempDir,
		manifest:        manifest,
		progressLogPath: filepath.Join(tempDir, "routing-progress.jsonl"),
		profile: Profile{
			Cluster: ClusterProfile{
				SlotCount:         cfg.SlotCount,
				ReplicationFactor: 3,
				RPCDeadline:       cfg.RPCDeadline,
			},
		},
	}

	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		h.processErrs <- RunCoordinatorProcess(ctx, CoordinatorProcessConfig{
			ManifestPath: manifestPath,
			DataDir:      filepath.Join(tempDir, "coordinator"),
			Liveness: coordserver.LivenessPolicy{
				SuspectAfter:  suspectAfter,
				DeadAfter:     deadAfter,
				FlapWindow:    maxDuration(deadAfter*2, 10*time.Second),
				FlapThreshold: 8,
			},
			Reconfiguration: coordinator.ReconfigurationPolicy{
				MaxChangedChains: cfg.MaxChangedChains,
			},
			TickInterval: cfg.LivenessInterval,
			RPCDeadline:  cfg.RPCDeadline,
		})
	}()
	if err := waitForHTTP200(ctx, "http://"+manifest.Coordinator.AdminAddress+"/livez", nil); err != nil {
		cancel()
		t.Fatalf("wait for local coordinator admin server returned error: %v", err)
	}

	for _, node := range manifest.Nodes {
		node := node
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()
			if cfg.ReporterDelay > 0 || cfg.ReporterJitter > 0 {
				h.processErrs <- runStorageProcessWithReporterLatency(ctx, StorageProcessConfig{
					ManifestPath:       manifestPath,
					NodeID:             node.ID,
					DataDir:            filepath.Join(tempDir, "storage-"+node.ID),
					HeartbeatInterval:  cfg.HeartbeatInterval,
					ActivationInterval: cfg.ActivationInterval,
					RPCDeadline:        cfg.RPCDeadline,
				}, cfg.ReporterDelay, cfg.ReporterJitter)
				return
			}
			h.processErrs <- RunStorageProcess(ctx, StorageProcessConfig{
				ManifestPath:       manifestPath,
				NodeID:             node.ID,
				DataDir:            filepath.Join(tempDir, "storage-"+node.ID),
				HeartbeatInterval:  cfg.HeartbeatInterval,
				ActivationInterval: cfg.ActivationInterval,
				RPCDeadline:        cfg.RPCDeadline,
			})
		}()
	}

	h.pool = grpcx.NewConnPool()
	h.admin = grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, h.pool)
	return h
}

func (h *startupScenarioHarness) Close(t *testing.T) {
	t.Helper()
	h.cancel()
	h.wg.Wait()
	close(h.processErrs)
	if h.pool != nil {
		_ = h.pool.Close()
	}
	for err := range h.processErrs {
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("startup scenario process returned error: %v", err)
		}
	}
}

func runStorageProcessWithReporterLatency(ctx context.Context, cfg StorageProcessConfig, delay time.Duration, jitter time.Duration) error {
	if cfg.ManifestPath == "" {
		return fmt.Errorf("storage manifest path must not be empty")
	}
	if cfg.NodeID == "" {
		return fmt.Errorf("storage node id must not be empty")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("storage data dir must not be empty")
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = time.Second
	}
	if cfg.ActivationInterval <= 0 {
		cfg.ActivationInterval = 250 * time.Millisecond
	}
	if cfg.RPCDeadline <= 0 {
		cfg.RPCDeadline = 5 * time.Second
	}

	manifest, err := quickstart.Load(cfg.ManifestPath)
	if err != nil {
		return err
	}
	nodeCfg, ok := manifest.NodeByID(cfg.NodeID)
	if !ok {
		return fmt.Errorf("node %q not found in manifest", cfg.NodeID)
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir storage data dir: %w", err)
	}

	store, err := badgerstore.Open(filepath.Join(cfg.DataDir, "storage"))
	if err != nil {
		return fmt.Errorf("open storage backend: %w", err)
	}
	defer func() { _ = store.Close() }()

	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()

	reporter := &latencyInjectingCoordinatorClient{
		delegate: grpcx.NewCoordinatorReporterClient(nodeCfg.ID, manifest.Coordinator.RPCAddress, pool),
		delay:    delay,
		jitter:   jitter,
	}
	repl := grpcx.NewReplicationTransport(pool)
	node, err := storage.OpenNode(
		ctx,
		storage.Config{
			NodeID:                    nodeCfg.ID,
			RPCAddress:                nodeCfg.RPCAddress,
			FailureDomains:            nodeCfg.FailureDomains,
			AutoActivateEmptyReplicas: true,
		},
		store.Backend(),
		store.LocalStateStore(),
		reporter,
		repl,
	)
	if err != nil {
		return fmt.Errorf("open storage node %q: %w", nodeCfg.ID, err)
	}
	defer func() { _ = node.Close() }()

	lis, err := net.Listen("tcp", nodeCfg.RPCAddress)
	if err != nil {
		return fmt.Errorf("listen storage grpc %q: %w", nodeCfg.RPCAddress, err)
	}
	grpcServer := grpcx.NewStorageGRPCServer(node)
	defer func() { _ = grpcServer.Close() }()
	go serveGRPC(grpcServer, lis)

	var admin *adminhttp.Server
	if nodeCfg.AdminAddress != "" {
		admin = adminhttp.NewStorage(node, adminhttp.Config{
			Address:  nodeCfg.AdminAddress,
			Gatherer: node.MetricsRegistry(),
		})
		defer func() {
			if admin != nil {
				_ = admin.Close(context.Background())
			}
		}()
		go serveAdmin(admin)
	}

	registered := false
	go runTicker(ctx, cfg.HeartbeatInterval, func() {
		if !registered {
			registerCtx, cancel := context.WithTimeout(ctx, cfg.RPCDeadline)
			err := node.Register(registerCtx)
			cancel()
			if err != nil {
				fmt.Fprintf(os.Stderr, "storage register failed node=%s error=%v\n", cfg.NodeID, err)
				return
			}
			registered = true
		}
		hbCtx, cancel := context.WithTimeout(ctx, cfg.RPCDeadline)
		err := node.ReportHeartbeatOnly(hbCtx)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "storage heartbeat failed node=%s error=%v\n", cfg.NodeID, err)
		}
	})
	go runTicker(ctx, cfg.ActivationInterval, func() {
		advanceReplicaLifecycle(ctx, node, cfg.NodeID, cfg.RPCDeadline)
	})

	<-ctx.Done()
	return nil
}

type latencyInjectingCoordinatorClient struct {
	delegate storage.CoordinatorClient
	delay    time.Duration
	jitter   time.Duration
	calls    atomic.Uint64
}

func (c *latencyInjectingCoordinatorClient) RegisterNode(ctx context.Context, reg storage.NodeRegistration) error {
	if err := c.sleep(ctx); err != nil {
		return err
	}
	return c.delegate.RegisterNode(ctx, reg)
}

func (c *latencyInjectingCoordinatorClient) ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error {
	if err := c.sleep(ctx); err != nil {
		return err
	}
	return c.delegate.ReportReplicaReady(ctx, slot, epoch)
}

func (c *latencyInjectingCoordinatorClient) ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error {
	if err := c.sleep(ctx); err != nil {
		return err
	}
	return c.delegate.ReportReplicaRemoved(ctx, slot, epoch)
}

func (c *latencyInjectingCoordinatorClient) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	if err := c.sleep(ctx); err != nil {
		return err
	}
	return c.delegate.ReportNodeRecovered(ctx, report)
}

func (c *latencyInjectingCoordinatorClient) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	if err := c.sleep(ctx); err != nil {
		return err
	}
	return c.delegate.ReportNodeHeartbeat(ctx, status)
}

func (c *latencyInjectingCoordinatorClient) sleep(ctx context.Context) error {
	delay := c.delay
	if c.jitter > 0 {
		delay += time.Duration(c.calls.Add(1)%3) * c.jitter
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func loadRoutingProgressRecords(path string) ([]routingProgressRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	records := make([]routingProgressRecord, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record routingProgressRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func requireBenchmarkCloudShapeSoak(t *testing.T) {
	t.Helper()
	if os.Getenv("CRAQ_RUN_BENCHMARK_SOAK_LOCAL") == "" {
		t.Skip("set CRAQ_RUN_BENCHMARK_SOAK_LOCAL=1 to run the local cloud-shape benchmark soak")
	}
}

func assertRoutingProgressHealthy(records []routingProgressRecord, slotCount int, stallBudget time.Duration) error {
	firstReadable := -1
	maxWritable := 0
	maxReadable := 0
	lastImprovementAt := records[0].Time

	for idx, record := range records {
		if record.ReadableSlots > 0 && firstReadable == -1 {
			firstReadable = idx
			lastImprovementAt = record.Time
		}
		improved := false
		if record.ReadableSlots > maxReadable {
			maxReadable = record.ReadableSlots
			improved = true
		}
		if record.WritableSlots > maxWritable {
			maxWritable = record.WritableSlots
			improved = true
		}
		if improved {
			lastImprovementAt = record.Time
		}
		if maxWritable >= max(1, slotCount/8) && record.WritableSlots == 0 {
			return fmt.Errorf("writable routing collapsed to zero after reaching %d writable slots", maxWritable)
		}
		if maxReadable >= max(1, slotCount/4) && record.ReadableSlots < maxReadable/2 {
			return fmt.Errorf("readable routing collapsed from %d to %d", maxReadable, record.ReadableSlots)
		}
		if firstReadable != -1 && !routingProgressReady(routingProgress{
			slotCount:           record.SlotCount,
			readableSlots:       record.ReadableSlots,
			writableSlots:       record.WritableSlots,
			settledSlots:        record.SettledSlots,
			pendingSlots:        record.PendingSlots,
			outboxEntries:       record.OutboxEntries,
			activePeerRefreshes: record.ActivePeerRefreshes,
			healthyNodes:        record.HealthyNodes,
			suspectNodes:        record.SuspectNodes,
			deadNodes:           record.DeadNodes,
		}) {
			if gap := record.Time.Sub(lastImprovementAt); gap > stallBudget {
				return fmt.Errorf("routing progress stalled for %s before settled routing", gap)
			}
		}
	}
	last := records[len(records)-1]
	if !routingProgressReady(routingProgress{
		slotCount:           last.SlotCount,
		readableSlots:       last.ReadableSlots,
		writableSlots:       last.WritableSlots,
		settledSlots:        last.SettledSlots,
		pendingSlots:        last.PendingSlots,
		outboxEntries:       last.OutboxEntries,
		activePeerRefreshes: last.ActivePeerRefreshes,
		healthyNodes:        last.HealthyNodes,
		suspectNodes:        last.SuspectNodes,
		deadNodes:           last.DeadNodes,
	}) {
		return fmt.Errorf(
			"final routing progress not settled: writable=%d/%d readable=%d/%d settled=%d/%d pending=%d outbox=%d refresh=%d health=%d suspect=%d dead=%d",
			last.WritableSlots,
			slotCount,
			last.ReadableSlots,
			slotCount,
			last.SettledSlots,
			slotCount,
			last.PendingSlots,
			last.OutboxEntries,
			last.ActivePeerRefreshes,
			last.HealthyNodes,
			last.SuspectNodes,
			last.DeadNodes,
		)
	}
	return nil
}

func assertCloudShapeRoutingProgressHealthy(records []routingProgressRecord, slotCount int, stallBudget time.Duration) error {
	if err := assertRoutingProgressHealthy(records, slotCount, stallBudget); err != nil {
		return err
	}

	maxWritable := 0
	maxReadable := 0
	readableFloor := max(16, slotCount/16)
	writableFloor := max(8, slotCount/128)
	for _, record := range records {
		if record.WritableSlots > maxWritable {
			maxWritable = record.WritableSlots
		}
		if record.ReadableSlots > maxReadable {
			maxReadable = record.ReadableSlots
		}
		if maxReadable >= readableFloor && record.PendingSlots >= slotCount {
			return fmt.Errorf("pending work reset to all %d slots after readable routing reached %d", slotCount, maxReadable)
		}
		if maxWritable >= writableFloor && record.WritableSlots == 0 {
			return fmt.Errorf("writable routing collapsed to zero after reaching %d writable slots", maxWritable)
		}
	}
	return nil
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
