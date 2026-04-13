package benchmark

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
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

const benchmarkStartupChangedChainFloor = 1024

type CoordinatorProcessConfig struct {
	ManifestPath    string                            `json:"manifest_path"`
	DataDir         string                            `json:"data_dir"`
	Liveness        coordserver.LivenessPolicy        `json:"liveness"`
	Reconfiguration coordinator.ReconfigurationPolicy `json:"reconfiguration"`
	TickInterval    time.Duration                     `json:"tick_interval"`
	RPCDeadline     time.Duration                     `json:"rpc_deadline"`
}

type StorageProcessConfig struct {
	ManifestPath               string        `json:"manifest_path"`
	NodeID                     string        `json:"node_id"`
	DataDir                    string        `json:"data_dir"`
	HeartbeatInterval          time.Duration `json:"heartbeat_interval"`
	ActivationInterval         time.Duration `json:"activation_interval"`
	RPCDeadline                time.Duration `json:"rpc_deadline"`
	WriteTraceOutput           string        `json:"write_trace_output"`
	WriteTraceSampleRate       int           `json:"write_trace_sample_rate"`
	WriteTimeoutArtifacts      string        `json:"write_timeout_artifacts"`
	JournalShards              int           `json:"journal_shards"`
	JournalBatchDelayLow       time.Duration `json:"journal_batch_delay_low"`
	JournalBatchDelayHigh      time.Duration `json:"journal_batch_delay_high"`
	JournalBatchDepthThreshold int           `json:"journal_batch_depth_threshold"`
	JournalBatchMaxOps         int           `json:"journal_batch_max_ops"`
	JournalExperiment          string        `json:"journal_experiment,omitempty"`
}

func RunCoordinatorProcess(ctx context.Context, cfg CoordinatorProcessConfig) error {
	if cfg.ManifestPath == "" {
		return fmt.Errorf("coordinator manifest path must not be empty")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("coordinator data dir must not be empty")
	}
	if cfg.TickInterval <= 0 {
		cfg.TickInterval = time.Second
	}
	if cfg.RPCDeadline <= 0 {
		cfg.RPCDeadline = 5 * time.Second
	}
	manifest, err := quickstart.Load(cfg.ManifestPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir coordinator data dir: %w", err)
	}

	store, err := coordruntime.OpenBadgerStore(filepath.Join(cfg.DataDir, "runtime"))
	if err != nil {
		return fmt.Errorf("open coordinator store: %w", err)
	}
	defer func() { _ = store.Close() }()

	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()

	server, err := coordserver.OpenWithConfig(ctx, store, nil, coordserver.ServerConfig{
		LivenessPolicy:        cfg.Liveness,
		ReconfigurationPolicy: cfg.Reconfiguration,
		StartupMaxChangedChains: benchmarkStartupMaxChangedChains(
			manifest.Coordinator.SlotCount,
			cfg.Reconfiguration.MaxChangedChains,
		),
		NodeClientFactory:     grpcx.NewDynamicNodeClientFactory(pool),
		DispatchRetryInterval: 200 * time.Millisecond,
	})
	if err != nil {
		return fmt.Errorf("open coordinator server: %w", err)
	}
	defer func() { _ = server.Close() }()

	if server.Current().Cluster.SlotCount == 0 {
		if _, err := server.Bootstrap(ctx, coordruntime.Command{
			ID:              "bench-bootstrap",
			ExpectedVersion: 0,
			Kind:            coordruntime.CommandKindBootstrap,
			Bootstrap: &coordruntime.BootstrapCommand{
				Config: coordinator.Config{
					SlotCount:         manifest.Coordinator.SlotCount,
					ReplicationFactor: manifest.Coordinator.ReplicationFactor,
				},
				Policy: cfg.Reconfiguration,
			},
		}); err != nil {
			return fmt.Errorf("bootstrap coordinator: %w", err)
		}
	}

	lis, err := net.Listen("tcp", manifest.Coordinator.RPCAddress)
	if err != nil {
		return fmt.Errorf("listen coordinator grpc %q: %w", manifest.Coordinator.RPCAddress, err)
	}
	grpcServer := grpcx.NewCoordinatorGRPCServer(server)
	defer func() { _ = grpcServer.Close() }()
	go serveGRPC(grpcServer, lis)

	var admin *adminhttp.Server
	if manifest.Coordinator.AdminAddress != "" {
		admin = adminhttp.NewCoordinator(server, adminhttp.Config{
			Address:  manifest.Coordinator.AdminAddress,
			Gatherer: server.MetricsRegistry(),
		})
		defer func() {
			if admin != nil {
				_ = admin.Close(context.Background())
			}
		}()
		go serveAdmin(admin)
	}

	adminClient := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	go runTicker(ctx, cfg.TickInterval, func() {
		evalCtx, cancel := context.WithTimeout(ctx, cfg.RPCDeadline)
		defer cancel()
		_ = adminClient.EvaluateLiveness(evalCtx)
	})

	<-ctx.Done()
	return nil
}

func RunStorageProcess(ctx context.Context, cfg StorageProcessConfig) error {
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

	reporter := grpcx.NewCoordinatorReporterClient(nodeCfg.ID, manifest.Coordinator.RPCAddress, pool)
	repl := grpcx.NewReplicationTransport(pool)
	node, err := storage.OpenNode(
		ctx,
		storage.Config{
			NodeID:                         nodeCfg.ID,
			RPCAddress:                     nodeCfg.RPCAddress,
			FailureDomains:                 nodeCfg.FailureDomains,
			WriteTraceOutputPath:           cfg.WriteTraceOutput,
			WriteTraceSampleRate:           cfg.WriteTraceSampleRate,
			WriteTimeoutArtifactOutputPath: cfg.WriteTimeoutArtifacts,
			JournalShards:                  cfg.JournalShards,
			JournalBatchDelayLow:           cfg.JournalBatchDelayLow,
			JournalBatchDelayHigh:          cfg.JournalBatchDelayHigh,
			JournalBatchDepthThreshold:     cfg.JournalBatchDepthThreshold,
			JournalBatchMaxOps:             cfg.JournalBatchMaxOps,
			JournalExperiment:              storage.JournalExperiment(cfg.JournalExperiment),
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
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return
				}
				fmt.Fprintf(os.Stderr, "storage register failed node=%s error=%v\n", cfg.NodeID, err)
				return
			}
			registered = true
		}
		hbCtx, cancel := context.WithTimeout(ctx, cfg.RPCDeadline)
		err := node.ReportHeartbeatOnly(hbCtx)
		cancel()
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			fmt.Fprintf(os.Stderr, "storage heartbeat failed node=%s error=%v\n", cfg.NodeID, err)
		}
	})
	go runTicker(ctx, cfg.ActivationInterval, func() {
		advanceReplicaLifecycle(ctx, node, cfg.NodeID, cfg.RPCDeadline)
	})

	<-ctx.Done()
	return nil
}

func RunSignalContext(fn func(context.Context) error) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return fn(ctx)
}

func serveGRPC(server interface{ Serve(net.Listener) error }, lis net.Listener) {
	if err := server.Serve(lis); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		fmt.Fprintf(os.Stderr, "grpc serve error: %v\n", err)
	}
}

func serveAdmin(server *adminhttp.Server) {
	if err := server.ListenAndServe(); err != nil &&
		!strings.Contains(err.Error(), "closed network connection") &&
		!strings.Contains(err.Error(), "Server closed") {
		fmt.Fprintf(os.Stderr, "admin serve error: %v\n", err)
	}
}

func runTicker(ctx context.Context, interval time.Duration, fn func()) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	fn()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}

func advanceReplicaLifecycle(ctx context.Context, node *storage.Node, nodeID string, rpcDeadline time.Duration) {
	processReplicaLifecycleSlots(ctx, node.CatchingUpSlots(), func(slot int) error {
		slotCtx, cancel := context.WithTimeout(ctx, rpcDeadline)
		defer cancel()
		return node.ActivateReplica(slotCtx, storage.ActivateReplicaCommand{Slot: slot})
	}, func(slot int, err error) {
		fmt.Fprintf(os.Stderr, "storage activate failed node=%s slot=%d error=%v\n", nodeID, slot, err)
	})
	processReplicaLifecycleSlots(ctx, node.LeavingSlots(), func(slot int) error {
		slotCtx, cancel := context.WithTimeout(ctx, rpcDeadline)
		defer cancel()
		return node.RemoveReplica(slotCtx, storage.RemoveReplicaCommand{Slot: slot})
	}, func(slot int, err error) {
		fmt.Fprintf(os.Stderr, "storage remove failed node=%s slot=%d error=%v\n", nodeID, slot, err)
	})
}

func processReplicaLifecycleSlots(
	ctx context.Context,
	slots []int,
	op func(slot int) error,
	logFailure func(slot int, err error),
) {
	if len(slots) == 0 {
		return
	}
	workerCount := lifecycleWorkerCount(len(slots))
	if workerCount == 1 {
		for _, slot := range slots {
			if ctx.Err() != nil {
				return
			}
			if err := op(slot); err != nil {
				if errors.Is(err, context.Canceled) || ctx.Err() != nil {
					return
				}
				logFailure(slot, err)
			}
		}
		return
	}

	slotCh := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for slot := range slotCh {
				if ctx.Err() != nil {
					return
				}
				if err := op(slot); err != nil {
					if errors.Is(err, context.Canceled) || ctx.Err() != nil {
						return
					}
					logFailure(slot, err)
				}
			}
		}()
	}

	for _, slot := range slots {
		if ctx.Err() != nil {
			break
		}
		slotCh <- slot
	}
	close(slotCh)
	wg.Wait()
}

func lifecycleWorkerCount(slotCount int) int {
	if slotCount <= 1 {
		return slotCount
	}
	// Cap at 16 so startup can activate replicas in parallel without
	// overwhelming the coordinator on smaller local runs.
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	if workers > 16 {
		workers = 16
	}
	if workers > slotCount {
		return slotCount
	}
	return workers
}

func benchmarkStartupMaxChangedChains(slotCount int, steadyStateBudget int) int {
	startupBudget := steadyStateBudget
	floor := slotCount
	if floor > benchmarkStartupChangedChainFloor {
		floor = benchmarkStartupChangedChainFloor
	}
	if floor > startupBudget {
		startupBudget = floor
	}
	return startupBudget
}
