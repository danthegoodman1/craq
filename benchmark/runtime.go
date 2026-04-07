package benchmark

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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

type CoordinatorProcessConfig struct {
	ManifestPath string                     `json:"manifest_path"`
	DataDir      string                     `json:"data_dir"`
	Liveness     coordserver.LivenessPolicy `json:"liveness"`
	TickInterval time.Duration              `json:"tick_interval"`
	RPCDeadline  time.Duration              `json:"rpc_deadline"`
}

type StorageProcessConfig struct {
	ManifestPath       string        `json:"manifest_path"`
	NodeID             string        `json:"node_id"`
	DataDir            string        `json:"data_dir"`
	HeartbeatInterval  time.Duration `json:"heartbeat_interval"`
	ActivationInterval time.Duration `json:"activation_interval"`
	RPCDeadline        time.Duration `json:"rpc_deadline"`
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
		evalCtx, cancel := context.WithTimeout(context.Background(), cfg.RPCDeadline)
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
			NodeID:         nodeCfg.ID,
			RPCAddress:     nodeCfg.RPCAddress,
			FailureDomains: nodeCfg.FailureDomains,
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
		hbCtx, cancel := context.WithTimeout(context.Background(), cfg.RPCDeadline)
		defer cancel()
		if !registered {
			if err := node.Register(hbCtx); err != nil {
				return
			}
			registered = true
		}
		_ = reporter.ReportNodeHeartbeat(hbCtx, nodeStatusFromState(node.State()))
	})
	go runTicker(ctx, cfg.ActivationInterval, func() {
		actCtx, cancel := context.WithTimeout(context.Background(), cfg.RPCDeadline)
		defer cancel()
		activateCatchingUpReplicas(actCtx, node)
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
	if err := server.ListenAndServe(); err != nil && !strings.Contains(err.Error(), "closed network connection") {
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

func activateCatchingUpReplicas(ctx context.Context, node *storage.Node) {
	for slot, replica := range node.State().Replicas {
		if replica.State != storage.ReplicaStateCatchingUp {
			continue
		}
		_ = node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot})
	}
}

func nodeStatusFromState(state storage.NodeState) storage.NodeStatus {
	status := storage.NodeStatus{NodeID: state.NodeID}
	for _, replica := range state.Replicas {
		status.ReplicaCount++
		switch replica.State {
		case storage.ReplicaStateActive:
			status.ActiveCount++
		case storage.ReplicaStateCatchingUp:
			status.CatchingUpCount++
		case storage.ReplicaStateLeaving:
			status.LeavingCount++
		}
	}
	return status
}
