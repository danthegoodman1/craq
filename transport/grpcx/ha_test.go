package grpcx_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

func TestCoordinatorFailoverClientsOverGRPC(t *testing.T) {
	ctx := context.Background()
	clock := &haFakeClock{now: time.Unix(100, 0).UTC()}
	haStore := coordserver.NewInMemoryHAStore()

	leader := mustOpenHAServer(t, clock, haStore, "coord-a", "leader")
	defer func() { _ = leader.server.Close() }()
	defer func() { _ = leader.grpc.Close() }()

	standby := mustOpenHAServer(t, clock, haStore, "coord-b", "standby")
	defer func() { _ = standby.server.Close() }()
	defer func() { _ = standby.grpc.Close() }()

	if isLeader, err := leader.server.StepHA(ctx); err != nil {
		t.Fatalf("leader StepHA returned error: %v", err)
	} else if !isLeader {
		t.Fatal("leader did not acquire initial lease")
	}
	if isLeader, err := standby.server.StepHA(ctx); err != nil {
		t.Fatalf("standby StepHA returned error: %v", err)
	} else if isLeader {
		t.Fatal("standby unexpectedly acquired initial lease")
	}

	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	admin := grpcx.NewCoordinatorAdminFailoverClient([]string{standby.address, leader.address}, pool)
	reporter := grpcx.NewCoordinatorReporterFailoverClient("n1", []string{standby.address, leader.address}, pool)

	if _, err := admin.Bootstrap(ctx, coordruntime.Command{
		ID:              "bootstrap",
		ExpectedVersion: 0,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 4, ReplicationFactor: 1},
			Nodes: []coordinator.Node{{
				ID:         "n1",
				RPCAddress: "storage-n1",
			}},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	if err := reporter.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat before failover returned error: %v", err)
	}
	if got := leader.server.Heartbeats()["n1"].ReplicaCount; got != 1 {
		t.Fatalf("leader heartbeat replica count = %d, want 1", got)
	}

	clock.Advance(3 * time.Second)
	if isLeader, err := standby.server.StepHA(ctx); err != nil {
		t.Fatalf("standby StepHA takeover returned error: %v", err)
	} else if !isLeader {
		t.Fatal("standby did not acquire takeover lease")
	}

	snapshot, err := admin.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after failover returned error: %v", err)
	}
	if got, want := snapshot.Version, uint64(1); got != want {
		t.Fatalf("routing snapshot version = %d, want %d", got, want)
	}

	if err := reporter.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 2}); err != nil {
		t.Fatalf("ReportNodeHeartbeat after failover returned error: %v", err)
	}
	if got := standby.server.Heartbeats()["n1"].ReplicaCount; got != 2 {
		t.Fatalf("standby heartbeat replica count = %d, want 2", got)
	}
}

func TestCoordinatorFailoverClientsSkipUnavailableAndStandbyEndpoints(t *testing.T) {
	ctx := context.Background()
	clock := &haFakeClock{now: time.Unix(200, 0).UTC()}
	haStore := coordserver.NewInMemoryHAStore()

	leader := mustOpenHAServer(t, clock, haStore, "coord-a", "leader")
	defer func() { _ = leader.server.Close() }()
	defer func() { _ = leader.grpc.Close() }()

	standby := mustOpenHAServer(t, clock, haStore, "coord-b", "standby")
	defer func() { _ = standby.server.Close() }()
	defer func() { _ = standby.grpc.Close() }()

	if isLeader, err := leader.server.StepHA(ctx); err != nil {
		t.Fatalf("leader StepHA returned error: %v", err)
	} else if !isLeader {
		t.Fatal("leader did not acquire initial lease")
	}
	if isLeader, err := standby.server.StepHA(ctx); err != nil {
		t.Fatalf("standby StepHA returned error: %v", err)
	} else if isLeader {
		t.Fatal("standby unexpectedly acquired initial lease")
	}

	deadListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen(deadListener) returned error: %v", err)
	}
	deadAddress := deadListener.Addr().String()
	_ = deadListener.Close()

	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	admin := grpcx.NewCoordinatorAdminFailoverClient([]string{deadAddress, standby.address, leader.address}, pool)
	reporter := grpcx.NewCoordinatorReporterFailoverClient("n1", []string{deadAddress, standby.address, leader.address}, pool)

	if _, err := admin.Bootstrap(ctx, coordruntime.Command{
		ID:              "bootstrap",
		ExpectedVersion: 0,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 4, ReplicationFactor: 1},
			Nodes: []coordinator.Node{{
				ID:         "n1",
				RPCAddress: "storage-n1",
			}},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := reporter.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 3}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	if got := leader.server.Heartbeats()["n1"].ReplicaCount; got != 3 {
		t.Fatalf("leader heartbeat replica count = %d, want 3", got)
	}
}

func TestStorageNodeMembershipSafetyAgainstHACoordinatorsOverGRPC(t *testing.T) {
	t.Run("auto_join_survives_failover", func(t *testing.T) {
		ctx := context.Background()
		clock := &haFakeClock{now: time.Unix(300, 0).UTC()}
		haStore := coordserver.NewInMemoryHAStore()

		leader := mustOpenHAServer(t, clock, haStore, "coord-a", "leader")
		defer func() { _ = leader.server.Close() }()
		defer func() { _ = leader.grpc.Close() }()

		standby := mustOpenHAServer(t, clock, haStore, "coord-b", "standby")
		defer func() { _ = standby.server.Close() }()
		defer func() { _ = standby.grpc.Close() }()

		if isLeader, err := leader.server.StepHA(ctx); err != nil {
			t.Fatalf("leader StepHA returned error: %v", err)
		} else if !isLeader {
			t.Fatal("leader did not acquire initial lease")
		}
		if isLeader, err := standby.server.StepHA(ctx); err != nil {
			t.Fatalf("standby StepHA returned error: %v", err)
		} else if isLeader {
			t.Fatal("standby unexpectedly acquired initial lease")
		}

		pool := grpcx.NewConnPool()
		t.Cleanup(func() { _ = pool.Close() })

		admin := grpcx.NewCoordinatorAdminFailoverClient([]string{standby.address, leader.address}, pool)
		reporter := grpcx.NewCoordinatorReporterFailoverClient("n1", []string{standby.address, leader.address}, pool)

		if _, err := admin.Bootstrap(ctx, coordruntime.Command{
			ID:              "bootstrap",
			ExpectedVersion: 0,
			Kind:            coordruntime.CommandKindBootstrap,
			Bootstrap: &coordruntime.BootstrapCommand{
				Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 2},
			},
		}); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}

		node := mustOpenNode(t, ctx, storage.Config{
			NodeID:         "n1",
			RPCAddress:     "storage-n1",
			FailureDomains: map[string]string{"host": "h1"},
		}, storage.NewInMemoryBackend(), reporter, storage.NewInMemoryReplicationTransport())

		if err := node.ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat before failover returned error: %v", err)
		}
		if _, ok := leader.server.Current().Cluster.NodesByID["n1"]; !ok {
			t.Fatalf("leader cluster missing auto-joined node: %#v", leader.server.Current().Cluster.NodesByID)
		}
		if !leader.server.Current().Cluster.ReadyNodeIDs["n1"] {
			t.Fatalf("leader cluster did not mark n1 ready: %#v", leader.server.Current().Cluster.ReadyNodeIDs)
		}

		clock.Advance(3 * time.Second)
		if isLeader, err := standby.server.StepHA(ctx); err != nil {
			t.Fatalf("standby StepHA takeover returned error: %v", err)
		} else if !isLeader {
			t.Fatal("standby did not acquire takeover lease")
		}

		if err := node.ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat after failover returned error: %v", err)
		}
		if _, ok := standby.server.Current().Cluster.NodesByID["n1"]; !ok {
			t.Fatalf("standby cluster missing auto-joined node after failover: %#v", standby.server.Current().Cluster.NodesByID)
		}
		if !standby.server.Current().Cluster.ReadyNodeIDs["n1"] {
			t.Fatalf("standby cluster did not mark n1 ready after failover: %#v", standby.server.Current().Cluster.ReadyNodeIDs)
		}
	})

	t.Run("identity_conflict_and_tombstone_after_failover", func(t *testing.T) {
		ctx := context.Background()
		clock := &haFakeClock{now: time.Unix(350, 0).UTC()}
		haStore := coordserver.NewInMemoryHAStore()

		leader := mustOpenHAServer(t, clock, haStore, "coord-a", "leader")
		defer func() { _ = leader.server.Close() }()
		defer func() { _ = leader.grpc.Close() }()

		standby := mustOpenHAServer(t, clock, haStore, "coord-b", "standby")
		defer func() { _ = standby.server.Close() }()
		defer func() { _ = standby.grpc.Close() }()

		if isLeader, err := leader.server.StepHA(ctx); err != nil {
			t.Fatalf("leader StepHA returned error: %v", err)
		} else if !isLeader {
			t.Fatal("leader did not acquire initial lease")
		}
		if isLeader, err := standby.server.StepHA(ctx); err != nil {
			t.Fatalf("standby StepHA returned error: %v", err)
		} else if isLeader {
			t.Fatal("standby unexpectedly acquired initial lease")
		}

		pool := grpcx.NewConnPool()
		t.Cleanup(func() { _ = pool.Close() })

		admin := grpcx.NewCoordinatorAdminFailoverClient([]string{standby.address, leader.address}, pool)
		reporterN1 := grpcx.NewCoordinatorReporterFailoverClient("n1", []string{standby.address, leader.address}, pool)
		reporterN2 := grpcx.NewCoordinatorReporterFailoverClient("n2", []string{standby.address, leader.address}, pool)

		if _, err := admin.Bootstrap(ctx, coordruntime.Command{
			ID:              "bootstrap",
			ExpectedVersion: 0,
			Kind:            coordruntime.CommandKindBootstrap,
			Bootstrap: &coordruntime.BootstrapCommand{
				Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
			},
		}); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		if err := reporterN1.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         "n1",
			RPCAddress:     "127.0.0.1:7411",
			FailureDomains: map[string]string{"host": "h1"},
		}); err != nil {
			t.Fatalf("RegisterNode(n1) returned error: %v", err)
		}

		clock.Advance(3 * time.Second)
		if isLeader, err := standby.server.StepHA(ctx); err != nil {
			t.Fatalf("standby StepHA takeover returned error: %v", err)
		} else if !isLeader {
			t.Fatal("standby did not acquire takeover lease")
		}

		if err := reporterN1.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         "n1",
			RPCAddress:     "127.0.0.1:7999",
			FailureDomains: map[string]string{"host": "h1"},
		}); err == nil {
			t.Fatal("conflicting RegisterNode(n1) unexpectedly succeeded after failover")
		}
		current := standby.server.Current()
		if _, err := admin.MarkNodeDead(ctx, coordruntime.Command{
			ID:              "dead-n1",
			ExpectedVersion: current.Version,
			Kind:            coordruntime.CommandKindReconfigure,
			Reconfigure: &coordruntime.ReconfigureCommand{
				Events: []coordinator.Event{{
					Kind:   coordinator.EventKindMarkNodeDead,
					NodeID: "n1",
				}},
			},
		}); err != nil {
			t.Fatalf("MarkNodeDead returned error: %v", err)
		}
		if err := reporterN1.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n1", ReplicaCount: 1}); err == nil {
			t.Fatal("ReportNodeHeartbeat(n1) unexpectedly succeeded after HA tombstone")
		}
		if err := reporterN2.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         "n2",
			RPCAddress:     "127.0.0.1:7412",
			FailureDomains: map[string]string{"host": "h2"},
		}); err != nil {
			t.Fatalf("RegisterNode(n2) returned error: %v", err)
		}
		if err := reporterN2.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "n2", ReplicaCount: 1}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(n2) returned error: %v", err)
		}
		if !standby.server.Current().Cluster.ReadyNodeIDs["n2"] {
			t.Fatalf("standby cluster did not mark n2 ready after replacement join: %#v", standby.server.Current().Cluster.ReadyNodeIDs)
		}
	})
}

type haServerHarness struct {
	server  *coordserver.Server
	grpc    *grpcx.CoordinatorGRPCServer
	address string
}

func mustOpenHAServer(t *testing.T, clock *haFakeClock, store coordserver.HAStore, coordinatorID string, advertise string) haServerHarness {
	t.Helper()

	server, err := coordserver.OpenWithConfig(context.Background(), coordruntime.NewInMemoryStore(), nil, coordserver.ServerConfig{
		Clock: clock,
		HA: &coordserver.HAConfig{
			CoordinatorID:          coordinatorID,
			AdvertiseAddress:       advertise,
			Store:                  store,
			LeaseTTL:               2 * time.Second,
			RenewInterval:          time.Second,
			DisableBackgroundLoops: true,
		},
	})
	if err != nil {
		t.Fatalf("coordserver.OpenWithConfig returned error: %v", err)
	}
	grpcServer := grpcx.NewCoordinatorGRPCServer(server)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen returned error: %v", err)
	}
	go func() { _ = grpcServer.Serve(lis) }()
	return haServerHarness{
		server:  server,
		grpc:    grpcServer,
		address: lis.Addr().String(),
	}
}

type haFakeClock struct {
	now time.Time
}

func (c *haFakeClock) Now() time.Time {
	return c.now
}

func (c *haFakeClock) Advance(delta time.Duration) {
	c.now = c.now.Add(delta)
}
