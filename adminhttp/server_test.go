package adminhttp_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/adminhttp"
	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
	"github.com/prometheus/client_golang/prometheus"
)

func TestStorageAdminHTTPExposesHealthMetricsAndState(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	node := mustNewNode(t, ctx, storage.Config{
		NodeID:          "node-a",
		MetricsRegistry: registry,
	}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport())
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if _, err := node.HandleClientPut(ctx, storage.ClientPutRequest{Slot: 1, Key: "alpha", Value: "one", ExpectedChainVersion: 1}); err != nil {
		t.Fatalf("HandleClientPut returned error: %v", err)
	}

	server := adminhttp.NewStorage(node, adminhttp.Config{Gatherer: node.MetricsRegistry()})
	lis, baseURL := mustStartAdminServer(t, server)

	assertStatus(t, baseURL+"/livez", http.StatusOK)
	assertStatus(t, baseURL+"/readyz", http.StatusOK)

	body := mustGET(t, baseURL+"/metrics")
	if !strings.Contains(body, "craq_storage_client_writes_total") {
		t.Fatalf("/metrics body missing storage metrics: %s", body)
	}

	resp := mustGETJSON[storage.AdminState](t, baseURL+"/admin/v1/state")
	if resp.Node.NodeID != "node-a" {
		t.Fatalf("admin state node id = %q, want %q", resp.Node.NodeID, "node-a")
	}
	if len(resp.Node.Replicas) != 1 {
		t.Fatalf("admin state replicas = %#v, want one replica", resp.Node.Replicas)
	}
	if len(resp.Recent) == 0 {
		t.Fatal("admin state recent events unexpectedly empty")
	}

	_ = lis
}

func TestCoordinatorAdminHTTPExposesHealthMetricsAndState(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	server, err := coordserver.OpenWithConfig(ctx, coordruntime.NewInMemoryStore(), nil, coordserver.ServerConfig{
		MetricsRegistry: registry,
	})
	if err != nil {
		t.Fatalf("OpenWithConfig returned error: %v", err)
	}
	if _, err := server.Bootstrap(ctx, coordruntime.Command{
		ID:   "bootstrap-admin-http",
		Kind: coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
			Nodes:  []coordinator.Node{{ID: "a", RPCAddress: "storage-a:443"}},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	adminServer := adminhttp.NewCoordinator(server, adminhttp.Config{Gatherer: server.MetricsRegistry()})
	_, baseURL := mustStartAdminServer(t, adminServer)

	assertStatus(t, baseURL+"/livez", http.StatusOK)
	assertStatus(t, baseURL+"/readyz", http.StatusOK)

	body := mustGET(t, baseURL+"/metrics")
	if !strings.Contains(body, "craq_coordserver_commands_total") {
		t.Fatalf("/metrics body missing coordserver metrics: %s", body)
	}

	resp := mustGETJSON[coordserver.AdminState](t, baseURL+"/admin/v1/state")
	if resp.Current.Version == 0 {
		t.Fatalf("admin state current version = %d, want > 0", resp.Current.Version)
	}
	if resp.RoutingSnapshot.SlotCount != 1 {
		t.Fatalf("routing snapshot slot count = %d, want 1", resp.RoutingSnapshot.SlotCount)
	}
	if len(resp.Recent) == 0 {
		t.Fatal("admin state recent events unexpectedly empty")
	}
}

func mustStartAdminServer(t *testing.T, server *adminhttp.Server) (net.Listener, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	go func() {
		if serveErr := server.Serve(lis); serveErr != nil && serveErr != http.ErrServerClosed {
			panic(serveErr)
		}
	}()
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Close(closeCtx)
	})
	return lis, "http://" + lis.Addr().String()
}

func assertStatus(t *testing.T, url string, want int) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s returned error: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("GET %s status = %d, want %d", url, resp.StatusCode, want)
	}
}

func mustGET(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s returned error: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}
	return string(body)
}

func mustGETJSON[T any](t *testing.T, url string) T {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s returned error: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s status = %d, want 200", url, resp.StatusCode)
	}
	var out T
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("Decode returned error: %v", err)
	}
	return out
}

func mustNewNode(
	t *testing.T,
	ctx context.Context,
	cfg storage.Config,
	backend storage.Backend,
	coord storage.CoordinatorClient,
	repl storage.ReplicationTransport,
) *storage.Node {
	t.Helper()
	node, err := storage.NewNode(ctx, cfg, backend, coord, repl)
	if err != nil {
		t.Fatalf("NewNode returned error: %v", err)
	}
	return node
}
