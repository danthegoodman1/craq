package grpcx_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/danthegoodman1/craq/storage"
	"github.com/danthegoodman1/craq/transport/grpcx"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
)

func TestGRPCObservabilityRecordsAuthFailures(t *testing.T) {
	fixture := newSecurityFixture(t)
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf)
	registry := prometheus.NewRegistry()

	serverFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
	clientFiles := fixture.mustWriteLeaf("storage", "storage-b", fixture.clusterCA)
	node := mustOpenNode(t, context.Background(), storage.Config{NodeID: "storage-a"}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport())
	server, address := mustStartSecureStorageServer(t, node, serverFiles, grpcx.ServerTLSConfig{
		CAFile:          fixture.clusterCA.caPath,
		CertFile:        serverFiles.certPath,
		KeyFile:         serverFiles.keyPath,
		ClientAuth:      grpcx.ClientAuthModeVerifyIfGiven,
		Logger:          &logger,
		MetricsRegistry: registry,
	})
	defer func() { _ = server.Close() }()

	pool := fixture.mustClientPool(grpcx.ClientTLSConfig{
		CAFile:          fixture.clusterCA.caPath,
		CertFile:        clientFiles.certPath,
		KeyFile:         clientFiles.keyPath,
		ServerName:      "storage",
		MetricsRegistry: registry,
		Logger:          &logger,
	})
	err := grpcx.NewStorageNodeClient(address, pool).AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
	})
	if !errors.Is(err, grpcx.ErrTransportPermissionDenied) {
		t.Fatalf("AddReplicaAsTail error = %v, want permission denied", err)
	}

	if got := metricCounterValueWithLabels(t, registry, "craq_grpc_auth_failures_total", map[string]string{
		"component": "storage",
		"method":    "/craq.v1.StorageService/AddReplicaAsTail",
		"code":      "PermissionDenied",
	}); got != 1 {
		t.Fatalf("grpc auth failures total = %v, want 1", got)
	}
	if !strings.Contains(logBuf.String(), "grpc request completed with error") {
		t.Fatalf("logs = %q, want grpc error log", logBuf.String())
	}
}

func metricCounterValueWithLabels(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather returned error: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metricLabelsMatch(metric, labels) && metric.Counter != nil {
				return metric.Counter.GetValue()
			}
		}
	}
	t.Fatalf("metric %q with labels %v not found", name, labels)
	return 0
}

func metricLabelsMatch(metric *dto.Metric, labels map[string]string) bool {
	got := map[string]string{}
	for _, label := range metric.GetLabel() {
		got[label.GetName()] = label.GetValue()
	}
	for key, want := range labels {
		if got[key] != want {
			return false
		}
	}
	return true
}
