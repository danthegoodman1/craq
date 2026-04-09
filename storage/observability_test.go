package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/rs/zerolog"
)

func TestNodeObservabilityRecordsAmbiguousWriteAndBackpressure(t *testing.T) {
	ctx := context.Background()
	var logBuf bytes.Buffer
	logger := zerolog.New(&logBuf)
	headRegistry := prometheus.NewRegistry()
	tailRegistry := prometheus.NewRegistry()

	repl := NewQueuedInMemoryReplicationTransport()
	headBackend := NewInMemoryBackend()
	head := mustNewNode(t, ctx, Config{
		NodeID:             "head",
		WriteCommitTimeout: 2 * time.Millisecond,
		Logger:             &logger,
		MetricsRegistry:    headRegistry,
	}, headBackend, NewInMemoryCoordinatorClient(), repl)
	tailBackend := NewInMemoryBackend()
	tail := mustNewNode(t, ctx, Config{
		NodeID:                            "tail",
		MaxBufferedReplicaMessagesPerSlot: 1,
		Logger:                            &logger,
		MetricsRegistry:                   tailRegistry,
	}, tailBackend, NewInMemoryCoordinatorClient(), repl)
	repl.Register("head", headBackend)
	repl.Register("tail", tailBackend)
	repl.RegisterNode("head", head)
	repl.RegisterNode("tail", tail)

	if err := head.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "tail"},
		},
	}); err != nil {
		t.Fatalf("head AddReplicaAsTail returned error: %v", err)
	}
	if err := head.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("head ActivateReplica returned error: %v", err)
	}
	if err := tail.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "head"},
		},
	}); err != nil {
		t.Fatalf("tail AddReplicaAsTail returned error: %v", err)
	}
	if err := tail.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("tail ActivateReplica returned error: %v", err)
	}
	head.repl = &blockingWriteTransport{}

	_, err := head.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 1,
		Key:                  "alpha",
		Value:                "one",
		ExpectedChainVersion: 1,
	})
	if err == nil || !errors.Is(err, ErrAmbiguousWrite) {
		t.Fatalf("HandleClientPut error = %v, want ambiguous write", err)
	}

	err = tail.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     1,
			Sequence: 2,
			Kind:     OperationKindPut,
			Key:      "beta",
			Value:    "two",
			Metadata: testObjectMetadata(2),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("first future HandleForwardWrite returned error: %v", err)
	}
	err = tail.HandleForwardWrite(ctx, ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     1,
			Sequence: 3,
			Kind:     OperationKindPut,
			Key:      "gamma",
			Value:    "three",
			Metadata: testObjectMetadata(3),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	})
	var pressure *BackpressureError
	if !errors.As(err, &pressure) || !errors.Is(err, ErrReplicaBackpressure) {
		t.Fatalf("second future HandleForwardWrite error = %v, want replica backpressure", err)
	}

	if got := metricCounterValue(t, headRegistry, "craq_storage_ambiguous_writes_total"); got != 1 {
		t.Fatalf("ambiguous writes total = %v, want 1", got)
	}
	if got := metricCounterValueWithLabels(t, tailRegistry, "craq_storage_backpressure_rejections_total", map[string]string{"resource": "replica_buffer"}); got != 1 {
		t.Fatalf("replica backpressure total = %v, want 1", got)
	}
	if got := metricCounterValueWithLabels(t, headRegistry, "craq_storage_client_writes_total", map[string]string{"kind": "put", "result": "ambiguous"}); got != 1 {
		t.Fatalf("ambiguous put counter = %v, want 1", got)
	}
	if !strings.Contains(logBuf.String(), "ambiguous_write") {
		t.Fatalf("logs = %q, want ambiguous_write entry", logBuf.String())
	}
	if !strings.Contains(logBuf.String(), "backpressure_rejected") {
		t.Fatalf("logs = %q, want backpressure_rejected entry", logBuf.String())
	}
	if len(head.RecentEvents()) == 0 {
		t.Fatal("head recent events unexpectedly empty")
	}
	if len(tail.RecentEvents()) == 0 {
		t.Fatal("tail recent events unexpectedly empty")
	}
}

func metricCounterValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	return metricCounterValueWithLabels(t, registry, name, nil)
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
			if metricLabelsMatch(metric, labels) {
				if metric.Counter != nil {
					return metric.Counter.GetValue()
				}
				if metric.Gauge != nil {
					return metric.Gauge.GetValue()
				}
			}
		}
	}
	t.Fatalf("metric %q with labels %v not found", name, labels)
	return 0
}

func metricLabelsMatch(metric *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}
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
