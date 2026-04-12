package storage

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestSingleReplicaWriteStageMetrics(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	node := mustNewNode(t, ctx, Config{
		NodeID:          "single",
		MetricsRegistry: registry,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 1, ReplicaAssignment{
		Slot:         1,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
	})

	result, err := node.SubmitPut(ctx, 1, "alpha", "one")
	if err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}
	assertAppliedCommitResult(t, result, 1, 1)

	assertWriteStageCount(t, registry, string(writeStageHeadGetCommitted), string(ReplicaRoleSingle), writeStageResultSuccess, 1)
	assertWriteStageCount(t, registry, string(writeStageHeadStageOp), string(ReplicaRoleSingle), writeStageResultSuccess, 1)
	assertWriteStageCount(t, registry, string(writeStageSingleApplyCommit), string(ReplicaRoleSingle), writeStageResultSuccess, 1)
	assertWriteStageCount(t, registry, string(writeStageHeadWaitForCommit), string(ReplicaRoleSingle), writeStageResultSuccess, 1)
	assertWriteStageCount(t, registry, string(writeStageHeadForwardRPC), string(ReplicaRoleSingle), writeStageResultSuccess, 0)
	assertWriteStageCount(t, registry, string(writeStageCommitUpstreamRPC), string(ReplicaRoleSingle), writeStageResultSuccess, 0)
	assertCounterValue(t, registry, "craq_storage_head_committed_metadata_lookups_total", map[string]string{"source": "miss"}, 1)
	assertCounterValue(t, registry, "craq_storage_head_committed_metadata_lookups_total", map[string]string{"source": "hit"}, 0)
}

func TestCommittedMetadataLookupMetricsRecordCacheHitAfterWarmup(t *testing.T) {
	ctx := context.Background()
	registry := prometheus.NewRegistry()
	backend := NewInMemoryBackend()
	node := mustNewNode(t, ctx, Config{
		NodeID:          "single",
		MetricsRegistry: registry,
		Clock:           &fakeClock{now: time.Unix(500, 0).UTC()},
	}, backend, NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 1, ReplicaAssignment{
		Slot:         1,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
	})
	if err := backend.Put(1, "alpha", "seed", ObjectMetadata{
		Version:   1,
		CreatedAt: time.Unix(100, 0).UTC(),
		UpdatedAt: time.Unix(100, 0).UTC(),
	}); err != nil {
		t.Fatalf("backend.Put returned error: %v", err)
	}

	if _, err := node.SubmitPut(ctx, 1, "alpha", "one"); err != nil {
		t.Fatalf("first SubmitPut returned error: %v", err)
	}
	if _, err := node.SubmitPut(ctx, 1, "alpha", "two"); err != nil {
		t.Fatalf("second SubmitPut returned error: %v", err)
	}

	assertCounterValue(t, registry, "craq_storage_head_committed_metadata_lookups_total", map[string]string{"source": "miss"}, 1)
	assertCounterValue(t, registry, "craq_storage_head_committed_metadata_lookups_total", map[string]string{"source": "hit"}, 1)
}

func TestThreeReplicaWriteStageMetricsRecordOncePerLogicalWrite(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	headRegistry := prometheus.NewRegistry()
	midRegistry := prometheus.NewRegistry()
	tailRegistry := prometheus.NewRegistry()

	head := mustNewNode(t, ctx, Config{NodeID: "head", MetricsRegistry: headRegistry}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mid := mustNewNode(t, ctx, Config{NodeID: "mid", MetricsRegistry: midRegistry}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	tail := mustNewNode(t, ctx, Config{NodeID: "tail", MetricsRegistry: tailRegistry}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)

	for nodeID, node := range map[string]*Node{"head": head, "mid": mid, "tail": tail} {
		transport.Register(nodeID, node.backend)
		transport.RegisterNode(nodeID, node)
	}

	mustActivateReplica(t, head, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "mid"},
	})
	mustActivateReplica(t, mid, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleMiddle,
		Peers:        ChainPeers{PredecessorNodeID: "head", SuccessorNodeID: "tail"},
	})
	mustActivateReplica(t, tail, 7, ReplicaAssignment{
		Slot:         7,
		ChainVersion: 1,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "mid"},
	})

	if _, err := head.SubmitPut(ctx, 7, "alpha", "one"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	}

	assertWriteStageCount(t, headRegistry, string(writeStageHeadGetCommitted), string(ReplicaRoleHead), writeStageResultSuccess, 1)
	assertWriteStageCount(t, headRegistry, string(writeStageHeadForwardRPC), string(ReplicaRoleHead), writeStageResultSuccess, 1)
	assertWriteStageCount(t, headRegistry, string(writeStageHeadWaitForCommit), string(ReplicaRoleHead), writeStageResultSuccess, 1)
	assertWriteStageCount(t, midRegistry, string(writeStageHeadForwardRPC), string(ReplicaRoleMiddle), writeStageResultSuccess, 1)
	assertWriteStageCount(t, midRegistry, string(writeStageTailApplyCommit), string(ReplicaRoleMiddle), writeStageResultSuccess, 1)
	assertWriteStageCount(t, midRegistry, string(writeStageCommitUpstreamAcceptRPC), string(ReplicaRoleMiddle), writeStageResultSuccess, 1)
	assertWriteStageCount(t, tailRegistry, string(writeStageTailApplyCommit), string(ReplicaRoleTail), writeStageResultSuccess, 1)
	assertWriteStageCount(t, tailRegistry, string(writeStageCommitUpstreamAcceptRPC), string(ReplicaRoleTail), writeStageResultSuccess, 1)

	before := histogramCountValue(t, headRegistry, "craq_storage_write_stage_seconds", map[string]string{
		"stage":  string(writeStageTailApplyCommit),
		"role":   string(ReplicaRoleHead),
		"result": writeStageResultSuccess,
	})
	if err := head.HandleCommitWrite(ctx, CommitWriteRequest{
		Slot:         7,
		Sequence:     1,
		FromNodeID:   "mid",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("duplicate HandleCommitWrite returned error: %v", err)
	}
	after := histogramCountValue(t, headRegistry, "craq_storage_write_stage_seconds", map[string]string{
		"stage":  string(writeStageTailApplyCommit),
		"role":   string(ReplicaRoleHead),
		"result": writeStageResultSuccess,
	})
	if before != after {
		t.Fatalf("head commit-apply count changed after duplicate ack: before=%d after=%d", before, after)
	}
}

func TestWriteStageMetricsRecordCommitWaitErrors(t *testing.T) {
	ctx := context.Background()
	transport := &blockingWriteTransport{}
	registry := prometheus.NewRegistry()
	node := mustNewNode(t, ctx, Config{
		NodeID:             "head",
		MetricsRegistry:    registry,
		WriteCommitTimeout: time.Nanosecond,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 3, ReplicaAssignment{
		Slot:         3,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
		Peers:        ChainPeers{SuccessorNodeID: "tail"},
	})

	if _, err := node.SubmitPut(ctx, 3, "alpha", "one"); err == nil {
		t.Fatal("SubmitPut unexpectedly succeeded")
	} else {
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want ErrWriteTimeout", err)
		}
	}

	assertWriteStageCount(t, registry, string(writeStageHeadForwardRPC), string(ReplicaRoleHead), writeStageResultSuccess, 1)
	assertWriteStageCount(t, registry, string(writeStageHeadWaitForCommit), string(ReplicaRoleHead), writeStageResultSuccess, 0)
	assertWriteStageCount(t, registry, string(writeStageHeadWaitForCommit), string(ReplicaRoleHead), writeStageResultError, 1)
}

func TestWriteTraceRecorderWritesJSONL(t *testing.T) {
	recorder, err := openWriteTraceRecorder(writeTraceConfig{
		NodeID:     "node-a",
		OutputPath: filepath.Join(t.TempDir(), "trace.jsonl"),
		SampleRate: 1,
	})
	if err != nil {
		t.Fatalf("openWriteTraceRecorder returned error: %v", err)
	}
	defer func() { _ = recorder.Close() }()
	recorder.record(7, 11, 3, ReplicaRoleHead, "head_accepted_write")
	recorder.record(7, 11, 3, ReplicaRoleHead, "waiter_released")
	if err := recorder.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(recorder.file.Name()), filepath.Base(recorder.file.Name())))
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("trace output was empty")
	}
}

func TestWriteTraceSamplerIsDeterministic(t *testing.T) {
	recorder, err := openWriteTraceRecorder(writeTraceConfig{
		NodeID:     "node-a",
		OutputPath: filepath.Join(t.TempDir(), "trace.jsonl"),
		SampleRate: 1024,
	})
	if err != nil {
		t.Fatalf("openWriteTraceRecorder returned error: %v", err)
	}
	defer func() { _ = recorder.Close() }()
	first := recorder.shouldSample(9, 12, 2)
	for i := 0; i < 10; i++ {
		if got := recorder.shouldSample(9, 12, 2); got != first {
			t.Fatalf("shouldSample changed across calls: got=%v want=%v", got, first)
		}
	}
}

func assertWriteStageCount(t *testing.T, registry *prometheus.Registry, stage string, role string, result string, want uint64) {
	t.Helper()
	got := histogramCountValue(t, registry, "craq_storage_write_stage_seconds", map[string]string{
		"stage":  stage,
		"role":   role,
		"result": result,
	})
	if got != want {
		t.Fatalf("write stage count stage=%s role=%s result=%s got=%d want=%d", stage, role, result, got, want)
	}
}

func histogramCountValue(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) uint64 {
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
			if metricLabelsMatch(metric, labels) && metric.Histogram != nil {
				return metric.Histogram.GetSampleCount()
			}
		}
	}
	return 0
}

func assertCounterValue(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string, want float64) {
	t.Helper()
	got := counterValue(t, registry, name, labels)
	if got != want {
		t.Fatalf("counter %s labels=%v got=%f want=%f", name, labels, got, want)
	}
}

func counterValue(t *testing.T, registry *prometheus.Registry, name string, labels map[string]string) float64 {
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
