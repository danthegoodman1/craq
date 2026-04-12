package benchmark

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

func TestAnalyzeRunGeneratesSummaryAndHTML(t *testing.T) {
	runDir := t.TempDir()
	artifactsDir := filepath.Join(runDir, "artifacts")
	if err := os.MkdirAll(filepath.Join(artifactsDir, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(artifactsDir, "coordinator"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, node := range []string{"storage-a", "storage-b", "storage-c"} {
		if err := os.MkdirAll(filepath.Join(artifactsDir, node), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(artifactsDir, node, "storage_floor"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	state := RunState{
		RunID:           "run-123",
		RunName:         "test",
		GitSHA:          "abc123",
		Topology:        "single-zone",
		ClientPlacement: "same-zone",
	}
	if err := SaveJSON(filepath.Join(runDir, RunStateFileName), state); err != nil {
		t.Fatal(err)
	}
	report := LoadGenReport{
		RunID: "run-123",
		Scenarios: []ScenarioReport{{
			Name:        "put-only-c32",
			Kind:        "put",
			Concurrency: 32,
			StartedAt:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			FinishedAt:  time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC),
			P50Millis:   12.5,
			P95Millis:   20.4,
			P99Millis:   27.9,
			Throughput:  321.5,
			TotalOps:    10,
			SuccessOps:  10,
		}},
	}
	if err := SaveJSON(filepath.Join(artifactsDir, "client", "loadgen-report.json"), report); err != nil {
		t.Fatal(err)
	}
	metrics := "chainrep_coordserver_pending_work 0\nchainrep_storage_in_flight_client_writes 1\n"
	if err := os.WriteFile(filepath.Join(artifactsDir, "coordinator", "metrics.prom"), []byte(metrics), 0o644); err != nil {
		t.Fatal(err)
	}
	probe := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","cpu":{"user":10,"nice":0,"system":5,"idle":85,"iowait":0,"irq":0,"softirq":0,"steal":0},"memory":{"total_bytes":1,"free_bytes":1,"available_bytes":1,"buffers_bytes":1,"cached_bytes":1},"network":{"eth0":{"rx_bytes":1000,"tx_bytes":2000}},"disks":{"nvme0n1":{"reads_completed":1,"writes_completed":1,"read_sectors":100,"write_sectors":200}}}`,
		`{"timestamp":"2026-01-01T00:00:01Z","cpu":{"user":20,"nice":0,"system":10,"idle":90,"iowait":0,"irq":0,"softirq":0,"steal":0},"memory":{"total_bytes":1,"free_bytes":1,"available_bytes":1,"buffers_bytes":1,"cached_bytes":1},"network":{"eth0":{"rx_bytes":2000,"tx_bytes":4000}},"disks":{"nvme0n1":{"reads_completed":2,"writes_completed":2,"read_sectors":200,"write_sectors":400}}}`,
	}, "\n")
	for _, node := range []string{"coordinator", "client", "storage-a", "storage-b", "storage-c"} {
		if err := os.WriteFile(filepath.Join(artifactsDir, node, "probe.jsonl"), []byte(probe), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, node := range []string{"storage-a", "storage-b", "storage-c"} {
		if err := os.WriteFile(filepath.Join(artifactsDir, node, "metrics.prom"), []byte("chainrep_storage_in_flight_client_writes 2\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeStart := filepath.Join(artifactsDir, "client", "metric-snapshots", "put-only-c32-start", "storage-a.prom")
	writeEnd := filepath.Join(artifactsDir, "client", "metric-snapshots", "put-only-c32-end", "storage-a.prom")
	if err := writeMetricSnapshot(t, writeStart, 0, 0, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := writeMetricSnapshot(t, writeEnd, 2, 0.02, 5, 0.11, 5, 0.09); err != nil {
		t.Fatal(err)
	}
	traceLine := `{"at":"2026-01-01T00:00:00.000Z","node_id":"a","slot":1,"sequence":1,"chain_version":1,"role":"head","stage":"head_accepted_write"}
{"at":"2026-01-01T00:00:00.001Z","node_id":"a","slot":1,"sequence":1,"chain_version":1,"role":"head","stage":"head_forward_accepted"}
{"at":"2026-01-01T00:00:00.002Z","node_id":"c","slot":1,"sequence":1,"chain_version":1,"role":"tail","stage":"tail_commit_intent_queued"}
{"at":"2026-01-01T00:00:00.003Z","node_id":"c","slot":1,"sequence":1,"chain_version":1,"role":"tail","stage":"tail_flush_start"}
{"at":"2026-01-01T00:00:00.004Z","node_id":"c","slot":1,"sequence":1,"chain_version":1,"role":"tail","stage":"tail_flush_end"}
{"at":"2026-01-01T00:00:00.005Z","node_id":"b","slot":1,"sequence":1,"chain_version":1,"role":"middle","stage":"middle_commit_accept_received"}
{"at":"2026-01-01T00:00:00.006Z","node_id":"b","slot":1,"sequence":1,"chain_version":1,"role":"middle","stage":"middle_commit_intent_queued"}
{"at":"2026-01-01T00:00:00.007Z","node_id":"b","slot":1,"sequence":1,"chain_version":1,"role":"middle","stage":"middle_flush_start"}
{"at":"2026-01-01T00:00:00.008Z","node_id":"b","slot":1,"sequence":1,"chain_version":1,"role":"middle","stage":"middle_flush_end"}
{"at":"2026-01-01T00:00:00.009Z","node_id":"a","slot":1,"sequence":1,"chain_version":1,"role":"head","stage":"head_commit_accept_received"}
{"at":"2026-01-01T00:00:00.010Z","node_id":"a","slot":1,"sequence":1,"chain_version":1,"role":"head","stage":"head_commit_intent_queued"}
{"at":"2026-01-01T00:00:00.011Z","node_id":"a","slot":1,"sequence":1,"chain_version":1,"role":"head","stage":"head_flush_start"}
{"at":"2026-01-01T00:00:00.012Z","node_id":"a","slot":1,"sequence":1,"chain_version":1,"role":"head","stage":"head_flush_end"}
{"at":"2026-01-01T00:00:00.013Z","node_id":"a","slot":1,"sequence":1,"chain_version":1,"role":"head","stage":"waiter_released"}
`
	if err := os.WriteFile(filepath.Join(artifactsDir, "storage-a", "write-pipeline-trace.jsonl"), []byte(traceLine), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, node := range []string{"storage-b", "storage-c"} {
		if err := os.WriteFile(filepath.Join(artifactsDir, node, "write-pipeline-trace.jsonl"), nil, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	timeoutArtifact := `{"at":"2026-01-01T00:00:00.100Z","node_id":"storage-b","slot":361,"sequence":203,"error":"storage write wait timed out or was canceled: context deadline exceeded","slot_state":{"next_sequence":203,"highest_committed_sequence":202,"materialized_committed_sequence":202,"journal_durable_high_water":202,"highest_upstream_confirmed_sequence":202,"commit_effect_in_flight":true,"commit_effect_sequence":203,"upstream_commit_in_flight":false,"last_accept_commit_received":{"sequence":203},"last_reconciled_from_journal":{"sequence":202},"last_applied_locally":{"sequence":202},"last_waiter_released":{"sequence":202}},"session_state":[{"kind":"commit","target":"storage-a","advertised_credit":1,"local_credit":0,"local_spool_depth":4,"last_enqueued_sequence":203,"last_transmitted_sequence":202,"last_acked_sequence":202,"blocked_since":"2026-01-01T00:00:00.090Z"}],"journal_state":{"durable_committed_high_water":202}}
{"at":"2026-01-01T00:00:02.100Z","node_id":"storage-b","slot":361,"sequence":204,"error":"storage write wait timed out or was canceled: context canceled","slot_state":{"next_sequence":203,"highest_committed_sequence":202,"materialized_committed_sequence":202,"journal_durable_high_water":202,"highest_upstream_confirmed_sequence":202,"commit_effect_in_flight":true,"commit_effect_sequence":203,"upstream_commit_in_flight":false,"last_accept_commit_received":{"sequence":203},"last_reconciled_from_journal":{"sequence":202},"last_applied_locally":{"sequence":202},"last_waiter_released":{"sequence":202}},"session_state":[{"kind":"commit","target":"storage-a","advertised_credit":1,"local_credit":0,"local_spool_depth":5,"last_enqueued_sequence":204,"last_transmitted_sequence":202,"last_acked_sequence":202,"blocked_since":"2026-01-01T00:00:00.090Z"}],"journal_state":{"durable_committed_high_water":202}}
`
	if err := os.WriteFile(filepath.Join(artifactsDir, "storage-b", "write-timeout-artifacts.jsonl"), []byte(timeoutArtifact), 0o644); err != nil {
		t.Fatal(err)
	}
	floor := DurabilityBenchReport{
		Label: "current_path",
		Tests: []DurabilityBenchmarkStat{
			{Name: "file_fsync_256b", Count: 10, P50Millis: 0.4},
			{Name: "badger_sync_put_256b", Count: 10, P50Millis: 0.7},
			{Name: "badger_batch_apply_256b", Count: 10, P50Millis: 0.8},
		},
	}
	control := DurabilityBenchReport{
		Label: "root_disk_control",
		Tests: []DurabilityBenchmarkStat{
			{Name: "file_fsync_256b", Count: 10, P50Millis: 0.5},
			{Name: "badger_sync_put_256b", Count: 10, P50Millis: 0.9},
			{Name: "badger_batch_apply_256b", Count: 10, P50Millis: 1.0},
		},
	}
	for _, node := range []string{"storage-a", "storage-b", "storage-c"} {
		if err := SaveJSON(filepath.Join(artifactsDir, node, "storage_floor", "current_path.json"), floor); err != nil {
			t.Fatal(err)
		}
		if err := SaveJSON(filepath.Join(artifactsDir, node, "storage_floor", "root_disk_control.json"), control); err != nil {
			t.Fatal(err)
		}
	}

	summary, err := AnalyzeRun(runDir)
	if err != nil {
		t.Fatalf("AnalyzeRun returned error: %v", err)
	}
	if len(summary.Scenarios) != 1 {
		t.Fatalf("len(summary.Scenarios) = %d, want 1", len(summary.Scenarios))
	}
	htmlData, err := os.ReadFile(filepath.Join(runDir, "analysis", "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(htmlData), "put-only-c32") {
		t.Fatalf("analysis html missing scenario name: %s", string(htmlData))
	}
	jsonData, err := os.ReadFile(filepath.Join(runDir, "analysis", "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(jsonData), `"run_id": "run-123"`) {
		t.Fatalf("analysis summary missing run id: %s", string(jsonData))
	}
	if got := summary.System["client"].CPUUtilization[0].Timestamp; got != (time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)) {
		t.Fatalf("first client cpu timestamp = %s", got)
	}
	if summary.WriteBudget["put-only-c32"].DominantStage != "head_wait_for_commit" {
		t.Fatalf("dominant stage = %q, want head_wait_for_commit", summary.WriteBudget["put-only-c32"].DominantStage)
	}
	budgetData, err := os.ReadFile(filepath.Join(runDir, "analysis", "write_latency_budget.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(budgetData), `"stage": "head_wait_for_commit"`) {
		t.Fatalf("write latency budget missing dominant stage: %s", string(budgetData))
	}
	if _, err := os.Stat(filepath.Join(runDir, "analysis", "storage_floor_summary.json")); err != nil {
		t.Fatalf("storage_floor_summary.json stat error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "analysis", "write_pipeline_summary.json")); err != nil {
		t.Fatalf("write_pipeline_summary.json stat error: %v", err)
	}
	if summary.StorageFloor.Aggregates["badger_sync_put_256b"].CurrentPathP50Millis == 0 {
		t.Fatalf("storage floor summary missing badger_sync_put_256b aggregate: %#v", summary.StorageFloor.Aggregates)
	}
	if summary.WritePipeline["put-only-c32"].EndToEndMeanMillis == 0 {
		t.Fatalf("write pipeline summary missing end_to_end mean: %#v", summary.WritePipeline["put-only-c32"])
	}
	if len(summary.TimeoutRoots) != 1 {
		t.Fatalf("len(summary.TimeoutRoots) = %d, want 1", len(summary.TimeoutRoots))
	}
	if summary.TimeoutRoots[0].LikelyStage != "sender_spool_or_credit" {
		t.Fatalf("timeout root likely stage = %q, want sender_spool_or_credit", summary.TimeoutRoots[0].LikelyStage)
	}
	if _, err := os.Stat(filepath.Join(runDir, "analysis", "timeout_root_causes.json")); err != nil {
		t.Fatalf("timeout_root_causes.json stat error: %v", err)
	}
}

func TestRunDurabilityBenchWritesStableJSON(t *testing.T) {
	outputPath := filepath.Join(t.TempDir(), "durability.json")
	if err := RunDurabilityBench(context.Background(), DurabilityBenchConfig{
		Path:       filepath.Join(t.TempDir(), "bench-data"),
		Label:      "test-path",
		OutputPath: outputPath,
		Count:      5,
	}); err != nil {
		t.Fatalf("RunDurabilityBench returned error: %v", err)
	}
	var report DurabilityBenchReport
	if err := LoadJSON(outputPath, &report); err != nil {
		t.Fatalf("LoadJSON returned error: %v", err)
	}
	if got, want := report.Label, "test-path"; got != want {
		t.Fatalf("report.Label = %q, want %q", got, want)
	}
	if len(report.Tests) != 3 {
		t.Fatalf("len(report.Tests) = %d, want 3", len(report.Tests))
	}
}

func writeMetricSnapshot(t *testing.T, path string, headCount uint64, headSum float64, waitCount uint64, waitSum float64, putCount uint64, putSum float64) error {
	t.Helper()
	registry := prometheus.NewRegistry()
	stageHist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "craq_storage_write_stage_seconds",
		Help: "stage metrics",
	}, []string{"stage", "role", "result"})
	grpcHist := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "craq_grpc_request_duration_seconds",
		Help: "grpc metrics",
	}, []string{"component", "method"})
	registry.MustRegister(stageHist, grpcHist)
	observeHistogram(stageHist.WithLabelValues("head_get_committed", "head", "success"), headCount, headSum)
	observeHistogram(stageHist.WithLabelValues("head_wait_for_commit", "head", "success"), waitCount, waitSum)
	observeHistogram(grpcHist.WithLabelValues("storage", "/craq.v1.StorageService/Put"), putCount, putSum)
	return writeRegistryMetrics(path, registry)
}

func observeHistogram(observer prometheus.Observer, count uint64, sum float64) {
	if count == 0 {
		return
	}
	value := sum / float64(count)
	for i := uint64(0); i < count; i++ {
		observer.Observe(value)
	}
}

func writeRegistryMetrics(path string, registry *prometheus.Registry) error {
	families, err := registry.Gather()
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	for _, family := range families {
		if _, err := expfmt.MetricFamilyToText(&buf, family); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, buf.Bytes(), 0o644)
}
