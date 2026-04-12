package benchmark

import (
	"bytes"
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
