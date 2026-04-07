package benchmark

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
			Name:        "get-only-c64",
			Kind:        "get",
			Concurrency: 64,
			P50Millis:   1.2,
			P95Millis:   2.4,
			P99Millis:   3.9,
			Throughput:  1234.5,
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
	if !strings.Contains(string(htmlData), "get-only-c64") {
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
}
