package benchmark

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunLocalWithInProcessLauncherWritesAnalysis(t *testing.T) {
	repoRoot := t.TempDir()
	profilePath := filepath.Join(repoRoot, "local_put_scaling.yaml")
	profile := strings.TrimSpace(`
name: local-put-scaling
gcp:
  project: CHANGEME_LOCAL_ONLY_PROFILE_DO_NOT_RUN
  region: us-central1
  ssh_user: ubuntu
  coordinator_machine_type: c4a-standard-8
  client_machine_type: c4a-standard-16
  storage_machine_type: c4a-standard-48-lssd
  coordinator_boot_disk_gib: 100
storage:
  journal_shards: 4
  journal_batch_delay_low: 25us
  journal_batch_delay_high: 100us
  journal_batch_depth_threshold: 4
cluster:
  slot_count: 16
  replication_factor: 3
  storage_node_count: 3
  heartbeat_interval: 50ms
  liveness_interval: 50ms
  suspect_after: 2s
  dead_after: 4s
  activation_interval: 10ms
  rpc_deadline: 500ms
  reconfiguration:
    max_changed_chains: 8
workload:
  preload_keys: 8
  value_bytes: 32
  request_timeout: 1s
  interval: 100ms
  per_scenario_pause: 100ms
  scenarios:
    - name: put-only-c4
      kind: put
      concurrency: 4
      warmup: 100ms
      duration: 300ms
      value_bytes: 32
telemetry:
  probe_interval: 1s
  command_interval: 1s
  extra_run_duration: 1s
  pprof_cpu_duration: 0s
artifacts:
  root_dir: artifacts/benchmarks
`) + "\n"
	if err := os.WriteFile(profilePath, []byte(profile), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runDir, err := runLocalWithLauncher(ctx, RunLocalOptions{
		ProfilePath: profilePath,
		RunName:     "run-local-test",
		RepoRoot:    repoRoot,
	}, inProcessLocalSmokeLauncher{})
	if err != nil {
		t.Fatalf("runLocalWithLauncher returned error: %v", err)
	}

	state, err := ReadRunState(filepath.Join(runDir, RunStateFileName))
	if err != nil {
		t.Fatalf("ReadRunState returned error: %v", err)
	}
	if got, want := state.Status, "local_run_ok"; got != want {
		t.Fatalf("state.Status = %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(runDir, "artifacts", "client", "loadgen-report.json")); err != nil {
		t.Fatalf("loadgen-report.json stat returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "analysis", "summary.json")); err != nil {
		t.Fatalf("summary.json stat returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "analysis", "write_pipeline_summary.json")); err != nil {
		t.Fatalf("write_pipeline_summary.json stat returned error: %v", err)
	}
}
