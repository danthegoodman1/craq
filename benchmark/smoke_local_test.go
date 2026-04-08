package benchmark

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSmokeLocalWithInProcessLauncherWritesArtifacts(t *testing.T) {
	repoRoot := t.TempDir()
	profilePath := filepath.Join(repoRoot, "local_smoke.yaml")
	profile := strings.TrimSpace(`
name: local-smoke
gcp:
  project: CHANGEME_LOCAL_ONLY_PROFILE_DO_NOT_RUN
  region: us-central1
  ssh_user: ubuntu
  coordinator_machine_type: c4a-standard-8
  client_machine_type: c4a-standard-16
  storage_machine_type: c4a-standard-48-lssd
  coordinator_boot_disk_gib: 100
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
  preload_keys: 1
  value_bytes: 32
  interval: 100ms
  per_scenario_pause: 100ms
  scenarios:
    - name: smoke
      kind: get
      concurrency: 1
      duration: 1s
telemetry:
  probe_interval: 1s
  command_interval: 1s
  extra_run_duration: 1s
artifacts:
  root_dir: artifacts/benchmarks
`) + "\n"
	if err := os.WriteFile(profilePath, []byte(profile), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runDir, err := smokeLocalWithLauncher(ctx, SmokeLocalOptions{
		ProfilePath: profilePath,
		RunName:     "smoke-local-test",
		RepoRoot:    repoRoot,
	}, inProcessLocalSmokeLauncher{})
	if err != nil {
		t.Fatalf("smokeLocalWithLauncher returned error: %v", err)
	}

	state, err := ReadRunState(filepath.Join(runDir, RunStateFileName))
	if err != nil {
		t.Fatalf("ReadRunState returned error: %v", err)
	}
	if got, want := state.Status, "local_smoke_ok"; got != want {
		t.Fatalf("state.Status = %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(runDir, "artifacts", "client", "smoke-result.json")); err != nil {
		t.Fatalf("smoke-result.json stat returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "artifacts", "coordinator", "routing-progress.jsonl")); err != nil {
		t.Fatalf("routing-progress.jsonl stat returned error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "artifacts", ArtifactBundleFileName)); err != nil {
		t.Fatalf("artifact bundle stat returned error: %v", err)
	}
}
