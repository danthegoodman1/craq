package benchmark

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderTerraformVarsGCP(t *testing.T) {
	tempDir := t.TempDir()
	pubKeyPath := filepath.Join(tempDir, "bench.pub")
	if err := os.WriteFile(pubKeyPath, []byte("ssh-ed25519 AAAATEST bench\n"), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	state := RunState{
		RunID:           "run-123",
		Region:          "us-central1",
		Topology:        "single-zone",
		ClientPlacement: "same-zone",
		SSHPublicKey:    pubKeyPath,
		Profile: Profile{
			GCP: GCPProfile{
				Project:                "example-project",
				OperatorCIDRs:          []string{"10.0.0.0/24"},
				SSHUser:                "ubuntu",
				CoordinatorMachineType: "c4a-standard-8",
				ClientMachineType:      "c4a-standard-16",
				StorageMachineType:     "c4a-standard-48-lssd",
				CoordinatorBootDiskGiB: 100,
			},
		},
	}

	vars, err := renderTerraformVars(state, tempDir)
	if err != nil {
		t.Fatalf("renderTerraformVars returned error: %v", err)
	}
	if got, want := vars["gcp_project"], "example-project"; got != want {
		t.Fatalf("gcp_project = %v, want %v", got, want)
	}
	if got, want := vars["storage_machine_type"], "c4a-standard-48-lssd"; got != want {
		t.Fatalf("storage_machine_type = %v, want %v", got, want)
	}
	if got, want := vars["ssh_public_key"], "ssh-ed25519 AAAATEST bench"; got != want {
		t.Fatalf("ssh_public_key = %v, want %v", got, want)
	}
}

func TestDecodeTerraformOutputsGCP(t *testing.T) {
	data, err := json.Marshal(map[string]any{
		"region":           map[string]any{"value": "us-central1"},
		"primary_zone":     map[string]any{"value": "us-central1-a"},
		"public_client_ip": map[string]any{"value": "34.1.2.3"},
		"private_ips": map[string]any{"value": map[string]string{
			"client":      "10.42.0.10",
			"coordinator": "10.42.0.11",
			"storage-a":   "10.42.0.12",
			"storage-b":   "10.42.1.12",
			"storage-c":   "10.42.2.12",
		}},
		"instance_names": map[string]any{"value": map[string]string{
			"client":      "client",
			"coordinator": "coordinator",
			"storage-a":   "storage-a",
			"storage-b":   "storage-b",
			"storage-c":   "storage-c",
		}},
	})
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}

	outputs, err := decodeTerraformOutputs(data)
	if err != nil {
		t.Fatalf("decodeTerraformOutputs returned error: %v", err)
	}
	if got, want := outputs.PrimaryZone, "us-central1-a"; got != want {
		t.Fatalf("PrimaryZone = %q, want %q", got, want)
	}
	if got, want := outputs.InstanceNames["storage-c"], "storage-c"; got != want {
		t.Fatalf("InstanceNames[storage-c] = %q, want %q", got, want)
	}
}

func TestNormalizeZoneTerminology(t *testing.T) {
	if got := NormalizeTopology("multi-az"); got != "multi-zone" {
		t.Fatalf("NormalizeTopology(multi-az) = %q, want multi-zone", got)
	}
	if got := NormalizeClientPlacement("remote-az"); got != "remote-zone" {
		t.Fatalf("NormalizeClientPlacement(remote-az) = %q, want remote-zone", got)
	}
}

func TestRunBenchmarkFailsWhenProjectUnset(t *testing.T) {
	repoRoot := filepath.Join("..")
	profilePath := filepath.Join(t.TempDir(), "gcp-placeholder.yaml")
	profileData := strings.TrimSpace(`
name: gcp-c4a-steady
gcp:
  project: CHANGEME_GCP_PROJECT
  region: us-central1
  ssh_user: ubuntu
  coordinator_machine_type: c4a-standard-8
  client_machine_type: c4a-standard-16
  storage_machine_type: c4a-standard-48-lssd
  coordinator_boot_disk_gib: 100
cluster:
  slot_count: 1024
  replication_factor: 3
  storage_node_count: 3
workload:
  scenarios:
    - name: get-only-c64
      kind: get
      concurrency: 64
      duration: 1s
telemetry:
  extra_run_duration: 1s
`) + "\n"
	if err := os.WriteFile(profilePath, []byte(profileData), 0o644); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	_, err := RunBenchmark(context.Background(), RunOptions{
		ProfilePath: profilePath,
		RepoRoot:    repoRoot,
		RunName:     "guardrail",
	})
	if err == nil {
		t.Fatal("RunBenchmark unexpectedly succeeded")
	}
	if got := err.Error(); got != "gcp.project must be replaced with a real GCP project before running a benchmark" {
		t.Fatalf("RunBenchmark error = %q", got)
	}
}
