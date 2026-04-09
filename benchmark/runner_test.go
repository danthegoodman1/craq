package benchmark

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
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
		"node_zones": map[string]any{"value": map[string]string{
			"client":      "us-central1-a",
			"coordinator": "us-central1-a",
			"storage-a":   "us-central1-a",
			"storage-b":   "us-central1-b",
			"storage-c":   "us-central1-c",
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
	if got, want := outputs.NodeZones["storage-b"], "us-central1-b"; got != want {
		t.Fatalf("NodeZones[storage-b] = %q, want %q", got, want)
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

func TestCoordinatorProcessConfigIncludesReconfigurationBudget(t *testing.T) {
	cfg := CoordinatorProcessConfig{
		ManifestPath: "/etc/craq-bench/manifest.json",
		DataDir:      "/var/lib/craq-bench/coordinator",
		Reconfiguration: coordinator.ReconfigurationPolicy{
			MaxChangedChains: 32,
		},
	}

	data := mustJSON(cfg)

	var decoded CoordinatorProcessConfig
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("json.Unmarshal returned error: %v", err)
	}
	if got, want := decoded.Reconfiguration.MaxChangedChains, 32; got != want {
		t.Fatalf("decoded reconfiguration max_changed_chains = %d, want %d", got, want)
	}
}

func TestDecodeRoutingProgress(t *testing.T) {
	data := []byte(`{
  "active_peer_refresh_count": 2,
  "routing_snapshot": {
    "Version": 17,
    "SlotCount": 3,
    "Slots": [
      {"Slot": 0, "Readable": true, "Writable": true},
      {"Slot": 1, "Readable": true, "Writable": false},
      {"Slot": 2, "Readable": false, "Writable": false}
    ]
  },
  "liveness": {
    "a": {"State": "healthy"},
    "b": {"State": "suspect"},
    "c": {"State": "dead"}
  },
  "pending": {
    "1": {"Slot": 1, "NodeID": "c", "Kind": "ready", "SlotVersion": 3, "Epoch": 0, "CommandID": "cmd-1"}
  },
  "current": {
    "Version": 17,
    "Cluster": {
      "ReplicationFactor": 3,
      "Chains": [
        {"Slot": 0, "Replicas": [{"NodeID": "a", "State": "active"}, {"NodeID": "b", "State": "active"}, {"NodeID": "c", "State": "active"}]},
        {"Slot": 1, "Replicas": [{"NodeID": "a", "State": "active"}, {"NodeID": "b", "State": "joining"}]},
        {"Slot": 2, "Replicas": [{"NodeID": "a", "State": "active"}]}
      ]
    },
    "Outbox": [{"ID":"x"}]
  }
}`)

	progress, err := decodeRoutingProgress(data)
	if err != nil {
		t.Fatalf("decodeRoutingProgress returned error: %v", err)
	}
	if got, want := progress.version, uint64(17); got != want {
		t.Fatalf("progress.version = %d, want %d", got, want)
	}
	if got, want := progress.slotCount, 3; got != want {
		t.Fatalf("progress.slotCount = %d, want %d", got, want)
	}
	if got, want := progress.readableSlots, 2; got != want {
		t.Fatalf("progress.readableSlots = %d, want %d", got, want)
	}
	if got, want := progress.writableSlots, 1; got != want {
		t.Fatalf("progress.writableSlots = %d, want %d", got, want)
	}
	if got, want := progress.pendingSlots, 1; got != want {
		t.Fatalf("progress.pendingSlots = %d, want %d", got, want)
	}
	if got, want := progress.outboxEntries, 1; got != want {
		t.Fatalf("progress.outboxEntries = %d, want %d", got, want)
	}
	if got, want := progress.activePeerRefreshes, 2; got != want {
		t.Fatalf("progress.activePeerRefreshes = %d, want %d", got, want)
	}
	if got, want := progress.settledSlots, 1; got != want {
		t.Fatalf("progress.settledSlots = %d, want %d", got, want)
	}
	if got, want := progress.healthyNodes, 1; got != want {
		t.Fatalf("progress.healthyNodes = %d, want %d", got, want)
	}
	if got, want := progress.suspectNodes, 1; got != want {
		t.Fatalf("progress.suspectNodes = %d, want %d", got, want)
	}
	if got, want := progress.deadNodes, 1; got != want {
		t.Fatalf("progress.deadNodes = %d, want %d", got, want)
	}
}

func TestRoutingReadyOverallTimeoutScalesWithClusterSize(t *testing.T) {
	profile := Profile{
		Cluster: ClusterProfile{
			SlotCount:         1024,
			ReplicationFactor: 3,
		},
	}
	if got, want := routingReadyOverallTimeout(profile), 25*time.Minute+36*time.Second; got != want {
		t.Fatalf("routingReadyOverallTimeout = %s, want %s", got, want)
	}
}

func TestRoutingReadyStallTimeoutHonorsRPCDeadline(t *testing.T) {
	profile := Profile{
		Cluster: ClusterProfile{
			RPCDeadline: 15 * time.Second,
		},
	}
	if got, want := routingReadyStallTimeout(profile), 90*time.Second; got != want {
		t.Fatalf("routingReadyStallTimeout = %s, want %s", got, want)
	}
}

func TestBenchmarkCoordinatorLivenessPolicyUsesTolerantFlapDetection(t *testing.T) {
	policy := benchmarkCoordinatorLivenessPolicy(ClusterProfile{
		SuspectAfter: 3 * time.Second,
		DeadAfter:    6 * time.Second,
	})

	if got, want := policy.SuspectAfter, 3*time.Second; got != want {
		t.Fatalf("policy.SuspectAfter = %s, want %s", got, want)
	}
	if got, want := policy.DeadAfter, 6*time.Second; got != want {
		t.Fatalf("policy.DeadAfter = %s, want %s", got, want)
	}
	if got, want := policy.FlapWindow, 24*time.Second; got != want {
		t.Fatalf("policy.FlapWindow = %s, want %s", got, want)
	}
	if got, want := policy.FlapThreshold, 8; got != want {
		t.Fatalf("policy.FlapThreshold = %d, want %d", got, want)
	}
}

func TestRoutingProgressReadyRequiresSettledCluster(t *testing.T) {
	progress := routingProgress{
		slotCount:           1024,
		readableSlots:       1024,
		writableSlots:       1024,
		settledSlots:        992,
		pendingSlots:        0,
		outboxEntries:       0,
		activePeerRefreshes: 0,
		healthyNodes:        3,
	}
	if routingProgressReady(progress) {
		t.Fatal("routingProgressReady unexpectedly accepted partially settled routing")
	}

	progress.settledSlots = 1024
	progress.pendingSlots = 12
	if routingProgressReady(progress) {
		t.Fatal("routingProgressReady unexpectedly accepted routing with pending work")
	}

	progress.pendingSlots = 0
	progress.outboxEntries = 4
	if routingProgressReady(progress) {
		t.Fatal("routingProgressReady unexpectedly accepted routing with outbox work")
	}

	progress.outboxEntries = 0
	progress.deadNodes = 1
	if routingProgressReady(progress) {
		t.Fatal("routingProgressReady unexpectedly accepted routing with dead nodes")
	}

	progress.deadNodes = 0
	if !routingProgressReady(progress) {
		t.Fatal("routingProgressReady rejected fully settled routing")
	}
}
