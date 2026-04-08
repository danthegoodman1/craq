package benchmark

import (
	"strings"
	"testing"
)

func TestLoadProfileDefaultsAndValidate(t *testing.T) {
	profile, err := LoadProfile("../profiles/bench/gcp_c4a_steady.yaml")
	if err != nil {
		t.Fatalf("LoadProfile returned error: %v", err)
	}
	if profile.GCP.Project == "" {
		t.Fatal("Project must not be empty")
	}
	if profile.GCP.ClientMachineType != "c4a-standard-16" {
		t.Fatalf("ClientMachineType = %q, want c4a-standard-16", profile.GCP.ClientMachineType)
	}
	if profile.Cluster.SlotCount != 1024 {
		t.Fatalf("SlotCount = %d, want 1024", profile.Cluster.SlotCount)
	}
	if len(profile.Workload.Scenarios) != 3 {
		t.Fatalf("len(Scenarios) = %d, want 3", len(profile.Workload.Scenarios))
	}
}

func TestLoadLocalSmokeProfileDefaultsAndValidate(t *testing.T) {
	profile, err := LoadProfile("../profiles/bench/local_smoke.yaml")
	if err != nil {
		t.Fatalf("LoadProfile returned error: %v", err)
	}
	if got, want := profile.Cluster.SlotCount, 256; got != want {
		t.Fatalf("SlotCount = %d, want %d", got, want)
	}
	if got, want := profile.Cluster.Reconfiguration.MaxChangedChains, 32; got != want {
		t.Fatalf("MaxChangedChains = %d, want %d", got, want)
	}
	if got := profile.GCP.Project; got == "" || !strings.Contains(got, "CHANGEME") {
		t.Fatalf("local smoke project = %q, want CHANGEME placeholder", got)
	}
}

func TestLoadLocalSmokeCloudShapeProfileDefaultsAndValidate(t *testing.T) {
	profile, err := LoadProfile("../profiles/bench/local_smoke_cloud_shape.yaml")
	if err != nil {
		t.Fatalf("LoadProfile returned error: %v", err)
	}
	if got, want := profile.Cluster.SlotCount, 1024; got != want {
		t.Fatalf("SlotCount = %d, want %d", got, want)
	}
	if got, want := profile.Cluster.Reconfiguration.MaxChangedChains, 32; got != want {
		t.Fatalf("MaxChangedChains = %d, want %d", got, want)
	}
	if got, want := profile.Cluster.HeartbeatInterval.String(), "1s"; got != want {
		t.Fatalf("HeartbeatInterval = %q, want %q", got, want)
	}
	if got := profile.GCP.Project; got == "" || !strings.Contains(got, "CHANGEME") {
		t.Fatalf("local cloud-shape project = %q, want CHANGEME placeholder", got)
	}
}
