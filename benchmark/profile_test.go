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
	if got, want := profile.GCP.StorageLayout, "single_nvme_ext4"; got != want {
		t.Fatalf("StorageLayout = %q, want %q", got, want)
	}
	if profile.Cluster.SlotCount != 1024 {
		t.Fatalf("SlotCount = %d, want 1024", profile.Cluster.SlotCount)
	}
	if len(profile.Workload.Scenarios) != 3 {
		t.Fatalf("len(Scenarios) = %d, want 3", len(profile.Workload.Scenarios))
	}
}

func TestLoadDiagnosticProfilesValidateStorageLayouts(t *testing.T) {
	tests := map[string]string{
		"../profiles/bench/gcp_c4a_diag_raid0_ext4.yaml":       "raid0_ext4",
		"../profiles/bench/gcp_c4a_diag_single_nvme_ext4.yaml": "single_nvme_ext4",
		"../profiles/bench/gcp_c4a_diag_single_nvme_xfs.yaml":  "single_nvme_xfs",
	}
	for path, wantLayout := range tests {
		profile, err := LoadProfile(path)
		if err != nil {
			t.Fatalf("LoadProfile(%s) returned error: %v", path, err)
		}
		if got := profile.GCP.StorageLayout; got != wantLayout {
			t.Fatalf("LoadProfile(%s) StorageLayout = %q, want %q", path, got, wantLayout)
		}
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
