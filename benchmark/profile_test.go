package benchmark

import "testing"

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
