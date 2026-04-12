package benchmark

import "testing"

func TestBuildFixedSlotKeyPoolsTargetsRequestedSlots(t *testing.T) {
	workload := WorkloadProfile{
		Scenarios: []ScenarioProfile{{
			Name:       "hot-slot",
			Kind:       "put",
			Concurrency: 8,
			Duration:   1,
			ValueBytes: 16,
			FixedSlots: []int{7, 19},
		}},
	}
	pools, err := buildFixedSlotKeyPools(64, workload)
	if err != nil {
		t.Fatalf("buildFixedSlotKeyPools returned error: %v", err)
	}
	if len(pools[7]) == 0 || len(pools[19]) == 0 {
		t.Fatalf("fixed slot key pools missing requested slots: %#v", pools)
	}
}
