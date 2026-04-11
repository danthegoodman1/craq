package benchmark

import (
	"testing"
	"time"
)

func TestSummarizeRoutingProgressRecords(t *testing.T) {
	start := time.Unix(1, 0).UTC()
	records := []routingProgressRecord{
		{
			Time: start,
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:           8,
				WritableSlots:       0,
				ReadableSlots:       0,
				PendingSlots:        8,
				OutboxEntries:       3,
				ActivePeerRefreshes: 0,
			},
		},
		{
			Time: start.Add(2 * time.Second),
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:           8,
				WritableSlots:       2,
				ReadableSlots:       4,
				PendingSlots:        6,
				OutboxEntries:       5,
				ActivePeerRefreshes: 1,
			},
		},
		{
			Time: start.Add(5 * time.Second),
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:           8,
				WritableSlots:       8,
				ReadableSlots:       8,
				PendingSlots:        0,
				OutboxEntries:       1,
				ActivePeerRefreshes: 2,
			},
		},
	}

	summary := summarizeRoutingProgressRecords(records)
	if got, want := summary.Samples, 3; got != want {
		t.Fatalf("Samples = %d, want %d", got, want)
	}
	if got, want := summary.TotalDuration, 5*time.Second; got != want {
		t.Fatalf("TotalDuration = %s, want %s", got, want)
	}
	if got, want := summary.TimeToFirstReadable, 2*time.Second; got != want {
		t.Fatalf("TimeToFirstReadable = %s, want %s", got, want)
	}
	if got, want := summary.TimeToAllWritable, 5*time.Second; got != want {
		t.Fatalf("TimeToAllWritable = %s, want %s", got, want)
	}
	if got, want := summary.MaxPendingSlots, 8; got != want {
		t.Fatalf("MaxPendingSlots = %d, want %d", got, want)
	}
	if got, want := summary.MaxOutboxEntries, 5; got != want {
		t.Fatalf("MaxOutboxEntries = %d, want %d", got, want)
	}
	if got, want := summary.MaxActivePeerRefreshes, 2; got != want {
		t.Fatalf("MaxActivePeerRefreshes = %d, want %d", got, want)
	}
}

func TestAssertCloudShapeRoutingProgressHealthyAllowsInitialAllPendingButRejectsReset(t *testing.T) {
	start := time.Unix(2, 0).UTC()

	initialFanout := []routingProgressRecord{
		{
			Time: start,
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:     1024,
				WritableSlots: 0,
				ReadableSlots: 0,
				PendingSlots:  0,
			},
		},
		{
			Time: start.Add(3 * time.Second),
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:     1024,
				WritableSlots: 32,
				ReadableSlots: 428,
				PendingSlots:  1024,
			},
		},
		{
			Time: start.Add(6 * time.Second),
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:           1024,
				WritableSlots:       256,
				ReadableSlots:       700,
				SettledSlots:        200,
				PendingSlots:        800,
				HealthyNodes:        3,
				ActivePeerRefreshes: 8,
			},
		},
		{
			Time: start.Add(9 * time.Second),
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:           1024,
				WritableSlots:       1024,
				ReadableSlots:       1024,
				SettledSlots:        1024,
				PendingSlots:        0,
				HealthyNodes:        3,
				ActivePeerRefreshes: 0,
			},
		},
	}
	if err := assertCloudShapeRoutingProgressHealthy(initialFanout, 1024, 30*time.Second); err != nil {
		t.Fatalf("assertCloudShapeRoutingProgressHealthy(initialFanout) returned error: %v", err)
	}

	resetAfterDrain := append([]routingProgressRecord(nil), initialFanout[:3]...)
	resetAfterDrain = append(resetAfterDrain,
		routingProgressRecord{
			Time: start.Add(8 * time.Second),
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:           1024,
				WritableSlots:       512,
				ReadableSlots:       900,
				SettledSlots:        400,
				PendingSlots:        400,
				HealthyNodes:        3,
				ActivePeerRefreshes: 4,
			},
		},
		routingProgressRecord{
			Time: start.Add(12 * time.Second),
			LocalSmokeProgress: LocalSmokeProgress{
				SlotCount:           1024,
				WritableSlots:       520,
				ReadableSlots:       910,
				SettledSlots:        410,
				PendingSlots:        1024,
				HealthyNodes:        3,
				ActivePeerRefreshes: 6,
			},
		},
	)
	if err := assertCloudShapeRoutingProgressHealthy(resetAfterDrain, 1024, 30*time.Second); err == nil {
		t.Fatal("assertCloudShapeRoutingProgressHealthy(resetAfterDrain) unexpectedly succeeded")
	}
}
