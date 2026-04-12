package benchmark

import (
	"encoding/json"
	"os"
	"strings"
	"time"
)

type RoutingProgressSummary struct {
	Samples                int           `json:"samples"`
	FirstObservedAt        time.Time     `json:"first_observed_at"`
	LastObservedAt         time.Time     `json:"last_observed_at"`
	TotalDuration          time.Duration `json:"total_duration"`
	TimeToFirstReadable    time.Duration `json:"time_to_first_readable"`
	TimeToAllWritable      time.Duration `json:"time_to_all_writable"`
	MaxPendingSlots        int           `json:"max_pending_slots"`
	MaxOutboxEntries       int           `json:"max_outbox_entries"`
	MaxActivePeerRefreshes int           `json:"max_active_peer_refreshes"`
}

func readRoutingProgressRecords(path string) ([]routingProgressRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	records := make([]routingProgressRecord, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var record routingProgressRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func summarizeRoutingProgressRecords(records []routingProgressRecord) RoutingProgressSummary {
	if len(records) == 0 {
		return RoutingProgressSummary{}
	}
	summary := RoutingProgressSummary{
		Samples:         len(records),
		FirstObservedAt: records[0].Time,
		LastObservedAt:  records[len(records)-1].Time,
	}
	summary.TotalDuration = summary.LastObservedAt.Sub(summary.FirstObservedAt)
	for _, record := range records {
		if summary.TimeToFirstReadable == 0 && record.ReadableSlots > 0 {
			summary.TimeToFirstReadable = record.Time.Sub(summary.FirstObservedAt)
		}
		if summary.TimeToAllWritable == 0 && record.SlotCount > 0 && record.WritableSlots == record.SlotCount {
			summary.TimeToAllWritable = record.Time.Sub(summary.FirstObservedAt)
		}
		if record.PendingSlots > summary.MaxPendingSlots {
			summary.MaxPendingSlots = record.PendingSlots
		}
		if record.OutboxEntries > summary.MaxOutboxEntries {
			summary.MaxOutboxEntries = record.OutboxEntries
		}
		if record.ActivePeerRefreshes > summary.MaxActivePeerRefreshes {
			summary.MaxActivePeerRefreshes = record.ActivePeerRefreshes
		}
	}
	return summary
}
