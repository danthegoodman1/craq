package storage

import "strings"

type JournalExperiment string

const (
	JournalExperimentBaselineJSONSync  JournalExperiment = "baseline_json_sync"
	JournalExperimentBinarySync        JournalExperiment = "binary_sync"
	JournalExperimentBinarySegmentSync JournalExperiment = "binary_segment_sync"
	JournalExperimentNoSyncBound       JournalExperiment = "nosync_bound"
	defaultJournalExperiment                             = JournalExperimentBaselineJSONSync
)

type CommitJournalOpenOptions struct {
	Experiment JournalExperiment
}

func NormalizeJournalExperiment(value JournalExperiment) JournalExperiment {
	switch JournalExperiment(strings.TrimSpace(string(value))) {
	case "", defaultJournalExperiment:
		return defaultJournalExperiment
	case JournalExperimentBinarySync:
		return JournalExperimentBinarySync
	case JournalExperimentBinarySegmentSync:
		return JournalExperimentBinarySegmentSync
	case JournalExperimentNoSyncBound:
		return JournalExperimentNoSyncBound
	default:
		return ""
	}
}

func ValidJournalExperiment(value JournalExperiment) bool {
	return NormalizeJournalExperiment(value) != ""
}
