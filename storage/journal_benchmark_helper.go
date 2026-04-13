package storage

import (
	"fmt"
	"strings"
	"time"
)

type CommitJournalBenchmarkBatchSpec struct {
	Slot          int
	ChainVersion  uint64
	StartSequence uint64
	BatchSize     int
	ValueBytes    int
}

func AppendPrepareBatchForBenchmark(store CommitJournalStore, spec CommitJournalBenchmarkBatchSpec) error {
	if store == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	if spec.BatchSize <= 0 {
		return fmt.Errorf("%w: batch size must be > 0", ErrInvalidConfig)
	}
	if spec.ValueBytes <= 0 {
		return fmt.Errorf("%w: value bytes must be > 0", ErrInvalidConfig)
	}
	records := make([]journalRecord, 0, spec.BatchSize)
	baseTime := time.Unix(0, 0).UTC()
	value := benchmarkSizedValue("prepare-bench", spec.ValueBytes)
	for i := 0; i < spec.BatchSize; i++ {
		sequence := spec.StartSequence + uint64(i)
		timestamp := baseTime.Add(time.Duration(sequence) * time.Nanosecond)
		records = append(records, journalRecord{
			Type:         journalRecordTypePrepare,
			Slot:         spec.Slot,
			ChainVersion: spec.ChainVersion,
			Sequence:     sequence,
			Kind:         OperationKindPut,
			Key:          fmt.Sprintf("bench-key-%06d", i),
			Value:        value,
			Metadata: ObjectMetadata{
				Version:   sequence,
				CreatedAt: timestamp,
				UpdatedAt: timestamp,
			},
		})
	}
	_, err := store.AppendBatch(records)
	return err
}

func benchmarkSizedValue(seed string, size int) string {
	if size <= len(seed) {
		return seed[:size]
	}
	var builder strings.Builder
	builder.Grow(size)
	for builder.Len() < size {
		builder.WriteString(seed)
		builder.WriteByte('-')
	}
	return builder.String()[:size]
}
