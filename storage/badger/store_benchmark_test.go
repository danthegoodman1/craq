package badger

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/storage"
)

func BenchmarkBackendApplyCommitted256B(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "badger"))
	if err != nil {
		b.Fatalf("Open returned error: %v", err)
	}
	defer func() { _ = store.Close() }()

	backend := store.Backend()
	if err := backend.CreateReplica(1); err != nil {
		b.Fatalf("CreateReplica returned error: %v", err)
	}

	value := benchSizedValue("value", 256)
	persisted := &storage.PersistedReplica{
		Assignment: storage.ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         storage.ReplicaRoleSingle,
		},
		LastKnownState:           storage.ReplicaStateActive,
		HighestCommittedSequence: 0,
		HasCommittedData:         true,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sequence := uint64(i + 1)
		op := storage.WriteOperation{
			Slot:     1,
			Sequence: sequence,
			Kind:     storage.OperationKindPut,
			Key:      fmt.Sprintf("key-%d", i),
			Value:    value,
			Metadata: storage.ObjectMetadata{
				Version:   sequence,
				CreatedAt: time.Unix(0, 0).UTC(),
				UpdatedAt: time.Unix(0, int64(sequence)).UTC(),
			},
		}
		persisted.HighestCommittedSequence = sequence
		if err := backend.ApplyCommitted(context.Background(), "bench-node", op, persisted); err != nil {
			b.Fatalf("ApplyCommitted returned error: %v", err)
		}
	}
}

func BenchmarkBackendStagePut256B(b *testing.B) {
	store, err := Open(filepath.Join(b.TempDir(), "badger"))
	if err != nil {
		b.Fatalf("Open returned error: %v", err)
	}
	defer func() { _ = store.Close() }()

	backend := store.Backend()
	if err := backend.CreateReplica(1); err != nil {
		b.Fatalf("CreateReplica returned error: %v", err)
	}

	value := benchSizedValue("stage-value", 256)
	base := time.Unix(0, 0).UTC()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sequence := uint64(i + 1)
		timestamp := base.Add(time.Duration(sequence) * time.Nanosecond)
		if err := backend.StagePut(1, sequence, fmt.Sprintf("stage-key-%d", i), value, storage.ObjectMetadata{
			Version:   sequence,
			CreatedAt: timestamp,
			UpdatedAt: timestamp,
		}); err != nil {
			b.Fatalf("StagePut returned error: %v", err)
		}
	}
}

func benchSizedValue(seed string, size int) string {
	if size <= len(seed) {
		return seed[:size]
	}
	buf := make([]byte, 0, size)
	for len(buf) < size {
		buf = append(buf, seed...)
		buf = append(buf, '-')
	}
	return string(buf[:size])
}
