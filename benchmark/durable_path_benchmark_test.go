package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/storage"
	badgerstore "github.com/danthegoodman1/craq/storage/badger"
)

func BenchmarkCommitJournalAppendBatchDurablePaths(b *testing.B) {
	for _, layout := range []LocalProfile{
		{StorageLayout: "repo_disk"},
		{StorageLayout: "ramdisk", RamdiskSizeMiB: 512},
	} {
		layout := layout
		b.Run(layout.StorageLayout, func(b *testing.B) {
			root, cleanup := mustPrepareBenchmarkStorageRoot(b, layout, "journal-append")
			defer cleanup()
			for _, experiment := range []storage.JournalExperiment{
				storage.JournalExperimentBaselineJSONSync,
				storage.JournalExperimentBinarySync,
				storage.JournalExperimentBinarySegmentSync,
				storage.JournalExperimentNoSyncBound,
			} {
				for _, valueBytes := range []int{256, 2048} {
					for _, batchSize := range []int{1, 8, 32, 64} {
						experiment := experiment
						valueBytes := valueBytes
						batchSize := batchSize
						b.Run(fmt.Sprintf("%s_v%d_b%d", experiment, valueBytes, batchSize), func(b *testing.B) {
							dir := filepath.Join(root, sanitizeBenchmarkName(b.Name()))
							if err := os.RemoveAll(dir); err != nil {
								b.Fatalf("RemoveAll returned error: %v", err)
							}
							if err := os.MkdirAll(dir, 0o755); err != nil {
								b.Fatalf("MkdirAll returned error: %v", err)
							}
							journal, err := storage.OpenFileCommitJournalForLocalStateWithOptions(
								filepath.Join(dir, "commit-journal.log"),
								storage.CommitJournalOpenOptions{Experiment: experiment},
							)
							if err != nil {
								b.Fatalf("OpenFileCommitJournalForLocalStateWithOptions returned error: %v", err)
							}
							defer func() { _ = journal.Close() }()

							spec := storage.CommitJournalBenchmarkBatchSpec{
								Slot:          1,
								ChainVersion:  1,
								StartSequence: 1,
								BatchSize:     batchSize,
								ValueBytes:    valueBytes,
							}
							b.ReportAllocs()
							b.SetBytes(int64(batchSize * valueBytes))
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								spec.StartSequence = uint64(i*batchSize + 1)
								if err := storage.AppendPrepareBatchForBenchmark(journal, spec); err != nil {
									b.Fatalf("AppendPrepareBatchForBenchmark returned error: %v", err)
								}
							}
						})
					}
				}
			}
		})
	}
}

func BenchmarkBadgerStagePutDurablePaths(b *testing.B) {
	for _, layout := range []LocalProfile{
		{StorageLayout: "repo_disk"},
		{StorageLayout: "ramdisk", RamdiskSizeMiB: 512},
	} {
		layout := layout
		b.Run(layout.StorageLayout, func(b *testing.B) {
			root, cleanup := mustPrepareBenchmarkStorageRoot(b, layout, "badger-stage-put")
			defer cleanup()
			for _, valueBytes := range []int{256, 2048} {
				valueBytes := valueBytes
				b.Run(fmt.Sprintf("v%d", valueBytes), func(b *testing.B) {
					dir := filepath.Join(root, sanitizeBenchmarkName(b.Name()))
					if err := os.RemoveAll(dir); err != nil {
						b.Fatalf("RemoveAll returned error: %v", err)
					}
					store, err := badgerstore.Open(dir)
					if err != nil {
						b.Fatalf("badgerstore.Open returned error: %v", err)
					}
					defer func() { _ = store.Close() }()
					backend := store.Backend()
					if err := backend.CreateReplica(1); err != nil {
						b.Fatalf("CreateReplica returned error: %v", err)
					}
					value := sizedBenchmarkValue("stage-put", valueBytes)
					base := time.Unix(0, 0).UTC()
					b.ReportAllocs()
					b.SetBytes(int64(valueBytes))
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						sequence := uint64(i + 1)
						timestamp := base.Add(time.Duration(sequence) * time.Nanosecond)
						if err := backend.StagePut(1, sequence, fmt.Sprintf("bench-key-%06d", i), value, storage.ObjectMetadata{
							Version:   sequence,
							CreatedAt: timestamp,
							UpdatedAt: timestamp,
						}); err != nil {
							b.Fatalf("StagePut returned error: %v", err)
						}
					}
				})
			}
		})
	}
}

func mustPrepareBenchmarkStorageRoot(tb testing.TB, layout LocalProfile, prefix string) (string, func()) {
	tb.Helper()
	runDir := tb.TempDir()
	root, cleanup, err := prepareLocalStorageRoot(context.Background(), runDir, sanitizeBenchmarkName(prefix), layout)
	if err != nil {
		tb.Fatalf("prepareLocalStorageRoot returned error: %v", err)
	}
	return root, cleanup
}

func sanitizeBenchmarkName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	replacer := strings.NewReplacer("/", "-", " ", "-", "=", "-", ",", "-", ".", "-")
	name = replacer.Replace(name)
	for strings.Contains(name, "--") {
		name = strings.ReplaceAll(name, "--", "-")
	}
	name = strings.Trim(name, "-")
	if name == "" {
		return "bench"
	}
	return name
}

func sizedBenchmarkValue(seed string, size int) string {
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
