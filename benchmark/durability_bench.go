package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	badgerdb "github.com/dgraph-io/badger/v4"

	"github.com/danthegoodman1/craq/storage"
	badgerstore "github.com/danthegoodman1/craq/storage/badger"
)

const durabilityBenchSlot = 1

type DurabilityBenchConfig struct {
	Path       string
	Label      string
	OutputPath string
	Count      int
}

type durabilityBatchBackend interface {
	ApplyCommittedBatch(ctx context.Context, nodeID string, commits []storage.DurableCommit) error
}

type DurabilityBenchReport struct {
	Label       string                    `json:"label"`
	Path        string                    `json:"path"`
	GeneratedAt time.Time                 `json:"generated_at"`
	Metadata    DurabilityPathMetadata    `json:"metadata"`
	Tests       []DurabilityBenchmarkStat `json:"tests"`
}

type DurabilityPathMetadata struct {
	TargetPath    string `json:"target_path"`
	Device        string `json:"device,omitempty"`
	Filesystem    string `json:"filesystem,omitempty"`
	MountOptions  string `json:"mount_options,omitempty"`
	MountPoint    string `json:"mount_point,omitempty"`
	KernelQueue   string `json:"kernel_queue,omitempty"`
	ResolvedPath  string `json:"resolved_path,omitempty"`
	CommandErrors string `json:"command_errors,omitempty"`
}

type DurabilityBenchmarkStat struct {
	Name       string  `json:"name"`
	Count      int     `json:"count"`
	MeanMillis float64 `json:"mean_ms"`
	P50Millis  float64 `json:"p50_ms"`
	P95Millis  float64 `json:"p95_ms"`
	P99Millis  float64 `json:"p99_ms"`
	MaxMillis  float64 `json:"max_ms"`
}

func RunDurabilityBench(ctx context.Context, cfg DurabilityBenchConfig) error {
	if strings.TrimSpace(cfg.Path) == "" {
		return fmt.Errorf("durability bench path must not be empty")
	}
	if strings.TrimSpace(cfg.OutputPath) == "" {
		return fmt.Errorf("durability bench output path must not be empty")
	}
	if cfg.Count <= 0 {
		cfg.Count = 1000
	}
	if cfg.Label == "" {
		cfg.Label = filepath.Base(cfg.Path)
	}
	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return fmt.Errorf("mkdir bench path: %w", err)
	}

	report := DurabilityBenchReport{
		Label:       cfg.Label,
		Path:        cfg.Path,
		GeneratedAt: time.Now().UTC(),
		Metadata:    captureDurabilityPathMetadata(cfg.Path),
	}
	tests := []struct {
		name string
		run  func(context.Context, string, int) (DurabilityBenchmarkStat, error)
	}{
		{name: "file_fsync_256b", run: runFileFsyncBenchmark},
		{name: "badger_sync_put_256b", run: runBadgerSyncPutBenchmark},
		{name: "badger_batch_apply_256b", run: runBadgerBatchApplyBenchmark},
	}
	for _, test := range tests {
		stat, err := test.run(ctx, cfg.Path, cfg.Count)
		if err != nil {
			return fmt.Errorf("%s: %w", test.name, err)
		}
		report.Tests = append(report.Tests, stat)
	}
	return SaveJSON(cfg.OutputPath, report)
}

func runFileFsyncBenchmark(ctx context.Context, root string, count int) (DurabilityBenchmarkStat, error) {
	dir := filepath.Join(root, "durability-file-fsync")
	if err := os.RemoveAll(dir); err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("remove fsync dir: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("mkdir fsync dir: %w", err)
	}
	file, err := os.OpenFile(filepath.Join(dir, "fsync.dat"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("open fsync file: %w", err)
	}
	defer file.Close()

	payload := []byte(sizedValue("durability-file", 256))
	samples := make([]float64, 0, count)
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return DurabilityBenchmarkStat{}, err
		}
		started := time.Now()
		if _, err := file.Write(payload); err != nil {
			return DurabilityBenchmarkStat{}, fmt.Errorf("write fsync payload: %w", err)
		}
		if err := file.Sync(); err != nil {
			return DurabilityBenchmarkStat{}, fmt.Errorf("sync fsync payload: %w", err)
		}
		samples = append(samples, millisSince(started))
	}
	return summarizeDurabilityBenchmark("file_fsync_256b", samples), nil
}

func runBadgerSyncPutBenchmark(ctx context.Context, root string, count int) (DurabilityBenchmarkStat, error) {
	dir := filepath.Join(root, "durability-badger-sync-put")
	if err := os.RemoveAll(dir); err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("remove badger sync dir: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("mkdir badger sync dir: %w", err)
	}

	opts := badgerdb.DefaultOptions(dir)
	opts.Logger = nil
	opts.SyncWrites = true
	opts.Dir = dir
	opts.ValueDir = dir
	db, err := badgerdb.Open(opts)
	if err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("open badger sync db: %w", err)
	}
	defer func() { _ = db.Close() }()

	value := []byte(sizedValue("durability-badger-sync", 256))
	samples := make([]float64, 0, count)
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return DurabilityBenchmarkStat{}, err
		}
		key := []byte(fmt.Sprintf("bench-key-%06d", i))
		started := time.Now()
		if err := db.Update(func(txn *badgerdb.Txn) error {
			return txn.Set(key, value)
		}); err != nil {
			return DurabilityBenchmarkStat{}, fmt.Errorf("badger sync put: %w", err)
		}
		samples = append(samples, millisSince(started))
	}
	return summarizeDurabilityBenchmark("badger_sync_put_256b", samples), nil
}

func runBadgerBatchApplyBenchmark(ctx context.Context, root string, count int) (DurabilityBenchmarkStat, error) {
	dir := filepath.Join(root, "durability-badger-batch-apply")
	if err := os.RemoveAll(dir); err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("remove badger batch dir: %w", err)
	}
	store, err := badgerstore.Open(dir)
	if err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("open badger batch store: %w", err)
	}
	defer func() { _ = store.Close() }()

	backend := store.Backend()
	batchBackend, ok := backend.(durabilityBatchBackend)
	if !ok {
		return DurabilityBenchmarkStat{}, fmt.Errorf("storage backend does not expose ApplyCommittedBatch")
	}
	if err := backend.CreateReplica(durabilityBenchSlot); err != nil {
		return DurabilityBenchmarkStat{}, fmt.Errorf("create durability bench replica: %w", err)
	}

	samples := make([]float64, 0, count)
	base := time.Unix(0, 0).UTC()
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return DurabilityBenchmarkStat{}, err
		}
		sequence := uint64(i + 1)
		started := time.Now()
		if err := batchBackend.ApplyCommittedBatch(context.Background(), "durability-bench", []storage.DurableCommit{{
			Operation: storage.WriteOperation{
				Slot:     durabilityBenchSlot,
				Sequence: sequence,
				Kind:     storage.OperationKindPut,
				Key:      "bench-key",
				Value:    sizedValue("durability-batch-apply", 256),
				Metadata: storage.ObjectMetadata{
					Version:   sequence,
					CreatedAt: base.Add(time.Duration(sequence) * time.Nanosecond),
					UpdatedAt: base.Add(time.Duration(sequence) * time.Nanosecond),
				},
			},
			UpstreamConfirmedSequence: sequence,
		}}); err != nil {
			return DurabilityBenchmarkStat{}, fmt.Errorf("badger batch apply: %w", err)
		}
		samples = append(samples, millisSince(started))
	}
	return summarizeDurabilityBenchmark("badger_batch_apply_256b", samples), nil
}

func summarizeDurabilityBenchmark(name string, samples []float64) DurabilityBenchmarkStat {
	if len(samples) == 0 {
		return DurabilityBenchmarkStat{Name: name}
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	sum := 0.0
	maxValue := sorted[len(sorted)-1]
	for _, sample := range sorted {
		sum += sample
	}
	return DurabilityBenchmarkStat{
		Name:       name,
		Count:      len(sorted),
		MeanMillis: sum / float64(len(sorted)),
		P50Millis:  percentileFloat(sorted, 50),
		P95Millis:  percentileFloat(sorted, 95),
		P99Millis:  percentileFloat(sorted, 99),
		MaxMillis:  maxValue,
	}
}

func percentileFloat(sorted []float64, pct float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := pct / 100 * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return sorted[lower]
	}
	weight := rank - float64(lower)
	return sorted[lower] + (sorted[upper]-sorted[lower])*weight
}

func millisSince(started time.Time) float64 {
	return float64(time.Since(started)) / float64(time.Millisecond)
}

func captureDurabilityPathMetadata(targetPath string) DurabilityPathMetadata {
	meta := DurabilityPathMetadata{TargetPath: targetPath}
	if resolved, err := filepath.EvalSymlinks(targetPath); err == nil {
		meta.ResolvedPath = resolved
	}
	var commandErrors []string

	if output, err := exec.Command("findmnt", "-no", "SOURCE,FSTYPE,OPTIONS,TARGET", "--target", targetPath).CombinedOutput(); err == nil {
		fields := strings.Fields(strings.TrimSpace(string(output)))
		if len(fields) >= 4 {
			meta.Device = fields[0]
			meta.Filesystem = fields[1]
			meta.MountOptions = fields[2]
			meta.MountPoint = fields[3]
		}
	} else {
		commandErrors = append(commandErrors, "findmnt: "+strings.TrimSpace(err.Error()))
	}

	deviceName := filepath.Base(meta.Device)
	if deviceName != "" && deviceName != "." && deviceName != "/" {
		queueDir := filepath.Join("/sys/block", deviceName, "queue")
		entries := []string{}
		for _, fileName := range []string{"scheduler", "nr_requests", "read_ahead_kb", "rotational", "write_cache"} {
			data, err := os.ReadFile(filepath.Join(queueDir, fileName))
			if err != nil {
				continue
			}
			entries = append(entries, fmt.Sprintf("%s=%s", fileName, strings.TrimSpace(string(data))))
		}
		meta.KernelQueue = strings.Join(entries, " ")
	}

	if len(commandErrors) > 0 {
		meta.CommandErrors = strings.Join(commandErrors, "; ")
	}
	return meta
}

func WriteDurabilityBenchReport(path string, report DurabilityBenchReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
