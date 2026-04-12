package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/danthegoodman1/craq/client"
	"github.com/danthegoodman1/craq/quickstart"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

type LoadGenProcessConfig struct {
	RunID        string           `json:"run_id"`
	ManifestPath string           `json:"manifest_path"`
	OutputDir    string           `json:"output_dir"`
	Workload     WorkloadProfile  `json:"workload"`
	Telemetry    TelemetryProfile `json:"telemetry"`
}

type latencySample struct {
	At         time.Time `json:"at"`
	Millis     float64   `json:"latency_ms"`
	Successful bool      `json:"successful"`
	Operation  string    `json:"operation"`
	Error      string    `json:"error,omitempty"`
}

func RunLoadGen(ctx context.Context, cfg LoadGenProcessConfig) (LoadGenReport, error) {
	if cfg.ManifestPath == "" {
		return LoadGenReport{}, fmt.Errorf("loadgen manifest path must not be empty")
	}
	if cfg.OutputDir == "" {
		return LoadGenReport{}, fmt.Errorf("loadgen output dir must not be empty")
	}
	manifest, err := quickstart.Load(cfg.ManifestPath)
	if err != nil {
		return LoadGenReport{}, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return LoadGenReport{}, fmt.Errorf("mkdir output dir: %w", err)
	}
	report := LoadGenReport{RunID: cfg.RunID, StartedAt: time.Now().UTC()}
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()
	router, err := client.NewRouter(
		grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool),
		grpcx.NewClientTransport(pool),
	)
	if err != nil {
		return LoadGenReport{}, fmt.Errorf("create benchmark client router: %w", err)
	}
	if err := router.Refresh(ctx); err != nil {
		return LoadGenReport{}, fmt.Errorf("refresh benchmark client router: %w", err)
	}
	snapshot, ok := router.Snapshot()
	if !ok {
		return LoadGenReport{}, fmt.Errorf("benchmark client router snapshot not loaded")
	}
	metricTargets := loadgenMetricSnapshotTargets(manifest)
	profileTargets := loadgenProfileTargets(manifest)
	rng := rand.New(rand.NewSource(cfg.Workload.Seed))
	fixedSlotKeys, err := buildFixedSlotKeyPools(snapshot.SlotCount, cfg.Workload)
	if err != nil {
		return LoadGenReport{}, err
	}

	preloadStarted := time.Now()
	for i := 0; i < cfg.Workload.PreloadKeys; i++ {
		key := fmt.Sprintf("preload-%06d", i)
		value := sizedValue("preload", cfg.Workload.ValueBytes)
		reqCtx, cancel := context.WithTimeout(ctx, cfg.Workload.RequestTimeout)
		_, err := router.Put(reqCtx, key, value)
		cancel()
		if err != nil {
			report.Errors = append(report.Errors, fmt.Sprintf("preload key %q: %v", key, err))
			return report, err
		}
	}
	for _, keys := range fixedSlotKeys {
		for _, key := range keys {
			reqCtx, cancel := context.WithTimeout(ctx, cfg.Workload.RequestTimeout)
			_, err := router.Put(reqCtx, key, sizedValue("preload-fixed", cfg.Workload.ValueBytes))
			cancel()
			if err != nil {
				report.Errors = append(report.Errors, fmt.Sprintf("preload fixed key %q: %v", key, err))
				return report, err
			}
		}
	}
	report.Preload = PreloadReport{
		Keys:     cfg.Workload.PreloadKeys,
		Duration: time.Since(preloadStarted),
	}
	if err := collectMetricSnapshot(ctx, cfg.OutputDir, "preload-end", metricTargets); err != nil {
		return report, err
	}

	for _, scenario := range cfg.Workload.Scenarios {
		select {
		case <-ctx.Done():
			return report, ctx.Err()
		default:
		}
		result, err := runScenario(ctx, router, cfg.OutputDir, cfg.Workload, scenario, rng, cfg.Telemetry, profileTargets, metricTargets, fixedSlotKeys)
		if err != nil {
			report.Errors = append(report.Errors, err.Error())
			return report, err
		}
		report.Scenarios = append(report.Scenarios, result)
		if cfg.Workload.PerScenarioPause > 0 {
			select {
			case <-ctx.Done():
				return report, ctx.Err()
			case <-time.After(cfg.Workload.PerScenarioPause):
			}
		}
	}
	report.FinishedAt = time.Now().UTC()
	if err := SaveJSON(filepath.Join(cfg.OutputDir, "loadgen-report.json"), report); err != nil {
		return LoadGenReport{}, err
	}
	if err := SaveJSON(filepath.Join(cfg.OutputDir, "loadgen-manifest.json"), manifest); err != nil {
		return LoadGenReport{}, err
	}
	return report, nil
}

func runScenario(
	ctx context.Context,
	router *client.Router,
	outputDir string,
	workload WorkloadProfile,
	scenario ScenarioProfile,
	rng *rand.Rand,
	telemetry TelemetryProfile,
	profileTargets []loadgenProfileTarget,
	metricTargets []metricSnapshotTarget,
	fixedSlotKeys map[int][]string,
) (ScenarioReport, error) {
	report := ScenarioReport{
		Name:        scenario.Name,
		Kind:        scenario.Kind,
		Concurrency: scenario.Concurrency,
		Warmup:      scenario.Warmup,
		Duration:    scenario.Duration,
		StartedAt:   time.Now().UTC(),
	}
	samplePath := filepath.Join(outputDir, scenario.Name+"-samples.jsonl")
	file, err := os.Create(samplePath)
	if err != nil {
		return ScenarioReport{}, fmt.Errorf("create sample file: %w", err)
	}
	defer file.Close()

	profiler := startScenarioProfiler(ctx, outputDir, scenario, telemetry, profileTargets)
	snapshotErrs := make(chan error, 2)
	var snapshotWG sync.WaitGroup
	scheduleSnapshot := func(delay time.Duration, suffix string) {
		snapshotWG.Add(1)
		go func() {
			defer snapshotWG.Done()
			if err := waitForScenarioProfileStart(ctx, delay); err != nil {
				if ctx.Err() != nil {
					return
				}
				snapshotErrs <- err
				return
			}
			snapCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := collectMetricSnapshot(snapCtx, outputDir, sanitizeScenarioName(scenario.Name)+"-"+suffix, metricTargets); err != nil {
				snapshotErrs <- err
			}
		}()
	}
	scheduleSnapshot(scenario.Warmup, "start")
	scheduleSnapshot(scenario.Warmup+scenario.Duration, "end")

	type opResult struct {
		startedAt time.Time
		at        time.Time
		latency   time.Duration
		success   bool
		operation string
		err       string
	}

	opCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var stopIssuing atomic.Bool
	var warmup atomic.Bool
	if scenario.Warmup > 0 {
		warmup.Store(true)
		go func() {
			select {
			case <-opCtx.Done():
			case <-time.After(scenario.Warmup):
				warmup.Store(false)
			}
		}()
	}
	endTimer := time.NewTimer(scenario.Warmup + scenario.Duration)
	defer endTimer.Stop()
	var drainTimer *time.Timer
	var drainTimerCh <-chan time.Time
	measuredEnded := false
	var measuredEndAt time.Time

	results := make(chan opResult, scenario.Concurrency*2)
	var wg sync.WaitGroup
	for workerID := 0; workerID < scenario.Concurrency; workerID++ {
		workerSeed := rng.Int63()
		wg.Add(1)
		go func(workerID int, seed int64) {
			defer wg.Done()
			workerRand := rand.New(rand.NewSource(seed))
			for {
				select {
				case <-opCtx.Done():
					return
				default:
				}
				if stopIssuing.Load() {
					return
				}
				start := time.Now()
				kind := scenario.Kind
				if kind == "mixed" {
					if workerRand.Intn(100) < scenario.ReadPercent {
						kind = "get"
					} else {
						kind = "put"
					}
				}
				key := pickScenarioKey(workerRand, workload, scenario, fixedSlotKeys)
				requestCtx, cancel := context.WithTimeout(opCtx, workload.RequestTimeout)
				var callErr error
				switch kind {
				case "get":
					_, callErr = router.Get(requestCtx, key)
				case "put":
					value := sizedValue(fmt.Sprintf("w-%d-%d", workerID, workerRand.Int63()), scenario.ValueBytes)
					_, callErr = router.Put(requestCtx, key, value)
				default:
					callErr = fmt.Errorf("unsupported scenario kind %q", kind)
				}
				cancel()
				result := opResult{
					startedAt: start.UTC(),
					at:        time.Now().UTC(),
					latency:   time.Since(start),
					success:   callErr == nil,
					operation: kind,
				}
				if callErr != nil {
					result.err = callErr.Error()
				}
				select {
				case <-opCtx.Done():
					return
				case results <- result:
				}
			}
		}(workerID, workerSeed)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
		close(results)
	}()

	var samples []float64
	var measuredSamples []latencySample
	var intervalSamples []latencySample
	intervalStarted := time.Now()
	flushInterval := func(now time.Time) {
		if len(intervalSamples) == 0 {
			return
		}
		report.Intervals = append(report.Intervals, buildIntervalSummary(now, intervalSamples))
		intervalSamples = intervalSamples[:0]
		intervalStarted = now
	}

loop:
	for {
		select {
	case <-ctx.Done():
			cancel()
			<-done
			return ScenarioReport{}, ctx.Err()
		case <-endTimer.C:
			stopIssuing.Store(true)
			measuredEnded = true
			measuredEndAt = time.Now().UTC()
			drainTimer = time.NewTimer(workload.RequestTimeout)
			drainTimerCh = drainTimer.C
		case <-drainTimerCh:
			cancel()
			drainTimerCh = nil
			if drainTimer != nil {
				drainTimer.Stop()
				drainTimer = nil
			}
		case result, ok := <-results:
			if !ok {
				break loop
			}
			sample := latencySample{
				At:         result.at,
				Millis:     float64(result.latency) / float64(time.Millisecond),
				Successful: result.success,
				Operation:  result.operation,
				Error:      result.err,
			}
			if err := writeJSONLine(file, sample); err != nil {
				return ScenarioReport{}, err
			}
			if warmup.Load() {
				continue
			}
			if measuredEnded && !result.startedAt.Before(measuredEndAt) {
				if !result.success && strings.Contains(result.err, context.Canceled.Error()) {
					report.IgnoredErrorOps++
				}
				continue
			}
			if measuredEnded && !result.success && strings.Contains(result.err, context.Canceled.Error()) && result.at.After(measuredEndAt) {
				report.IgnoredErrorOps++
				continue
			}
			report.TotalOps++
			if result.success {
				report.SuccessOps++
			} else {
				report.ErrorOps++
			}
			if result.operation == "get" {
				report.ReadOps++
			} else {
				report.WriteOps++
			}
			samples = append(samples, sample.Millis)
			measuredSamples = append(measuredSamples, sample)
			intervalSamples = append(intervalSamples, sample)
			if sample.At.Sub(intervalStarted) >= workload.Interval {
				flushInterval(sample.At)
			}
		}
	}
	flushInterval(time.Now().UTC())
	report.FinishedAt = time.Now().UTC()
	if drainTimer != nil {
		drainTimer.Stop()
	}
	if err := profiler.wait(); err != nil {
		return report, err
	}
	report.P50Millis = percentile(samples, 50)
	report.P95Millis = percentile(samples, 95)
	report.P99Millis = percentile(samples, 99)
	report.MaxMillis = percentile(samples, 100)
	measuredDuration := scenario.Duration
	if !measuredEnded {
		measuredDuration = report.FinishedAt.Sub(report.StartedAt) - scenario.Warmup
	}
	if measuredDuration > 0 {
		report.Throughput = float64(report.TotalOps) / measuredDuration.Seconds()
	}
	report.Operations = buildOperationSummaries(measuredSamples, measuredDuration)
	report.Histogram = histogram(samples)
	snapshotWG.Wait()
	close(snapshotErrs)
	for err := range snapshotErrs {
		if err != nil {
			return report, err
		}
	}
	return report, nil
}

func buildOperationSummaries(samples []latencySample, measuredDuration time.Duration) []OperationSummary {
	if len(samples) == 0 {
		return nil
	}
	latenciesByOperation := map[string][]float64{}
	summariesByOperation := map[string]*OperationSummary{}
	for _, sample := range samples {
		latenciesByOperation[sample.Operation] = append(latenciesByOperation[sample.Operation], sample.Millis)
		summary, ok := summariesByOperation[sample.Operation]
		if !ok {
			summary = &OperationSummary{Operation: sample.Operation}
			summariesByOperation[sample.Operation] = summary
		}
		summary.TotalOps++
		if sample.Successful {
			summary.SuccessOps++
		} else {
			summary.ErrorOps++
		}
	}
	names := make([]string, 0, len(summariesByOperation))
	for name := range summariesByOperation {
		names = append(names, name)
	}
	sort.Strings(names)
	summaries := make([]OperationSummary, 0, len(names))
	for _, name := range names {
		latencies := latenciesByOperation[name]
		summary := *summariesByOperation[name]
		summary.P50Millis = percentile(latencies, 50)
		summary.P95Millis = percentile(latencies, 95)
		summary.P99Millis = percentile(latencies, 99)
		summary.MaxMillis = percentile(latencies, 100)
		if measuredDuration > 0 {
			summary.Throughput = float64(summary.TotalOps) / measuredDuration.Seconds()
		}
		summaries = append(summaries, summary)
	}
	return summaries
}

func buildIntervalSummary(at time.Time, samples []latencySample) IntervalSample {
	latencies := make([]float64, 0, len(samples))
	var ops, errs, reads, writes int64
	for _, sample := range samples {
		ops++
		if !sample.Successful {
			errs++
		}
		if sample.Operation == "get" {
			reads++
		} else {
			writes++
		}
		latencies = append(latencies, sample.Millis)
	}
	return IntervalSample{
		Timestamp: at.UTC(),
		Ops:       ops,
		Errors:    errs,
		Reads:     reads,
		Writes:    writes,
		P50Millis: percentile(latencies, 50),
		P95Millis: percentile(latencies, 95),
		P99Millis: percentile(latencies, 99),
	}
}

func histogram(samples []float64) []HistogramBucket {
	if len(samples) == 0 {
		return nil
	}
	limits := []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 50, 100, 250, 500, 1000}
	maxSample := 0.0
	for _, sample := range samples {
		if sample > maxSample {
			maxSample = sample
		}
	}
	buckets := make([]HistogramBucket, 0, len(limits)+1)
	for _, limit := range limits {
		buckets = append(buckets, HistogramBucket{UpperMillis: limit})
	}
	if maxSample > limits[len(limits)-1] {
		buckets = append(buckets, HistogramBucket{UpperMillis: math.Ceil(maxSample)})
	}
	for _, sample := range samples {
		for i := range buckets {
			if sample <= buckets[i].UpperMillis {
				buckets[i].Count++
				break
			}
		}
	}
	return buckets
}

func percentile(samples []float64, pct float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	cloned := slices.Clone(samples)
	sort.Float64s(cloned)
	if pct >= 100 {
		return cloned[len(cloned)-1]
	}
	rank := (pct / 100) * float64(len(cloned)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return cloned[lo]
	}
	frac := rank - float64(lo)
	return cloned[lo] + (cloned[hi]-cloned[lo])*frac
}

func writeJSONLine(file *os.File, value any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal jsonl: %w", err)
	}
	if _, err := file.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write jsonl: %w", err)
	}
	return nil
}

func sizedValue(seed string, size int) string {
	if size <= len(seed) {
		return seed[:size]
	}
	var b strings.Builder
	for b.Len() < size {
		b.WriteString(seed)
		b.WriteByte('-')
	}
	out := b.String()
	return out[:size]
}

func pickScenarioKey(rng *rand.Rand, workload WorkloadProfile, scenario ScenarioProfile, fixedSlotKeys map[int][]string) string {
	if len(scenario.FixedSlots) == 0 {
		keyIndex := rng.Intn(workload.PreloadKeys)
		return fmt.Sprintf("preload-%06d", keyIndex)
	}
	slot := scenario.FixedSlots[rng.Intn(len(scenario.FixedSlots))]
	keys := fixedSlotKeys[slot]
	if len(keys) == 0 {
		return fmt.Sprintf("preload-%06d", rng.Intn(workload.PreloadKeys))
	}
	return keys[rng.Intn(len(keys))]
}

func buildFixedSlotKeyPools(slotCount int, workload WorkloadProfile) (map[int][]string, error) {
	slots := map[int]struct{}{}
	perSlot := 64
	for _, scenario := range workload.Scenarios {
		if scenario.Concurrency*4 > perSlot {
			perSlot = scenario.Concurrency * 4
		}
		for _, slot := range scenario.FixedSlots {
			slots[slot] = struct{}{}
		}
	}
	if len(slots) == 0 {
		return nil, nil
	}
	keys := make([]int, 0, len(slots))
	for slot := range slots {
		keys = append(keys, slot)
	}
	sort.Ints(keys)
	out := make(map[int][]string, len(keys))
	targets := make(map[int]int, len(keys))
	for _, slot := range keys {
		targets[slot] = perSlot
		out[slot] = make([]string, 0, perSlot)
	}
	for candidate := 0; ; candidate++ {
		done := true
		for _, slot := range keys {
			if len(out[slot]) < targets[slot] {
				done = false
				break
			}
		}
		if done {
			return out, nil
		}
		key := fmt.Sprintf("fixed-slot-%06d", candidate)
		slot := int(crc32.ChecksumIEEE([]byte(key)) % uint32(slotCount))
		if len(out[slot]) >= targets[slot] {
			continue
		}
		out[slot] = append(out[slot], key)
		if candidate > slotCount*perSlot*128 {
			return nil, fmt.Errorf("unable to build fixed slot keys for slots %v", keys)
		}
	}
}
