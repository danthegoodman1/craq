package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"sync"
	"time"
)

const defaultTimeoutArtifactSampleLimit = 128

type queueDepthSample struct {
	At    time.Time `json:"at"`
	Queue string    `json:"queue"`
	Depth int       `json:"depth"`
}

type materializerLagSample struct {
	At                  time.Time `json:"at"`
	Slot                int       `json:"slot"`
	HighestCommitted    uint64    `json:"highest_committed_sequence"`
	MaterializedThrough uint64    `json:"materialized_committed_sequence"`
	Lag                 uint64    `json:"lag"`
}

type runtimePauseSnapshot struct {
	NumGoroutine             int     `json:"num_goroutine"`
	NumGC                    uint32  `json:"num_gc"`
	GCPauseTotalNs           uint64  `json:"gc_pause_total_ns"`
	SchedulerLatencyP99Secs  float64 `json:"scheduler_latency_p99_seconds,omitempty"`
	GCPauseLatencyP99Secs    float64 `json:"gc_pause_latency_p99_seconds,omitempty"`
}

type writeTimeoutArtifact struct {
	At                    time.Time              `json:"at"`
	NodeID                string                 `json:"node_id"`
	Slot                  int                    `json:"slot"`
	Sequence              uint64                 `json:"sequence"`
	Role                  ReplicaRole            `json:"role"`
	Error                 string                 `json:"error"`
	JournalQueueDepths    []queueDepthSample     `json:"journal_queue_depths"`
	MaterializerQueues    []queueDepthSample     `json:"materializer_queue_depths"`
	MaterializerLags      []materializerLagSample `json:"materializer_lags"`
	RecentWriteTrace      []writeTraceEvent      `json:"recent_write_trace_events"`
	Runtime               runtimePauseSnapshot   `json:"runtime"`
}

type writeTimeoutArtifactRecorder struct {
	file *os.File
	mu   sync.Mutex

	journalQueue []queueDepthSample
	materializer []queueDepthSample
	lags         []materializerLagSample
}

func openWriteTimeoutArtifactRecorder(path string) (*writeTimeoutArtifactRecorder, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir write timeout artifact dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open write timeout artifact output: %w", err)
	}
	return &writeTimeoutArtifactRecorder{file: file}, nil
}

func (r *writeTimeoutArtifactRecorder) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	return r.file.Close()
}

func (r *writeTimeoutArtifactRecorder) recordJournalQueueDepth(depth int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.journalQueue = appendBoundedQueueSample(r.journalQueue, queueDepthSample{
		At:    time.Now().UTC(),
		Queue: "journal",
		Depth: depth,
	}, defaultTimeoutArtifactSampleLimit)
}

func (r *writeTimeoutArtifactRecorder) recordMaterializerQueueDepth(depth int) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.materializer = appendBoundedQueueSample(r.materializer, queueDepthSample{
		At:    time.Now().UTC(),
		Queue: "materializer",
		Depth: depth,
	}, defaultTimeoutArtifactSampleLimit)
}

func (r *writeTimeoutArtifactRecorder) recordMaterializerLag(slot int, highestCommitted uint64, materializedCommitted uint64) {
	if r == nil {
		return
	}
	lag := uint64(0)
	if highestCommitted > materializedCommitted {
		lag = highestCommitted - materializedCommitted
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lags = appendBoundedLagSample(r.lags, materializerLagSample{
		At:                  time.Now().UTC(),
		Slot:                slot,
		HighestCommitted:    highestCommitted,
		MaterializedThrough: materializedCommitted,
		Lag:                 lag,
	}, defaultTimeoutArtifactSampleLimit)
}

func (r *writeTimeoutArtifactRecorder) capture(nodeID string, slot int, sequence uint64, role ReplicaRole, err error, recent []writeTraceEvent) {
	if r == nil || r.file == nil {
		return
	}
	artifact := writeTimeoutArtifact{
		At:                 time.Now().UTC(),
		NodeID:             nodeID,
		Slot:               slot,
		Sequence:           sequence,
		Role:               role,
		Error:              errString(err),
		RecentWriteTrace:   append([]writeTraceEvent(nil), recent...),
		Runtime:            captureRuntimePauseSnapshot(),
	}
	r.mu.Lock()
	artifact.JournalQueueDepths = append([]queueDepthSample(nil), r.journalQueue...)
	artifact.MaterializerQueues = append([]queueDepthSample(nil), r.materializer...)
	artifact.MaterializerLags = append([]materializerLagSample(nil), r.lags...)
	data, marshalErr := json.Marshal(artifact)
	if marshalErr == nil {
		_, _ = r.file.Write(append(data, '\n'))
	}
	r.mu.Unlock()
}

func appendBoundedQueueSample(samples []queueDepthSample, sample queueDepthSample, limit int) []queueDepthSample {
	samples = append(samples, sample)
	if len(samples) <= limit {
		return samples
	}
	copy(samples, samples[len(samples)-limit:])
	return samples[:limit]
}

func appendBoundedLagSample(samples []materializerLagSample, sample materializerLagSample, limit int) []materializerLagSample {
	samples = append(samples, sample)
	if len(samples) <= limit {
		return samples
	}
	copy(samples, samples[len(samples)-limit:])
	return samples[:limit]
}

func captureRuntimePauseSnapshot() runtimePauseSnapshot {
	snapshot := runtimePauseSnapshot{
		NumGoroutine: runtime.NumGoroutine(),
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	snapshot.NumGC = mem.NumGC
	snapshot.GCPauseTotalNs = mem.PauseTotalNs

	samples := []runtimemetrics.Sample{
		{Name: "/sched/latencies:seconds"},
		{Name: "/gc/pauses:seconds"},
	}
	runtimemetrics.Read(samples)
	snapshot.SchedulerLatencyP99Secs = histogramQuantile(samples[0], 0.99)
	snapshot.GCPauseLatencyP99Secs = histogramQuantile(samples[1], 0.99)
	return snapshot
}

func histogramQuantile(sample runtimemetrics.Sample, quantile float64) float64 {
	if sample.Value.Kind() != runtimemetrics.KindFloat64Histogram {
		return 0
	}
	hist := sample.Value.Float64Histogram()
	if hist == nil || len(hist.Buckets) < 2 || len(hist.Counts) == 0 {
		return 0
	}
	var total uint64
	for _, count := range hist.Counts {
		total += count
	}
	if total == 0 {
		return 0
	}
	target := uint64(float64(total) * quantile)
	if target == 0 {
		target = 1
	}
	var seen uint64
	for i, count := range hist.Counts {
		seen += count
		if seen >= target {
			return hist.Buckets[i+1]
		}
	}
	return hist.Buckets[len(hist.Buckets)-1]
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (n *Node) recordTimeoutJournalQueueDepth(depth int) {
	if n == nil || n.timeoutArtifacts == nil {
		return
	}
	n.timeoutArtifacts.recordJournalQueueDepth(depth)
}

func (n *Node) recordTimeoutMaterializerQueueDepth(depth int) {
	if n == nil || n.timeoutArtifacts == nil {
		return
	}
	n.timeoutArtifacts.recordMaterializerQueueDepth(depth)
}

func (n *Node) recordTimeoutMaterializerLag(slot int, highestCommitted uint64, materializedCommitted uint64) {
	if n == nil || n.timeoutArtifacts == nil {
		return
	}
	n.timeoutArtifacts.recordMaterializerLag(slot, highestCommitted, materializedCommitted)
}

func (n *Node) captureWriteTimeout(slot int, sequence uint64, role ReplicaRole, err error) {
	if n == nil || n.timeoutArtifacts == nil {
		return
	}
	var recent []writeTraceEvent
	if n.writeTrace != nil {
		recent = n.writeTrace.recentForSlot(slot, sequence, 32)
	}
	n.timeoutArtifacts.capture(n.nodeID, slot, sequence, role, err, recent)
}
