package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const defaultWriteTraceSampleRate = 1024
const defaultRecentWriteTraceEvents = 512

type writeTraceConfig struct {
	NodeID     string
	OutputPath string
	SampleRate int
}

type writeTraceRecorder struct {
	nodeID     string
	sampleRate uint64
	file       *os.File
	mu         sync.Mutex
	recent     []writeTraceEvent
}

type writeTraceEvent struct {
	At           time.Time   `json:"at"`
	NodeID       string      `json:"node_id"`
	Slot         int         `json:"slot"`
	Sequence     uint64      `json:"sequence"`
	ChainVersion uint64      `json:"chain_version"`
	Role         ReplicaRole `json:"role"`
	Stage        string      `json:"stage"`
}

func openWriteTraceRecorder(cfg writeTraceConfig) (*writeTraceRecorder, error) {
	if cfg.OutputPath == "" {
		return nil, nil
	}
	if cfg.SampleRate <= 0 {
		cfg.SampleRate = defaultWriteTraceSampleRate
	}
	if err := os.MkdirAll(filepath.Dir(cfg.OutputPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir write trace dir: %w", err)
	}
	file, err := os.OpenFile(cfg.OutputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open write trace output: %w", err)
	}
	return &writeTraceRecorder{
		nodeID:     cfg.NodeID,
		sampleRate: uint64(cfg.SampleRate),
		file:       file,
	}, nil
}

func (r *writeTraceRecorder) Close() error {
	if r == nil || r.file == nil {
		return nil
	}
	return r.file.Close()
}

func (r *writeTraceRecorder) shouldSample(slot int, sequence uint64, chainVersion uint64) bool {
	if r == nil || r.sampleRate <= 1 {
		return r != nil
	}
	h := fnv.New64a()
	var buf [24]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(slot))
	binary.LittleEndian.PutUint64(buf[8:16], sequence)
	binary.LittleEndian.PutUint64(buf[16:24], chainVersion)
	_, _ = h.Write(buf[:])
	return h.Sum64()%r.sampleRate == 0
}

func (r *writeTraceRecorder) record(slot int, sequence uint64, chainVersion uint64, role ReplicaRole, stage string) {
	if r == nil || r.file == nil {
		return
	}
	if !r.shouldSample(slot, sequence, chainVersion) {
		return
	}
	event := writeTraceEvent{
		At:           time.Now().UTC(),
		NodeID:       r.nodeID,
		Slot:         slot,
		Sequence:     sequence,
		ChainVersion: chainVersion,
		Role:         role,
		Stage:        stage,
	}
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recent = appendBoundedWriteTraceEvent(r.recent, event, defaultRecentWriteTraceEvents)
	_, _ = r.file.Write(append(data, '\n'))
}

func (r *writeTraceRecorder) recentForSlot(slot int, sequence uint64, limit int) []writeTraceEvent {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		limit = 32
	}
	result := make([]writeTraceEvent, 0, limit)
	for i := len(r.recent) - 1; i >= 0 && len(result) < limit; i-- {
		event := r.recent[i]
		if event.Slot != slot {
			continue
		}
		if sequence > 0 {
			if event.Sequence+8 < sequence || event.Sequence > sequence+8 {
				continue
			}
		}
		result = append(result, event)
	}
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

func (n *Node) traceWriteEvent(assignment ReplicaAssignment, sequence uint64, stage string) {
	if n == nil || n.writeTrace == nil {
		return
	}
	n.writeTrace.record(assignment.Slot, sequence, assignment.ChainVersion, assignment.Role, stage)
}

func appendBoundedWriteTraceEvent(events []writeTraceEvent, event writeTraceEvent, limit int) []writeTraceEvent {
	events = append(events, event)
	if len(events) <= limit {
		return events
	}
	copy(events, events[len(events)-limit:])
	return events[:limit]
}

func writeTraceCommitIntentStage(role ReplicaRole) string {
	switch role {
	case ReplicaRoleHead:
		return "head_commit_intent_queued"
	case ReplicaRoleMiddle:
		return "middle_commit_intent_queued"
	case ReplicaRoleTail:
		return "tail_commit_intent_queued"
	case ReplicaRoleSingle:
		return "single_commit_intent_queued"
	default:
		return ""
	}
}

func writeTraceFlushStartStage(role ReplicaRole) string {
	switch role {
	case ReplicaRoleHead:
		return "head_flush_start"
	case ReplicaRoleMiddle:
		return "middle_flush_start"
	case ReplicaRoleTail:
		return "tail_flush_start"
	case ReplicaRoleSingle:
		return "single_flush_start"
	default:
		return ""
	}
}

func writeTraceFlushEndStage(role ReplicaRole) string {
	switch role {
	case ReplicaRoleHead:
		return "head_flush_end"
	case ReplicaRoleMiddle:
		return "middle_flush_end"
	case ReplicaRoleTail:
		return "tail_flush_end"
	case ReplicaRoleSingle:
		return "single_flush_end"
	default:
		return ""
	}
}

func writeTraceCommitAcceptReceivedStage(role ReplicaRole) string {
	switch role {
	case ReplicaRoleHead:
		return "head_commit_accept_received"
	case ReplicaRoleMiddle:
		return "middle_commit_accept_received"
	default:
		return ""
	}
}

type pipelineMetrics struct {
	commitFlushDuration *prometheus.HistogramVec
	commitOwnerCallback *prometheus.HistogramVec
	commitBatchOps      prometheus.Histogram
	commitBatchBytes    prometheus.Histogram
	sessionQueueWait    *prometheus.HistogramVec
	sessionQueueDepth   *prometheus.GaugeVec
}

func newPipelineMetrics(registry *prometheus.Registry) *pipelineMetrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	m := &pipelineMetrics{
		commitFlushDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_commit_flush_seconds",
			Help:    "Latency of durable commit-engine flushes.",
			Buckets: prometheus.DefBuckets,
		}, []string{"role", "result"}),
		commitOwnerCallback: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_commit_owner_callback_delay_seconds",
			Help:    "Delay between durable completion and owner-loop callback execution.",
			Buckets: prometheus.DefBuckets,
		}, []string{"role", "result"}),
		commitBatchOps: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_commit_batch_ops",
			Help:    "Durable commit-engine batch sizes in operations.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		}),
		commitBatchBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_commit_batch_bytes",
			Help:    "Estimated durable commit-engine batch sizes in bytes.",
			Buckets: []float64{256, 512, 1024, 2048, 4096, 8192, 16384, 65536, 262144, 1048576},
		}),
		sessionQueueWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_replication_session_queue_wait_seconds",
			Help:    "Queue wait before a replication request is written to the session stream.",
			Buckets: prometheus.DefBuckets,
		}, []string{"kind", "target"}),
		sessionQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "craq_storage_replication_session_queue_depth_high_water",
			Help: "High-water mark for per-target replication session queue depth.",
		}, []string{"kind", "target"}),
	}
	registry.MustRegister(
		m.commitFlushDuration,
		m.commitOwnerCallback,
		m.commitBatchOps,
		m.commitBatchBytes,
		m.sessionQueueWait,
		m.sessionQueueDepth,
	)
	return m
}

func (n *Node) observeCommitFlush(role ReplicaRole, result string, dur time.Duration) {
	if n == nil || n.pipelineMetrics == nil {
		return
	}
	if result == "" {
		result = writeStageResultSuccess
	}
	n.pipelineMetrics.commitFlushDuration.WithLabelValues(string(role), result).Observe(dur.Seconds())
}

func (n *Node) observeCommitOwnerCallbackDelay(role ReplicaRole, result string, dur time.Duration) {
	if n == nil || n.pipelineMetrics == nil {
		return
	}
	if result == "" {
		result = writeStageResultSuccess
	}
	n.pipelineMetrics.commitOwnerCallback.WithLabelValues(string(role), result).Observe(dur.Seconds())
}

func (n *Node) observeCommitBatchSize(ops int, bytes int) {
	if n == nil || n.pipelineMetrics == nil {
		return
	}
	n.pipelineMetrics.commitBatchOps.Observe(float64(ops))
	n.pipelineMetrics.commitBatchBytes.Observe(float64(bytes))
}

func (n *Node) ObserveReplicationSessionQueueWait(kind string, target string, wait time.Duration) {
	if n == nil || n.pipelineMetrics == nil {
		return
	}
	n.pipelineMetrics.sessionQueueWait.WithLabelValues(kind, target).Observe(wait.Seconds())
}

func (n *Node) ObserveReplicationSessionQueueDepthHighWater(kind string, target string, depth int) {
	if n == nil || n.pipelineMetrics == nil {
		return
	}
	n.pipelineMetrics.sessionQueueDepth.WithLabelValues(kind, target).Set(float64(depth))
}
