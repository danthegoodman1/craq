package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const defaultWriteTraceSampleRate = 1024
const defaultRecentWriteTraceEvents = 512
const defaultFailureWriteTraceEvents = 256
const defaultFailureWriteTraceIdle = 60 * time.Second

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
	failure    map[int]slotWriteTraceRing
}

type slotWriteTraceRing struct {
	updatedAt time.Time
	events    []writeTraceEvent
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
		failure:    map[int]slotWriteTraceRing{},
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
	event := writeTraceEvent{
		At:           time.Now().UTC(),
		NodeID:       r.nodeID,
		Slot:         slot,
		Sequence:     sequence,
		ChainVersion: chainVersion,
		Role:         role,
		Stage:        stage,
	}
	shouldSample := r.shouldSample(slot, sequence, chainVersion)
	var data []byte
	if shouldSample {
		encoded, err := json.Marshal(event)
		if err != nil {
			return
		}
		data = encoded
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.recordFailureEventLocked(event)
	if !shouldSample {
		return
	}
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

func (r *writeTraceRecorder) failureRecentForSlot(slot int, sequence uint64, limit int) []writeTraceEvent {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if limit <= 0 {
		limit = defaultFailureWriteTraceEvents
	}
	trace, ok := r.failure[slot]
	if !ok {
		return nil
	}
	result := make([]writeTraceEvent, 0, limit)
	for i := len(trace.events) - 1; i >= 0 && len(result) < limit; i-- {
		event := trace.events[i]
		if sequence > 0 {
			if event.Sequence+128 < sequence || event.Sequence > sequence+128 {
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

func (r *writeTraceRecorder) recordFailureEventLocked(event writeTraceEvent) {
	if r.failure == nil {
		r.failure = map[int]slotWriteTraceRing{}
	}
	now := event.At
	for slot, trace := range r.failure {
		if slot == event.Slot {
			continue
		}
		if now.Sub(trace.updatedAt) > defaultFailureWriteTraceIdle {
			delete(r.failure, slot)
		}
	}
	trace := r.failure[event.Slot]
	trace.updatedAt = now
	trace.events = appendBoundedWriteTraceEvent(trace.events, event, defaultFailureWriteTraceEvents)
	r.failure[event.Slot] = trace
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

func writeTracePrepareFlushStartStage(role ReplicaRole) string {
	switch role {
	case ReplicaRoleHead:
		return "head_prepare_flush_start"
	case ReplicaRoleMiddle:
		return "middle_prepare_flush_start"
	case ReplicaRoleTail:
		return "tail_prepare_flush_start"
	case ReplicaRoleSingle:
		return "single_prepare_flush_start"
	default:
		return ""
	}
}

func writeTracePrepareFlushEndStage(role ReplicaRole) string {
	switch role {
	case ReplicaRoleHead:
		return "head_prepare_flush_end"
	case ReplicaRoleMiddle:
		return "middle_prepare_flush_end"
	case ReplicaRoleTail:
		return "tail_prepare_flush_end"
	case ReplicaRoleSingle:
		return "single_prepare_flush_end"
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
	commitFlushDuration    *prometheus.HistogramVec
	commitOwnerCallback    *prometheus.HistogramVec
	commitBatchOps         prometheus.Histogram
	commitBatchSlots       prometheus.Histogram
	commitBatchBytes       prometheus.Histogram
	confirmBatchCount      prometheus.Histogram
	coalescedConfirms      prometheus.Counter
	journalFlushStage      *prometheus.HistogramVec
	journalFlushBatchOps   *prometheus.HistogramVec
	journalFlushBatchBytes *prometheus.HistogramVec
	journalFlushBatchSlots *prometheus.HistogramVec
	journalFlushRecords    *prometheus.CounterVec
	journalPendingItems    *prometheus.GaugeVec
	journalPendingSlots    *prometheus.GaugeVec
	journalPendingConfs    *prometheus.GaugeVec
	sessionQueueWait       *prometheus.HistogramVec
	sessionQueueDepth      *prometheus.GaugeVec
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
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
		}),
		commitBatchSlots: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_commit_batch_slots",
			Help:    "Unique slots touched by a durable commit-engine batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
		}),
		commitBatchBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_commit_batch_bytes",
			Help:    "Estimated durable commit-engine batch sizes in bytes.",
			Buckets: []float64{256, 512, 1024, 2048, 4096, 8192, 16384, 65536, 262144, 1048576},
		}),
		confirmBatchCount: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_confirm_batch_count",
			Help:    "Coalesced upstream-confirm records appended in a journal batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
		}),
		coalescedConfirms: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "craq_storage_confirm_coalesced_total",
			Help: "Upstream confirm intents merged into an existing pending confirm item.",
		}),
		journalFlushStage: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_journal_flush_stage_seconds",
			Help:    "Breakdown of journal append batches by flush sub-stage.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stage", "shard", "result", "experiment"}),
		journalFlushBatchOps: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_journal_flush_batch_ops",
			Help:    "Operations appended in a journal flush batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128},
		}, []string{"shard", "experiment"}),
		journalFlushBatchBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_journal_flush_batch_bytes",
			Help:    "Encoded bytes appended in a journal flush batch.",
			Buckets: []float64{256, 512, 1024, 2048, 4096, 8192, 16384, 65536, 262144, 1048576},
		}, []string{"shard", "experiment"}),
		journalFlushBatchSlots: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_journal_flush_batch_slots",
			Help:    "Unique slots touched by a journal flush batch.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
		}, []string{"shard", "experiment"}),
		journalFlushRecords: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_journal_flush_records_total",
			Help: "Journal records appended by type.",
		}, []string{"type", "shard", "experiment"}),
		journalPendingItems: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "craq_storage_journal_pending_commit_items",
			Help: "Current pending commit items per journal shard.",
		}, []string{"shard"}),
		journalPendingSlots: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "craq_storage_journal_pending_commit_slots",
			Help: "Current slots with pending commit items per journal shard.",
		}, []string{"shard"}),
		journalPendingConfs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "craq_storage_journal_pending_confirm_items",
			Help: "Current pending coalesced confirm items per journal shard.",
		}, []string{"shard"}),
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
		m.commitBatchSlots,
		m.commitBatchBytes,
		m.confirmBatchCount,
		m.coalescedConfirms,
		m.journalFlushStage,
		m.journalFlushBatchOps,
		m.journalFlushBatchBytes,
		m.journalFlushBatchSlots,
		m.journalFlushRecords,
		m.journalPendingItems,
		m.journalPendingSlots,
		m.journalPendingConfs,
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

func (n *Node) observeJournalCommitBatchSlots(slots int) {
	if n == nil || n.pipelineMetrics == nil || slots <= 0 {
		return
	}
	n.pipelineMetrics.commitBatchSlots.Observe(float64(slots))
}

func (n *Node) observeJournalConfirmBatchCount(count int) {
	if n == nil || n.pipelineMetrics == nil || count <= 0 {
		return
	}
	n.pipelineMetrics.confirmBatchCount.Observe(float64(count))
}

func (n *Node) observeJournalCommitWatermarkBatchCount(count int) {
	if n == nil || n.pipelineMetrics == nil || count <= 0 {
		return
	}
	n.pipelineMetrics.confirmBatchCount.Observe(float64(count))
}

func (n *Node) observeJournalCoalescedConfirm(count int) {
	if n == nil || n.pipelineMetrics == nil || count <= 0 {
		return
	}
	n.pipelineMetrics.coalescedConfirms.Add(float64(count))
}

func (n *Node) observeJournalFlushBreakdown(shard int, report journalAppendReport, result string) {
	if n == nil || n.pipelineMetrics == nil {
		return
	}
	if result == "" {
		result = writeStageResultSuccess
	}
	experiment := string(NormalizeJournalExperiment(report.Experiment))
	if experiment == "" {
		experiment = string(defaultJournalExperiment)
	}
	shardLabel := strconv.Itoa(shard)
	for stage, dur := range map[string]time.Duration{
		"encode": report.Encode,
		"write":  report.Write,
		"sync":   report.Sync,
		"total":  report.Total,
	} {
		if dur <= 0 {
			continue
		}
		n.pipelineMetrics.journalFlushStage.WithLabelValues(stage, shardLabel, result, experiment).Observe(dur.Seconds())
	}
	if report.BatchOps > 0 {
		n.pipelineMetrics.journalFlushBatchOps.WithLabelValues(shardLabel, experiment).Observe(float64(report.BatchOps))
	}
	if report.BatchBytes > 0 {
		n.pipelineMetrics.journalFlushBatchBytes.WithLabelValues(shardLabel, experiment).Observe(float64(report.BatchBytes))
	}
	if report.Slots > 0 {
		n.pipelineMetrics.journalFlushBatchSlots.WithLabelValues(shardLabel, experiment).Observe(float64(report.Slots))
	}
	for recordType, count := range report.Records {
		if count <= 0 {
			continue
		}
		n.pipelineMetrics.journalFlushRecords.WithLabelValues(string(recordType), shardLabel, experiment).Add(float64(count))
	}
}

func (n *Node) observeJournalShardPending(shard int, commitItems int, commitSlots int, confirmItems int) {
	if n == nil || n.pipelineMetrics == nil {
		return
	}
	label := strconv.Itoa(shard)
	n.pipelineMetrics.journalPendingItems.WithLabelValues(label).Set(float64(commitItems))
	n.pipelineMetrics.journalPendingSlots.WithLabelValues(label).Set(float64(commitSlots))
	n.pipelineMetrics.journalPendingConfs.WithLabelValues(label).Set(float64(confirmItems))
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
