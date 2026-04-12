package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	runtimemetrics "runtime/metrics"
	"sort"
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

type slotSequenceBreadcrumb struct {
	Sequence uint64    `json:"sequence"`
	At       time.Time `json:"at,omitempty"`
}

type sequenceWindowSnapshot struct {
	Count     int      `json:"count"`
	Lowest    uint64   `json:"lowest_sequence,omitempty"`
	Highest   uint64   `json:"highest_sequence,omitempty"`
	Sequences []uint64 `json:"sequences,omitempty"`
}

type sequenceCountSnapshot struct {
	Sequence uint64 `json:"sequence"`
	Count    int    `json:"count"`
}

type acceptedCommitSnapshot struct {
	Sequence    uint64              `json:"sequence"`
	Stage       acceptedCommitStage `json:"stage"`
	WaiterCount int                 `json:"waiter_count"`
}

type slotOwnerTimeoutSnapshot struct {
	Exists                           bool                     `json:"exists"`
	Assignment                       ReplicaAssignment        `json:"assignment"`
	State                            ReplicaState             `json:"state"`
	NextSequence                     uint64                   `json:"next_sequence"`
	HighestCommittedSequence         uint64                   `json:"highest_committed_sequence"`
	MaterializedCommittedSequence    uint64                   `json:"materialized_committed_sequence"`
	JournalDurableHighWater          uint64                   `json:"journal_durable_high_water"`
	HighestUpstreamConfirmedSequence uint64                   `json:"highest_upstream_confirmed_sequence"`
	CommitEffectInFlight             bool                     `json:"commit_effect_in_flight"`
	CommitEffectSequence             uint64                   `json:"commit_effect_sequence"`
	UpstreamCommitInFlight           bool                     `json:"upstream_commit_in_flight"`
	ProgressionGap                   bool                     `json:"progression_gap"`
	ProgressionGapSequence           uint64                   `json:"progression_gap_sequence"`
	BufferedForwards                 sequenceWindowSnapshot   `json:"buffered_forwards"`
	BufferedCommits                  sequenceWindowSnapshot   `json:"buffered_commits"`
	ParkedCommitAcceptWaiters        []sequenceCountSnapshot  `json:"parked_commit_accept_waiters,omitempty"`
	AcceptedCommitLedger             []acceptedCommitSnapshot `json:"accepted_commit_ledger,omitempty"`
	LastAcceptCommitReceived         slotSequenceBreadcrumb   `json:"last_accept_commit_received,omitempty"`
	LastDuplicateCommitParked        slotSequenceBreadcrumb   `json:"last_duplicate_commit_parked,omitempty"`
	LastReconciledFromJournal        slotSequenceBreadcrumb   `json:"last_reconciled_from_journal,omitempty"`
	LastAppliedLocally               slotSequenceBreadcrumb   `json:"last_applied_locally,omitempty"`
	LastWaiterReleased               slotSequenceBreadcrumb   `json:"last_waiter_released,omitempty"`
}

type journalRecordSnapshot struct {
	Type                      string        `json:"type"`
	ChainVersion              uint64        `json:"chain_version"`
	Sequence                  uint64        `json:"sequence"`
	Kind                      OperationKind `json:"kind,omitempty"`
	Key                       string        `json:"key,omitempty"`
	UpstreamConfirmedSequence uint64        `json:"upstream_confirmed_sequence,omitempty"`
}

type journalSlotSnapshot struct {
	Shard                     int                    `json:"shard"`
	QueueDepth                int                    `json:"queue_depth"`
	LastFlushAt               time.Time              `json:"last_flush_at,omitempty"`
	DurableCommittedHighWater uint64                 `json:"durable_committed_high_water"`
	RecentRecords             []journalRecordSnapshot `json:"recent_records,omitempty"`
}

type writeTimeoutArtifact struct {
	At                 time.Time                         `json:"at"`
	NodeID             string                            `json:"node_id"`
	Slot               int                               `json:"slot"`
	Sequence           uint64                            `json:"sequence"`
	Role               ReplicaRole                       `json:"role"`
	Error              string                            `json:"error"`
	JournalQueueDepths []queueDepthSample                `json:"journal_queue_depths"`
	MaterializerQueues []queueDepthSample                `json:"materializer_queue_depths"`
	MaterializerLags   []materializerLagSample           `json:"materializer_lags"`
	RecentWriteTrace   []writeTraceEvent                 `json:"recent_write_trace_events,omitempty"`
	RecentSlotTrace    []writeTraceEvent                 `json:"recent_slot_trace_events,omitempty"`
	SlotState          slotOwnerTimeoutSnapshot          `json:"slot_state"`
	SessionState       []ReplicationSessionSlotSnapshot  `json:"session_state,omitempty"`
	JournalState       *journalSlotSnapshot              `json:"journal_state,omitempty"`
	Runtime            runtimePauseSnapshot              `json:"runtime"`
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

func (r *writeTimeoutArtifactRecorder) capture(artifact writeTimeoutArtifact) {
	if r == nil || r.file == nil {
		return
	}
	artifact.At = time.Now().UTC()
	artifact.Runtime = captureRuntimePauseSnapshot()
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
	var slotTrace []writeTraceEvent
	if n.writeTrace != nil {
		recent = n.writeTrace.recentForSlot(slot, sequence, 32)
		slotTrace = n.writeTrace.failureRecentForSlot(slot, sequence, defaultFailureWriteTraceEvents)
	}
	n.timeoutArtifacts.capture(writeTimeoutArtifact{
		NodeID:           n.nodeID,
		Slot:             slot,
		Sequence:         sequence,
		Role:             role,
		Error:            errString(err),
		RecentWriteTrace: recent,
		RecentSlotTrace:  slotTrace,
		SlotState:        n.timeoutSlotSnapshot(slot),
		SessionState:     n.timeoutSessionSnapshots(slot),
		JournalState:     n.timeoutJournalSnapshot(slot),
	})
}

func (n *Node) timeoutSlotSnapshot(slot int) slotOwnerTimeoutSnapshot {
	owner := n.existingSlotOwner(slot)
	if owner == nil {
		return slotOwnerTimeoutSnapshot{JournalDurableHighWater: n.durableCommittedSequence(slot)}
	}
	respCh := make(chan slotOwnerTimeoutSnapshot, 1)
	if err := owner.dispatch(n.runtimeCtx, func(runtime *slotRuntime) {
		respCh <- runtime.timeoutSnapshot()
	}); err != nil {
		return slotOwnerTimeoutSnapshot{JournalDurableHighWater: n.durableCommittedSequence(slot)}
	}
	select {
	case <-n.done:
		return slotOwnerTimeoutSnapshot{JournalDurableHighWater: n.durableCommittedSequence(slot)}
	case snapshot := <-respCh:
		if snapshot.JournalDurableHighWater == 0 {
			snapshot.JournalDurableHighWater = n.durableCommittedSequence(slot)
		}
		return snapshot
	}
}

func (n *Node) timeoutSessionSnapshots(slot int) []ReplicationSessionSlotSnapshot {
	snapshotter, ok := n.repl.(replicationTransportSessionSnapshotter)
	if !ok {
		return nil
	}
	return snapshotter.ReplicationSessionSnapshots(slot)
}

func (n *Node) timeoutJournalSnapshot(slot int) *journalSlotSnapshot {
	if n == nil || n.commitJournal == nil {
		return nil
	}
	return n.commitJournal.snapshot(slot)
}

func (rt *slotRuntime) timeoutSnapshot() slotOwnerTimeoutSnapshot {
	snapshot := slotOwnerTimeoutSnapshot{
		Exists:                  rt.exists,
		CommitEffectInFlight:    rt.commitEffectInFlight,
		CommitEffectSequence:    rt.commitEffectSequence,
		UpstreamCommitInFlight:  rt.upstreamCommitInFlight,
		ProgressionGap:          rt.progressionGap,
		ProgressionGapSequence:  rt.progressionGapSequence,
		JournalDurableHighWater: rt.node.durableCommittedSequence(rt.slot),
	}
	if !rt.exists {
		return snapshot
	}
	record := ensureProtocolReplicaState(rt.record)
	snapshot.Assignment = cloneAssignment(record.assignment)
	snapshot.State = record.state
	snapshot.NextSequence = record.nextSequence
	snapshot.HighestCommittedSequence = record.highestCommittedSequence
	snapshot.MaterializedCommittedSequence = record.materializedCommittedSequence
	snapshot.HighestUpstreamConfirmedSequence = record.highestUpstreamConfirmedSequence
	snapshot.BufferedForwards = snapshotSequenceWindow(record.bufferedForwards)
	snapshot.BufferedCommits = snapshotCommitSequenceWindow(record.bufferedCommits)
	snapshot.ParkedCommitAcceptWaiters = snapshotAcceptedWaiterCounts(rt.acceptedCommits)
	snapshot.AcceptedCommitLedger = snapshotAcceptedCommitLedger(rt.acceptedCommits)
	snapshot.LastAcceptCommitReceived = rt.lastAcceptCommitReceived
	snapshot.LastDuplicateCommitParked = rt.lastDuplicateCommitParked
	snapshot.LastReconciledFromJournal = rt.lastReconciledFromJournal
	snapshot.LastAppliedLocally = rt.lastAppliedLocally
	snapshot.LastWaiterReleased = rt.lastWaiterReleased
	return snapshot
}

func snapshotSequenceWindow(requests map[uint64]ForwardWriteRequest) sequenceWindowSnapshot {
	if len(requests) == 0 {
		return sequenceWindowSnapshot{}
	}
	sequences := make([]uint64, 0, len(requests))
	for sequence := range requests {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequenceWindowSnapshot{
		Count:     len(sequences),
		Lowest:    sequences[0],
		Highest:   sequences[len(sequences)-1],
		Sequences: sequences,
	}
}

func snapshotCommitSequenceWindow(requests map[uint64]CommitWriteRequest) sequenceWindowSnapshot {
	if len(requests) == 0 {
		return sequenceWindowSnapshot{}
	}
	sequences := make([]uint64, 0, len(requests))
	for sequence := range requests {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequenceWindowSnapshot{
		Count:     len(sequences),
		Lowest:    sequences[0],
		Highest:   sequences[len(sequences)-1],
		Sequences: sequences,
	}
}

func snapshotAcceptedWaiterCounts(entries map[uint64]*acceptedCommitEntry) []sequenceCountSnapshot {
	if len(entries) == 0 {
		return nil
	}
	sequences := make([]uint64, 0, len(entries))
	for sequence := range entries {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	out := make([]sequenceCountSnapshot, 0, len(sequences))
	for _, sequence := range sequences {
		entry := entries[sequence]
		if entry == nil || len(entry.waiters) == 0 {
			continue
		}
		out = append(out, sequenceCountSnapshot{
			Sequence: sequence,
			Count:    len(entry.waiters),
		})
	}
	return out
}

func snapshotAcceptedCommitLedger(entries map[uint64]*acceptedCommitEntry) []acceptedCommitSnapshot {
	if len(entries) == 0 {
		return nil
	}
	sequences := make([]uint64, 0, len(entries))
	for sequence := range entries {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	out := make([]acceptedCommitSnapshot, 0, len(sequences))
	for _, sequence := range sequences {
		entry := entries[sequence]
		if entry == nil {
			continue
		}
		out = append(out, acceptedCommitSnapshot{
			Sequence:    sequence,
			Stage:       entry.stage,
			WaiterCount: len(entry.waiters),
		})
	}
	return out
}
