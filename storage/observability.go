package storage

import (
	"errors"
	"time"

	"github.com/danthegoodman1/craq/gologger"
	"github.com/danthegoodman1/craq/ops"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type ResourceUsage struct {
	InFlightClientWritesPerNode    int         `json:"in_flight_client_writes_per_node"`
	InFlightClientWritesPerSlot    map[int]int `json:"in_flight_client_writes_per_slot"`
	BufferedReplicaMessagesPerNode int         `json:"buffered_replica_messages_per_node"`
	BufferedReplicaMessagesPerSlot map[int]int `json:"buffered_replica_messages_per_slot"`
	DirtyKeysPerSlot               map[int]int `json:"dirty_keys_per_slot"`
	ActiveCatchups                 int         `json:"active_catchups"`
}

type AdminState struct {
	Node      NodeState     `json:"node"`
	Resources ResourceUsage `json:"resources"`
	Recent    []ops.Event   `json:"recent_events"`
}

type eventRecorder struct {
	component string
	nodeID    string
	ring      *ops.EventRing
}

type nodeMetrics struct {
	registry               *prometheus.Registry
	clientReads            *prometheus.CounterVec
	clientWrites           *prometheus.CounterVec
	ambiguousWrites        prometheus.Counter
	conditionFailures      prometheus.Counter
	writeWaitDuration      prometheus.Histogram
	writeStageTransitions  *prometheus.CounterVec
	writeStageDuration     *prometheus.HistogramVec
	tailResolutions        *prometheus.CounterVec
	tailResolutionDuration prometheus.Histogram
	readDependencyFailures prometheus.Counter
	replicationForwards    *prometheus.CounterVec
	replicationCommits     *prometheus.CounterVec
	catchupOps             *prometheus.CounterVec
	catchupDuration        prometheus.Histogram
	backpressureRejections *prometheus.CounterVec
	inFlightWrites         prometheus.Gauge
	bufferedReplicaMsgs    prometheus.Gauge
	catchups               prometheus.Gauge
}

func loggerFromConfig(logger *zerolog.Logger) zerolog.Logger {
	if logger != nil {
		return logger.With().Logger()
	}
	return gologger.NewLogger()
}

func newEventRecorder(component string, nodeID string) *eventRecorder {
	return &eventRecorder{
		component: component,
		nodeID:    nodeID,
		ring:      ops.NewEventRing(64),
	}
}

func (r *eventRecorder) record(
	logger zerolog.Logger,
	level zerolog.Level,
	kind string,
	message string,
	slot *int,
	chainVersion *uint64,
	sequence *uint64,
	peerNodeID string,
	commandID string,
	err error,
) {
	if r == nil {
		return
	}
	event := ops.Event{
		Time:         time.Now().UTC(),
		Level:        level.String(),
		Component:    r.component,
		Kind:         kind,
		Message:      message,
		NodeID:       r.nodeID,
		Slot:         slot,
		ChainVersion: chainVersion,
		Sequence:     sequence,
		PeerNodeID:   peerNodeID,
		CommandID:    commandID,
	}
	if err != nil {
		event.Error = err.Error()
	}
	r.ring.Add(event)

	scoped := logger.With().
		Str("component", r.component).
		Str("node_id", r.nodeID).
		Str("kind", kind).
		Logger()
	entry := scoped.WithLevel(level)
	if slot != nil {
		entry = entry.Int("slot", *slot)
	}
	if chainVersion != nil {
		entry = entry.Uint64("chain_version", *chainVersion)
	}
	if sequence != nil {
		entry = entry.Uint64("sequence", *sequence)
	}
	if peerNodeID != "" {
		entry = entry.Str("peer_node_id", peerNodeID)
	}
	if commandID != "" {
		entry = entry.Str("command_id", commandID)
	}
	if err != nil {
		entry = entry.AnErr("error", err)
	}
	entry.Msg(message)
}

func (r *eventRecorder) snapshot() []ops.Event {
	if r == nil {
		return nil
	}
	return r.ring.Snapshot()
}

func newNodeMetrics(registry *prometheus.Registry) *nodeMetrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	m := &nodeMetrics{
		registry: registry,
		clientReads: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_client_reads_total",
			Help: "Client read requests handled by storage nodes.",
		}, []string{"consistency", "result"}),
		clientWrites: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_client_writes_total",
			Help: "Client write and delete requests handled by storage nodes.",
		}, []string{"kind", "result"}),
		ambiguousWrites: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "craq_storage_ambiguous_writes_total",
			Help: "Ambiguous writes returned by storage nodes.",
		}),
		conditionFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "craq_storage_condition_failures_total",
			Help: "Conditional write failures returned by storage nodes.",
		}),
		writeWaitDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_write_wait_seconds",
			Help:    "Client write latency observed at storage nodes.",
			Buckets: prometheus.DefBuckets,
		}),
		writeStageTransitions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_write_stage_total",
			Help: "Completed write-path stages observed at storage nodes.",
		}, []string{"stage", "role", "result"}),
		writeStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "craq_storage_write_stage_seconds",
			Help:    "Latency of fixed write-path stages observed at storage nodes.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stage", "role", "result"}),
		tailResolutions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_tail_resolutions_total",
			Help: "Tail committed-sequence queries performed for CRAQ linearizable reads.",
		}, []string{"result"}),
		tailResolutionDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_tail_resolution_seconds",
			Help:    "Latency of CRAQ tail committed-sequence queries.",
			Buckets: prometheus.DefBuckets,
		}),
		readDependencyFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "craq_storage_read_dependency_failures_total",
			Help: "Linearizable CRAQ reads that failed to resolve through the tail.",
		}),
		replicationForwards: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_replication_forwards_total",
			Help: "Forward write RPCs handled by storage nodes.",
		}, []string{"result"}),
		replicationCommits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_replication_commits_total",
			Help: "Commit write RPCs handled by storage nodes.",
		}, []string{"result"}),
		catchupOps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_catchup_operations_total",
			Help: "Catch-up and recovery operations handled by storage nodes.",
		}, []string{"kind", "result"}),
		catchupDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_storage_catchup_duration_seconds",
			Help:    "Catch-up and recovery operation durations.",
			Buckets: prometheus.DefBuckets,
		}),
		backpressureRejections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_storage_backpressure_rejections_total",
			Help: "Backpressure rejections by resource.",
		}, []string{"resource"}),
		inFlightWrites: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "craq_storage_in_flight_client_writes",
			Help: "Current admitted in-flight client writes.",
		}),
		bufferedReplicaMsgs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "craq_storage_buffered_replica_messages",
			Help: "Current buffered replica messages across slots.",
		}),
		catchups: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "craq_storage_active_catchups",
			Help: "Current active catch-up operations.",
		}),
	}
	registry.MustRegister(
		m.clientReads,
		m.clientWrites,
		m.ambiguousWrites,
		m.conditionFailures,
		m.writeWaitDuration,
		m.writeStageTransitions,
		m.writeStageDuration,
		m.tailResolutions,
		m.tailResolutionDuration,
		m.readDependencyFailures,
		m.replicationForwards,
		m.replicationCommits,
		m.catchupOps,
		m.catchupDuration,
		m.backpressureRejections,
		m.inFlightWrites,
		m.bufferedReplicaMsgs,
		m.catchups,
	)
	return m
}

func (m *nodeMetrics) Registry() *prometheus.Registry {
	if m == nil {
		return prometheus.NewRegistry()
	}
	return m.registry
}

func (n *Node) MetricsRegistry() *prometheus.Registry {
	return n.metrics.Registry()
}

func (n *Node) RecentEvents() []ops.Event {
	return n.events.snapshot()
}

func (n *Node) ResourceUsage() ResourceUsage {
	replicas := n.publishedReplicaMapSnapshot()
	n.mu.RLock()
	usage := ResourceUsage{
		InFlightClientWritesPerNode:    n.inFlightClientWrites,
		InFlightClientWritesPerSlot:    make(map[int]int, len(replicas)),
		BufferedReplicaMessagesPerNode: n.publishedBufferedReplicaMessages,
		BufferedReplicaMessagesPerSlot: make(map[int]int, len(replicas)),
		DirtyKeysPerSlot:               make(map[int]int, len(replicas)),
		ActiveCatchups:                 n.inFlightCatchups,
	}
	n.mu.RUnlock()
	for slot, record := range replicas {
		usage.InFlightClientWritesPerSlot[slot] = record.inFlightClientWrites
		usage.BufferedReplicaMessagesPerSlot[slot] = record.bufferedReplicaMessages()
		usage.DirtyKeysPerSlot[slot] = record.dirtyKeyCount
	}
	return usage
}

type writeStage string

const (
	writeStageHeadGetCommitted  writeStage = "head_get_committed"
	writeStageHeadStageOp       writeStage = "head_stage_operation"
	writeStageHeadForwardRPC    writeStage = "head_forward_rpc"
	writeStageHeadWaitForCommit writeStage = "head_wait_for_commit"
	writeStageTailApplyCommit   writeStage = "tail_apply_committed"
	writeStageCommitUpstreamRPC writeStage = "commit_upstream_rpc"
	writeStageSingleApplyCommit writeStage = "single_apply_committed"
)

const (
	writeStageResultSuccess = "success"
	writeStageResultError   = "error"
)

func (n *Node) observeWriteStage(stage writeStage, role ReplicaRole, result string, dur time.Duration) {
	if n.metrics == nil {
		return
	}
	stageLabel := string(stage)
	roleLabel := string(role)
	if roleLabel == "" {
		roleLabel = "unknown"
	}
	if result == "" {
		result = writeStageResultSuccess
	}
	n.metrics.writeStageTransitions.WithLabelValues(stageLabel, roleLabel, result).Inc()
	n.metrics.writeStageDuration.WithLabelValues(stageLabel, roleLabel, result).Observe(dur.Seconds())
}

func writeStageResult(err error) string {
	if err != nil {
		return writeStageResultError
	}
	return writeStageResultSuccess
}

func writeCommitApplyStage(role ReplicaRole) (writeStage, bool) {
	switch role {
	case ReplicaRoleSingle:
		return writeStageSingleApplyCommit, true
	case ReplicaRoleHead, ReplicaRoleMiddle, ReplicaRoleTail:
		return writeStageTailApplyCommit, true
	default:
		return "", false
	}
}

func (n *Node) AdminState() AdminState {
	n.refreshMetricGauges()
	return AdminState{
		Node:      n.State(),
		Resources: n.ResourceUsage(),
		Recent:    n.RecentEvents(),
	}
}

func (n *Node) refreshMetricGauges() {
	if n.metrics == nil {
		return
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	n.refreshMetricGaugesLocked()
}

func (n *Node) refreshMetricGaugesLocked() {
	if n.metrics == nil {
		return
	}
	inFlightWrites := n.inFlightClientWrites
	bufferedReplicaMessages := n.publishedBufferedReplicaMessages
	catchups := n.inFlightCatchups
	n.metrics.inFlightWrites.Set(float64(inFlightWrites))
	n.metrics.bufferedReplicaMsgs.Set(float64(bufferedReplicaMessages))
	n.metrics.catchups.Set(float64(catchups))
}

func (n *Node) observeBackpressure(err error) {
	var pressure *BackpressureError
	if !errors.As(err, &pressure) {
		return
	}
	if n.metrics != nil {
		n.metrics.backpressureRejections.WithLabelValues(string(pressure.Resource)).Inc()
	}
	n.events.record(
		n.logger,
		zerolog.WarnLevel,
		"backpressure_rejected",
		"storage backpressure rejected work",
		ops.IntPtr(pressure.Slot),
		nil,
		nil,
		"",
		"",
		err,
	)
}
