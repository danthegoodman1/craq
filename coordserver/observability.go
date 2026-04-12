package coordserver

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/gologger"
	"github.com/danthegoodman1/craq/ops"
	"github.com/danthegoodman1/craq/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

type AdminState struct {
	Current             coordruntime.State                         `json:"current"`
	RoutingSnapshot     RoutingSnapshot                            `json:"routing_snapshot"`
	Pending             map[int]PendingWork                        `json:"pending"`
	ActivePeerRefreshes int                                        `json:"active_peer_refresh_count"`
	Heartbeats          map[string]storage.NodeStatus              `json:"heartbeats"`
	Liveness            map[string]coordruntime.NodeLivenessRecord `json:"liveness"`
	UnavailableReplicas map[string][]int                           `json:"unavailable_replicas"`
	LastRecoveryReports map[string]storage.NodeRecoveryReport      `json:"last_recovery_reports"`
	Recent              []ops.Event                                `json:"recent_events"`
}

type RoutingStatus struct {
	RoutingSnapshot        RoutingSnapshot `json:"routing_snapshot"`
	PendingCount           int             `json:"pending_count"`
	OutboxCount            int             `json:"outbox_count"`
	ActivePeerRefreshCount int             `json:"active_peer_refresh_count"`
	SettledSlotCount       int             `json:"settled_slot_count"`
	HealthyNodeCount       int             `json:"healthy_node_count"`
	SuspectNodeCount       int             `json:"suspect_node_count"`
	DeadNodeCount          int             `json:"dead_node_count"`
}

type serverEventRecorder struct {
	ring *ops.EventRing
}

type serverMetrics struct {
	registry            *prometheus.Registry
	commands            *prometheus.CounterVec
	dispatches          *prometheus.CounterVec
	dispatchDuration    prometheus.Histogram
	livenessEvaluations prometheus.Counter
	deadDetections      prometheus.Counter
	flapDetections      prometheus.Counter
	repairs             *prometheus.CounterVec
	pendingGauge        prometheus.Gauge
	unavailableGauge    prometheus.Gauge
}

func coordLoggerFromConfig(logger *zerolog.Logger) zerolog.Logger {
	if logger != nil {
		return logger.With().Logger()
	}
	return gologger.NewLogger()
}

func newServerEventRecorder() *serverEventRecorder {
	return &serverEventRecorder{ring: ops.NewEventRing(64)}
}

func (r *serverEventRecorder) record(
	logger zerolog.Logger,
	level zerolog.Level,
	kind string,
	message string,
	nodeID string,
	slot *int,
	chainVersion *uint64,
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
		Component:    "coordserver",
		Kind:         kind,
		Message:      message,
		NodeID:       nodeID,
		Slot:         slot,
		ChainVersion: chainVersion,
		PeerNodeID:   peerNodeID,
		CommandID:    commandID,
	}
	if err != nil {
		event.Error = err.Error()
	}
	r.ring.Add(event)

	scoped := logger.With().Str("component", "coordserver").Str("kind", kind).Logger()
	entry := scoped.WithLevel(level)
	if nodeID != "" {
		entry = entry.Str("node_id", nodeID)
	}
	if slot != nil {
		entry = entry.Int("slot", *slot)
	}
	if chainVersion != nil {
		entry = entry.Uint64("chain_version", *chainVersion)
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

func (r *serverEventRecorder) snapshot() []ops.Event {
	if r == nil {
		return nil
	}
	return r.ring.Snapshot()
}

func newServerMetrics(registry *prometheus.Registry) *serverMetrics {
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	m := &serverMetrics{
		registry: registry,
		commands: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_coordserver_commands_total",
			Help: "Coordinator commands handled by kind and result.",
		}, []string{"kind", "result"}),
		dispatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_coordserver_dispatches_total",
			Help: "Coordinator dispatch attempts by rpc kind and result.",
		}, []string{"kind", "result"}),
		dispatchDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "craq_coordserver_dispatch_duration_seconds",
			Help:    "Coordinator dispatch duration.",
			Buckets: prometheus.DefBuckets,
		}),
		livenessEvaluations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "craq_coordserver_liveness_evaluations_total",
			Help: "Liveness evaluations run by the coordinator.",
		}),
		deadDetections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "craq_coordserver_dead_detections_total",
			Help: "Nodes transitioned to dead by coordinator liveness policy.",
		}),
		flapDetections: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "craq_coordserver_flap_detections_total",
			Help: "Nodes evicted by coordinator flapping detection.",
		}),
		repairs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "craq_coordserver_repairs_total",
			Help: "Repair operations by kind and result.",
		}, []string{"kind", "result"}),
		pendingGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "craq_coordserver_pending_work",
			Help: "Current pending coordinator work items.",
		}),
		unavailableGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "craq_coordserver_unavailable_replicas",
			Help: "Current unavailable replica slots tracked by coordinator.",
		}),
	}
	registry.MustRegister(
		m.commands,
		m.dispatches,
		m.dispatchDuration,
		m.livenessEvaluations,
		m.deadDetections,
		m.flapDetections,
		m.repairs,
		m.pendingGauge,
		m.unavailableGauge,
	)
	return m
}

func (m *serverMetrics) Registry() *prometheus.Registry {
	if m == nil {
		return prometheus.NewRegistry()
	}
	return m.registry
}

func (s *Server) MetricsRegistry() *prometheus.Registry {
	return s.metrics.Registry()
}

func (s *Server) RecentEvents() []ops.Event {
	return s.events.snapshot()
}

func (s *Server) UnavailableReplicas() map[string][]int {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	out := make(map[string][]int, len(s.unavailableReplicas))
	for nodeID, slots := range s.unavailableReplicas {
		if len(slots) == 0 {
			continue
		}
		nodeSlots := make([]int, 0, len(slots))
		for slot := range slots {
			nodeSlots = append(nodeSlots, slot)
		}
		sort.Ints(nodeSlots)
		out[nodeID] = nodeSlots
	}
	return out
}

func (s *Server) LastRecoveryReports() map[string]storage.NodeRecoveryReport {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	out := make(map[string]storage.NodeRecoveryReport, len(s.lastRecoveryReports))
	for nodeID, report := range s.lastRecoveryReports {
		out[nodeID] = cloneRecoveryReport(report)
	}
	return out
}

func (s *Server) AdminState(ctx context.Context) AdminState {
	s.refreshMetricGauges()
	snapshot := s.routingSnapshotReadOnly()
	current := s.Current()
	pending := s.Pending()
	heartbeats := s.Heartbeats()
	liveness := s.Liveness()
	unavailable := s.UnavailableReplicas()
	lastRecovery := s.LastRecoveryReports()
	if s.ha != nil {
		if published, err := s.readHASnapshot(ctx); err == nil {
			current = published.State
			pending = pendingFromHASnapshot(published)
			heartbeats = published.Heartbeats
			liveness = mergeLivenessRecords(nil, published.State.NodeLivenessByID)
			unavailable = make(map[string][]int, len(published.UnavailableReplicas))
			for nodeID, slots := range published.UnavailableReplicas {
				nodeSlots := make([]int, 0, len(slots))
				for slot := range slots {
					nodeSlots = append(nodeSlots, slot)
				}
				sort.Ints(nodeSlots)
				unavailable[nodeID] = nodeSlots
			}
			lastRecovery = published.LastRecoveryReports
		}
	}
	return AdminState{
		Current:             current,
		RoutingSnapshot:     snapshot,
		Pending:             pending,
		ActivePeerRefreshes: len(s.snapshotActivePeerRefreshSlots()),
		Heartbeats:          heartbeats,
		Liveness:            liveness,
		UnavailableReplicas: unavailable,
		LastRecoveryReports: lastRecovery,
		Recent:              s.RecentEvents(),
	}
}

func (s *Server) RoutingStatus(ctx context.Context) RoutingStatus {
	s.refreshMetricGauges()
	snapshot := s.routingSnapshotReadOnly()
	current := s.currentState()
	pendingCount := 0
	liveness := map[string]coordruntime.NodeLivenessRecord{}
	outboxCount := len(current.Outbox)
	if s.ha != nil {
		if published, err := s.readHASnapshot(ctx); err == nil {
			current = published.State
			pendingCount = len(published.State.PendingBySlot)
			liveness = mergeLivenessRecords(nil, published.State.NodeLivenessByID)
			outboxCount = len(published.Outbox)
		}
	} else {
		s.viewMu.RLock()
		pendingCount = len(s.pending)
		liveness = mergeLivenessRecords(nil, s.liveness)
		s.viewMu.RUnlock()
	}
	activePeerRefreshCount := len(s.snapshotActivePeerRefreshSlots())
	settledSlotCount := 0
	for _, chain := range current.Cluster.Chains {
		if chainHasReplicaState(chain, coordinator.ReplicaStateJoining) || chainHasReplicaState(chain, coordinator.ReplicaStateLeaving) {
			continue
		}
		if activeReplicaCount(chain) == current.Cluster.ReplicationFactor {
			settledSlotCount++
		}
	}
	healthyNodeCount := 0
	suspectNodeCount := 0
	deadNodeCount := 0
	for _, record := range liveness {
		switch record.State {
		case coordruntime.NodeLivenessStateHealthy:
			healthyNodeCount++
		case coordruntime.NodeLivenessStateSuspect:
			suspectNodeCount++
		case coordruntime.NodeLivenessStateDead:
			deadNodeCount++
		}
	}
	return RoutingStatus{
		RoutingSnapshot:        snapshot,
		PendingCount:           pendingCount,
		OutboxCount:            outboxCount,
		ActivePeerRefreshCount: activePeerRefreshCount,
		SettledSlotCount:       settledSlotCount,
		HealthyNodeCount:       healthyNodeCount,
		SuspectNodeCount:       suspectNodeCount,
		DeadNodeCount:          deadNodeCount,
	}
}

func (s *Server) refreshMetricGauges() {
	if s.metrics == nil {
		return
	}
	s.viewMu.RLock()
	pendingCount := len(s.pending)
	count := 0
	for _, slots := range s.unavailableReplicas {
		count += len(slots)
	}
	s.viewMu.RUnlock()
	s.metrics.pendingGauge.Set(float64(pendingCount))
	s.metrics.unavailableGauge.Set(float64(count))
}

func (s *Server) observeDispatchResult(kind string, start time.Time, err error) {
	if s.metrics != nil {
		result := "success"
		if err != nil {
			result = "error"
		}
		s.metrics.dispatches.WithLabelValues(kind, result).Inc()
		s.metrics.dispatchDuration.Observe(time.Since(start).Seconds())
	}
	level := zerolog.InfoLevel
	if err != nil {
		level = zerolog.Level(gologger.LvlForErr(err))
	}
	s.events.record(s.logger, level, "dispatch_"+kind, "coordinator dispatch completed", "", nil, nil, "", "", err)
	s.refreshMetricGauges()
}

func (s *Server) observeCommandResult(kind string, err error) {
	if s.metrics != nil {
		result := "success"
		if err != nil {
			result = "error"
		}
		s.metrics.commands.WithLabelValues(kind, result).Inc()
	}
}

func (s *Server) observeRepair(kind string, result string, nodeID string, slot *int, err error) {
	if s.metrics != nil {
		s.metrics.repairs.WithLabelValues(kind, result).Inc()
	}
	level := zerolog.InfoLevel
	if err != nil {
		level = zerolog.ErrorLevel
	}
	s.events.record(s.logger, level, "repair_"+kind, "coordinator repair event", nodeID, slot, nil, "", "", err)
	s.refreshMetricGauges()
}

func (s *Server) observeTimeoutOrFailure(kind string, err error) {
	if err == nil {
		return
	}
	level := zerolog.Level(gologger.LvlForErr(err))
	var timeout bool
	timeout = errors.Is(err, ErrDispatchTimeout)
	eventKind := kind
	if timeout {
		eventKind = kind + "_timeout"
	}
	s.events.record(s.logger, level, eventKind, "coordinator operation failed", "", nil, nil, "", "", err)
}
