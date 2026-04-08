package coordserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/ops"
	"github.com/danthegoodman1/craq/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

var (
	ErrUnknownNode          = errors.New("unknown coordinator server node")
	ErrNotLeader            = errors.New("coordinator server is not the active leader")
	ErrDispatchFailed       = errors.New("coordinator server dispatch failed")
	ErrDispatchTimeout      = errors.New("coordinator server dispatch timed out or was canceled")
	ErrUnexpectedProgress   = errors.New("unexpected coordinator server progress")
	ErrConflictingPending   = errors.New("conflicting coordinator server pending work")
	ErrStateMismatch        = errors.New("coordinator server state mismatch")
	ErrInvalidServerCommand = errors.New("invalid coordinator server command")
	ErrRecoveryFailed       = errors.New("coordinator server recovery failed")
	ErrInvalidServerConfig  = errors.New("invalid coordinator server config")
)

type NotLeaderError struct {
	LeaderEndpoint string
}

func (e *NotLeaderError) Error() string {
	if e == nil || e.LeaderEndpoint == "" {
		return ErrNotLeader.Error()
	}
	return fmt.Sprintf("%s: leader=%s", ErrNotLeader, e.LeaderEndpoint)
}

func (e *NotLeaderError) Unwrap() error {
	return ErrNotLeader
}

type StorageNodeClient interface {
	AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error
	ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error
	MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error
	RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error
	UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error
	ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error
	RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error
	DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error
}

type pendingKind string

const (
	pendingKindReady   pendingKind = "ready"
	pendingKindRemoved pendingKind = "removed"
)

type PendingWork struct {
	Slot        int
	NodeID      string
	Kind        pendingKind
	SlotVersion uint64
	Epoch       uint64
	CommandID   string
}

type ReadReplicaRoute struct {
	NodeID   string
	Endpoint string
	Role     storage.ReplicaRole
}

type SlotRoute struct {
	Slot         int
	ChainVersion uint64
	HeadNodeID   string
	HeadEndpoint string
	TailNodeID   string
	TailEndpoint string
	ReadReplicas []ReadReplicaRoute
	Writable     bool
	Readable     bool
}

type RoutingSnapshot struct {
	Version   uint64
	SlotCount int
	Slots     []SlotRoute
}

type LivenessPolicy struct {
	SuspectAfter  time.Duration
	DeadAfter     time.Duration
	FlapWindow    time.Duration
	FlapThreshold int
}

type Clock interface {
	Now() time.Time
}

type ServerConfig struct {
	LivenessPolicy         LivenessPolicy
	ReconfigurationPolicy  coordinator.ReconfigurationPolicy
	Clock                  Clock
	AsyncHotPathDispatch   bool
	DispatchTimeout        time.Duration
	DispatchRetryInterval  time.Duration
	RecoveryCommandTimeout time.Duration
	NodeClientFactory      NodeClientFactory
	HA                     *HAConfig
	Logger                 *zerolog.Logger
	MetricsRegistry        *prometheus.Registry
}

type NodeClientFactory interface {
	ClientForNode(node coordinator.Node) (StorageNodeClient, error)
}

type Server struct {
	rt                     *coordruntime.Runtime
	runtimeMu              sync.RWMutex
	nodes                  map[string]StorageNodeClient
	nodesMu                sync.RWMutex
	heartbeats             map[string]storage.NodeStatus
	liveness               map[string]coordruntime.NodeLivenessRecord
	pending                map[int]PendingWork
	completed              map[int][]coordruntime.CompletedProgressRecord
	routingSnapshot        RoutingSnapshot
	routingSnapshotMu      sync.RWMutex
	viewMu                 sync.RWMutex
	lastPolicy             coordinator.ReconfigurationPolicy
	unavailableReplicas    map[string]map[int]bool
	lastRecoveryReports    map[string]storage.NodeRecoveryReport
	livenessPolicy         LivenessPolicy
	clock                  Clock
	asyncHotPathDispatch   bool
	activePeerRefresh      map[int]struct{}
	activePeerRefreshMu    sync.Mutex
	dispatchTimeout        time.Duration
	dispatchRetryInterval  time.Duration
	recoveryCommandTimeout time.Duration
	nodeClientFactory      NodeClientFactory
	logger                 zerolog.Logger
	metrics                *serverMetrics
	events                 *serverEventRecorder
	ha                     *haController
	dispatchNotify         chan struct{}
	closeOnce              sync.Once
	closeCh                chan struct{}
}

const (
	defaultDispatchTimeout        = 5 * time.Second
	defaultDispatchRetryInterval  = 200 * time.Millisecond
	defaultRecoveryCommandTimeout = 5 * time.Second
	defaultFlapWindow             = 30 * time.Second
	defaultFlapThreshold          = 3
	runtimeVersionRetryLimit      = 1024
)

func Open(ctx context.Context, store coordruntime.Store, nodes map[string]StorageNodeClient) (*Server, error) {
	return OpenWithConfig(ctx, store, nodes, ServerConfig{})
}

func OpenWithConfig(
	ctx context.Context,
	store coordruntime.Store,
	nodes map[string]StorageNodeClient,
	cfg ServerConfig,
) (*Server, error) {
	if err := validateServerConfig(cfg); err != nil {
		return nil, fmt.Errorf("err in validateServerConfig: %w", err)
	}
	rt, err := coordruntime.Open(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("err in coordruntime.Open: %w", err)
	}

	clonedNodes := make(map[string]StorageNodeClient, len(nodes))
	for nodeID, node := range nodes {
		clonedNodes[nodeID] = node
	}

	server := &Server{
		rt:                     rt,
		nodes:                  clonedNodes,
		heartbeats:             map[string]storage.NodeStatus{},
		liveness:               map[string]coordruntime.NodeLivenessRecord{},
		pending:                map[int]PendingWork{},
		completed:              map[int][]coordruntime.CompletedProgressRecord{},
		unavailableReplicas:    map[string]map[int]bool{},
		lastRecoveryReports:    map[string]storage.NodeRecoveryReport{},
		livenessPolicy:         normalizeLivenessPolicy(cfg.LivenessPolicy),
		asyncHotPathDispatch:   cfg.AsyncHotPathDispatch,
		activePeerRefresh:      map[int]struct{}{},
		lastPolicy:             cfg.ReconfigurationPolicy,
		clock:                  cfg.Clock,
		dispatchTimeout:        cfg.DispatchTimeout,
		dispatchRetryInterval:  cfg.DispatchRetryInterval,
		recoveryCommandTimeout: cfg.RecoveryCommandTimeout,
		nodeClientFactory:      cfg.NodeClientFactory,
		logger:                 coordLoggerFromConfig(cfg.Logger),
		metrics:                newServerMetrics(cfg.MetricsRegistry),
		events:                 newServerEventRecorder(),
		dispatchNotify:         make(chan struct{}, 1),
		closeCh:                make(chan struct{}),
	}
	if server.clock == nil {
		server.clock = realClock{}
	}
	if server.dispatchTimeout == 0 {
		server.dispatchTimeout = defaultDispatchTimeout
	}
	if server.dispatchRetryInterval == 0 {
		server.dispatchRetryInterval = defaultDispatchRetryInterval
	}
	if server.recoveryCommandTimeout == 0 {
		server.recoveryCommandTimeout = defaultRecoveryCommandTimeout
	}
	server.syncViewsFromRuntime()
	if server.lastPolicy == (coordinator.ReconfigurationPolicy{}) && cfg.ReconfigurationPolicy != (coordinator.ReconfigurationPolicy{}) {
		server.lastPolicy = cfg.ReconfigurationPolicy
	}
	server.rebuildRoutingSnapshot()
	if cfg.HA != nil {
		if err := server.enableHA(*cfg.HA); err != nil {
			return nil, fmt.Errorf("err in server.enableHA: %w", err)
		}
	} else {
		server.startDispatchLoop()
	}
	return server, nil
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		close(s.closeCh)
		if s.ha != nil && s.ha.stop != nil {
			close(s.ha.stop)
			if s.ha.done != nil {
				<-s.ha.done
			}
		}
	})
	return nil
}

func (s *Server) Current() coordruntime.State {
	if s.ha != nil {
		if snapshot, err := s.ha.cfg.Store.LoadSnapshot(context.Background()); err == nil {
			s.syncFromHASnapshot(snapshot)
		}
	}
	return s.currentState()
}

func (s *Server) Heartbeats() map[string]storage.NodeStatus {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	cloned := make(map[string]storage.NodeStatus, len(s.heartbeats))
	for nodeID, status := range s.heartbeats {
		cloned[nodeID] = status
	}
	return cloned
}

func (s *Server) Liveness() map[string]coordruntime.NodeLivenessRecord {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	cloned := make(map[string]coordruntime.NodeLivenessRecord, len(s.liveness))
	for nodeID, record := range s.liveness {
		cloned[nodeID] = cloneLivenessRecord(record)
	}
	return cloned
}

func (s *Server) Pending() map[int]PendingWork {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	cloned := make(map[int]PendingWork, len(s.pending))
	for slot, pending := range s.pending {
		cloned[slot] = pending
	}
	return cloned
}

func (s *Server) pendingWork(slot int) (PendingWork, bool) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	pending, ok := s.pending[slot]
	return pending, ok
}

func (s *Server) completedRecords(slot int) []coordruntime.CompletedProgressRecord {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	return append([]coordruntime.CompletedProgressRecord(nil), s.completed[slot]...)
}

func (s *Server) livenessRecord(nodeID string) (coordruntime.NodeLivenessRecord, bool) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	record, ok := s.liveness[nodeID]
	return cloneLivenessRecord(record), ok
}

func (s *Server) RoutingSnapshot(ctx context.Context) (RoutingSnapshot, error) {
	if s.ha != nil {
		if err := s.ensureLeader(ctx); err != nil {
			return RoutingSnapshot{}, err
		}
	}
	s.routingSnapshotMu.RLock()
	defer s.routingSnapshotMu.RUnlock()
	return cloneRoutingSnapshot(s.routingSnapshot), nil
}

func (s *Server) clientForNodeID(nodeID string) (StorageNodeClient, error) {
	s.nodesMu.RLock()
	if client, ok := s.nodes[nodeID]; ok {
		s.nodesMu.RUnlock()
		return client, nil
	}
	s.nodesMu.RUnlock()
	if s.nodeClientFactory == nil {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, nodeID)
	}
	node, ok := s.currentState().Cluster.NodesByID[nodeID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownNode, nodeID)
	}
	client, err := s.nodeClientFactory.ClientForNode(node)
	if err != nil {
		return nil, fmt.Errorf("err in s.nodeClientFactory.ClientForNode: %w", err)
	}
	s.nodesMu.Lock()
	s.nodes[nodeID] = client
	s.nodesMu.Unlock()
	return client, nil
}

func (s *Server) Bootstrap(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if s.ha != nil {
		return s.applyHAWithPlanner(ctx, nil, func(planner *Server) (coordruntime.State, error) {
			return planner.Bootstrap(ctx, cmd)
		})
	}
	defer s.refreshMetricGauges()
	if cmd.Kind != coordruntime.CommandKindBootstrap || cmd.Bootstrap == nil {
		err := fmt.Errorf("%w: bootstrap requires bootstrap command payload", ErrInvalidServerCommand)
		s.observeCommandResult("bootstrap", err)
		return coordruntime.State{}, err
	}
	rt := s.runtimeRef()
	state, err := rt.Bootstrap(ctx, cmd)
	if err != nil {
		err = fmt.Errorf("err in rt.Bootstrap: %w", err)
		s.observeCommandResult("bootstrap", err)
		s.observeTimeoutOrFailure("bootstrap", err)
		return coordruntime.State{}, err
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	s.observeCommandResult("bootstrap", nil)
	s.events.record(s.logger, zerolog.InfoLevel, "bootstrap", "coordinator bootstrapped cluster", "", nil, ops.Uint64Ptr(state.Version), "", cmd.ID, nil)
	return state, nil
}

func (s *Server) AddNode(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if s.ha != nil {
		seed := []string{}
		if cmd.Reconfigure != nil && len(cmd.Reconfigure.Events) > 0 {
			seed = append(seed, cmd.Reconfigure.Events[0].Node.ID)
		}
		return s.applyHAWithPlanner(ctx, seed, func(planner *Server) (coordruntime.State, error) {
			return planner.AddNode(ctx, cmd)
		})
	}
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindAddNode)
}

func (s *Server) BeginDrainNode(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if s.ha != nil {
		return s.applyHAWithPlanner(ctx, nil, func(planner *Server) (coordruntime.State, error) {
			return planner.BeginDrainNode(ctx, cmd)
		})
	}
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindBeginDrainNode)
}

func (s *Server) MarkNodeDead(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if s.ha != nil {
		return s.applyHAWithPlanner(ctx, nil, func(planner *Server) (coordruntime.State, error) {
			return planner.MarkNodeDead(ctx, cmd)
		})
	}
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindMarkNodeDead)
}

func (s *Server) ReportReplicaReady(ctx context.Context, nodeID string, slot int, epoch uint64, commandID string) (coordruntime.State, error) {
	if s.ha != nil {
		return s.applyHAWithPlanner(ctx, []string{nodeID}, func(planner *Server) (coordruntime.State, error) {
			return planner.ReportReplicaReady(ctx, nodeID, slot, epoch, commandID)
		})
	}
	defer s.refreshMetricGauges()
	rt := s.runtimeRef()
	duplicateCompleted := false
	updated, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.State, attempt int) (coordruntime.State, error) {
		slotVersion := current.SlotVersions[slot]
		pending, ok := current.PendingBySlot[slot]
		if !ok || pending.Kind != coordruntime.PendingKindReady || pending.NodeID != nodeID {
			if matchesCompletedRecords(current.CompletedProgressBySlot, slot, nodeID, pendingKindReady, slotVersion) {
				duplicateCompleted = true
				return current, nil
			}
			return coordruntime.State{}, fmt.Errorf(
				"%w: unexpected ready report for node %q slot %d",
				ErrUnexpectedProgress,
				nodeID,
				slot,
			)
		}
		if commandID != "" && pending.CommandID != "" && pending.CommandID != commandID {
			return coordruntime.State{}, fmt.Errorf(
				"%w: ready report command %q does not match pending %q",
				ErrUnexpectedProgress,
				commandID,
				pending.CommandID,
			)
		}
		if pending.SlotVersion != slotVersion {
			return coordruntime.State{}, fmt.Errorf(
				"%w: ready report slot version %d does not match pending version %d",
				ErrUnexpectedProgress,
				slotVersion,
				pending.SlotVersion,
			)
		}
		if !slotContainsReplicaInState(current.Cluster, slot, nodeID, coordinator.ReplicaStateJoining) {
			return coordruntime.State{}, fmt.Errorf(
				"%w: node %q slot %d is not joining in current coordinator state",
				ErrStateMismatch,
				nodeID,
				slot,
			)
		}
		progressID := commandID
		if progressID == "" {
			progressID = fmt.Sprintf("server-progress-ready-%s-%d-r%d-v%d", nodeID, slot, attempt, current.Version)
		}
		return rt.ApplyProgress(ctx, coordruntime.Command{
			ID:              progressID,
			ExpectedVersion: current.Version,
			Kind:            coordruntime.CommandKindProgress,
			Progress: &coordruntime.ProgressCommand{
				Event: coordinator.Event{
					Kind:   coordinator.EventKindReplicaBecameActive,
					NodeID: nodeID,
					Slot:   slot,
				},
			},
		})
	})
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in rt.ApplyProgress: %w", err)
	}
	if duplicateCompleted {
		return updated, nil
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	if s.asyncHotPathDispatch {
		if s.shouldDispatchActivePeerRefresh(slot) {
			s.enqueueActivePeerRefresh(slot)
		}
		s.notifyDispatchLoop()
	} else {
		if err := s.reconcileAndDispatch(ctx); err != nil {
			return coordruntime.State{}, err
		}
		if s.shouldDispatchActivePeerRefresh(slot) {
			if err := s.dispatchActivePeerUpdates(ctx, slot); err != nil {
				return coordruntime.State{}, err
			}
		}
	}
	slotVersion := updated.SlotVersions[slot]
	s.events.record(s.logger, zerolog.InfoLevel, "replica_ready", "coordinator accepted replica ready progress", nodeID, ops.IntPtr(slot), ops.Uint64Ptr(slotVersion), "", commandID, nil)
	return s.currentState(), nil
}

func (s *Server) ReportReplicaRemoved(ctx context.Context, nodeID string, slot int, epoch uint64, commandID string) (coordruntime.State, error) {
	if s.ha != nil {
		return s.applyHAWithPlanner(ctx, []string{nodeID}, func(planner *Server) (coordruntime.State, error) {
			return planner.ReportReplicaRemoved(ctx, nodeID, slot, epoch, commandID)
		})
	}
	defer s.refreshMetricGauges()
	rt := s.runtimeRef()
	duplicateCompleted := false
	updated, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.State, attempt int) (coordruntime.State, error) {
		slotVersion := current.SlotVersions[slot]
		pending, ok := current.PendingBySlot[slot]
		if !ok || pending.Kind != coordruntime.PendingKindRemoved || pending.NodeID != nodeID {
			if matchesCompletedRecords(current.CompletedProgressBySlot, slot, nodeID, pendingKindRemoved, slotVersion) {
				duplicateCompleted = true
				return current, nil
			}
			return coordruntime.State{}, fmt.Errorf(
				"%w: unexpected removed report for node %q slot %d",
				ErrUnexpectedProgress,
				nodeID,
				slot,
			)
		}
		if commandID != "" && pending.CommandID != "" && pending.CommandID != commandID {
			return coordruntime.State{}, fmt.Errorf(
				"%w: removed report command %q does not match pending %q",
				ErrUnexpectedProgress,
				commandID,
				pending.CommandID,
			)
		}
		if pending.SlotVersion != slotVersion {
			return coordruntime.State{}, fmt.Errorf(
				"%w: removed report slot version %d does not match pending version %d",
				ErrUnexpectedProgress,
				slotVersion,
				pending.SlotVersion,
			)
		}
		if !slotContainsReplicaInState(current.Cluster, slot, nodeID, coordinator.ReplicaStateLeaving) {
			return coordruntime.State{}, fmt.Errorf(
				"%w: node %q slot %d is not leaving in current coordinator state",
				ErrStateMismatch,
				nodeID,
				slot,
			)
		}
		progressID := commandID
		if progressID == "" {
			progressID = fmt.Sprintf("server-progress-removed-%s-%d-r%d-v%d", nodeID, slot, attempt, current.Version)
		}
		return rt.ApplyProgress(ctx, coordruntime.Command{
			ID:              progressID,
			ExpectedVersion: current.Version,
			Kind:            coordruntime.CommandKindProgress,
			Progress: &coordruntime.ProgressCommand{
				Event: coordinator.Event{
					Kind:   coordinator.EventKindReplicaRemoved,
					NodeID: nodeID,
					Slot:   slot,
				},
			},
		})
	})
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in rt.ApplyProgress: %w", err)
	}
	if duplicateCompleted {
		return updated, nil
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	if s.asyncHotPathDispatch {
		if s.shouldDispatchActivePeerRefresh(slot) {
			s.enqueueActivePeerRefresh(slot)
		}
		s.notifyDispatchLoop()
	} else {
		if err := s.reconcileAndDispatch(ctx); err != nil {
			return coordruntime.State{}, err
		}
		if s.shouldDispatchActivePeerRefresh(slot) {
			if err := s.dispatchActivePeerUpdates(ctx, slot); err != nil {
				return coordruntime.State{}, err
			}
		}
	}
	slotVersion := updated.SlotVersions[slot]
	s.events.record(s.logger, zerolog.InfoLevel, "replica_removed", "coordinator accepted replica removed progress", nodeID, ops.IntPtr(slot), ops.Uint64Ptr(slotVersion), "", commandID, nil)
	return updated, nil
}

func (s *Server) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	if s.ha != nil {
		_, err := s.applyHAWithPlanner(ctx, []string{status.NodeID}, func(planner *Server) (coordruntime.State, error) {
			if err := planner.ReportNodeHeartbeat(ctx, status); err != nil {
				return coordruntime.State{}, err
			}
			return planner.currentState(), nil
		})
		return err
	}
	if _, ok := s.currentState().Cluster.NodesByID[status.NodeID]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	current := s.currentState()
	currentRecord, hadCurrentRecord := current.NodeLivenessByID[status.NodeID]
	wasDead := currentRecord.State == coordruntime.NodeLivenessStateDead ||
		nodeMarkedDead(current.Cluster, status.NodeID)
	if nodeMarkedDead(current.Cluster, status.NodeID) {
		if _, err := s.applyLivenessTransition(ctx, status.NodeID, coordruntime.NodeLivenessStateDead, s.clock.Now().UnixNano(), true); err != nil {
			return err
		}
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	observedAt := s.clock.Now().UnixNano()
	rt := s.runtimeRef()
	_, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.State, attempt int) (coordruntime.State, error) {
		return rt.Heartbeat(ctx, coordruntime.Command{
			ID:              fmt.Sprintf("server-heartbeat-%s-%d-r%d-v%d", status.NodeID, observedAt, attempt, current.Version),
			ExpectedVersion: current.Version,
			Kind:            coordruntime.CommandKindHeartbeat,
			Heartbeat: &coordruntime.HeartbeatCommand{
				Status:             status,
				ObservedAtUnixNano: observedAt,
				FlapWindowNanos:    s.livenessPolicy.FlapWindow.Nanoseconds(),
			},
		})
	})
	if err != nil {
		err = fmt.Errorf("err in rt.Heartbeat: %w", err)
		s.observeTimeoutOrFailure("heartbeat", err)
		return err
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	if s.asyncHotPathDispatch {
		s.notifyDispatchLoop()
	} else {
		if err := s.reconcileAndDispatch(ctx); err != nil {
			if errors.Is(err, ErrDispatchFailed) || errors.Is(err, ErrDispatchTimeout) {
				s.logger.Warn().Err(err).Str("component", "coordserver").Str("node_id", status.NodeID).Msg("coordinator heartbeat triggered durable repair work that will retry later")
			} else {
				return err
			}
		}
	}
	if currentLiveness, ok := s.livenessRecord(status.NodeID); wasDead && ok && currentLiveness.State != coordruntime.NodeLivenessStateDead {
		deadActionFired := hadCurrentRecord && currentRecord.DeadActionFired
		if nodeMarkedDead(current.Cluster, status.NodeID) {
			deadActionFired = true
		}
		if _, err := s.applyLivenessTransition(ctx, status.NodeID, coordruntime.NodeLivenessStateDead, observedAt, deadActionFired); err != nil {
			return err
		}
	}
	s.events.record(s.logger, zerolog.DebugLevel, "heartbeat", "coordinator recorded node heartbeat", status.NodeID, nil, nil, "", "", nil)
	s.refreshMetricGauges()
	return nil
}

func (s *Server) EvaluateLiveness(ctx context.Context) error {
	if s.ha != nil {
		_, err := s.applyHAWithPlanner(ctx, nil, func(planner *Server) (coordruntime.State, error) {
			if err := planner.EvaluateLiveness(ctx); err != nil {
				return coordruntime.State{}, err
			}
			return planner.currentState(), nil
		})
		return err
	}
	if s.livenessPolicy.SuspectAfter <= 0 || s.livenessPolicy.DeadAfter <= 0 {
		return nil
	}
	if s.metrics != nil {
		s.metrics.livenessEvaluations.Inc()
	}
	nowUnix := s.clock.Now().UnixNano()

	liveness := s.Liveness()
	nodeIDs := make([]string, 0, len(liveness))
	for nodeID := range liveness {
		nodeIDs = append(nodeIDs, nodeID)
	}
	sort.Strings(nodeIDs)

	for _, nodeID := range nodeIDs {
		record := liveness[nodeID]
		age := time.Duration(nowUnix - record.LastHeartbeatUnixNano)
		target := record.State
		switch {
		case age >= s.livenessPolicy.DeadAfter:
			target = coordruntime.NodeLivenessStateDead
		case age >= s.livenessPolicy.SuspectAfter:
			target = coordruntime.NodeLivenessStateSuspect
		default:
			target = coordruntime.NodeLivenessStateHealthy
		}

		if target != record.State {
			updated, err := s.applyLivenessTransition(ctx, nodeID, target, nowUnix, record.DeadActionFired)
			if err != nil {
				return err
			}
			record = updated
			if target == coordruntime.NodeLivenessStateDead && s.metrics != nil {
				s.metrics.deadDetections.Inc()
			}
			s.events.record(s.logger, zerolog.InfoLevel, "liveness_transition", "coordinator node liveness transitioned", nodeID, nil, nil, "", "", nil)
			if target == coordruntime.NodeLivenessStateSuspect && flapDetectionEnabled(s.livenessPolicy) {
				flapCount := len(record.SuspectTransitionsUnixNano)
				s.events.record(s.logger, zerolog.WarnLevel, "flap_suspect_transition", "coordinator recorded suspect transition for flap detection", nodeID, nil, nil, "", "", nil)
				if flapCount >= s.livenessPolicy.FlapThreshold {
					updated, err := s.applyLivenessTransition(ctx, nodeID, coordruntime.NodeLivenessStateDead, nowUnix, record.DeadActionFired)
					if err != nil {
						return err
					}
					record = updated
					if s.metrics != nil {
						s.metrics.deadDetections.Inc()
						s.metrics.flapDetections.Inc()
					}
					s.events.record(s.logger, zerolog.WarnLevel, "flap_eviction", "coordinator evicted flapping node", nodeID, nil, nil, "", "", nil)
				}
			}
		}

		if record.State == coordruntime.NodeLivenessStateDead && !record.DeadActionFired {
			if len(s.Pending()) > 0 {
				continue
			}
			currentState := s.currentState()
			if nodeMarkedDead(currentState.Cluster, nodeID) || !isRuntimeInitialized(currentState) {
				updated, err := s.applyLivenessTransition(ctx, nodeID, coordruntime.NodeLivenessStateDead, nowUnix, true)
				if err != nil {
					return err
				}
				record = updated
				_ = record
				continue
			}

			currentState = s.currentState()
			if _, err := s.MarkNodeDead(ctx, coordruntime.Command{
				ID:              fmt.Sprintf("server-auto-dead-%s-v%d", nodeID, currentState.Version),
				ExpectedVersion: currentState.Version,
				Kind:            coordruntime.CommandKindReconfigure,
				Reconfigure: &coordruntime.ReconfigureCommand{
					Events: []coordinator.Event{{
						Kind:   coordinator.EventKindMarkNodeDead,
						NodeID: nodeID,
					}},
					Policy: s.reconfigurationPolicy(),
				},
			}); err != nil {
				return fmt.Errorf("err in s.MarkNodeDead: %w", err)
			}
			if _, err := s.applyLivenessTransition(ctx, nodeID, coordruntime.NodeLivenessStateDead, nowUnix, true); err != nil {
				return err
			}
			s.observeRepair("mark_dead", "success", nodeID, nil, nil)
		}
	}
	s.refreshMetricGauges()
	return nil
}

func (s *Server) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	if s.ha != nil {
		_, err := s.applyHAWithPlanner(ctx, []string{report.NodeID}, func(planner *Server) (coordruntime.State, error) {
			if err := planner.ReportNodeRecovered(ctx, report); err != nil {
				return coordruntime.State{}, err
			}
			return planner.currentState(), nil
		})
		return err
	}
	defer s.refreshMetricGauges()
	if _, ok := s.currentState().Cluster.NodesByID[report.NodeID]; !ok {
		s.nodesMu.RLock()
		_, fallbackOK := s.nodes[report.NodeID]
		s.nodesMu.RUnlock()
		if !fallbackOK {
			return fmt.Errorf("%w: %q", ErrUnknownNode, report.NodeID)
		}
	}
	if prior, ok := s.lastRecoveryReport(report.NodeID); ok && reflect.DeepEqual(prior, report) && !s.nodeHasUnavailableSlots(report.NodeID) {
		return nil
	}

	reportSlots := make(map[int]storage.RecoveredReplica, len(report.Replicas))
	s.markUnavailableReplicas(report)

	state := s.currentState()
	for _, recovered := range report.Replicas {
		reportSlots[recovered.Assignment.Slot] = recovered
		currentAssignment, ok := currentAssignmentForNode(state, report.NodeID, recovered.Assignment.Slot)
		if !ok {
			if err := s.dropRecoveredReplica(ctx, report.NodeID, recovered.Assignment.Slot); err != nil {
				return err
			}
			continue
		}

		switch {
		case canResumeRecoveredReplica(recovered, currentAssignment):
			if err := s.resumeRecoveredReplica(ctx, report.NodeID, currentAssignment); err != nil {
				return err
			}
		default:
			sourceNodeID, ok := recoverySourceNodeID(state.Cluster.Chains[recovered.Assignment.Slot], report.NodeID)
			if !ok {
				return fmt.Errorf(
					"%w: slot %d node %q has no valid recovery source",
					ErrRecoveryFailed,
					recovered.Assignment.Slot,
					report.NodeID,
				)
			}
			if err := s.recoverReplica(ctx, report.NodeID, currentAssignment, sourceNodeID); err != nil {
				return err
			}
		}
	}

	s.viewMu.Lock()
	s.lastRecoveryReports[report.NodeID] = cloneRecoveryReport(report)
	s.viewMu.Unlock()
	s.rebuildRoutingSnapshot()
	s.events.record(s.logger, zerolog.InfoLevel, "node_recovered_report", "coordinator processed node recovered report", report.NodeID, nil, nil, "", "", nil)
	return nil
}

func (s *Server) RegisterNode(ctx context.Context, reg storage.NodeRegistration) (coordruntime.State, error) {
	current := s.currentState()
	if existing, ok := current.Cluster.NodesByID[reg.NodeID]; ok &&
		current.Cluster.NodeHealthByID[reg.NodeID] != coordinator.NodeHealthDead &&
		existing.RPCAddress == reg.RPCAddress &&
		reflect.DeepEqual(existing.FailureDomains, reg.FailureDomains) {
		return current, nil
	}
	if s.ha != nil {
		return s.applyHAWithPlanner(ctx, []string{reg.NodeID}, func(planner *Server) (coordruntime.State, error) {
			return planner.RegisterNode(ctx, reg)
		})
	}
	rt := s.runtimeRef()
	return retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.State, attempt int) (coordruntime.State, error) {
		if existing, ok := current.Cluster.NodesByID[reg.NodeID]; ok &&
			current.Cluster.NodeHealthByID[reg.NodeID] != coordinator.NodeHealthDead &&
			existing.RPCAddress == reg.RPCAddress &&
			reflect.DeepEqual(existing.FailureDomains, reg.FailureDomains) {
			return current, nil
		}
		return s.applyMembershipMutation(ctx, coordruntime.Command{
			ID:              fmt.Sprintf("server-register-%s-r%d-v%d", reg.NodeID, attempt, current.Version),
			ExpectedVersion: current.Version,
			Kind:            coordruntime.CommandKindReconfigure,
			Reconfigure: &coordruntime.ReconfigureCommand{
				Policy: s.reconfigurationPolicy(),
				Events: []coordinator.Event{{
					Kind: coordinator.EventKindRegisterNode,
					Node: coordinator.Node{
						ID:             reg.NodeID,
						RPCAddress:     reg.RPCAddress,
						FailureDomains: cloneFailureDomains(reg.FailureDomains),
					},
				}},
			},
		}, coordinator.EventKindRegisterNode)
	})
}

func (s *Server) applyMembershipMutation(
	ctx context.Context,
	cmd coordruntime.Command,
	expectedEvent coordinator.EventKind,
) (coordruntime.State, error) {
	defer s.refreshMetricGauges()
	if cmd.Kind != coordruntime.CommandKindReconfigure || cmd.Reconfigure == nil {
		err := fmt.Errorf("%w: mutation requires reconfigure command payload", ErrInvalidServerCommand)
		s.observeCommandResult(string(expectedEvent), err)
		return coordruntime.State{}, err
	}
	if len(cmd.Reconfigure.Events) != 1 || cmd.Reconfigure.Events[0].Kind != expectedEvent {
		err := fmt.Errorf(
			"%w: expected exactly one %q event",
			ErrInvalidServerCommand,
			expectedEvent,
		)
		s.observeCommandResult(string(expectedEvent), err)
		return coordruntime.State{}, err
	}

	rt := s.runtimeRef()
	plan, state, err := rt.Reconfigure(ctx, cmd)
	if err != nil {
		err = fmt.Errorf("err in rt.Reconfigure: %w", err)
		s.observeCommandResult(string(expectedEvent), err)
		s.observeTimeoutOrFailure(string(expectedEvent), err)
		return coordruntime.State{}, err
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	if err := s.dispatchRuntimeOutbox(ctx); err != nil {
		s.observeCommandResult(string(expectedEvent), err)
		return coordruntime.State{}, err
	}
	for _, slotPlan := range plan.ChangedSlots {
		if !s.shouldDispatchActivePeerRefresh(slotPlan.Slot) {
			continue
		}
		if err := s.dispatchActivePeerUpdates(ctx, slotPlan.Slot); err != nil {
			s.observeCommandResult(string(expectedEvent), err)
			return coordruntime.State{}, err
		}
	}
	s.observeCommandResult(string(expectedEvent), nil)
	s.events.record(s.logger, zerolog.InfoLevel, "membership_mutation", "coordinator applied membership mutation", "", nil, ops.Uint64Ptr(state.Version), "", cmd.ID, nil)
	return state, nil
}

func (s *Server) reconcileAndDispatch(ctx context.Context) error {
	if err := s.reconcileState(ctx); err != nil {
		return err
	}
	if err := s.dispatchRuntimeOutbox(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Server) reconcileState(ctx context.Context) error {
	rt := s.runtimeRef()
	_, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.State, attempt int) (coordruntime.State, error) {
		preview, err := coordinator.PlanReconfiguration(current.Cluster, nil, s.reconfigurationPolicy())
		if err != nil {
			return coordruntime.State{}, fmt.Errorf("err in coordinator.PlanReconfiguration preview: %w", err)
		}
		if len(preview.ChangedSlots) == 0 && reflect.DeepEqual(preview.UpdatedState, current.Cluster) {
			return current, nil
		}

		_, next, err := rt.Reconfigure(ctx, coordruntime.Command{
			ID:              fmt.Sprintf("server-reconcile-r%d-v%d", attempt, current.Version),
			ExpectedVersion: current.Version,
			Kind:            coordruntime.CommandKindReconfigure,
			Reconfigure: &coordruntime.ReconfigureCommand{
				Events: nil,
				Policy: s.reconfigurationPolicy(),
			},
		})
		if err != nil {
			return coordruntime.State{}, err
		}
		return next, nil
	})
	if err != nil {
		return fmt.Errorf("err in rt.Reconcile: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	return nil
}

func (s *Server) startDispatchLoop() {
	go func() {
		ticker := time.NewTicker(s.dispatchRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-s.closeCh:
				return
			case <-ticker.C:
				s.runBackgroundDispatchOnce()
			case <-s.dispatchNotify:
				s.runBackgroundDispatchOnce()
			}
		}
	}()
}

func (s *Server) notifyDispatchLoop() {
	select {
	case s.dispatchNotify <- struct{}{}:
	default:
	}
}

func (s *Server) runBackgroundDispatchOnce() {
	var err error
	if s.asyncHotPathDispatch {
		err = s.reconcileAndDispatch(context.Background())
		if err == nil {
			err = s.dispatchQueuedActivePeerRefreshes(context.Background())
		}
	} else {
		err = s.dispatchRuntimeOutbox(context.Background())
	}
	if err != nil && !errors.Is(err, ErrDispatchFailed) && !errors.Is(err, ErrDispatchTimeout) {
		s.logger.Warn().Err(err).Str("component", "coordserver").Msg("non-ha dispatch loop observed error")
	}
}

func (s *Server) dispatchRuntimeOutbox(ctx context.Context) error {
	entries := cloneRuntimeOutbox(s.currentState().Outbox)
	for _, entry := range entries {
		if err := s.dispatchRuntimeOutboxEntry(ctx, entry); err != nil {
			return err
		}
		rt := s.runtimeRef()
		if _, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.State, attempt int) (coordruntime.State, error) {
			return rt.AcknowledgeOutbox(ctx, coordruntime.Command{
				ID:              fmt.Sprintf("server-ack-outbox-%s-r%d-v%d", entry.ID, attempt, current.Version),
				ExpectedVersion: current.Version,
				Kind:            coordruntime.CommandKindAcknowledgeOutbox,
				AcknowledgeOutbox: &coordruntime.AcknowledgeOutboxCommand{
					EntryID: entry.ID,
				},
			})
		}); err != nil {
			return fmt.Errorf("err in rt.AcknowledgeOutbox: %w", err)
		}
		s.syncViewsFromRuntime()
	}
	s.rebuildRoutingSnapshot()
	return nil
}

func (s *Server) dispatchRuntimeOutboxEntry(ctx context.Context, entry coordruntime.OutboxEntry) error {
	start := time.Now()
	client, err := s.clientForNodeID(entry.NodeID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDispatchFailed, err)
	}
	switch entry.Kind {
	case coordruntime.OutboxCommandKindAddReplicaAsTail:
		dispatchCtx, cancel := deriveDeadlineContext(ctx, s.dispatchTimeout)
		defer cancel()
		if err := client.AddReplicaAsTail(dispatchCtx, storage.AddReplicaAsTailCommand{Assignment: entry.Assignment}); err != nil {
			s.observeDispatchResult(string(entry.Kind), start, err)
			if isContextTimeoutOrCancel(err) {
				return fmt.Errorf("%w: err in node[%q].AddReplicaAsTail: %w", ErrDispatchTimeout, entry.NodeID, err)
			}
			return fmt.Errorf("%w: err in node[%q].AddReplicaAsTail: %v", ErrDispatchFailed, entry.NodeID, err)
		}
	case coordruntime.OutboxCommandKindMarkReplicaLeaving:
		dispatchCtx, cancel := deriveDeadlineContext(ctx, s.dispatchTimeout)
		defer cancel()
		if err := client.MarkReplicaLeaving(dispatchCtx, storage.MarkReplicaLeavingCommand{Slot: entry.Slot}); err != nil {
			s.observeDispatchResult(string(entry.Kind), start, err)
			if isContextTimeoutOrCancel(err) {
				return fmt.Errorf("%w: err in node[%q].MarkReplicaLeaving: %w", ErrDispatchTimeout, entry.NodeID, err)
			}
			return fmt.Errorf("%w: err in node[%q].MarkReplicaLeaving: %v", ErrDispatchFailed, entry.NodeID, err)
		}
	case coordruntime.OutboxCommandKindUpdateChainPeers:
		dispatchCtx, cancel := deriveDeadlineContext(ctx, s.dispatchTimeout)
		defer cancel()
		if err := client.UpdateChainPeers(dispatchCtx, storage.UpdateChainPeersCommand{Assignment: entry.Assignment}); err != nil {
			s.observeDispatchResult(string(entry.Kind), start, err)
			if isContextTimeoutOrCancel(err) {
				return fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %w", ErrDispatchTimeout, entry.NodeID, err)
			}
			return fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %v", ErrDispatchFailed, entry.NodeID, err)
		}
	default:
		return fmt.Errorf("%w: unsupported outbox command %q", ErrDispatchFailed, entry.Kind)
	}
	s.observeDispatchResult(string(entry.Kind), start, nil)
	return nil
}

func cloneRuntimeOutbox(current []coordruntime.OutboxEntry) []coordruntime.OutboxEntry {
	cloned := make([]coordruntime.OutboxEntry, len(current))
	copy(cloned, current)
	return cloned
}

func runtimeOutboxHasSlot(entries []coordruntime.OutboxEntry, slot int) bool {
	for _, entry := range entries {
		if entry.Slot == slot {
			return true
		}
	}
	return false
}

func (s *Server) shouldDispatchActivePeerRefresh(slot int) bool {
	s.viewMu.RLock()
	_, pending := s.pending[slot]
	s.viewMu.RUnlock()
	if pending {
		return false
	}
	return !runtimeOutboxHasSlot(s.currentState().Outbox, slot)
}

func (s *Server) enqueueActivePeerRefresh(slot int) {
	s.activePeerRefreshMu.Lock()
	s.activePeerRefresh[slot] = struct{}{}
	s.activePeerRefreshMu.Unlock()
}

func (s *Server) clearActivePeerRefresh(slot int) {
	s.activePeerRefreshMu.Lock()
	delete(s.activePeerRefresh, slot)
	s.activePeerRefreshMu.Unlock()
}

func (s *Server) snapshotActivePeerRefreshSlots() []int {
	s.activePeerRefreshMu.Lock()
	defer s.activePeerRefreshMu.Unlock()
	slots := make([]int, 0, len(s.activePeerRefresh))
	for slot := range s.activePeerRefresh {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	return slots
}

func (s *Server) dispatchQueuedActivePeerRefreshes(ctx context.Context) error {
	for _, slot := range s.snapshotActivePeerRefreshSlots() {
		if !s.shouldDispatchActivePeerRefresh(slot) {
			s.clearActivePeerRefresh(slot)
			continue
		}
		if err := s.dispatchActivePeerUpdates(ctx, slot); err != nil {
			return err
		}
		s.clearActivePeerRefresh(slot)
	}
	return nil
}

func (s *Server) resumeRecoveredReplica(
	ctx context.Context,
	nodeID string,
	assignment storage.ReplicaAssignment,
) error {
	start := time.Now()
	client, err := s.clientForNodeID(nodeID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrRecoveryFailed, err)
	}
	dispatchCtx, cancel := deriveDeadlineContext(ctx, s.recoveryCommandTimeout)
	defer cancel()
	if err := client.ResumeRecoveredReplica(dispatchCtx, storage.ResumeRecoveredReplicaCommand{Assignment: assignment}); err != nil {
		s.observeDispatchResult("resume_recovered_replica", start, err)
		s.observeRepair("resume_recovered_replica", "error", nodeID, ops.IntPtr(assignment.Slot), err)
		if isContextTimeoutOrCancel(err) {
			return fmt.Errorf("%w: err in node[%q].ResumeRecoveredReplica: %w", ErrDispatchTimeout, nodeID, err)
		}
		return fmt.Errorf("%w: err in node[%q].ResumeRecoveredReplica: %v", ErrRecoveryFailed, nodeID, err)
	}
	s.clearUnavailable(nodeID, assignment.Slot)
	s.observeDispatchResult("resume_recovered_replica", start, nil)
	s.observeRepair("resume_recovered_replica", "success", nodeID, ops.IntPtr(assignment.Slot), nil)
	return nil
}

func (s *Server) recoverReplica(
	ctx context.Context,
	nodeID string,
	assignment storage.ReplicaAssignment,
	sourceNodeID string,
) error {
	start := time.Now()
	client, err := s.clientForNodeID(nodeID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrRecoveryFailed, err)
	}
	if _, ok := s.currentState().Cluster.NodesByID[sourceNodeID]; !ok {
		return fmt.Errorf("%w: %w: %q", ErrRecoveryFailed, ErrUnknownNode, sourceNodeID)
	}
	dispatchCtx, cancel := deriveDeadlineContext(ctx, s.recoveryCommandTimeout)
	defer cancel()
	if err := client.RecoverReplica(dispatchCtx, storage.RecoverReplicaCommand{
		Assignment:   assignment,
		SourceNodeID: sourceNodeID,
	}); err != nil {
		s.observeDispatchResult("recover_replica", start, err)
		s.observeRepair("recover_replica", "error", nodeID, ops.IntPtr(assignment.Slot), err)
		if isContextTimeoutOrCancel(err) {
			return fmt.Errorf("%w: err in node[%q].RecoverReplica: %w", ErrDispatchTimeout, nodeID, err)
		}
		return fmt.Errorf("%w: err in node[%q].RecoverReplica: %v", ErrRecoveryFailed, nodeID, err)
	}
	s.clearUnavailable(nodeID, assignment.Slot)
	s.observeDispatchResult("recover_replica", start, nil)
	s.observeRepair("recover_replica", "success", nodeID, ops.IntPtr(assignment.Slot), nil)
	return nil
}

func (s *Server) dropRecoveredReplica(ctx context.Context, nodeID string, slot int) error {
	start := time.Now()
	client, err := s.clientForNodeID(nodeID)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrRecoveryFailed, err)
	}
	dispatchCtx, cancel := deriveDeadlineContext(ctx, s.recoveryCommandTimeout)
	defer cancel()
	if err := client.DropRecoveredReplica(dispatchCtx, storage.DropRecoveredReplicaCommand{Slot: slot}); err != nil {
		s.observeDispatchResult("drop_recovered_replica", start, err)
		s.observeRepair("drop_recovered_replica", "error", nodeID, ops.IntPtr(slot), err)
		if isContextTimeoutOrCancel(err) {
			return fmt.Errorf("%w: err in node[%q].DropRecoveredReplica: %w", ErrDispatchTimeout, nodeID, err)
		}
		return fmt.Errorf("%w: err in node[%q].DropRecoveredReplica: %v", ErrRecoveryFailed, nodeID, err)
	}
	s.clearUnavailable(nodeID, slot)
	s.observeDispatchResult("drop_recovered_replica", start, nil)
	s.observeRepair("drop_recovered_replica", "success", nodeID, ops.IntPtr(slot), nil)
	return nil
}

func (s *Server) dispatchActivePeerUpdates(ctx context.Context, slot int) error {
	state := s.currentState()
	var chain *coordinator.Chain
	for i := range state.Cluster.Chains {
		if state.Cluster.Chains[i].Slot == slot {
			current := activeServingChain(state.Cluster.Chains[i])
			chain = &current
			break
		}
	}
	if chain == nil {
		return nil
	}
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		client, err := s.clientForNodeID(replica.NodeID)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrDispatchFailed, err)
		}
		assignment, err := assignmentForNode(*chain, state.Cluster.NodesByID, replica.NodeID, state.SlotVersions[slot])
		if err != nil {
			return fmt.Errorf("err in assignmentForNode(update active peers): %w", err)
		}
		dispatchCtx, cancel := deriveDeadlineContext(ctx, s.dispatchTimeout)
		err = client.UpdateChainPeers(dispatchCtx, storage.UpdateChainPeersCommand{Assignment: assignment})
		cancel()
		if err != nil {
			if isContextTimeoutOrCancel(err) {
				return fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %w", ErrDispatchTimeout, replica.NodeID, err)
			}
			return fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %v", ErrDispatchFailed, replica.NodeID, err)
		}
	}
	return nil
}

func assignmentForNode(
	chain coordinator.Chain,
	nodesByID map[string]coordinator.Node,
	nodeID string,
	chainVersion uint64,
) (storage.ReplicaAssignment, error) {
	position := -1
	for i, replica := range chain.Replicas {
		if replica.NodeID == nodeID {
			position = i
			break
		}
	}
	if position < 0 {
		return storage.ReplicaAssignment{}, fmt.Errorf("%w: node %q not found in chain %d", ErrStateMismatch, nodeID, chain.Slot)
	}

	role := storage.ReplicaRoleMiddle
	switch len(chain.Replicas) {
	case 1:
		role = storage.ReplicaRoleSingle
	default:
		switch position {
		case 0:
			role = storage.ReplicaRoleHead
		case len(chain.Replicas) - 1:
			role = storage.ReplicaRoleTail
		default:
			role = storage.ReplicaRoleMiddle
		}
	}

	assignment := storage.ReplicaAssignment{
		Slot:         chain.Slot,
		ChainVersion: chainVersion,
		Role:         role,
	}
	if position > 0 {
		assignment.Peers.PredecessorNodeID = chain.Replicas[position-1].NodeID
		assignment.Peers.PredecessorTarget = nodesByID[assignment.Peers.PredecessorNodeID].RPCAddress
	}
	if position+1 < len(chain.Replicas) {
		assignment.Peers.SuccessorNodeID = chain.Replicas[position+1].NodeID
		assignment.Peers.SuccessorTarget = nodesByID[assignment.Peers.SuccessorNodeID].RPCAddress
	}
	tailNodeID := ""
	for i := len(chain.Replicas) - 1; i >= 0; i-- {
		if chain.Replicas[i].State != coordinator.ReplicaStateActive {
			continue
		}
		tailNodeID = chain.Replicas[i].NodeID
		break
	}
	if tailNodeID == "" && len(chain.Replicas) > 0 {
		tailNodeID = chain.Replicas[len(chain.Replicas)-1].NodeID
	}
	if tailNodeID != "" {
		assignment.Peers.TailNodeID = tailNodeID
		assignment.Peers.TailTarget = nodesByID[tailNodeID].RPCAddress
	}
	return assignment, nil
}

func activeAfterNodeIDs(chain coordinator.Chain, skipped map[string]bool) []string {
	nodeIDs := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		if skipped[replica.NodeID] {
			continue
		}
		nodeIDs = append(nodeIDs, replica.NodeID)
	}
	return nodeIDs
}

func (s *Server) rebuildRoutingSnapshot() {
	state := s.currentState()
	s.viewMu.RLock()
	unavailable := cloneUnavailableReplicasMap(s.unavailableReplicas)
	s.viewMu.RUnlock()
	snapshot := RoutingSnapshot{
		Version:   state.Version,
		SlotCount: state.Cluster.SlotCount,
		Slots:     make([]SlotRoute, 0, len(state.Cluster.Chains)),
	}
	for _, chain := range state.Cluster.Chains {
		route := SlotRoute{
			Slot:         chain.Slot,
			ChainVersion: state.SlotVersions[chain.Slot],
		}
		if chainHasUnavailableReplica(chain, unavailable) {
			snapshot.Slots = append(snapshot.Slots, route)
			continue
		}
		for _, replica := range chain.Replicas {
			if replica.State != coordinator.ReplicaStateActive {
				continue
			}
			if replicaUnavailable(unavailable, replica.NodeID, chain.Slot) {
				continue
			}
			role := storage.ReplicaRoleMiddle
			if route.HeadNodeID == "" {
				route.HeadNodeID = replica.NodeID
				route.HeadEndpoint = state.Cluster.NodesByID[replica.NodeID].RPCAddress
				role = storage.ReplicaRoleHead
			}
			route.TailNodeID = replica.NodeID
			route.TailEndpoint = state.Cluster.NodesByID[replica.NodeID].RPCAddress
			route.ReadReplicas = append(route.ReadReplicas, ReadReplicaRoute{
				NodeID:   replica.NodeID,
				Endpoint: state.Cluster.NodesByID[replica.NodeID].RPCAddress,
				Role:     role,
			})
		}
		switch len(route.ReadReplicas) {
		case 1:
			route.ReadReplicas[0].Role = storage.ReplicaRoleSingle
		case 2:
			route.ReadReplicas[1].Role = storage.ReplicaRoleTail
		default:
			if len(route.ReadReplicas) > 1 {
				route.ReadReplicas[len(route.ReadReplicas)-1].Role = storage.ReplicaRoleTail
			}
		}
		if route.HeadNodeID != "" {
			route.Writable = !chainHasReplicaState(chain, coordinator.ReplicaStateJoining)
		}
		if len(route.ReadReplicas) > 0 {
			route.Readable = true
		}
		snapshot.Slots = append(snapshot.Slots, route)
	}
	s.routingSnapshotMu.Lock()
	s.routingSnapshot = snapshot
	s.routingSnapshotMu.Unlock()
}

func cloneRoutingSnapshot(snapshot RoutingSnapshot) RoutingSnapshot {
	clonedSlots := make([]SlotRoute, 0, len(snapshot.Slots))
	for _, slot := range snapshot.Slots {
		cloned := slot
		cloned.ReadReplicas = append([]ReadReplicaRoute(nil), slot.ReadReplicas...)
		clonedSlots = append(clonedSlots, cloned)
	}
	return RoutingSnapshot{
		Version:   snapshot.Version,
		SlotCount: snapshot.SlotCount,
		Slots:     clonedSlots,
	}
}

func (s *Server) matchesCompleted(slot int, nodeID string, kind pendingKind, slotVersion uint64, _ string) bool {
	return matchesCompletedSlice(s.completedRecords(slot), nodeID, kind, slotVersion)
}

func matchesCompletedRecords(recordsBySlot map[int][]coordruntime.CompletedProgressRecord, slot int, nodeID string, kind pendingKind, slotVersion uint64) bool {
	return matchesCompletedSlice(recordsBySlot[slot], nodeID, kind, slotVersion)
}

func matchesCompletedSlice(records []coordruntime.CompletedProgressRecord, nodeID string, kind pendingKind, slotVersion uint64) bool {
	if len(records) == 0 {
		return false
	}
	matches := 0
	for _, record := range records {
		if record.NodeID != nodeID {
			continue
		}
		switch kind {
		case pendingKindReady:
			if record.Kind == coordruntime.CompletedProgressKindReady && record.SlotVersion == slotVersion {
				return true
			}
			if record.Kind == coordruntime.CompletedProgressKindReady {
				matches++
			}
		case pendingKindRemoved:
			if record.Kind == coordruntime.CompletedProgressKindRemoved && record.SlotVersion == slotVersion {
				return true
			}
			if record.Kind == coordruntime.CompletedProgressKindRemoved {
				matches++
			}
		}
	}
	return matches == 1
}

func activeServingChain(chain coordinator.Chain) coordinator.Chain {
	serving := coordinator.Chain{
		Slot:     chain.Slot,
		Replicas: make([]coordinator.Replica, 0, len(chain.Replicas)),
	}
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		serving.Replicas = append(serving.Replicas, replica)
	}
	return serving
}

func chainHasReplicaState(chain coordinator.Chain, want coordinator.ReplicaState) bool {
	for _, replica := range chain.Replicas {
		if replica.State == want {
			return true
		}
	}
	return false
}

func slotContainsReplicaInState(
	state coordinator.ClusterState,
	slot int,
	nodeID string,
	wantState coordinator.ReplicaState,
) bool {
	if slot < 0 || slot >= len(state.Chains) {
		return false
	}
	for _, replica := range state.Chains[slot].Replicas {
		if replica.NodeID == nodeID && replica.State == wantState {
			return true
		}
	}
	return false
}

func (s *Server) markUnavailableReplicas(report storage.NodeRecoveryReport) {
	s.viewMu.Lock()
	slots := s.unavailableReplicas[report.NodeID]
	if slots == nil {
		slots = map[int]bool{}
		s.unavailableReplicas[report.NodeID] = slots
	}
	for _, replica := range report.Replicas {
		slots[replica.Assignment.Slot] = true
	}
	s.viewMu.Unlock()
	s.rebuildRoutingSnapshot()
}

func (s *Server) clearUnavailable(nodeID string, slot int) {
	s.viewMu.Lock()
	slots, ok := s.unavailableReplicas[nodeID]
	if !ok {
		s.viewMu.Unlock()
		return
	}
	delete(slots, slot)
	if len(slots) == 0 {
		delete(s.unavailableReplicas, nodeID)
	}
	s.viewMu.Unlock()
	s.rebuildRoutingSnapshot()
}

func replicaUnavailable(unavailable map[string]map[int]bool, nodeID string, slot int) bool {
	slots, ok := unavailable[nodeID]
	if !ok {
		return false
	}
	return slots[slot]
}

func (s *Server) nodeHasUnavailableSlots(nodeID string) bool {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	slots, ok := s.unavailableReplicas[nodeID]
	return ok && len(slots) > 0
}

func chainHasUnavailableReplica(chain coordinator.Chain, unavailable map[string]map[int]bool) bool {
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		if unavailable[replica.NodeID][chain.Slot] {
			return true
		}
	}
	return false
}

func currentAssignmentForNode(
	state coordruntime.State,
	nodeID string,
	slot int,
) (storage.ReplicaAssignment, bool) {
	if slot < 0 || slot >= len(state.Cluster.Chains) {
		return storage.ReplicaAssignment{}, false
	}
	chain := state.Cluster.Chains[slot]
	for _, replica := range chain.Replicas {
		if replica.NodeID != nodeID {
			continue
		}
		assignment, err := assignmentForNode(chain, state.Cluster.NodesByID, nodeID, state.SlotVersions[slot])
		if err != nil {
			return storage.ReplicaAssignment{}, false
		}
		return assignment, true
	}
	return storage.ReplicaAssignment{}, false
}

func canResumeRecoveredReplica(recovered storage.RecoveredReplica, current storage.ReplicaAssignment) bool {
	return replicaRoleCanResumeRecovered(current.Role) &&
		recovered.HasCommittedData &&
		recovered.LastKnownState == storage.ReplicaStateActive &&
		assignmentsEquivalentForRecovery(recovered.Assignment, current)
}

func replicaRoleCanResumeRecovered(role storage.ReplicaRole) bool {
	switch role {
	case storage.ReplicaRoleSingle, storage.ReplicaRoleTail:
		return true
	default:
		return false
	}
}

func assignmentsEquivalentForRecovery(left storage.ReplicaAssignment, right storage.ReplicaAssignment) bool {
	return left.Slot == right.Slot &&
		left.ChainVersion == right.ChainVersion &&
		left.Role == right.Role &&
		left.Peers.PredecessorNodeID == right.Peers.PredecessorNodeID &&
		left.Peers.SuccessorNodeID == right.Peers.SuccessorNodeID &&
		(left.Peers.TailNodeID == right.Peers.TailNodeID || left.Peers.TailNodeID == "" || right.Peers.TailNodeID == "")
}

func recoverySourceNodeID(chain coordinator.Chain, recoveringNodeID string) (string, bool) {
	for i, replica := range chain.Replicas {
		if replica.NodeID != recoveringNodeID {
			continue
		}
		if i > 0 {
			return chain.Replicas[i-1].NodeID, true
		}
		if i+1 < len(chain.Replicas) {
			return chain.Replicas[i+1].NodeID, true
		}
		return "", false
	}
	return "", false
}

func cloneRecoveryReport(report storage.NodeRecoveryReport) storage.NodeRecoveryReport {
	cloned := storage.NodeRecoveryReport{
		NodeID:   report.NodeID,
		Replicas: make([]storage.RecoveredReplica, 0, len(report.Replicas)),
	}
	for _, replica := range report.Replicas {
		cloned.Replicas = append(cloned.Replicas, storage.RecoveredReplica{
			Assignment:               replica.Assignment,
			LastKnownState:           replica.LastKnownState,
			HighestCommittedSequence: replica.HighestCommittedSequence,
			HasCommittedData:         replica.HasCommittedData,
		})
	}
	return cloned
}

func (s *Server) applyLivenessTransition(
	ctx context.Context,
	nodeID string,
	state coordruntime.NodeLivenessState,
	evaluatedAtUnixNano int64,
	deadActionFired bool,
) (coordruntime.NodeLivenessRecord, error) {
	rt := s.runtimeRef()
	if _, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.State, attempt int) (coordruntime.State, error) {
		return rt.ApplyLiveness(ctx, coordruntime.Command{
			ID:              fmt.Sprintf("server-liveness-%s-%s-%d-%t-r%d-v%d", nodeID, state, evaluatedAtUnixNano, deadActionFired, attempt, current.Version),
			ExpectedVersion: current.Version,
			Kind:            coordruntime.CommandKindLiveness,
			Liveness: &coordruntime.LivenessCommand{
				NodeID:              nodeID,
				State:               state,
				EvaluatedAtUnixNano: evaluatedAtUnixNano,
				DeadActionFired:     deadActionFired,
				FlapWindowNanos:     s.livenessPolicy.FlapWindow.Nanoseconds(),
			},
		})
	}); err != nil {
		return coordruntime.NodeLivenessRecord{}, fmt.Errorf("err in rt.ApplyLiveness: %w", err)
	}
	s.syncViewsFromRuntime()
	s.rebuildRoutingSnapshot()
	record, _ := s.livenessRecord(nodeID)
	return record, nil
}

func retryOnRuntimeVersionMismatch[T any](
	ctx context.Context,
	rt *coordruntime.Runtime,
	op func(current coordruntime.State, attempt int) (T, error),
) (T, error) {
	var zero T
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		value, err := op(rt.Current(), attempt)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, coordruntime.ErrVersionMismatch) || attempt >= runtimeVersionRetryLimit {
			return zero, err
		}
		time.Sleep(time.Millisecond)
	}
}

func (s *Server) syncViewsFromRuntime() {
	current := s.currentState()
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	s.heartbeats = make(map[string]storage.NodeStatus, len(current.NodeLivenessByID))
	s.liveness = make(map[string]coordruntime.NodeLivenessRecord, len(current.NodeLivenessByID))
	s.pending = make(map[int]PendingWork, len(current.PendingBySlot))
	s.completed = make(map[int][]coordruntime.CompletedProgressRecord, len(current.CompletedProgressBySlot))
	for nodeID, record := range current.NodeLivenessByID {
		s.heartbeats[nodeID] = cloneNodeStatus(record.LastStatus)
		s.liveness[nodeID] = cloneLivenessRecord(record)
	}
	for slot, pending := range current.PendingBySlot {
		s.pending[slot] = PendingWork{
			Slot:        pending.Slot,
			NodeID:      pending.NodeID,
			Kind:        pendingKind(pending.Kind),
			SlotVersion: pending.SlotVersion,
			CommandID:   pending.CommandID,
		}
	}
	for slot, records := range current.CompletedProgressBySlot {
		s.completed[slot] = append([]coordruntime.CompletedProgressRecord(nil), records...)
	}
	s.lastPolicy = current.LastPolicy
}

func (s *Server) runtimeRef() *coordruntime.Runtime {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.rt
}

func (s *Server) currentState() coordruntime.State {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.rt.Current()
}

func (s *Server) replaceRuntime(rt *coordruntime.Runtime) {
	s.runtimeMu.Lock()
	s.rt = rt
	s.runtimeMu.Unlock()
}

func (s *Server) reconfigurationPolicy() coordinator.ReconfigurationPolicy {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	return s.lastPolicy
}

func (s *Server) lastRecoveryReport(nodeID string) (storage.NodeRecoveryReport, bool) {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	report, ok := s.lastRecoveryReports[nodeID]
	return cloneRecoveryReport(report), ok
}

func cloneUnavailableReplicasMap(unavailable map[string]map[int]bool) map[string]map[int]bool {
	cloned := make(map[string]map[int]bool, len(unavailable))
	for nodeID, slots := range unavailable {
		nodeSlots := make(map[int]bool, len(slots))
		for slot, value := range slots {
			nodeSlots[slot] = value
		}
		cloned[nodeID] = nodeSlots
	}
	return cloned
}

func validateServerConfig(cfg ServerConfig) error {
	if cfg.LivenessPolicy.SuspectAfter < 0 || cfg.LivenessPolicy.DeadAfter < 0 || cfg.LivenessPolicy.FlapWindow < 0 {
		return fmt.Errorf("%w: liveness durations must be >= 0", ErrInvalidServerConfig)
	}
	if cfg.LivenessPolicy.DeadAfter > 0 &&
		cfg.LivenessPolicy.SuspectAfter > 0 &&
		cfg.LivenessPolicy.DeadAfter < cfg.LivenessPolicy.SuspectAfter {
		return fmt.Errorf("%w: dead timeout must be >= suspect timeout", ErrInvalidServerConfig)
	}
	normalizedLiveness := normalizeLivenessPolicy(cfg.LivenessPolicy)
	if flapDetectionEnabled(normalizedLiveness) && normalizedLiveness.FlapThreshold < 2 {
		return fmt.Errorf("%w: flap threshold must be >= 2 when enabled", ErrInvalidServerConfig)
	}
	if cfg.DispatchTimeout < 0 {
		return fmt.Errorf("%w: dispatch timeout must be >= 0", ErrInvalidServerConfig)
	}
	if cfg.ReconfigurationPolicy.MaxChangedChains < 0 {
		return fmt.Errorf("%w: max changed chains must be >= 0", ErrInvalidServerConfig)
	}
	if cfg.DispatchRetryInterval < 0 {
		return fmt.Errorf("%w: dispatch retry interval must be >= 0", ErrInvalidServerConfig)
	}
	if cfg.RecoveryCommandTimeout < 0 {
		return fmt.Errorf("%w: recovery command timeout must be >= 0", ErrInvalidServerConfig)
	}
	if cfg.HA != nil {
		if cfg.HA.CoordinatorID == "" {
			return fmt.Errorf("%w: ha coordinator id must not be empty", ErrInvalidServerConfig)
		}
		if cfg.HA.Store == nil {
			return fmt.Errorf("%w: ha store must not be nil", ErrInvalidServerConfig)
		}
		if cfg.HA.LeaseTTL < 0 {
			return fmt.Errorf("%w: ha lease ttl must be >= 0", ErrInvalidServerConfig)
		}
		if cfg.HA.RenewInterval < 0 {
			return fmt.Errorf("%w: ha renew interval must be >= 0", ErrInvalidServerConfig)
		}
	}
	return nil
}

func deriveDeadlineContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) <= timeout {
			return ctx, func() {}
		}
	}
	return context.WithTimeout(ctx, timeout)
}

func isContextTimeoutOrCancel(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func cloneLivenessRecord(record coordruntime.NodeLivenessRecord) coordruntime.NodeLivenessRecord {
	return coordruntime.NodeLivenessRecord{
		LastHeartbeatUnixNano:      record.LastHeartbeatUnixNano,
		State:                      record.State,
		LastStatus:                 cloneNodeStatus(record.LastStatus),
		DeadActionFired:            record.DeadActionFired,
		SuspectTransitionsUnixNano: append([]int64(nil), record.SuspectTransitionsUnixNano...),
	}
}

func normalizeLivenessPolicy(policy LivenessPolicy) LivenessPolicy {
	if policy.SuspectAfter > 0 &&
		policy.DeadAfter > 0 &&
		policy.FlapThreshold == 0 &&
		policy.FlapWindow == 0 {
		policy.FlapThreshold = defaultFlapThreshold
		policy.FlapWindow = defaultFlapWindow
	}
	return policy
}

func flapDetectionEnabled(policy LivenessPolicy) bool {
	return policy.FlapThreshold > 0 && policy.FlapWindow > 0
}

func cloneNodeStatus(status storage.NodeStatus) storage.NodeStatus {
	return storage.NodeStatus{
		NodeID:          status.NodeID,
		ReplicaCount:    status.ReplicaCount,
		ActiveCount:     status.ActiveCount,
		CatchingUpCount: status.CatchingUpCount,
		LeavingCount:    status.LeavingCount,
	}
}

func cloneFailureDomains(domains map[string]string) map[string]string {
	cloned := make(map[string]string, len(domains))
	for key, value := range domains {
		cloned[key] = value
	}
	return cloned
}

func nodeMarkedDead(cluster coordinator.ClusterState, nodeID string) bool {
	return cluster.NodeHealthByID[nodeID] == coordinator.NodeHealthDead
}

func isRuntimeInitialized(state coordruntime.State) bool {
	return state.Cluster.SlotCount > 0
}
