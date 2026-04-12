package coordserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
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
	LivenessPolicy          LivenessPolicy
	ReconfigurationPolicy   coordinator.ReconfigurationPolicy
	StartupMaxChangedChains int
	Clock                   Clock
	AsyncHotPathDispatch    bool
	DisableBackgroundLoops  bool
	DispatchTimeout         time.Duration
	DispatchRetryInterval   time.Duration
	RecoveryCommandTimeout  time.Duration
	NodeClientFactory       NodeClientFactory
	HA                      *HAConfig
	Logger                  *zerolog.Logger
	MetricsRegistry         *prometheus.Registry
}

type NodeClientFactory interface {
	ClientForNode(node coordinator.Node) (StorageNodeClient, error)
}

type Server struct {
	rt                      *coordruntime.Runtime
	runtimeMu               sync.RWMutex
	nodes                   map[string]StorageNodeClient
	nodesMu                 sync.RWMutex
	heartbeats              map[string]storage.NodeStatus
	liveness                map[string]coordruntime.NodeLivenessRecord
	pending                 map[int]PendingWork
	completed               map[int][]coordruntime.CompletedProgressRecord
	runtimeVersion          uint64
	runtimeOutbox           []coordruntime.OutboxEntry
	routingSnapshot         RoutingSnapshot
	routingSnapshotMu       sync.RWMutex
	viewMu                  sync.RWMutex
	lastPolicy              coordinator.ReconfigurationPolicy
	startupMaxChangedChains int
	startupBudgetActive     bool
	unavailableReplicas     map[string]map[int]bool
	lastRecoveryReports     map[string]storage.NodeRecoveryReport
	livenessPolicy          LivenessPolicy
	clock                   Clock
	asyncHotPathDispatch    bool
	activePeerRefresh       map[int]activePeerRefreshState
	activePeerRefreshMu     sync.Mutex
	dispatchTimeout         time.Duration
	dispatchRetryInterval   time.Duration
	recoveryCommandTimeout  time.Duration
	nodeClientFactory       NodeClientFactory
	logger                  zerolog.Logger
	metrics                 *serverMetrics
	events                  *serverEventRecorder
	ha                      *haController
	dispatchEngine          *dispatchEngine
	dispatchNotify          chan struct{}
	closeOnce               sync.Once
	closeCh                 chan struct{}
	backgroundCtx           context.Context
	backgroundCancel        context.CancelFunc
}

type activePeerRefreshState struct {
	fallbackServingChain coordinator.Chain
	assignmentChain      coordinator.Chain
	useFallbackRoute     bool
	allowWhilePending    bool
	remainingNodeIDs     map[string]bool
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
	backgroundCtx, backgroundCancel := context.WithCancel(context.Background())

	server := &Server{
		rt:                      rt,
		nodes:                   clonedNodes,
		heartbeats:              map[string]storage.NodeStatus{},
		liveness:                map[string]coordruntime.NodeLivenessRecord{},
		pending:                 map[int]PendingWork{},
		completed:               map[int][]coordruntime.CompletedProgressRecord{},
		unavailableReplicas:     map[string]map[int]bool{},
		lastRecoveryReports:     map[string]storage.NodeRecoveryReport{},
		livenessPolicy:          normalizeLivenessPolicy(cfg.LivenessPolicy),
		asyncHotPathDispatch:    cfg.AsyncHotPathDispatch,
		activePeerRefresh:       map[int]activePeerRefreshState{},
		lastPolicy:              cfg.ReconfigurationPolicy,
		startupMaxChangedChains: cfg.StartupMaxChangedChains,
		startupBudgetActive:     cfg.StartupMaxChangedChains > 0,
		clock:                   cfg.Clock,
		dispatchTimeout:         cfg.DispatchTimeout,
		dispatchRetryInterval:   cfg.DispatchRetryInterval,
		recoveryCommandTimeout:  cfg.RecoveryCommandTimeout,
		nodeClientFactory:       cfg.NodeClientFactory,
		logger:                  coordLoggerFromConfig(cfg.Logger),
		metrics:                 newServerMetrics(cfg.MetricsRegistry),
		events:                  newServerEventRecorder(),
		dispatchNotify:          make(chan struct{}, 1),
		closeCh:                 make(chan struct{}),
		backgroundCtx:           backgroundCtx,
		backgroundCancel:        backgroundCancel,
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
	server.startDispatchLoop(cfg.HA == nil && !cfg.DisableBackgroundLoops)
	if cfg.HA != nil {
		if err := server.enableHA(ctx, *cfg.HA); err != nil {
			server.backgroundCancel()
			return nil, fmt.Errorf("err in server.enableHA: %w", err)
		}
	}
	return server, nil
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		if s.backgroundCancel != nil {
			s.backgroundCancel()
		}
		close(s.closeCh)
		if s.dispatchEngine != nil && s.dispatchEngine.done != nil {
			<-s.dispatchEngine.done
		}
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
	return s.currentState()
}

func (s *Server) CurrentVersion() uint64 {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.rt.CurrentVersion()
}

func (s *Server) setNodeClient(nodeID string, client StorageNodeClient) {
	s.nodesMu.Lock()
	s.nodes[nodeID] = client
	s.nodesMu.Unlock()
}

func (s *Server) deleteNodeClient(nodeID string) {
	s.nodesMu.Lock()
	delete(s.nodes, nodeID)
	s.nodesMu.Unlock()
}

func (s *Server) nodeClient(nodeID string) (StorageNodeClient, bool) {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	client, ok := s.nodes[nodeID]
	return client, ok
}

func (s *Server) nodeClientCount() int {
	s.nodesMu.RLock()
	defer s.nodesMu.RUnlock()
	return len(s.nodes)
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

func (s *Server) routingSnapshotReadOnly() RoutingSnapshot {
	s.routingSnapshotMu.RLock()
	defer s.routingSnapshotMu.RUnlock()
	return cloneRoutingSnapshot(s.routingSnapshot)
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
	node, ok := s.currentStateView().Cluster.NodesByID[nodeID]
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
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		return submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (coordruntime.State, error) {
			return s.Bootstrap(engineCtx, cmd)
		})
	}
	if s.ha != nil {
		return s.applyHABootstrap(ctx, cmd)
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
	if s.dispatchEngine != nil {
		if err := s.syncAndSubmitDispatch(ctx, nil, true, false); err != nil {
			s.observeCommandResult("bootstrap", err)
			return coordruntime.State{}, err
		}
	} else {
		s.syncViewsFromRuntime()
		s.rebuildRoutingSnapshot()
	}
	s.observeCommandResult("bootstrap", nil)
	s.events.record(s.logger, zerolog.InfoLevel, "bootstrap", "coordinator bootstrapped cluster", "", nil, ops.Uint64Ptr(state.Version), "", cmd.ID, nil)
	return state, nil
}

func (s *Server) AddNode(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		return submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (coordruntime.State, error) {
			return s.AddNode(engineCtx, cmd)
		})
	}
	if s.ha != nil {
		return s.applyHAMembershipMutation(ctx, cmd, coordinator.EventKindAddNode)
	}
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindAddNode)
}

func (s *Server) BeginDrainNode(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		return submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (coordruntime.State, error) {
			return s.BeginDrainNode(engineCtx, cmd)
		})
	}
	if s.ha != nil {
		return s.applyHAMembershipMutation(ctx, cmd, coordinator.EventKindBeginDrainNode)
	}
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindBeginDrainNode)
}

func (s *Server) MarkNodeDead(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		return submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (coordruntime.State, error) {
			return s.MarkNodeDead(engineCtx, cmd)
		})
	}
	if s.ha != nil {
		return s.applyHAMembershipMutation(ctx, cmd, coordinator.EventKindMarkNodeDead)
	}
	return s.applyMembershipMutation(ctx, cmd, coordinator.EventKindMarkNodeDead)
}

func (s *Server) ReportReplicaReady(ctx context.Context, nodeID string, slot int, epoch uint64, commandID string) (coordruntime.State, error) {
	if s.ha != nil {
		return s.applyHAReplicaReady(ctx, nodeID, slot, epoch, commandID)
	}
	return s.applyReplicaReadyDirect(ctx, nodeID, slot, epoch, commandID)
}

func (s *Server) applyReplicaReadyDirect(ctx context.Context, nodeID string, slot int, epoch uint64, commandID string) (coordruntime.State, error) {
	defer s.refreshMetricGauges()
	rt := s.runtimeRef()
	duplicateCompleted := false
	refreshState := activePeerRefreshState{}
	enqueuePeerRefresh := false
	updatedSlotVersion := uint64(0)
	updated, err := retryOnRuntimeSlotProgressVersionMismatch(ctx, rt, slot, func(current coordruntime.SlotProgressView, attempt int) (coordruntime.State, error) {
		reduction, err := reduceReplicaReadyProgress(current, nodeID, commandID, attempt)
		if err != nil {
			return coordruntime.State{}, err
		}
		updatedSlotVersion = reduction.slotVersion
		if reduction.duplicateCompleted {
			duplicateCompleted = true
			return coordruntime.State{Version: current.Version}, nil
		}
		enqueuePeerRefresh = reduction.enqueuePeerRefresh
		refreshState = reduction.peerRefreshState
		version, err := rt.ApplyProgressVersion(ctx, coordruntime.Command{
			ID:              reduction.progressCommandID,
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
		if err != nil {
			return coordruntime.State{}, err
		}
		return coordruntime.State{Version: version}, nil
	})
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in rt.ApplyProgress: %w", err)
	}
	if duplicateCompleted {
		return updated, nil
	}
	if s.dispatchEngine != nil {
		var changes []dispatchPeerRefreshChange
		if enqueuePeerRefresh {
			changes = append(changes, dispatchPeerRefreshChange{
				slot:  slot,
				state: refreshState,
			})
		}
		wait := !s.asyncHotPathDispatch && !s.dispatchEngine.isBusy()
		if err := s.syncAndSubmitDispatch(ctx, changes, wait, true); err != nil {
			return coordruntime.State{}, err
		}
	} else if s.asyncHotPathDispatch {
		// Defer the expensive sync+rebuild to the dispatch loop. The
		// runtime already has the authoritative state; the server's views
		// and routing snapshot will be refreshed on the next dispatch pass.
		// This keeps the hot-path progress handler O(1) per slot instead
		// of O(N) where N = total slot count.
		if enqueuePeerRefresh {
			s.enqueueActivePeerRefresh(slot, refreshState)
		}
		s.notifyDispatchLoop()
	} else {
		s.syncViewsFromRuntime()
		if enqueuePeerRefresh {
			s.enqueueActivePeerRefresh(slot, refreshState)
		}
		s.rebuildRoutingSnapshot()
		if err := s.reconcileAndDispatch(ctx); err != nil {
			return coordruntime.State{}, err
		}
		if enqueuePeerRefresh {
			if err := s.dispatchQueuedActivePeerRefreshes(ctx); err != nil {
				return coordruntime.State{}, err
			}
		}
	}
	s.events.record(s.logger, zerolog.InfoLevel, "replica_ready", "coordinator accepted replica ready progress", nodeID, ops.IntPtr(slot), ops.Uint64Ptr(updatedSlotVersion), "", commandID, nil)
	return updated, nil
}

func (s *Server) ReportReplicaRemoved(ctx context.Context, nodeID string, slot int, epoch uint64, commandID string) (coordruntime.State, error) {
	if s.ha != nil {
		return s.applyHAReplicaRemoved(ctx, nodeID, slot, epoch, commandID)
	}
	return s.applyReplicaRemovedDirect(ctx, nodeID, slot, epoch, commandID)
}

func (s *Server) applyReplicaRemovedDirect(ctx context.Context, nodeID string, slot int, epoch uint64, commandID string) (coordruntime.State, error) {
	defer s.refreshMetricGauges()
	rt := s.runtimeRef()
	duplicateCompleted := false
	updatedSlotVersion := uint64(0)
	updated, err := retryOnRuntimeSlotProgressVersionMismatch(ctx, rt, slot, func(current coordruntime.SlotProgressView, attempt int) (coordruntime.State, error) {
		reduction, err := reduceReplicaRemovedProgress(current, nodeID, commandID, attempt)
		if err != nil {
			return coordruntime.State{}, err
		}
		updatedSlotVersion = reduction.slotVersion
		if reduction.duplicateCompleted {
			duplicateCompleted = true
			return coordruntime.State{Version: current.Version}, nil
		}
		version, err := rt.ApplyProgressVersion(ctx, coordruntime.Command{
			ID:              reduction.progressCommandID,
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
		if err != nil {
			return coordruntime.State{}, err
		}
		return coordruntime.State{Version: version}, nil
	})
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in rt.ApplyProgress: %w", err)
	}
	if duplicateCompleted {
		return updated, nil
	}
	if s.dispatchEngine != nil {
		wait := !s.asyncHotPathDispatch && !s.dispatchEngine.isBusy()
		if err := s.syncAndSubmitDispatch(ctx, []dispatchPeerRefreshChange{{
			slot:  slot,
			state: activePeerRefreshState{},
		}}, wait, true); err != nil {
			return coordruntime.State{}, err
		}
	} else if s.asyncHotPathDispatch {
		s.enqueueActivePeerRefresh(slot, activePeerRefreshState{})
		s.notifyDispatchLoop()
	} else {
		s.syncViewsFromRuntime()
		s.enqueueActivePeerRefresh(slot, activePeerRefreshState{})
		s.rebuildRoutingSnapshot()
		if err := s.reconcileAndDispatch(ctx); err != nil {
			return coordruntime.State{}, err
		}
		if err := s.dispatchQueuedActivePeerRefreshes(ctx); err != nil {
			return coordruntime.State{}, err
		}
	}
	s.events.record(s.logger, zerolog.InfoLevel, "replica_removed", "coordinator accepted replica removed progress", nodeID, ops.IntPtr(slot), ops.Uint64Ptr(updatedSlotVersion), "", commandID, nil)
	return updated, nil
}

func (s *Server) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	if s.ha != nil {
		return s.applyHAHeartbeat(ctx, status)
	}
	if _, ok := s.currentStateView().Cluster.NodesByID[status.NodeID]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	current := s.currentStateView()
	currentRecord, hadCurrentRecord := s.livenessRecord(status.NodeID)
	wasDead := currentRecord.State == coordruntime.NodeLivenessStateDead ||
		nodeMarkedDead(current.Cluster, status.NodeID)
	if nodeMarkedDead(current.Cluster, status.NodeID) {
		if _, err := s.applyLivenessTransition(ctx, status.NodeID, coordruntime.NodeLivenessStateDead, s.clock.Now().UnixNano(), true); err != nil {
			return err
		}
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	observedAt := s.clock.Now().UnixNano()
	s.recordFreshHeartbeat(status, observedAt)
	if currentRecord.State == coordruntime.NodeLivenessStateSuspect {
		if _, err := s.applyLivenessTransition(
			ctx,
			status.NodeID,
			coordruntime.NodeLivenessStateHealthy,
			observedAt,
			currentRecord.DeadActionFired,
		); err != nil {
			return err
		}
	}
	becameReady := false
	if !current.Cluster.ReadyNodeIDs[status.NodeID] {
		rt := s.runtimeRef()
		_, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.View, attempt int) (struct{}, error) {
			if current.Cluster.ReadyNodeIDs[status.NodeID] {
				return struct{}{}, nil
			}
			_, err := rt.MarkNodeReady(ctx, coordruntime.Command{
				ID:              fmt.Sprintf("server-node-ready-%s-%d-r%d-v%d", status.NodeID, observedAt, attempt, current.Version),
				ExpectedVersion: current.Version,
				Kind:            coordruntime.CommandKindNodeReady,
				NodeReady: &coordruntime.NodeReadyCommand{
					Status:             status,
					ObservedAtUnixNano: observedAt,
					FlapWindowNanos:    s.livenessPolicy.FlapWindow.Nanoseconds(),
				},
			})
			return struct{}{}, err
		})
		if err != nil {
			err = fmt.Errorf("err in rt.MarkNodeReady: %w", err)
			s.observeTimeoutOrFailure("node_ready", err)
			return err
		}
		becameReady = true
		if s.dispatchEngine != nil {
			if err := s.syncAndSubmitDispatch(ctx, nil, !s.asyncHotPathDispatch, true); err != nil {
				if errors.Is(err, ErrDispatchFailed) || errors.Is(err, ErrDispatchTimeout) {
					s.logger.Warn().Err(err).Str("component", "coordserver").Str("node_id", status.NodeID).Msg("coordinator heartbeat triggered durable repair work that will retry later")
				} else {
					return err
				}
			}
		} else if !s.asyncHotPathDispatch {
			s.syncViewsFromRuntime()
			s.rebuildRoutingSnapshot()
		}
	}
	if becameReady && s.dispatchEngine == nil {
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
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		_, err := submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (struct{}, error) {
			return struct{}{}, s.EvaluateLiveness(engineCtx)
		})
		return err
	}
	if s.livenessPolicy.SuspectAfter > 0 && s.livenessPolicy.DeadAfter > 0 {
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
			target := reduceNodeLivenessTarget(record, nowUnix, s.livenessPolicy)

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
				if s.currentLivenessView().PendingBySlotCount > 0 {
					continue
				}
				current := s.currentLivenessView()
				if current.NodeHealthByID[nodeID] == coordinator.NodeHealthDead || !current.Initialized {
					updated, err := s.applyLivenessTransition(ctx, nodeID, coordruntime.NodeLivenessStateDead, nowUnix, true)
					if err != nil {
						return err
					}
					record = updated
					_ = record
					continue
				}

				current = s.currentLivenessView()
				if _, err := s.applyMembershipMutation(ctx, coordruntime.Command{
					ID:              fmt.Sprintf("server-auto-dead-%s-v%d", nodeID, current.Version),
					ExpectedVersion: current.Version,
					Kind:            coordruntime.CommandKindReconfigure,
					Reconfigure: &coordruntime.ReconfigureCommand{
						Events: []coordinator.Event{{
							Kind:   coordinator.EventKindMarkNodeDead,
							NodeID: nodeID,
						}},
						Policy: s.reconfigurationPolicy(),
					},
				}, coordinator.EventKindMarkNodeDead); err != nil {
					return fmt.Errorf("err in s.applyMembershipMutation(mark_dead): %w", err)
				}
				if _, err := s.applyLivenessTransition(ctx, nodeID, coordruntime.NodeLivenessStateDead, nowUnix, true); err != nil {
					return err
				}
				s.observeRepair("mark_dead", "success", nodeID, nil, nil)
			}
		}
	}
	current := s.currentLivenessView()
	if current.NeedsPeriodicAdvance &&
		current.PendingBySlotCount == 0 &&
		current.OutboxCount == 0 &&
		len(s.snapshotActivePeerRefreshSlots()) == 0 {
		if err := s.reconcileState(ctx); err != nil {
			return err
		}
		if s.ha != nil {
			s.refreshMetricGauges()
			return nil
		}
		if s.dispatchEngine != nil {
			if err := s.syncAndSubmitDispatch(ctx, nil, !s.asyncHotPathDispatch, false); err != nil {
				if errors.Is(err, ErrDispatchFailed) || errors.Is(err, ErrDispatchTimeout) {
					s.logger.Warn().Err(err).Str("component", "coordserver").Msg("periodic liveness evaluation triggered repair work that will retry later")
				} else {
					return err
				}
			}
		} else if s.asyncHotPathDispatch {
			s.notifyDispatchLoop()
		} else {
			if err := s.dispatchRuntimeOutbox(ctx); err != nil {
				if errors.Is(err, ErrDispatchFailed) || errors.Is(err, ErrDispatchTimeout) {
					s.logger.Warn().Err(err).Str("component", "coordserver").Msg("periodic liveness evaluation triggered repair work that will retry later")
				} else {
					return err
				}
			}
		}
	}
	s.refreshMetricGauges()
	return nil
}

func (s *Server) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		_, err := submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (struct{}, error) {
			return struct{}{}, s.ReportNodeRecovered(engineCtx, report)
		})
		return err
	}
	if s.ha != nil {
		return s.applyHARecoveryReport(ctx, report)
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
	if s.dispatchEngine != nil {
		if err := s.syncAndSubmitDispatch(ctx, nil, !s.asyncHotPathDispatch, false); err != nil {
			return err
		}
	} else {
		s.rebuildRoutingSnapshot()
	}
	s.events.record(s.logger, zerolog.InfoLevel, "node_recovered_report", "coordinator processed node recovered report", report.NodeID, nil, nil, "", "", nil)
	return nil
}

func (s *Server) RegisterNode(ctx context.Context, reg storage.NodeRegistration) (coordruntime.State, error) {
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		return submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (coordruntime.State, error) {
			return s.RegisterNode(engineCtx, reg)
		})
	}
	current := s.currentStateView()
	if existing, ok := current.Cluster.NodesByID[reg.NodeID]; ok &&
		current.Cluster.NodeHealthByID[reg.NodeID] != coordinator.NodeHealthDead &&
		existing.RPCAddress == reg.RPCAddress &&
		reflect.DeepEqual(existing.FailureDomains, reg.FailureDomains) {
		return s.currentState(), nil
	}
	if s.ha != nil {
		return s.applyHARegisterNode(ctx, reg)
	}
	rt := s.runtimeRef()
	return retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.View, attempt int) (coordruntime.State, error) {
		if existing, ok := current.Cluster.NodesByID[reg.NodeID]; ok &&
			current.Cluster.NodeHealthByID[reg.NodeID] != coordinator.NodeHealthDead &&
			existing.RPCAddress == reg.RPCAddress &&
			reflect.DeepEqual(existing.FailureDomains, reg.FailureDomains) {
			return s.currentState(), nil
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
	if s.ha != nil {
		return s.applyHAMembershipMutation(ctx, cmd, expectedEvent)
	}
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
	if s.dispatchEngine != nil {
		syncChanges := make([]dispatchPeerRefreshChange, 0, len(plan.ChangedSlots))
		asyncChanges := make([]dispatchPeerRefreshChange, 0, len(plan.ChangedSlots))
		for _, slotPlan := range plan.ChangedSlots {
			change := dispatchPeerRefreshChange{
				slot:  slotPlan.Slot,
				state: activePeerRefreshState{},
			}
			if slotPlanHasOnlyStepKind(slotPlan, coordinator.StepKindAppendTail) {
				change.state = activePeerRefreshState{
					assignmentChain:   cloneCoordinatorChain(slotPlan.After),
					allowWhilePending: true,
				}
				if s.asyncHotPathDispatch {
					asyncChanges = append(asyncChanges, change)
					continue
				}
			}
			syncChanges = append(syncChanges, change)
		}
		if err := s.syncAndSubmitDispatch(ctx, syncChanges, true, false); err != nil {
			s.observeCommandResult(string(expectedEvent), err)
			return coordruntime.State{}, err
		}
		if len(asyncChanges) > 0 {
			if err := s.syncAndSubmitDispatch(ctx, asyncChanges, false, false); err != nil {
				s.observeCommandResult(string(expectedEvent), err)
				return coordruntime.State{}, err
			}
		}
		s.observeCommandResult(string(expectedEvent), nil)
		s.events.record(s.logger, zerolog.InfoLevel, "membership_mutation", "coordinator applied membership mutation", "", nil, ops.Uint64Ptr(state.Version), "", cmd.ID, nil)
		return state, nil
	}
	s.rebuildRoutingSnapshot()
	if err := s.dispatchRuntimeOutbox(ctx); err != nil {
		s.observeCommandResult(string(expectedEvent), err)
		return coordruntime.State{}, err
	}
	for _, slotPlan := range plan.ChangedSlots {
		if slotPlanHasOnlyStepKind(slotPlan, coordinator.StepKindAppendTail) {
			refreshState := activePeerRefreshState{
				assignmentChain:   cloneCoordinatorChain(slotPlan.After),
				allowWhilePending: true,
			}
			if s.asyncHotPathDispatch {
				s.enqueueActivePeerRefresh(slotPlan.Slot, refreshState)
				s.rebuildRoutingSnapshot()
				s.notifyDispatchLoop()
			} else {
				nextRefresh, err := s.dispatchActivePeerUpdates(ctx, slotPlan.Slot, refreshState)
				if len(nextRefresh.remainingNodeIDs) > 0 {
					s.enqueueActivePeerRefresh(slotPlan.Slot, nextRefresh)
					s.rebuildRoutingSnapshot()
				}
				if err != nil {
					s.observeCommandResult(string(expectedEvent), err)
					return coordruntime.State{}, err
				}
			}
			continue
		}
		if !s.shouldDispatchActivePeerRefresh(slotPlan.Slot) {
			continue
		}
		nextRefresh, err := s.dispatchActivePeerUpdates(ctx, slotPlan.Slot, activePeerRefreshState{})
		if len(nextRefresh.remainingNodeIDs) > 0 {
			s.enqueueActivePeerRefresh(slotPlan.Slot, nextRefresh)
			s.rebuildRoutingSnapshot()
		}
		if err != nil {
			s.observeCommandResult(string(expectedEvent), err)
			return coordruntime.State{}, err
		}
	}
	s.observeCommandResult(string(expectedEvent), nil)
	s.events.record(s.logger, zerolog.InfoLevel, "membership_mutation", "coordinator applied membership mutation", "", nil, ops.Uint64Ptr(state.Version), "", cmd.ID, nil)
	return state, nil
}

func (s *Server) reconcileAndDispatch(ctx context.Context) error {
	if s.dispatchEngine != nil {
		if contextInCoordinatorEngine(ctx) {
			return s.reconcileAndDispatchOwned(ctx)
		}
		return s.dispatchEngine.drainUntilIdle(ctx, nil, true)
	}
	return s.reconcileAndDispatchOwned(ctx)
}

func (s *Server) reconcileAndDispatchOwned(ctx context.Context) error {
	if err := s.reconcileState(ctx); err != nil {
		return err
	}
	if err := s.dispatchRuntimeOutboxOwned(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Server) reconcileState(ctx context.Context) error {
	if s.ha != nil {
		return s.applyHAReconcile(ctx)
	}
	rt := s.runtimeRef()
	changed := false
	_, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.View, attempt int) (coordruntime.State, error) {
		preview, err := coordinator.PlanReconfiguration(current.Cluster, nil, s.reconfigurationPolicy())
		if err != nil {
			return coordruntime.State{}, fmt.Errorf("err in coordinator.PlanReconfiguration preview: %w", err)
		}
		if len(preview.ChangedSlots) == 0 && reflect.DeepEqual(preview.UpdatedState, current.Cluster) {
			return coordruntime.State{Version: current.Version}, nil
		}
		changed = true

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
	if changed {
		s.syncViewsFromRuntime()
		if s.dispatchEngine == nil {
			s.rebuildRoutingSnapshot()
		}
	}
	return nil
}

func (s *Server) startDispatchLoop(backgroundEnabled bool) {
	s.dispatchEngine = newDispatchEngine(s, backgroundEnabled)
	s.dispatchEngine.start()
}

func (s *Server) notifyDispatchLoop() {
	if s.dispatchEngine != nil {
		s.dispatchEngine.wake(false)
		return
	}
	select {
	case s.dispatchNotify <- struct{}{}:
	default:
	}
}

func (s *Server) backgroundContext() context.Context {
	if s.backgroundCtx == nil {
		return context.Background()
	}
	return s.backgroundCtx
}

func (s *Server) runBackgroundDispatchOnce() {
	if s.dispatchEngine != nil {
		if err := s.dispatchEngine.drainUntilIdle(s.backgroundContext(), nil, false); err != nil {
			s.logDispatchLoopError(err)
		}
		return
	}
}

type backgroundDispatchState struct {
	Version             uint64
	OutboxEntries       int
	PendingEntries      int
	ActivePeerRefreshes int
}

func (s *Server) backgroundDispatchSignature() backgroundDispatchState {
	s.viewMu.RLock()
	version := s.runtimeVersion
	outboxEntries := len(s.runtimeOutbox)
	pendingEntries := len(s.pending)
	s.viewMu.RUnlock()
	return backgroundDispatchState{
		Version:             version,
		OutboxEntries:       outboxEntries,
		PendingEntries:      pendingEntries,
		ActivePeerRefreshes: len(s.snapshotActivePeerRefreshSlots()),
	}
}

func (s backgroundDispatchState) hasWork() bool {
	return s.OutboxEntries > 0 || s.PendingEntries > 0 || s.ActivePeerRefreshes > 0
}

func (s *Server) runtimeOutboxSnapshot() []coordruntime.OutboxEntry {
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	return cloneRuntimeOutbox(s.runtimeOutbox)
}

func (s *Server) dispatchRuntimeOutbox(ctx context.Context) error {
	if s.dispatchEngine != nil {
		if contextInCoordinatorEngine(ctx) {
			return s.dispatchRuntimeOutboxOwned(ctx)
		}
		return s.dispatchEngine.drainUntilIdle(ctx, nil, false)
	}
	return s.dispatchRuntimeOutboxOwned(ctx)
}

func (s *Server) dispatchRuntimeOutboxOwned(ctx context.Context) error {
	_, err := s.dispatchRuntimeOutboxBatchOwned(ctx, -1)
	return err
}

func (s *Server) dispatchRuntimeOutboxBatchOwned(ctx context.Context, maxEntries int) (bool, error) {
	entries := s.runtimeOutboxSnapshot()
	if len(entries) == 0 {
		s.rebuildRoutingSnapshot()
		return false, nil
	}
	if maxEntries < 0 {
		maxEntries = len(entries)
	} else if maxEntries == 0 {
		maxEntries = outboxDispatchWorkerCount(len(entries))
	}
	more := len(entries) > maxEntries
	if more {
		entries = append([]coordruntime.OutboxEntry(nil), entries[:maxEntries]...)
	}

	results := make([]error, len(entries))
	if len(entries) == 1 {
		results[0] = s.dispatchRuntimeOutboxEntry(ctx, entries[0])
	} else {
		workerCount := outboxDispatchWorkerCount(len(entries))
		taskCh := make(chan dispatchTask)
		var wg sync.WaitGroup
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range taskCh {
					if ctx.Err() != nil {
						results[task.index] = ctx.Err()
						continue
					}
					results[task.index] = s.dispatchRuntimeOutboxEntry(ctx, entries[task.index])
				}
			}()
		}
		for index, entry := range entries {
			_ = entry
			taskCh <- dispatchTask{index: index}
		}
		close(taskCh)
		wg.Wait()
	}

	for index := range entries {
		if results[index] == nil {
			continue
		}
		firstErr := results[index]
		successIDs := acknowledgedOutboxEntryIDs(entries, results)
		if len(successIDs) > 0 {
			rt := s.runtimeRef()
			if _, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.View, attempt int) (coordruntime.State, error) {
				version, err := rt.AcknowledgeOutboxVersion(ctx, coordruntime.Command{
					ID:              outboxAckCommandID(current.Version, successIDs),
					ExpectedVersion: current.Version,
					Kind:            coordruntime.CommandKindAcknowledgeOutbox,
					AcknowledgeOutbox: &coordruntime.AcknowledgeOutboxCommand{
						EntryIDs: append([]string(nil), successIDs...),
					},
				})
				if err != nil {
					return coordruntime.State{}, err
				}
				return coordruntime.State{Version: version}, nil
			}); err != nil {
				return more, fmt.Errorf("err in rt.AcknowledgeOutbox: %w", err)
			}
			s.syncViewsFromRuntime()
		}
		s.rebuildRoutingSnapshot()
		if len(s.runtimeOutboxSnapshot()) > 0 {
			more = true
		}
		return more, firstErr
	}
	successIDs := acknowledgedOutboxEntryIDs(entries, results)
	if len(successIDs) > 0 {
		rt := s.runtimeRef()
		if _, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.View, attempt int) (coordruntime.State, error) {
			version, err := rt.AcknowledgeOutboxVersion(ctx, coordruntime.Command{
				ID:              outboxAckCommandID(current.Version, successIDs),
				ExpectedVersion: current.Version,
				Kind:            coordruntime.CommandKindAcknowledgeOutbox,
				AcknowledgeOutbox: &coordruntime.AcknowledgeOutboxCommand{
					EntryIDs: append([]string(nil), successIDs...),
				},
			})
			if err != nil {
				return coordruntime.State{}, err
			}
			return coordruntime.State{Version: version}, nil
		}); err != nil {
			return more, fmt.Errorf("err in rt.AcknowledgeOutbox: %w", err)
		}
		s.syncViewsFromRuntime()
	}
	s.rebuildRoutingSnapshot()
	if len(s.runtimeOutboxSnapshot()) > 0 {
		more = true
	}
	return more, nil
}

func outboxDispatchWorkerCount(entryCount int) int {
	if entryCount <= 1 {
		return entryCount
	}
	// Cap at 8 to limit concurrent add_replica_as_tail dispatches. Each
	// dispatch can trigger an inline auto-activation that calls
	// ReportReplicaReady back to the coordinator. Too many concurrent
	// callers saturate the runtime's single mutex, starving the dispatch
	// loop and causing cascading timeouts.
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	if workers > 8 {
		workers = 8
	}
	if workers > entryCount {
		return entryCount
	}
	return workers
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

func (s *Server) shouldDispatchActivePeerRefresh(slot int, refresh ...activePeerRefreshState) bool {
	if len(refresh) > 0 && refresh[0].allowWhilePending {
		return true
	}
	s.viewMu.RLock()
	_, pending := s.pending[slot]
	s.viewMu.RUnlock()
	if pending {
		return false
	}
	return !runtimeOutboxHasSlot(s.currentStateView().Outbox, slot)
}

func (s *Server) enqueueActivePeerRefresh(slot int, state activePeerRefreshState) {
	s.activePeerRefreshMu.Lock()
	s.activePeerRefresh[slot] = cloneActivePeerRefreshState(state)
	s.activePeerRefreshMu.Unlock()
}

func (s *Server) updateActivePeerRefresh(slot int, state activePeerRefreshState) {
	s.activePeerRefreshMu.Lock()
	s.activePeerRefresh[slot] = cloneActivePeerRefreshState(state)
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

func (s *Server) snapshotActivePeerRefreshStates() map[int]activePeerRefreshState {
	s.activePeerRefreshMu.Lock()
	defer s.activePeerRefreshMu.Unlock()
	cloned := make(map[int]activePeerRefreshState, len(s.activePeerRefresh))
	for slot, state := range s.activePeerRefresh {
		cloned[slot] = cloneActivePeerRefreshState(state)
	}
	return cloned
}

func (s *Server) dispatchQueuedActivePeerRefreshes(ctx context.Context) error {
	if s.dispatchEngine != nil {
		if contextInCoordinatorEngine(ctx) {
			return s.dispatchQueuedActivePeerRefreshesOwned(ctx)
		}
		return s.dispatchEngine.drainUntilIdle(ctx, nil, false)
	}
	return s.dispatchQueuedActivePeerRefreshesOwned(ctx)
}

func (s *Server) dispatchQueuedActivePeerRefreshesOwned(ctx context.Context) error {
	_, err := s.dispatchQueuedActivePeerRefreshesBatchOwned(ctx, -1)
	return err
}

func (s *Server) dispatchQueuedActivePeerRefreshesBatchOwned(ctx context.Context, maxSlots int) (bool, error) {
	slots := s.snapshotActivePeerRefreshSlots()
	if len(slots) == 0 {
		return false, nil
	}
	if maxSlots < 0 {
		maxSlots = len(slots)
	} else if maxSlots == 0 {
		maxSlots = activePeerRefreshWorkerCount(len(slots))
	}
	more := len(slots) > maxSlots
	if more {
		slots = append([]int(nil), slots[:maxSlots]...)
	}
	type task struct {
		index int
		slot  int
	}
	results := make([]error, len(slots))
	updated := make([]bool, len(slots))
	nextStates := make([]activePeerRefreshState, len(slots))
	workerCount := activePeerRefreshWorkerCount(len(slots))
	if workerCount == 1 {
		states := s.snapshotActivePeerRefreshStates()
		for index, slot := range slots {
			state, ok := states[slot]
			if !ok {
				continue
			}
			if !s.shouldDispatchActivePeerRefresh(slot, state) {
				continue
			}
			nextState, err := s.dispatchActivePeerUpdates(ctx, slot, state)
			nextStates[index] = nextState
			if err != nil {
				results[index] = err
				continue
			}
			updated[index] = len(nextState.remainingNodeIDs) == 0
		}
	} else {
		taskCh := make(chan task)
		states := s.snapshotActivePeerRefreshStates()
		var wg sync.WaitGroup
		for i := 0; i < workerCount; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range taskCh {
					if ctx.Err() != nil {
						results[task.index] = ctx.Err()
						continue
					}
					state, ok := states[task.slot]
					if !ok {
						continue
					}
					if !s.shouldDispatchActivePeerRefresh(task.slot, state) {
						continue
					}
					nextState, err := s.dispatchActivePeerUpdates(ctx, task.slot, state)
					nextStates[task.index] = nextState
					if err != nil {
						results[task.index] = err
						continue
					}
					updated[task.index] = len(nextState.remainingNodeIDs) == 0
				}
			}()
		}
		for index, slot := range slots {
			taskCh <- task{index: index, slot: slot}
		}
		close(taskCh)
		wg.Wait()
	}
	anyUpdated := false
	var firstErr error
	for index, slot := range slots {
		if len(nextStates[index].remainingNodeIDs) > 0 {
			s.updateActivePeerRefresh(slot, nextStates[index])
		}
		if !updated[index] {
			if results[index] != nil && firstErr == nil {
				firstErr = results[index]
			}
			continue
		}
		s.clearActivePeerRefresh(slot)
		anyUpdated = true
		if results[index] != nil && firstErr == nil {
			firstErr = results[index]
		}
	}
	if anyUpdated {
		s.rebuildRoutingSnapshot()
	}
	if len(s.snapshotActivePeerRefreshSlots()) > 0 {
		more = true
	}
	return more, firstErr
}

func activePeerRefreshWorkerCount(slotCount int) int {
	if slotCount <= 1 {
		return slotCount
	}
	workers := runtime.GOMAXPROCS(0)
	if workers < 2 {
		workers = 2
	}
	if workers > 32 {
		workers = 32
	}
	if workers > slotCount {
		return slotCount
	}
	return workers
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

func (s *Server) dispatchActivePeerUpdates(ctx context.Context, slot int, refresh activePeerRefreshState) (activePeerRefreshState, error) {
	state := s.currentStateView()
	var chain *coordinator.Chain
	for i := range state.Cluster.Chains {
		if state.Cluster.Chains[i].Slot == slot {
			current := activeServingChain(state.Cluster.Chains[i])
			chain = &current
			break
		}
	}
	if len(refresh.assignmentChain.Replicas) > 0 && refresh.assignmentChain.Slot == slot {
		assignment := cloneCoordinatorChain(refresh.assignmentChain)
		chain = &assignment
	}
	if chain == nil {
		return activePeerRefreshState{}, nil
	}
	nextRefresh := cloneActivePeerRefreshState(refresh)
	if len(nextRefresh.remainingNodeIDs) == 0 {
		nextRefresh.remainingNodeIDs = make(map[string]bool)
		for _, replica := range chain.Replicas {
			if replica.State == coordinator.ReplicaStateActive {
				nextRefresh.remainingNodeIDs[replica.NodeID] = true
			}
		}
	}
	var firstErr error
	for _, replica := range chain.Replicas {
		if replica.State != coordinator.ReplicaStateActive {
			continue
		}
		if len(nextRefresh.remainingNodeIDs) > 0 && !nextRefresh.remainingNodeIDs[replica.NodeID] {
			continue
		}
		client, err := s.clientForNodeID(replica.NodeID)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("%w: %w", ErrDispatchFailed, err)
			}
			continue
		}
		assignment, err := assignmentForNode(*chain, state.Cluster.NodesByID, replica.NodeID, state.SlotVersions[slot])
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("err in assignmentForNode(update active peers): %w", err)
			}
			continue
		}
		dispatchCtx, cancel := deriveDeadlineContext(ctx, s.dispatchTimeout)
		err = client.UpdateChainPeers(dispatchCtx, storage.UpdateChainPeersCommand{Assignment: assignment})
		cancel()
		if err != nil {
			if firstErr == nil {
				if isContextTimeoutOrCancel(err) {
					firstErr = fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %w", ErrDispatchTimeout, replica.NodeID, err)
				} else {
					firstErr = fmt.Errorf("%w: err in node[%q].UpdateChainPeers: %v", ErrDispatchFailed, replica.NodeID, err)
				}
			}
			continue
		}
		delete(nextRefresh.remainingNodeIDs, replica.NodeID)
	}
	if len(nextRefresh.remainingNodeIDs) == 0 {
		nextRefresh.remainingNodeIDs = nil
	}
	return nextRefresh, firstErr
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
	state := s.currentStateView()
	s.viewMu.RLock()
	unavailable := cloneUnavailableReplicasMap(s.unavailableReplicas)
	s.viewMu.RUnlock()
	queuedPeerRefresh := s.snapshotActivePeerRefreshStates()
	snapshot := reduceRoutingSnapshot(state, unavailable, queuedPeerRefresh)
	s.routingSnapshotMu.Lock()
	s.routingSnapshot = snapshot
	s.routingSnapshotMu.Unlock()
}

func buildSlotRoute(
	chain coordinator.Chain,
	nodesByID map[string]coordinator.Node,
	chainVersion uint64,
	unavailable map[string]map[int]bool,
) SlotRoute {
	route := SlotRoute{
		Slot:         chain.Slot,
		ChainVersion: chainVersion,
	}
	if chainHasUnavailableReplica(chain, unavailable) {
		return route
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
			route.HeadEndpoint = nodesByID[replica.NodeID].RPCAddress
			role = storage.ReplicaRoleHead
		}
		route.TailNodeID = replica.NodeID
		route.TailEndpoint = nodesByID[replica.NodeID].RPCAddress
		route.ReadReplicas = append(route.ReadReplicas, ReadReplicaRoute{
			NodeID:   replica.NodeID,
			Endpoint: nodesByID[replica.NodeID].RPCAddress,
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
		route.Writable = true
	}
	if len(route.ReadReplicas) > 0 {
		route.Readable = true
	}
	return route
}

func readyProgressFallbackServingChain(current coordruntime.View, slot int) (coordinator.Chain, bool) {
	if slot < 0 || slot >= len(current.Cluster.Chains) {
		return coordinator.Chain{}, false
	}
	return readyProgressFallbackServingChainForChain(current.Cluster.Chains[slot], current.Cluster.ReplicationFactor)
}

func readyProgressFallbackServingChainForChain(chain coordinator.Chain, replicationFactor int) (coordinator.Chain, bool) {
	if hasLeavingReplica(chain) {
		return coordinator.Chain{}, false
	}
	if activeReplicaCount(chain) >= replicationFactor {
		return coordinator.Chain{}, false
	}
	serving := activeServingChain(chain)
	if len(serving.Replicas) == 0 {
		return coordinator.Chain{}, false
	}
	return cloneCoordinatorChain(serving), true
}

func activeReplicaCount(chain coordinator.Chain) int {
	count := 0
	for _, replica := range chain.Replicas {
		if replica.State == coordinator.ReplicaStateActive {
			count++
		}
	}
	return count
}

func hasLeavingReplica(chain coordinator.Chain) bool {
	for _, replica := range chain.Replicas {
		if replica.State == coordinator.ReplicaStateLeaving {
			return true
		}
	}
	return false
}

func cloneCoordinatorChain(chain coordinator.Chain) coordinator.Chain {
	cloned := coordinator.Chain{
		Slot:     chain.Slot,
		Replicas: make([]coordinator.Replica, len(chain.Replicas)),
	}
	copy(cloned.Replicas, chain.Replicas)
	return cloned
}

func cloneActivePeerRefreshState(state activePeerRefreshState) activePeerRefreshState {
	cloned := state
	if state.useFallbackRoute {
		cloned.fallbackServingChain = cloneCoordinatorChain(state.fallbackServingChain)
	}
	if len(state.assignmentChain.Replicas) > 0 {
		cloned.assignmentChain = cloneCoordinatorChain(state.assignmentChain)
	}
	if len(state.remainingNodeIDs) > 0 {
		cloned.remainingNodeIDs = make(map[string]bool, len(state.remainingNodeIDs))
		for nodeID, remaining := range state.remainingNodeIDs {
			cloned.remainingNodeIDs[nodeID] = remaining
		}
	}
	return cloned
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
	return chainContainsReplicaInState(state.Chains[slot], nodeID, wantState)
}

func chainContainsReplicaInState(chain coordinator.Chain, nodeID string, wantState coordinator.ReplicaState) bool {
	for _, replica := range chain.Replicas {
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
	if s.ha != nil {
		return s.applyHALivenessTransition(ctx, nodeID, state, evaluatedAtUnixNano, deadActionFired)
	}
	rt := s.runtimeRef()
	if _, err := retryOnRuntimeVersionMismatch(ctx, rt, func(current coordruntime.View, attempt int) (coordruntime.State, error) {
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
	if s.dispatchEngine == nil {
		s.rebuildRoutingSnapshot()
	}
	record, _ := s.livenessRecord(nodeID)
	return record, nil
}

func retryOnRuntimeVersionMismatch[T any](
	ctx context.Context,
	rt *coordruntime.Runtime,
	op func(current coordruntime.View, attempt int) (T, error),
) (T, error) {
	var zero T
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		value, err := op(rt.CurrentView(), attempt)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, coordruntime.ErrVersionMismatch) || attempt >= runtimeVersionRetryLimit {
			return zero, err
		}
		// No sleep: the runtime lock provides natural backpressure.
		// Yielding the goroutine is sufficient to let the successful
		// writer complete its lock-protected section.
		runtime.Gosched()
	}
}

func retryOnRuntimeSlotProgressVersionMismatch[T any](
	ctx context.Context,
	rt *coordruntime.Runtime,
	slot int,
	op func(current coordruntime.SlotProgressView, attempt int) (T, error),
) (T, error) {
	var zero T
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		value, err := op(rt.CurrentSlotProgressView(slot), attempt)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, coordruntime.ErrVersionMismatch) || attempt >= runtimeVersionRetryLimit {
			return zero, err
		}
		runtime.Gosched()
	}
}

func (s *Server) syncViewsFromRuntime() {
	current := s.currentStateView()
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	s.heartbeats = mergeHeartbeatStatuses(s.heartbeats, current.NodeLivenessByID)
	s.liveness = mergeLivenessRecords(s.liveness, current.NodeLivenessByID)
	s.pending = make(map[int]PendingWork, len(current.PendingBySlot))
	s.completed = make(map[int][]coordruntime.CompletedProgressRecord, len(current.CompletedProgressBySlot))
	s.runtimeVersion = current.Version
	s.runtimeOutbox = cloneRuntimeOutbox(current.Outbox)
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
	if s.startupBudgetActive && clusterFullySettledForStartup(current.Cluster) {
		s.startupBudgetActive = false
	}
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

func (s *Server) currentStateView() coordruntime.View {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.rt.CurrentView()
}

func (s *Server) currentLivenessView() coordruntime.LivenessView {
	s.runtimeMu.RLock()
	defer s.runtimeMu.RUnlock()
	return s.rt.CurrentLivenessView()
}

func (s *Server) replaceRuntime(rt *coordruntime.Runtime) {
	s.runtimeMu.Lock()
	s.rt = rt
	s.runtimeMu.Unlock()
}

func (s *Server) reconfigurationPolicy() coordinator.ReconfigurationPolicy {
	s.viewMu.RLock()
	policy := s.lastPolicy
	startupMax := s.startupMaxChangedChains
	startupActive := s.startupBudgetActive
	s.viewMu.RUnlock()
	if !startupActive || startupMax <= policy.MaxChangedChains {
		return policy
	}
	if !clusterFullySettledForStartup(s.currentStateView().Cluster) {
		policy.MaxChangedChains = startupMax
		return policy
	}
	s.viewMu.Lock()
	if s.startupBudgetActive {
		s.startupBudgetActive = false
	}
	s.viewMu.Unlock()
	return policy
}

func clusterFullySettledForStartup(cluster coordinator.ClusterState) bool {
	if cluster.SlotCount == 0 || cluster.ReplicationFactor <= 0 || len(cluster.Chains) != cluster.SlotCount {
		return false
	}
	for _, chain := range cluster.Chains {
		activeCount := 0
		if len(chain.Replicas) != cluster.ReplicationFactor {
			return false
		}
		for _, replica := range chain.Replicas {
			if replica.State != coordinator.ReplicaStateActive {
				return false
			}
			activeCount++
		}
		if activeCount != cluster.ReplicationFactor {
			return false
		}
	}
	return true
}

func clusterNeedsPeriodicAdvance(cluster coordinator.ClusterState) bool {
	return !clusterFullySettledForStartup(cluster)
}

func slotPlanHasOnlyStepKind(slotPlan coordinator.SlotPlan, kind coordinator.StepKind) bool {
	if len(slotPlan.Steps) == 0 {
		return false
	}
	for _, step := range slotPlan.Steps {
		if step.Kind != kind {
			return false
		}
	}
	return true
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
	if cfg.StartupMaxChangedChains < 0 {
		return fmt.Errorf("%w: startup max changed chains must be >= 0", ErrInvalidServerConfig)
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
		UpdatedAtUnixNano:          record.UpdatedAtUnixNano,
		State:                      record.State,
		LastStatus:                 cloneNodeStatus(record.LastStatus),
		DeadActionFired:            record.DeadActionFired,
		SuspectTransitionsUnixNano: append([]int64(nil), record.SuspectTransitionsUnixNano...),
	}
}

func (s *Server) recordFreshHeartbeat(status storage.NodeStatus, observedAtUnixNano int64) {
	s.viewMu.Lock()
	defer s.viewMu.Unlock()
	if s.heartbeats == nil {
		s.heartbeats = map[string]storage.NodeStatus{}
	}
	if s.liveness == nil {
		s.liveness = map[string]coordruntime.NodeLivenessRecord{}
	}
	s.heartbeats[status.NodeID] = cloneNodeStatus(status)
	record := s.liveness[status.NodeID]
	record.LastHeartbeatUnixNano = observedAtUnixNano
	record.UpdatedAtUnixNano = observedAtUnixNano
	record.LastStatus = cloneNodeStatus(status)
	record.SuspectTransitionsUnixNano = pruneLivenessTransitions(
		record.SuspectTransitionsUnixNano,
		observedAtUnixNano,
		s.livenessPolicy.FlapWindow.Nanoseconds(),
	)
	if record.State != coordruntime.NodeLivenessStateDead {
		record.State = coordruntime.NodeLivenessStateHealthy
		record.DeadActionFired = false
	}
	s.liveness[status.NodeID] = record
}

func mergeHeartbeatStatuses(
	existing map[string]storage.NodeStatus,
	durable map[string]coordruntime.NodeLivenessRecord,
) map[string]storage.NodeStatus {
	merged := make(map[string]storage.NodeStatus, len(existing)+len(durable))
	for nodeID, record := range durable {
		if isZeroNodeStatus(record.LastStatus) {
			continue
		}
		merged[nodeID] = cloneNodeStatus(record.LastStatus)
	}
	for nodeID, status := range existing {
		merged[nodeID] = cloneNodeStatus(status)
	}
	return merged
}

func mergeLivenessRecords(
	existing map[string]coordruntime.NodeLivenessRecord,
	durable map[string]coordruntime.NodeLivenessRecord,
) map[string]coordruntime.NodeLivenessRecord {
	merged := make(map[string]coordruntime.NodeLivenessRecord, len(existing)+len(durable))
	for nodeID, record := range durable {
		merged[nodeID] = cloneLivenessRecord(record)
	}
	for nodeID, record := range existing {
		if durableRecord, ok := merged[nodeID]; ok {
			merged[nodeID] = mergeLivenessRecord(durableRecord, record)
			continue
		}
		merged[nodeID] = cloneLivenessRecord(record)
	}
	return merged
}

func mergeLivenessRecord(
	durable coordruntime.NodeLivenessRecord,
	fresh coordruntime.NodeLivenessRecord,
) coordruntime.NodeLivenessRecord {
	merged := cloneLivenessRecord(durable)
	if fresh.UpdatedAtUnixNano > merged.UpdatedAtUnixNano {
		merged.LastHeartbeatUnixNano = fresh.LastHeartbeatUnixNano
		merged.UpdatedAtUnixNano = fresh.UpdatedAtUnixNano
		merged.LastStatus = cloneNodeStatus(fresh.LastStatus)
		if merged.State != coordruntime.NodeLivenessStateDead {
			merged.State = fresh.State
			merged.DeadActionFired = fresh.DeadActionFired
			merged.SuspectTransitionsUnixNano = append([]int64(nil), fresh.SuspectTransitionsUnixNano...)
		}
		return merged
	}
	if isZeroNodeStatus(merged.LastStatus) && !isZeroNodeStatus(fresh.LastStatus) {
		merged.LastStatus = cloneNodeStatus(fresh.LastStatus)
	}
	return merged
}

func pruneLivenessTransitions(current []int64, observedAtUnixNano int64, flapWindowNanos int64) []int64 {
	if len(current) == 0 {
		return nil
	}
	if observedAtUnixNano == 0 || flapWindowNanos <= 0 {
		return append([]int64(nil), current...)
	}
	cutoff := observedAtUnixNano - flapWindowNanos
	pruned := make([]int64, 0, len(current))
	for _, ts := range current {
		if ts >= cutoff {
			pruned = append(pruned, ts)
		}
	}
	return pruned
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

func isZeroNodeStatus(status storage.NodeStatus) bool {
	return status.NodeID == "" &&
		status.ReplicaCount == 0 &&
		status.ActiveCount == 0 &&
		status.CatchingUpCount == 0 &&
		status.LeavingCount == 0
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
