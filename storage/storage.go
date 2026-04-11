package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/danthegoodman1/craq/gologger"
	"github.com/danthegoodman1/craq/ops"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

var (
	ErrInvalidConfig             = errors.New("invalid storage config")
	ErrReplicaExists             = errors.New("storage replica already exists")
	ErrUnknownReplica            = errors.New("unknown storage replica")
	ErrInvalidTransition         = errors.New("invalid storage replica transition")
	ErrSnapshotSourceUnavailable = errors.New("storage snapshot source unavailable")
	ErrWriteRejected             = errors.New("storage write rejected")
	ErrSequenceMismatch          = errors.New("storage sequence mismatch")
	ErrPeerMismatch              = errors.New("storage peer mismatch")
	ErrStateMismatch             = errors.New("storage state mismatch")
	ErrProtocolConflict          = errors.New("storage replica protocol conflict")
	ErrReplicaBackpressure       = errors.New("storage replica backpressure")
	ErrBufferedMessageLimit      = ErrReplicaBackpressure
	ErrWriteBackpressure         = errors.New("storage client write backpressure")
	ErrCatchupBackpressure       = errors.New("storage catch-up backpressure")
	ErrRoutingMismatch           = errors.New("storage routing mismatch")
	ErrWriteTimeout              = errors.New("storage write wait timed out or was canceled")
	ErrAmbiguousWrite            = errors.New("storage client write outcome is ambiguous")
	ErrConditionFailed           = errors.New("storage write conditions not satisfied")
	ErrReadDependencyUnavailable = errors.New("storage linearizable read dependency unavailable")
)

type Config struct {
	NodeID                            string
	RPCAddress                        string
	FailureDomains                    map[string]string
	AutoActivateEmptyReplicas         bool
	MaxInFlightClientWritesPerNode    int
	MaxInFlightClientWritesPerSlot    int
	MaxBufferedReplicaMessagesPerNode int
	MaxBufferedReplicaMessagesPerSlot int
	MaxConcurrentCatchups             int
	WriteCommitTimeout                time.Duration
	Clock                             Clock
	Logger                            *zerolog.Logger
	MetricsRegistry                   *prometheus.Registry
}

type Clock interface {
	Now() time.Time
}

type ObjectMetadata struct {
	Version   uint64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type CommittedObject struct {
	Value    string
	Metadata ObjectMetadata
}

type Snapshot map[string]CommittedObject

type ComparisonOperator string

const (
	ComparisonOperatorEqual              ComparisonOperator = "eq"
	ComparisonOperatorLessThan           ComparisonOperator = "lt"
	ComparisonOperatorLessThanOrEqual    ComparisonOperator = "lte"
	ComparisonOperatorGreaterThan        ComparisonOperator = "gt"
	ComparisonOperatorGreaterThanOrEqual ComparisonOperator = "gte"
)

type VersionComparison struct {
	Operator ComparisonOperator
	Value    uint64
}

type TimeComparison struct {
	Operator ComparisonOperator
	Value    time.Time
}

type WriteConditions struct {
	Exists    *bool
	Version   *VersionComparison
	UpdatedAt *TimeComparison
}

type ReadConsistency string

const (
	ReadConsistencyLinearizable   ReadConsistency = "linearizable"
	ReadConsistencyLocalCommitted ReadConsistency = "local_committed"
)

type Backend interface {
	CreateReplica(slot int) error
	DeleteReplica(slot int) error
	Snapshot(slot int) (Snapshot, error)
	InstallSnapshot(slot int, snap Snapshot) error
	SetHighestCommittedSequence(slot int, sequence uint64) error
	ApplyCommitted(ctx context.Context, nodeID string, operation WriteOperation, persisted *PersistedReplica) error
	StagePut(slot int, sequence uint64, key string, value string, metadata ObjectMetadata) error
	StageDelete(slot int, sequence uint64, key string, metadata ObjectMetadata) error
	CommitSequence(slot int, sequence uint64) error
	CommittedSnapshot(slot int) (Snapshot, error)
	GetCommitted(slot int, key string) (CommittedObject, bool, error)
	HighestCommittedSequence(slot int) (uint64, error)
	StagedSequences(slot int) ([]uint64, error)
	Close() error
}

type LocalStateStore interface {
	LoadNode(ctx context.Context, nodeID string) (PersistedNodeState, error)
	UpsertReplica(ctx context.Context, nodeID string, replica PersistedReplica) error
	DeleteReplica(ctx context.Context, nodeID string, slot int) error
	SetHighestAcceptedCoordinatorEpoch(ctx context.Context, nodeID string, epoch uint64) error
	Close() error
}

type localStateBinder interface {
	BindLocalStateStore(local LocalStateStore)
}

type CoordinatorClient interface {
	RegisterNode(ctx context.Context, reg NodeRegistration) error
	ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error
	ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error
	ReportNodeRecovered(ctx context.Context, report NodeRecoveryReport) error
	ReportNodeHeartbeat(ctx context.Context, status NodeStatus) error
}

type NodeRegistration struct {
	NodeID         string
	RPCAddress     string
	FailureDomains map[string]string
}

type ReplicationTransport interface {
	// FetchSnapshot returns the committed snapshot AND the highest committed
	// sequence atomically. Both values must be consistent (the snapshot must
	// reflect all operations up to and including the returned sequence).
	FetchSnapshot(ctx context.Context, fromNodeID string, slot int) (Snapshot, uint64, error)
	FetchCommittedSequence(ctx context.Context, fromNodeID string, slot int) (uint64, error)
	ForwardWrite(ctx context.Context, toNodeID string, req ForwardWriteRequest) error
	CommitWrite(ctx context.Context, toNodeID string, req CommitWriteRequest) error
}

type OperationKind string

const (
	OperationKindPut    OperationKind = "put"
	OperationKindDelete OperationKind = "delete"
)

type WriteOperation struct {
	Slot     int
	Sequence uint64
	Kind     OperationKind
	Key      string
	Value    string
	Metadata ObjectMetadata
}

type ForwardWriteRequest struct {
	Operation    WriteOperation
	FromNodeID   string
	ChainVersion uint64
}

type CommitWriteRequest struct {
	Slot         int
	Sequence     uint64
	FromNodeID   string
	ChainVersion uint64
}

type ClientGetRequest struct {
	Slot                 int
	Key                  string
	ExpectedChainVersion uint64
	Consistency          ReadConsistency
}

type ClientPutRequest struct {
	Slot                 int
	Key                  string
	Value                string
	ExpectedChainVersion uint64
	Conditions           WriteConditions
}

type ClientDeleteRequest struct {
	Slot                 int
	Key                  string
	ExpectedChainVersion uint64
	Conditions           WriteConditions
}

type ReadResult struct {
	Slot         int
	ChainVersion uint64
	Found        bool
	Value        string
	Metadata     *ObjectMetadata
}

type CommitResult struct {
	Slot     int
	Sequence uint64
	Applied  bool
	Metadata *ObjectMetadata
}

type AmbiguousWriteError struct {
	Slot                 int
	Kind                 OperationKind
	ExpectedChainVersion uint64
	Cause                error
}

type ConditionFailedError struct {
	Slot                 int
	Kind                 OperationKind
	ExpectedChainVersion uint64
	CurrentExists        bool
	CurrentMetadata      *ObjectMetadata
}

type ReadDependencyError struct {
	Slot                 int
	ExpectedChainVersion uint64
	TailNodeID           string
	Cause                error
}

type BackpressureResource string

const (
	BackpressureResourceClientWrite   BackpressureResource = "client_write"
	BackpressureResourceReplicaBuffer BackpressureResource = "replica_buffer"
	BackpressureResourceCatchup       BackpressureResource = "catchup"
)

type BackpressureError struct {
	Slot     int
	Current  int
	Limit    int
	Resource BackpressureResource
	Cause    error
}

func (e *BackpressureError) Error() string {
	if e.Slot >= 0 {
		return fmt.Sprintf(
			"%s: %s slot %d current=%d limit=%d",
			e.Cause,
			e.Resource,
			e.Slot,
			e.Current,
			e.Limit,
		)
	}
	return fmt.Sprintf("%s: %s current=%d limit=%d", e.Cause, e.Resource, e.Current, e.Limit)
}

func (e *BackpressureError) Unwrap() error {
	return e.Cause
}

func (e *BackpressureError) Is(target error) bool {
	return target == e.Cause
}

func (e *AmbiguousWriteError) Error() string {
	return fmt.Sprintf(
		"%s: %s on slot %d version %d may or may not have committed: %v",
		ErrAmbiguousWrite,
		e.Kind,
		e.Slot,
		e.ExpectedChainVersion,
		e.Cause,
	)
}

func (e *AmbiguousWriteError) Unwrap() error {
	return e.Cause
}

func (e *AmbiguousWriteError) Is(target error) bool {
	return target == ErrAmbiguousWrite
}

func (e *ConditionFailedError) Error() string {
	if e.CurrentExists && e.CurrentMetadata != nil {
		return fmt.Sprintf(
			"%s: %s on slot %d version %d failed against current version %d updated_at %s",
			ErrConditionFailed,
			e.Kind,
			e.Slot,
			e.ExpectedChainVersion,
			e.CurrentMetadata.Version,
			e.CurrentMetadata.UpdatedAt.UTC().Format(time.RFC3339Nano),
		)
	}
	return fmt.Sprintf(
		"%s: %s on slot %d version %d failed against absent object",
		ErrConditionFailed,
		e.Kind,
		e.Slot,
		e.ExpectedChainVersion,
	)
}

func (e *ConditionFailedError) Unwrap() error {
	return ErrConditionFailed
}

func (e *ReadDependencyError) Error() string {
	return fmt.Sprintf(
		"%s: slot %d version %d tail %q: %v",
		ErrReadDependencyUnavailable,
		e.Slot,
		e.ExpectedChainVersion,
		e.TailNodeID,
		e.Cause,
	)
}

func (e *ReadDependencyError) Unwrap() error {
	return e.Cause
}

func (e *ReadDependencyError) Is(target error) bool {
	return target == ErrReadDependencyUnavailable
}

type RoutingMismatchReason string

const (
	RoutingMismatchReasonUnknownSlot     RoutingMismatchReason = "unknown_slot"
	RoutingMismatchReasonWrongVersion    RoutingMismatchReason = "wrong_version"
	RoutingMismatchReasonWrongRole       RoutingMismatchReason = "wrong_role"
	RoutingMismatchReasonInactiveReplica RoutingMismatchReason = "inactive_replica"
)

type RoutingMismatchError struct {
	Slot                 int
	ExpectedChainVersion uint64
	CurrentChainVersion  uint64
	CurrentRole          ReplicaRole
	CurrentState         ReplicaState
	Reason               RoutingMismatchReason
}

func (e *RoutingMismatchError) Error() string {
	return fmt.Sprintf(
		"%s: slot %d expected version %d, current version %d, role %q, state %q, reason %q",
		ErrRoutingMismatch,
		e.Slot,
		e.ExpectedChainVersion,
		e.CurrentChainVersion,
		e.CurrentRole,
		e.CurrentState,
		e.Reason,
	)
}

func (e *RoutingMismatchError) Unwrap() error {
	return ErrRoutingMismatch
}

type ReplicaState string

const (
	ReplicaStatePending    ReplicaState = "pending"
	ReplicaStateCatchingUp ReplicaState = "catching_up"
	ReplicaStateActive     ReplicaState = "active"
	ReplicaStateLeaving    ReplicaState = "leaving"
	ReplicaStateRecovered  ReplicaState = "recovered"
	ReplicaStateRemoved    ReplicaState = "removed"
)

type ReplicaRole string

const (
	ReplicaRoleSingle ReplicaRole = "single"
	ReplicaRoleHead   ReplicaRole = "head"
	ReplicaRoleMiddle ReplicaRole = "middle"
	ReplicaRoleTail   ReplicaRole = "tail"
)

type ChainPeers struct {
	PredecessorNodeID string
	PredecessorTarget string
	SuccessorNodeID   string
	SuccessorTarget   string
	TailNodeID        string
	TailTarget        string
}

type ReplicaAssignment struct {
	Slot         int
	ChainVersion uint64
	Role         ReplicaRole
	Peers        ChainPeers
}

type ReplicaStatus struct {
	Assignment ReplicaAssignment
	State      ReplicaState
}

type NodeState struct {
	NodeID   string
	Replicas map[int]ReplicaStatus
}

type NodeStatus struct {
	NodeID          string
	ReplicaCount    int
	ActiveCount     int
	CatchingUpCount int
	LeavingCount    int
}

type PersistedReplica struct {
	Assignment               ReplicaAssignment
	LastKnownState           ReplicaState
	HighestCommittedSequence uint64
	HasCommittedData         bool
}

type PersistedNodeState struct {
	NodeID                          string
	HighestAcceptedCoordinatorEpoch uint64
	Replicas                        []PersistedReplica
}

type RecoveredReplica struct {
	Assignment               ReplicaAssignment
	LastKnownState           ReplicaState
	HighestCommittedSequence uint64
	HasCommittedData         bool
}

type NodeRecoveryReport struct {
	NodeID   string
	Replicas []RecoveredReplica
}

type AddReplicaAsTailCommand struct {
	Assignment ReplicaAssignment
	Epoch      uint64
}

type ActivateReplicaCommand struct {
	Slot  int
	Epoch uint64
}

type MarkReplicaLeavingCommand struct {
	Slot  int
	Epoch uint64
}

type RemoveReplicaCommand struct {
	Slot  int
	Epoch uint64
}

type UpdateChainPeersCommand struct {
	Assignment ReplicaAssignment
	Epoch      uint64
}

type ResumeRecoveredReplicaCommand struct {
	Assignment ReplicaAssignment
	Epoch      uint64
}

type RecoverReplicaCommand struct {
	Assignment   ReplicaAssignment
	SourceNodeID string
	Epoch        uint64
}

type DropRecoveredReplicaCommand struct {
	Slot  int
	Epoch uint64
}

type replicaRecord struct {
	assignment               ReplicaAssignment
	state                    ReplicaState
	nextSequence             uint64
	highestCommittedSequence uint64
	localDataPresent         bool
	lastKnownState           ReplicaState
	pendingWrites            map[uint64]pendingWrite
	stagedForwards           map[uint64]ForwardWriteRequest
	bufferedForwards         map[uint64]ForwardWriteRequest
	bufferedCommits          map[uint64]CommitWriteRequest
	recentCommittedForwards  map[uint64]ForwardWriteRequest
	recentCommittedCommits   map[uint64]CommitWriteRequest
	recentForwardOrder       []uint64
	recentCommitOrder        []uint64
	dirtyByKey               map[string][]dirtyReadEntry
	inFlightClientWrites     int
}

type pendingWrite struct {
	completed bool
	result    CommitResult
	operation *WriteOperation
}

type dirtyReadEntry struct {
	Sequence  uint64
	Operation WriteOperation
}

type Node struct {
	mu                                sync.RWMutex
	slotMuMu                          sync.Mutex
	slotMu                            map[int]*sync.Mutex
	nodeID                            string
	backend                           Backend
	local                             LocalStateStore
	coord                             CoordinatorClient
	repl                              ReplicationTransport
	registration                      NodeRegistration
	replicas                          map[int]replicaRecord
	activatingReplicas                map[int]struct{}
	maxInFlightClientWritesPerNode    int
	maxInFlightClientWritesPerSlot    int
	maxBufferedReplicaMessagesPerNode int
	maxBufferedReplicaMessagesPerSlot int
	maxConcurrentCatchups             int
	writeCommitTimeout                time.Duration
	clock                             Clock
	inFlightClientWrites              int
	inFlightCatchups                  int
	highestAcceptedCoordinatorEpoch   uint64
	closeOnce                         sync.Once
	closeErr                          error
	closed                            bool
	logger                            zerolog.Logger
	metrics                           *nodeMetrics
	events                            *eventRecorder
	autoActivateEmptyReplicas         bool
}

const defaultWriteCommitTimeout = 5 * time.Second
const defaultAutoActivationReadyTimeout = 30 * time.Second
const writeCommitPollInterval = time.Millisecond

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func NewNode(
	ctx context.Context,
	cfg Config,
	backend Backend,
	coord CoordinatorClient,
	repl ReplicationTransport,
) (*Node, error) {
	return OpenNode(ctx, cfg, backend, NewInMemoryLocalStateStore(), coord, repl)
}

func OpenNode(
	ctx context.Context,
	cfg Config,
	backend Backend,
	local LocalStateStore,
	coord CoordinatorClient,
	repl ReplicationTransport,
) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("%w: node ID must not be empty", ErrInvalidConfig)
	}
	if cfg.MaxInFlightClientWritesPerNode < 0 {
		return nil, fmt.Errorf("%w: max in-flight client writes per node must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxInFlightClientWritesPerSlot < 0 {
		return nil, fmt.Errorf("%w: max in-flight client writes per slot must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxBufferedReplicaMessagesPerNode < 0 {
		return nil, fmt.Errorf("%w: max buffered replica messages per node must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxBufferedReplicaMessagesPerSlot < 0 {
		return nil, fmt.Errorf("%w: max buffered replica messages per slot must be >= 0", ErrInvalidConfig)
	}
	if cfg.MaxConcurrentCatchups < 0 {
		return nil, fmt.Errorf("%w: max concurrent catchups must be >= 0", ErrInvalidConfig)
	}
	if cfg.WriteCommitTimeout < 0 {
		return nil, fmt.Errorf("%w: write commit timeout must be >= 0", ErrInvalidConfig)
	}
	if backend == nil {
		return nil, fmt.Errorf("%w: backend must not be nil", ErrInvalidConfig)
	}
	if local == nil {
		return nil, fmt.Errorf("%w: local state store must not be nil", ErrInvalidConfig)
	}
	if coord == nil {
		return nil, fmt.Errorf("%w: coordinator client must not be nil", ErrInvalidConfig)
	}
	if repl == nil {
		return nil, fmt.Errorf("%w: replication transport must not be nil", ErrInvalidConfig)
	}
	if binder, ok := backend.(localStateBinder); ok {
		binder.BindLocalStateStore(local)
	}

	node := &Node{
		nodeID:  cfg.NodeID,
		backend: backend,
		local:   local,
		coord:   coord,
		repl:    repl,
		registration: NodeRegistration{
			NodeID:         cfg.NodeID,
			RPCAddress:     cfg.RPCAddress,
			FailureDomains: cloneFailureDomains(cfg.FailureDomains),
		},
		replicas:                          make(map[int]replicaRecord),
		slotMu:                            make(map[int]*sync.Mutex),
		activatingReplicas:                make(map[int]struct{}),
		maxInFlightClientWritesPerNode:    cfg.MaxInFlightClientWritesPerNode,
		maxInFlightClientWritesPerSlot:    cfg.MaxInFlightClientWritesPerSlot,
		maxBufferedReplicaMessagesPerNode: cfg.MaxBufferedReplicaMessagesPerNode,
		maxBufferedReplicaMessagesPerSlot: cfg.MaxBufferedReplicaMessagesPerSlot,
		maxConcurrentCatchups:             cfg.MaxConcurrentCatchups,
		writeCommitTimeout:                cfg.WriteCommitTimeout,
		clock:                             cfg.Clock,
		logger:                            loggerFromConfig(cfg.Logger),
		metrics:                           newNodeMetrics(cfg.MetricsRegistry),
		events:                            newEventRecorder("storage", cfg.NodeID),
		autoActivateEmptyReplicas:         cfg.AutoActivateEmptyReplicas,
	}
	if node.maxBufferedReplicaMessagesPerSlot == 0 {
		node.maxBufferedReplicaMessagesPerSlot = 64
	}
	if node.writeCommitTimeout == 0 {
		node.writeCommitTimeout = defaultWriteCommitTimeout
	}
	if node.clock == nil {
		node.clock = realClock{}
	}

	persisted, err := node.local.LoadNode(ctx, cfg.NodeID)
	if err != nil {
		return nil, fmt.Errorf("err in node.local.LoadNode: %w", err)
	}
	node.highestAcceptedCoordinatorEpoch = persisted.HighestAcceptedCoordinatorEpoch
	for _, replica := range persisted.Replicas {
		record := replicaRecord{
			assignment:               cloneAssignment(replica.Assignment),
			state:                    ReplicaStateRecovered,
			nextSequence:             replica.HighestCommittedSequence + 1,
			highestCommittedSequence: replica.HighestCommittedSequence,
			localDataPresent:         false,
			lastKnownState:           replica.LastKnownState,
		}

		if sequence, err := backend.HighestCommittedSequence(replica.Assignment.Slot); err == nil {
			record.localDataPresent = true
			record.highestCommittedSequence = sequence
			record.nextSequence = sequence + 1
		} else if !errors.Is(err, ErrUnknownReplica) {
			return nil, fmt.Errorf("err in backend.HighestCommittedSequence: %w", err)
		}
		record = ensureProtocolReplicaState(record)

		node.replicas[replica.Assignment.Slot] = record
	}

	return node, nil
}

func (n *Node) Register(ctx context.Context) error {
	if err := n.coord.RegisterNode(ctx, n.registration); err != nil {
		return fmt.Errorf("err in n.coord.RegisterNode: %w", err)
	}
	return nil
}

func (n *Node) AddReplicaAsTail(ctx context.Context, cmd AddReplicaAsTailCommand) error {
	start := time.Now()
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	if cmd.Assignment.Slot < 0 {
		err := fmt.Errorf("%w: slot must be >= 0", ErrInvalidConfig)
		n.events.record(n.logger, zerolog.ErrorLevel, "add_replica_failed", "storage add replica as tail failed", ops.IntPtr(cmd.Assignment.Slot), nil, nil, cmd.Assignment.Peers.PredecessorNodeID, "", err)
		return err
	}
	slotMu := n.getSlotMu(cmd.Assignment.Slot)
	slotMu.Lock()
	if existing, exists := n.replicaRecordSnapshot(cmd.Assignment.Slot); exists {
		if reflect.DeepEqual(existing.assignment, cmd.Assignment) &&
			existing.state != ReplicaStateRemoved {
			slotMu.Unlock()
			return nil
		}
		slotMu.Unlock()
		err := fmt.Errorf("%w: slot %d", ErrReplicaExists, cmd.Assignment.Slot)
		n.events.record(n.logger, zerolog.ErrorLevel, "add_replica_failed", "storage add replica as tail failed", ops.IntPtr(cmd.Assignment.Slot), ops.Uint64Ptr(cmd.Assignment.ChainVersion), nil, cmd.Assignment.Peers.PredecessorNodeID, "", err)
		return err
	}
	needsCatchup := cmd.Assignment.Peers.PredecessorNodeID != ""
	autoActivate := !needsCatchup
	if needsCatchup {
		if err := n.admitCatchup(); err != nil {
			slotMu.Unlock()
			n.observeBackpressure(err)
			return err
		}
		defer n.releaseCatchup()
	}

	if err := n.backend.CreateReplica(cmd.Assignment.Slot); err != nil {
		slotMu.Unlock()
		if errors.Is(err, ErrReplicaExists) && n.waitForReplicaCreationReplay(ctx, cmd.Assignment) {
			return nil
		}
		return fmt.Errorf("err in n.backend.CreateReplica: %w", err)
	}

	rollback := true
	defer func() {
		if rollback {
			_ = n.backend.DeleteReplica(cmd.Assignment.Slot)
		}
	}()

	record := replicaRecord{
		assignment:       cloneAssignment(cmd.Assignment),
		state:            ReplicaStatePending,
		nextSequence:     1,
		localDataPresent: true,
		lastKnownState:   ReplicaStatePending,
	}
	record = ensureProtocolReplicaState(record)
	n.setReplicaRecord(cmd.Assignment.Slot, record)

	if sourceNodeID := cmd.Assignment.Peers.PredecessorNodeID; sourceNodeID != "" {
		snapshot, highestCommittedSequence, err := n.repl.FetchSnapshot(
			ctx,
			peerTransportTarget(cmd.Assignment.Peers.PredecessorTarget, sourceNodeID),
			cmd.Assignment.Slot,
		)
		if err != nil {
			n.deleteReplicaRecord(cmd.Assignment.Slot)
			slotMu.Unlock()
			return fmt.Errorf("err in n.repl.FetchSnapshot: %w", err)
		}
		if err := n.backend.InstallSnapshot(cmd.Assignment.Slot, snapshot); err != nil {
			n.deleteReplicaRecord(cmd.Assignment.Slot)
			slotMu.Unlock()
			return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
		}
		if err := n.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
			n.deleteReplicaRecord(cmd.Assignment.Slot)
			slotMu.Unlock()
			return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
		}
		record.highestCommittedSequence = highestCommittedSequence
		record.nextSequence = highestCommittedSequence + 1
		autoActivate = len(snapshot) == 0 && highestCommittedSequence == 0
	}

	record.state = ReplicaStateCatchingUp
	record.lastKnownState = ReplicaStateCatchingUp
	n.setReplicaRecord(cmd.Assignment.Slot, record)
	if err := n.persistReplica(ctx, record); err != nil {
		n.deleteReplicaRecord(cmd.Assignment.Slot)
		slotMu.Unlock()
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	rollback = false
	if n.metrics != nil {
		n.metrics.catchupOps.WithLabelValues("add_replica_as_tail", "success").Inc()
		n.metrics.catchupDuration.Observe(time.Since(start).Seconds())
	}
	n.refreshMetricGauges()
	n.events.record(n.logger, zerolog.InfoLevel, "add_replica", "storage replica added as tail", ops.IntPtr(cmd.Assignment.Slot), ops.Uint64Ptr(cmd.Assignment.ChainVersion), nil, cmd.Assignment.Peers.PredecessorNodeID, "", nil)
	// Release slot lock before auto-activation which re-acquires it
	slotMu.Unlock()
	if autoActivate && n.autoActivateEmptyReplicas {
		activateCtx, cancel := autoActivationReadyContext(ctx)
		if err := n.activateReplicaOnce(activateCtx, cmd.Assignment.Slot); err != nil &&
			!errors.Is(err, context.Canceled) &&
			!errors.Is(err, context.DeadlineExceeded) {
			n.events.record(n.logger, zerolog.WarnLevel, "auto_activate_failed", "storage replica auto-activation fell back to background activation", ops.IntPtr(cmd.Assignment.Slot), ops.Uint64Ptr(cmd.Assignment.ChainVersion), nil, "", "", err)
		}
		cancel()
	}
	return nil
}

func (n *Node) ActivateReplica(ctx context.Context, cmd ActivateReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	return n.activateReplicaOnce(ctx, cmd.Slot)
}

func (n *Node) finishReplicaActivation(ctx context.Context, slot int) error {
	if err := n.coord.ReportReplicaReady(ctx, slot, n.HighestAcceptedCoordinatorEpoch()); err != nil {
		return fmt.Errorf("err in n.coord.ReportReplicaReady: %w", err)
	}

	slotMu := n.getSlotMu(slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	record, ok := n.replicaRecordSnapshot(slot)
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	record.state = ReplicaStateActive
	record.lastKnownState = ReplicaStateActive
	n.setReplicaRecord(slot, record)
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	n.events.record(n.logger, zerolog.InfoLevel, "activate_replica", "storage replica activated", ops.IntPtr(slot), ops.Uint64Ptr(record.assignment.ChainVersion), nil, "", "", nil)
	return nil
}

var (
	errReplicaActivationInFlight = errors.New("storage replica activation already in flight")
	errReplicaAlreadyActive      = errors.New("storage replica already active")
)

func (n *Node) activateReplicaOnce(ctx context.Context, slot int) error {
	if err := n.beginReplicaActivation(slot); err != nil {
		switch {
		case errors.Is(err, errReplicaActivationInFlight):
			return nil
		case errors.Is(err, errReplicaAlreadyActive):
			return nil
		case errors.Is(err, ErrInvalidTransition):
			record, ok := n.replicaRecordSnapshot(slot)
			if ok && record.state == ReplicaStateActive {
				return nil
			}
			return err
		default:
			return err
		}
	}
	defer n.endReplicaActivation(slot)
	return n.finishReplicaActivation(ctx, slot)
}

func (n *Node) beginReplicaActivation(slot int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	record, ok := n.replicas[slot]
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if record.state == ReplicaStateActive {
		return errReplicaAlreadyActive
	}
	if record.state != ReplicaStateCatchingUp {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, slot, record.state)
	}
	if _, exists := n.activatingReplicas[slot]; exists {
		return errReplicaActivationInFlight
	}
	n.activatingReplicas[slot] = struct{}{}
	return nil
}

func (n *Node) endReplicaActivation(slot int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.activatingReplicas, slot)
}

func (n *Node) MarkReplicaLeaving(ctx context.Context, cmd MarkReplicaLeavingCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	slotMu := n.getSlotMu(cmd.Slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	record, ok := n.replicaRecordSnapshot(cmd.Slot)
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state == ReplicaStateLeaving || record.state == ReplicaStateRemoved {
		return nil
	}
	if record.state != ReplicaStateActive {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}

	record.state = ReplicaStateLeaving
	record.lastKnownState = ReplicaStateLeaving
	n.setReplicaRecord(cmd.Slot, record)
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	n.events.record(n.logger, zerolog.InfoLevel, "mark_leaving", "storage replica marked leaving", ops.IntPtr(cmd.Slot), ops.Uint64Ptr(record.assignment.ChainVersion), nil, "", "", nil)
	return nil
}

func (n *Node) RemoveReplica(ctx context.Context, cmd RemoveReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	slotMu := n.getSlotMu(cmd.Slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	record, ok := n.replicaRecordSnapshot(cmd.Slot)
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state != ReplicaStateLeaving && record.state != ReplicaStateRemoved {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}

	// Report removal to the coordinator BEFORE deleting local data so that
	// if the report fails, the data is still intact for a retry attempt.
	if err := n.coord.ReportReplicaRemoved(ctx, cmd.Slot, n.HighestAcceptedCoordinatorEpoch()); err != nil {
		return fmt.Errorf("err in n.coord.ReportReplicaRemoved: %w", err)
	}

	if record.state == ReplicaStateLeaving {
		if err := n.backend.DeleteReplica(cmd.Slot); err != nil {
			return fmt.Errorf("err in n.backend.DeleteReplica: %w", err)
		}
		if err := n.local.DeleteReplica(ctx, n.nodeID, cmd.Slot); err != nil {
			return fmt.Errorf("err in n.local.DeleteReplica: %w", err)
		}
	}
	n.deleteReplicaRecord(cmd.Slot)
	n.refreshMetricGauges()
	n.events.record(n.logger, zerolog.InfoLevel, "remove_replica", "storage replica removed", ops.IntPtr(cmd.Slot), nil, nil, "", "", nil)
	return nil
}

func (n *Node) UpdateChainPeers(ctx context.Context, cmd UpdateChainPeersCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	slotMu := n.getSlotMu(cmd.Assignment.Slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	record, ok := n.replicaRecordSnapshot(cmd.Assignment.Slot)
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Assignment.Slot)
	}
	if reflect.DeepEqual(record.assignment, cmd.Assignment) {
		return nil
	}
	record.assignment = cloneAssignment(cmd.Assignment)
	n.setReplicaRecord(cmd.Assignment.Slot, record)
	if record.state != ReplicaStateRecovered {
		record.lastKnownState = record.state
		n.setReplicaRecord(cmd.Assignment.Slot, record)
	}
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	n.events.record(n.logger, zerolog.InfoLevel, "update_chain_peers", "storage replica peers updated", ops.IntPtr(cmd.Assignment.Slot), ops.Uint64Ptr(cmd.Assignment.ChainVersion), nil, "", "", nil)
	return nil
}

func (n *Node) ReportHeartbeat(ctx context.Context) error {
	if err := n.Register(ctx); err != nil {
		return fmt.Errorf("err in n.Register: %w", err)
	}
	return n.ReportHeartbeatOnly(ctx)
}

func (n *Node) ReportHeartbeatOnly(ctx context.Context) error {
	status := n.snapshotNodeStatus()
	if err := n.coord.ReportNodeHeartbeat(ctx, status); err != nil {
		return fmt.Errorf("err in n.coord.ReportNodeHeartbeat: %w", err)
	}
	return nil
}

func (n *Node) ReportRecoveredState(ctx context.Context) error {
	report := NodeRecoveryReport{
		NodeID:   n.nodeID,
		Replicas: make([]RecoveredReplica, 0),
	}
	replicas := n.replicaMapSnapshot()
	slots := sortedReplicaSlots(replicas)
	for _, slot := range slots {
		record := replicas[slot]
		if record.state != ReplicaStateRecovered {
			continue
		}
		report.Replicas = append(report.Replicas, RecoveredReplica{
			Assignment:               cloneAssignment(record.assignment),
			LastKnownState:           record.lastKnownState,
			HighestCommittedSequence: record.highestCommittedSequence,
			HasCommittedData:         record.localDataPresent,
		})
	}
	if err := n.coord.ReportNodeRecovered(ctx, report); err != nil {
		return fmt.Errorf("err in n.coord.ReportNodeRecovered: %w", err)
	}
	return nil
}

func (n *Node) ResumeRecoveredReplica(ctx context.Context, cmd ResumeRecoveredReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	slotMu := n.getSlotMu(cmd.Assignment.Slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	record, ok := n.replicaRecordSnapshot(cmd.Assignment.Slot)
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Assignment.Slot)
	}
	if record.state == ReplicaStateActive && record.localDataPresent && reflect.DeepEqual(record.assignment, cmd.Assignment) {
		return nil
	}
	if record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Assignment.Slot, record.state)
	}
	if !record.localDataPresent {
		return fmt.Errorf("%w: slot %d has no committed data to resume", ErrStateMismatch, cmd.Assignment.Slot)
	}
	record.assignment = cloneAssignment(cmd.Assignment)
	record.state = ReplicaStateActive
	record.lastKnownState = ReplicaStateActive
	record.nextSequence = record.highestCommittedSequence + 1
	n.setReplicaRecord(cmd.Assignment.Slot, record)
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	n.events.record(n.logger, zerolog.InfoLevel, "resume_recovered_replica", "storage recovered replica resumed", ops.IntPtr(cmd.Assignment.Slot), ops.Uint64Ptr(cmd.Assignment.ChainVersion), nil, "", "", nil)
	return nil
}

func (n *Node) RecoverReplica(ctx context.Context, cmd RecoverReplicaCommand) error {
	start := time.Now()
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	slotMu := n.getSlotMu(cmd.Assignment.Slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	record, exists := n.replicaRecordSnapshot(cmd.Assignment.Slot)
	if exists && record.state == ReplicaStateActive && reflect.DeepEqual(record.assignment, cmd.Assignment) {
		return nil
	}
	if exists && record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Assignment.Slot, record.state)
	}
	if err := n.admitCatchup(); err != nil {
		n.observeBackpressure(err)
		return err
	}
	defer n.releaseCatchup()
	if err := n.ensureBackendReplica(cmd.Assignment.Slot); err != nil {
		return fmt.Errorf("err in n.ensureBackendReplica: %w", err)
	}
	snapshot, highestCommittedSequence, err := n.repl.FetchSnapshot(
		ctx,
		peerTransportTarget(cmd.Assignment.Peers.PredecessorTarget, cmd.SourceNodeID),
		cmd.Assignment.Slot,
	)
	if err != nil {
		return fmt.Errorf("err in n.repl.FetchSnapshot: %w", err)
	}
	if err := n.backend.InstallSnapshot(cmd.Assignment.Slot, snapshot); err != nil {
		return fmt.Errorf("err in n.backend.InstallSnapshot: %w", err)
	}
	if err := n.backend.SetHighestCommittedSequence(cmd.Assignment.Slot, highestCommittedSequence); err != nil {
		return fmt.Errorf("err in n.backend.SetHighestCommittedSequence: %w", err)
	}

	record = replicaRecord{
		assignment:               cloneAssignment(cmd.Assignment),
		state:                    ReplicaStateActive,
		nextSequence:             highestCommittedSequence + 1,
		highestCommittedSequence: highestCommittedSequence,
		localDataPresent:         true,
		lastKnownState:           ReplicaStateActive,
	}
	record = ensureProtocolReplicaState(record)
	n.setReplicaRecord(cmd.Assignment.Slot, record)
	if err := n.persistReplica(ctx, record); err != nil {
		return fmt.Errorf("err in n.persistReplica: %w", err)
	}
	if n.metrics != nil {
		n.metrics.catchupOps.WithLabelValues("recover_replica", "success").Inc()
		n.metrics.catchupDuration.Observe(time.Since(start).Seconds())
	}
	n.refreshMetricGauges()
	n.events.record(n.logger, zerolog.InfoLevel, "recover_replica", "storage replica recovered from peer", ops.IntPtr(cmd.Assignment.Slot), ops.Uint64Ptr(cmd.Assignment.ChainVersion), nil, cmd.SourceNodeID, "", nil)
	return nil
}

func (n *Node) DropRecoveredReplica(ctx context.Context, cmd DropRecoveredReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	slotMu := n.getSlotMu(cmd.Slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	record, ok := n.replicaRecordSnapshot(cmd.Slot)
	if !ok {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, cmd.Slot)
	}
	if record.state != ReplicaStateRecovered {
		return fmt.Errorf("%w: slot %d is %q", ErrInvalidTransition, cmd.Slot, record.state)
	}
	if err := n.backend.DeleteReplica(cmd.Slot); err != nil && !errors.Is(err, ErrUnknownReplica) {
		return fmt.Errorf("err in n.backend.DeleteReplica: %w", err)
	}
	if err := n.local.DeleteReplica(ctx, n.nodeID, cmd.Slot); err != nil {
		return fmt.Errorf("err in n.local.DeleteReplica: %w", err)
	}
	n.deleteReplicaRecord(cmd.Slot)
	n.events.record(n.logger, zerolog.InfoLevel, "drop_recovered_replica", "storage recovered replica dropped", ops.IntPtr(cmd.Slot), nil, nil, "", "", nil)
	return nil
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.closed = true
		var errs []error
		if sameOwnedResource(n.backend, n.local) {
			errs = append(errs, n.backend.Close())
		} else {
			errs = append(errs, n.backend.Close(), n.local.Close())
		}
		n.closeErr = errors.Join(errs...)
	})
	return n.closeErr
}

func cloneFailureDomains(domains map[string]string) map[string]string {
	cloned := make(map[string]string, len(domains))
	for key, value := range domains {
		cloned[key] = value
	}
	return cloned
}

func (n *Node) SubmitPut(ctx context.Context, slot int, key string, value string) (CommitResult, error) {
	return n.submitWrite(ctx, slot, OperationKindPut, key, value, WriteConditions{})
}

func (n *Node) SubmitDelete(ctx context.Context, slot int, key string) (CommitResult, error) {
	return n.submitWrite(ctx, slot, OperationKindDelete, key, "", WriteConditions{})
}

func (n *Node) HandleClientGet(ctx context.Context, req ClientGetRequest) (ReadResult, error) {
	n.mu.RLock()
	record, ok := n.replicas[req.Slot]
	if !ok {
		n.mu.RUnlock()
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, replicaRecord{}, RoutingMismatchReasonUnknownSlot)
	}
	if record.state != ReplicaStateActive {
		n.mu.RUnlock()
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, record, RoutingMismatchReasonInactiveReplica)
	}
	if record.assignment.ChainVersion != req.ExpectedChainVersion {
		n.mu.RUnlock()
		return ReadResult{}, newRoutingMismatch(req.Slot, req.ExpectedChainVersion, record, RoutingMismatchReasonWrongVersion)
	}
	assignment := cloneAssignment(record.assignment)
	dirtyEntries := n.dirtyEntriesForKey(record, req.Key)
	n.mu.RUnlock()

	consistency := normalizeReadConsistency(req.Consistency)
	object, found, err := n.resolveRead(ctx, req, assignment, dirtyEntries, consistency)
	if err != nil {
		if n.metrics != nil {
			n.metrics.clientReads.WithLabelValues(string(consistency), "error").Inc()
		}
		return ReadResult{}, err
	}
	result := ReadResult{
		Slot:         req.Slot,
		ChainVersion: assignment.ChainVersion,
		Found:        found,
	}
	if found {
		result.Value = object.Value
		result.Metadata = cloneObjectMetadataPtr(&object.Metadata)
	}
	if n.metrics != nil {
		resultLabel := "miss"
		if found {
			resultLabel = "hit"
		}
		n.metrics.clientReads.WithLabelValues(string(consistency), resultLabel).Inc()
	}
	return result, nil
}

func (n *Node) HandleClientPut(ctx context.Context, req ClientPutRequest) (CommitResult, error) {
	start := time.Now()
	if err := n.validateClientWrite(req.Slot, req.ExpectedChainVersion); err != nil {
		if n.metrics != nil {
			n.metrics.clientWrites.WithLabelValues("put", "routing_mismatch").Inc()
		}
		return CommitResult{}, err
	}
	result, err := n.submitWrite(ctx, req.Slot, OperationKindPut, req.Key, req.Value, req.Conditions)
	if err != nil {
		if errors.Is(err, ErrConditionFailed) {
			if n.metrics != nil {
				n.metrics.clientWrites.WithLabelValues("put", "condition_failed").Inc()
				n.metrics.conditionFailures.Inc()
			}
			n.events.record(n.logger, zerolog.WarnLevel, "condition_failed", "storage conditional put failed", ops.IntPtr(req.Slot), ops.Uint64Ptr(req.ExpectedChainVersion), nil, "", "", err)
			current, found, currentErr := n.backend.GetCommitted(req.Slot, req.Key)
			if currentErr != nil {
				return CommitResult{}, fmt.Errorf("err in n.backend.GetCommitted: %w", currentErr)
			}
			return CommitResult{}, newConditionFailedError(req.Slot, OperationKindPut, req.ExpectedChainVersion, found, current)
		}
		if isAmbiguousWriteCause(err) {
			if n.metrics != nil {
				n.metrics.clientWrites.WithLabelValues("put", "ambiguous").Inc()
				n.metrics.ambiguousWrites.Inc()
			}
			n.events.record(n.logger, gologger.LvlForErr(err), "ambiguous_write", "storage put outcome is ambiguous", ops.IntPtr(req.Slot), ops.Uint64Ptr(req.ExpectedChainVersion), nil, "", "", err)
			return CommitResult{}, newAmbiguousWriteError(req.Slot, OperationKindPut, req.ExpectedChainVersion, err)
		}
		n.observeBackpressure(err)
		if n.metrics != nil {
			n.metrics.clientWrites.WithLabelValues("put", "error").Inc()
		}
		return CommitResult{}, err
	}
	if n.metrics != nil {
		n.metrics.clientWrites.WithLabelValues("put", "success").Inc()
		n.metrics.writeWaitDuration.Observe(time.Since(start).Seconds())
	}
	return result, nil
}

func (n *Node) HandleClientDelete(ctx context.Context, req ClientDeleteRequest) (CommitResult, error) {
	start := time.Now()
	if err := n.validateClientWrite(req.Slot, req.ExpectedChainVersion); err != nil {
		if n.metrics != nil {
			n.metrics.clientWrites.WithLabelValues("delete", "routing_mismatch").Inc()
		}
		return CommitResult{}, err
	}
	result, err := n.submitWrite(ctx, req.Slot, OperationKindDelete, req.Key, "", req.Conditions)
	if err != nil {
		if errors.Is(err, ErrConditionFailed) {
			if n.metrics != nil {
				n.metrics.clientWrites.WithLabelValues("delete", "condition_failed").Inc()
				n.metrics.conditionFailures.Inc()
			}
			n.events.record(n.logger, zerolog.WarnLevel, "condition_failed", "storage conditional delete failed", ops.IntPtr(req.Slot), ops.Uint64Ptr(req.ExpectedChainVersion), nil, "", "", err)
			current, found, currentErr := n.backend.GetCommitted(req.Slot, req.Key)
			if currentErr != nil {
				return CommitResult{}, fmt.Errorf("err in n.backend.GetCommitted: %w", currentErr)
			}
			return CommitResult{}, newConditionFailedError(req.Slot, OperationKindDelete, req.ExpectedChainVersion, found, current)
		}
		if isAmbiguousWriteCause(err) {
			if n.metrics != nil {
				n.metrics.clientWrites.WithLabelValues("delete", "ambiguous").Inc()
				n.metrics.ambiguousWrites.Inc()
			}
			n.events.record(n.logger, gologger.LvlForErr(err), "ambiguous_write", "storage delete outcome is ambiguous", ops.IntPtr(req.Slot), ops.Uint64Ptr(req.ExpectedChainVersion), nil, "", "", err)
			return CommitResult{}, newAmbiguousWriteError(req.Slot, OperationKindDelete, req.ExpectedChainVersion, err)
		}
		n.observeBackpressure(err)
		if n.metrics != nil {
			n.metrics.clientWrites.WithLabelValues("delete", "error").Inc()
		}
		return CommitResult{}, err
	}
	if n.metrics != nil {
		n.metrics.clientWrites.WithLabelValues("delete", "success").Inc()
		n.metrics.writeWaitDuration.Observe(time.Since(start).Seconds())
	}
	return result, nil
}

func (n *Node) HandleForwardWrite(ctx context.Context, req ForwardWriteRequest) error {
	slot := req.Operation.Slot
	slotMu := n.getSlotMu(slot)
	slotMu.Lock()

	record, err := n.activeReplicaRecord(slot)
	if err != nil {
		slotMu.Unlock()
		if n.metrics != nil {
			n.metrics.replicationForwards.WithLabelValues("error").Inc()
		}
		return err
	}
	record = ensureProtocolReplicaState(record)
	reduction, err := reduceForwardWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit:    n.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    n.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: n.bufferedReplicaMessagesForNode(),
	})
	if err != nil {
		slotMu.Unlock()
		if req.Operation.Sequence > record.nextSequence {
			n.observeBackpressure(err)
			if n.metrics != nil {
				n.metrics.replicationForwards.WithLabelValues("buffer_error").Inc()
			}
		}
		return err
	}
	switch reduction.Action {
	case slotReducerActionIgnore:
		slotMu.Unlock()
		return nil
	case slotReducerActionBuffer:
		n.setReplicaRecord(slot, reduction.Record)
		slotMu.Unlock()
		n.refreshMetricGauges()
		if n.metrics != nil {
			n.metrics.replicationForwards.WithLabelValues("buffered").Inc()
		}
		return nil
	}

	// In-order forward: apply state changes under lock, then RPC after unlock
	n.setReplicaRecord(slot, reduction.Record)
	err = n.applyForwardLocked(ctx, slotMu, reduction.Record, req)
	if n.metrics != nil {
		label := "success"
		if err != nil {
			label = "error"
		}
		n.metrics.replicationForwards.WithLabelValues(label).Inc()
	}
	if err != nil {
		n.events.record(n.logger, zerolog.ErrorLevel, "replication_forward_failed", "storage forward write failed", ops.IntPtr(slot), nil, ops.Uint64Ptr(req.Operation.Sequence), req.FromNodeID, "", err)
	}
	return err
}

func (n *Node) HandleCommitWrite(ctx context.Context, req CommitWriteRequest) error {
	slotMu := n.getSlotMu(req.Slot)
	slotMu.Lock()

	record, err := n.activeReplicaRecord(req.Slot)
	if err != nil {
		slotMu.Unlock()
		if n.metrics != nil {
			n.metrics.replicationCommits.WithLabelValues("error").Inc()
		}
		return err
	}
	record = ensureProtocolReplicaState(record)
	reduction, err := reduceCommitWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit:    n.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    n.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: n.bufferedReplicaMessagesForNode(),
	})
	if err != nil {
		slotMu.Unlock()
		if req.Sequence > record.highestCommittedSequence+1 || !reduceHasCommittableSequence(record, req.Sequence) {
			n.observeBackpressure(err)
			if n.metrics != nil {
				n.metrics.replicationCommits.WithLabelValues("buffer_error").Inc()
			}
		}
		return err
	}
	switch reduction.Action {
	case slotReducerActionIgnore:
		slotMu.Unlock()
		return nil
	case slotReducerActionBuffer:
		n.setReplicaRecord(req.Slot, reduction.Record)
		slotMu.Unlock()
		n.refreshMetricGauges()
		if n.metrics != nil {
			n.metrics.replicationCommits.WithLabelValues("buffered").Inc()
		}
		return nil
	}

	// In-order commit: apply state changes under lock, then RPC after unlock
	err = n.applyCommitLocked(ctx, slotMu, reduction.Record, req)
	if n.metrics != nil {
		label := "success"
		if err != nil {
			label = "error"
		}
		n.metrics.replicationCommits.WithLabelValues(label).Inc()
	}
	if err != nil {
		n.events.record(n.logger, zerolog.ErrorLevel, "replication_commit_failed", "storage commit write failed", ops.IntPtr(req.Slot), nil, ops.Uint64Ptr(req.Sequence), req.FromNodeID, "", err)
	}
	return err
}

func validateForwardSource(record replicaRecord, req ForwardWriteRequest) error {
	expectedNodeID := record.assignment.Peers.PredecessorNodeID
	expectedChainVersion := record.assignment.ChainVersion
	if existing, ok := record.stagedForwards[req.Operation.Sequence]; ok {
		expectedNodeID = existing.FromNodeID
		expectedChainVersion = existing.ChainVersion
	} else if existing, ok := record.bufferedForwards[req.Operation.Sequence]; ok {
		expectedNodeID = existing.FromNodeID
		expectedChainVersion = existing.ChainVersion
	} else if existing, ok := record.recentCommittedForwards[req.Operation.Sequence]; ok {
		expectedNodeID = existing.FromNodeID
		expectedChainVersion = existing.ChainVersion
	}
	if expectedNodeID == "" || expectedNodeID != req.FromNodeID {
		return fmt.Errorf(
			"%w: slot %d expected predecessor %q, got %q",
			ErrPeerMismatch,
			req.Operation.Slot,
			expectedNodeID,
			req.FromNodeID,
		)
	}
	if expectedChainVersion != req.ChainVersion {
		return fmt.Errorf(
			"%w: slot %d expected chain version %d, got %d",
			ErrPeerMismatch,
			req.Operation.Slot,
			expectedChainVersion,
			req.ChainVersion,
		)
	}
	return nil
}

func validateCommitSource(record replicaRecord, req CommitWriteRequest) error {
	expectedNodeID := record.assignment.Peers.SuccessorNodeID
	expectedChainVersion := record.assignment.ChainVersion
	if existing, ok := record.bufferedCommits[req.Sequence]; ok {
		expectedNodeID = existing.FromNodeID
		expectedChainVersion = existing.ChainVersion
	} else if existing, ok := record.recentCommittedCommits[req.Sequence]; ok {
		expectedNodeID = existing.FromNodeID
		expectedChainVersion = existing.ChainVersion
	} else if staged, ok := record.stagedForwards[req.Sequence]; ok {
		expectedChainVersion = staged.ChainVersion
	}
	if expectedNodeID == "" || expectedNodeID != req.FromNodeID {
		return fmt.Errorf(
			"%w: slot %d expected successor %q, got %q",
			ErrPeerMismatch,
			req.Slot,
			expectedNodeID,
			req.FromNodeID,
		)
	}
	if expectedChainVersion != req.ChainVersion {
		return fmt.Errorf(
			"%w: slot %d expected chain version %d, got %d",
			ErrPeerMismatch,
			req.Slot,
			expectedChainVersion,
			req.ChainVersion,
		)
	}
	return nil
}

func (n *Node) CommittedSnapshot(slot int) (Snapshot, error) {
	snapshot, err := n.backend.CommittedSnapshot(slot)
	if err != nil {
		return nil, fmt.Errorf("err in n.backend.CommittedSnapshot: %w", err)
	}
	return snapshot, nil
}

// CommittedSnapshotWithSequence returns the committed snapshot and highest
// committed sequence atomically under the per-slot lock so that new
// commits cannot interleave between the two reads.
func (n *Node) CommittedSnapshotWithSequence(slot int) (Snapshot, uint64, error) {
	slotMu := n.getSlotMu(slot)
	slotMu.Lock()
	defer slotMu.Unlock()

	snapshot, err := n.backend.CommittedSnapshot(slot)
	if err != nil {
		return nil, 0, fmt.Errorf("err in n.backend.CommittedSnapshot: %w", err)
	}
	n.mu.RLock()
	record, ok := n.replicas[slot]
	n.mu.RUnlock()
	if !ok {
		return snapshot, 0, nil
	}
	return snapshot, record.highestCommittedSequence, nil
}

func (n *Node) StagedSequences(slot int) ([]uint64, error) {
	record, ok := n.replicaRecordSnapshot(slot)
	if !ok {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	record = ensureProtocolReplicaState(record)
	unique := map[uint64]struct{}{}
	for sequence := range record.pendingWrites {
		unique[sequence] = struct{}{}
	}
	for sequence := range record.stagedForwards {
		unique[sequence] = struct{}{}
	}
	if sequences, err := n.backend.StagedSequences(slot); err == nil {
		for _, sequence := range sequences {
			unique[sequence] = struct{}{}
		}
	} else if !errors.Is(err, ErrUnknownReplica) {
		return nil, fmt.Errorf("err in n.backend.StagedSequences: %w", err)
	}
	sequences := make([]uint64, 0, len(unique))
	for sequence := range unique {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (n *Node) BufferedForwardSequences(slot int) ([]uint64, error) {
	record, ok := n.replicaRecordSnapshot(slot)
	if !ok {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	record = ensureProtocolReplicaState(record)
	sequences := make([]uint64, 0, len(record.bufferedForwards))
	for sequence := range record.bufferedForwards {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (n *Node) BufferedCommitSequences(slot int) ([]uint64, error) {
	record, ok := n.replicaRecordSnapshot(slot)
	if !ok {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	record = ensureProtocolReplicaState(record)
	sequences := make([]uint64, 0, len(record.bufferedCommits))
	for sequence := range record.bufferedCommits {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (n *Node) HighestCommittedSequence(slot int) (uint64, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	record, ok := n.replicas[slot]
	if !ok {
		return 0, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return record.highestCommittedSequence, nil
}

func (n *Node) InFlightClientWrites() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.inFlightClientWrites
}

func (n *Node) InFlightClientWritesForSlot(slot int) (int, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	record, ok := n.replicas[slot]
	if !ok {
		return 0, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return record.inFlightClientWrites, nil
}

func (n *Node) BufferedReplicaMessages() int {
	return n.bufferedReplicaMessagesForNode()
}

func (n *Node) CatchupCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.inFlightCatchups
}

func (n *Node) State() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	state := NodeState{
		NodeID:   n.nodeID,
		Replicas: make(map[int]ReplicaStatus, len(n.replicas)),
	}
	for slot, record := range n.replicas {
		state.Replicas[slot] = ReplicaStatus{
			Assignment: cloneAssignment(record.assignment),
			State:      record.state,
		}
	}
	return state
}

func (n *Node) CatchingUpSlots() []int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	slots := make([]int, 0, len(n.replicas))
	for slot, record := range n.replicas {
		if record.state == ReplicaStateCatchingUp {
			slots = append(slots, slot)
		}
	}
	sort.Ints(slots)
	return slots
}

func (n *Node) LeavingSlots() []int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	slots := make([]int, 0, len(n.replicas))
	for slot, record := range n.replicas {
		if record.state == ReplicaStateLeaving {
			slots = append(slots, slot)
		}
	}
	sort.Ints(slots)
	return slots
}

func (n *Node) snapshotNodeStatus() NodeStatus {
	n.mu.RLock()
	defer n.mu.RUnlock()
	status := NodeStatus{NodeID: n.nodeID}
	slots := sortedReplicaSlots(n.replicas)
	for _, slot := range slots {
		record := n.replicas[slot]
		status.ReplicaCount++
		switch record.state {
		case ReplicaStateActive:
			status.ActiveCount++
		case ReplicaStateCatchingUp:
			status.CatchingUpCount++
		case ReplicaStateLeaving:
			status.LeavingCount++
		}
	}
	return status
}

func normalizeReadConsistency(consistency ReadConsistency) ReadConsistency {
	switch consistency {
	case ReadConsistencyLocalCommitted:
		return ReadConsistencyLocalCommitted
	default:
		return ReadConsistencyLinearizable
	}
}

func (n *Node) resolveRead(
	ctx context.Context,
	req ClientGetRequest,
	assignment ReplicaAssignment,
	dirtyEntries []dirtyReadEntry,
	consistency ReadConsistency,
) (CommittedObject, bool, error) {
	object, found, err := n.backend.GetCommitted(req.Slot, req.Key)
	if err != nil {
		return CommittedObject{}, false, fmt.Errorf("err in n.backend.GetCommitted: %w", err)
	}
	if consistency == ReadConsistencyLocalCommitted ||
		assignment.Role == ReplicaRoleTail ||
		assignment.Role == ReplicaRoleSingle {
		return object, found, nil
	}
	if len(dirtyEntries) == 0 {
		return object, found, nil
	}

	tailTarget := peerTransportTarget(assignment.Peers.TailTarget, assignment.Peers.TailNodeID)
	if tailTarget == "" {
		return CommittedObject{}, false, newReadDependencyError(req.Slot, req.ExpectedChainVersion, assignment.Peers.TailNodeID, ErrStateMismatch)
	}
	start := time.Now()
	sequence, err := n.repl.FetchCommittedSequence(ctx, tailTarget, req.Slot)
	if n.metrics != nil {
		result := "success"
		if err != nil {
			result = "error"
		}
		n.metrics.tailResolutions.WithLabelValues(result).Inc()
		n.metrics.tailResolutionDuration.Observe(time.Since(start).Seconds())
	}
	if err != nil {
		if n.metrics != nil {
			n.metrics.readDependencyFailures.Inc()
		}
		return CommittedObject{}, false, newReadDependencyError(req.Slot, req.ExpectedChainVersion, assignment.Peers.TailNodeID, err)
	}
	for i := len(dirtyEntries) - 1; i >= 0; i-- {
		entry := dirtyEntries[i]
		if entry.Sequence > sequence {
			continue
		}
		switch entry.Operation.Kind {
		case OperationKindPut:
			return CommittedObject{
				Value:    entry.Operation.Value,
				Metadata: cloneObjectMetadata(entry.Operation.Metadata),
			}, true, nil
		case OperationKindDelete:
			return CommittedObject{}, false, nil
		default:
			return CommittedObject{}, false, fmt.Errorf("%w: unsupported operation kind %q", ErrInvalidConfig, entry.Operation.Kind)
		}
	}
	return object, found, nil
}

func newReadDependencyError(slot int, expectedChainVersion uint64, tailNodeID string, cause error) error {
	return &ReadDependencyError{
		Slot:                 slot,
		ExpectedChainVersion: expectedChainVersion,
		TailNodeID:           tailNodeID,
		Cause:                cause,
	}
}

func cloneAssignment(assignment ReplicaAssignment) ReplicaAssignment {
	return ReplicaAssignment{
		Slot:         assignment.Slot,
		ChainVersion: assignment.ChainVersion,
		Role:         assignment.Role,
		Peers: ChainPeers{
			PredecessorNodeID: assignment.Peers.PredecessorNodeID,
			PredecessorTarget: assignment.Peers.PredecessorTarget,
			SuccessorNodeID:   assignment.Peers.SuccessorNodeID,
			SuccessorTarget:   assignment.Peers.SuccessorTarget,
			TailNodeID:        assignment.Peers.TailNodeID,
			TailTarget:        assignment.Peers.TailTarget,
		},
	}
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	cloned := make(Snapshot, len(snapshot))
	for key, object := range snapshot {
		cloned[key] = cloneCommittedObject(object)
	}
	return cloned
}

func cloneWriteOperation(operation WriteOperation) WriteOperation {
	return WriteOperation{
		Slot:     operation.Slot,
		Sequence: operation.Sequence,
		Kind:     operation.Kind,
		Key:      operation.Key,
		Value:    operation.Value,
		Metadata: cloneObjectMetadata(operation.Metadata),
	}
}

func cloneCommittedObject(object CommittedObject) CommittedObject {
	return CommittedObject{
		Value:    object.Value,
		Metadata: cloneObjectMetadata(object.Metadata),
	}
}

func cloneObjectMetadata(metadata ObjectMetadata) ObjectMetadata {
	return ObjectMetadata{
		Version:   metadata.Version,
		CreatedAt: metadata.CreatedAt,
		UpdatedAt: metadata.UpdatedAt,
	}
}

func cloneObjectMetadataPtr(metadata *ObjectMetadata) *ObjectMetadata {
	if metadata == nil {
		return nil
	}
	cloned := cloneObjectMetadata(*metadata)
	return &cloned
}

func peerTransportTarget(target string, fallbackNodeID string) string {
	if target != "" {
		return target
	}
	return fallbackNodeID
}

func (n *Node) nextObjectMetadata(found bool, current CommittedObject) ObjectMetadata {
	now := n.clock.Now().UTC()
	if !found {
		return ObjectMetadata{
			Version:   1,
			CreatedAt: now,
			UpdatedAt: now,
		}
	}
	return ObjectMetadata{
		Version:   current.Metadata.Version + 1,
		CreatedAt: current.Metadata.CreatedAt,
		UpdatedAt: now,
	}
}

func (n *Node) submitWrite(
	ctx context.Context,
	slot int,
	kind OperationKind,
	key string,
	value string,
	conditions WriteConditions,
) (CommitResult, error) {
	slotMu := n.getSlotMu(slot)
	slotMu.Lock()

	record, err := n.activeReplicaRecord(slot)
	if err != nil {
		slotMu.Unlock()
		return CommitResult{}, err
	}
	if record.assignment.Role != ReplicaRoleHead && record.assignment.Role != ReplicaRoleSingle {
		slotMu.Unlock()
		return CommitResult{}, fmt.Errorf(
			"%w: slot %d role %q cannot accept writes",
			ErrWriteRejected,
			slot,
			record.assignment.Role,
		)
	}
	current, found, err := n.backend.GetCommitted(slot, key)
	if err != nil {
		slotMu.Unlock()
		return CommitResult{}, fmt.Errorf("err in n.backend.GetCommitted: %w", err)
	}
	if err := evaluateWriteConditions(conditions, found, current); err != nil {
		slotMu.Unlock()
		return CommitResult{}, err
	}
	if kind == OperationKindDelete && !found {
		slotMu.Unlock()
		return CommitResult{Slot: slot}, nil
	}
	if err := n.admitClientWrite(slot); err != nil {
		slotMu.Unlock()
		return CommitResult{}, err
	}
	releasedAdmission := false
	defer func() {
		if releasedAdmission {
			return
		}
		n.releaseClientWrite(slot)
		releasedAdmission = true
	}()

	n.mu.Lock()
	record = n.replicas[slot]
	if record.state != ReplicaStateActive {
		n.mu.Unlock()
		slotMu.Unlock()
		return CommitResult{}, fmt.Errorf("%w: slot %d is %q", ErrWriteRejected, slot, record.state)
	}
	operation := WriteOperation{
		Slot:     slot,
		Sequence: record.nextSequence,
		Kind:     kind,
		Key:      key,
		Value:    value,
		Metadata: n.nextObjectMetadata(found, current),
	}
	submitReduction := reduceSubmitWrite(record, operation)
	record = submitReduction.Record
	n.replicas[slot] = record
	role := record.assignment.Role
	n.mu.Unlock()

	switch role {
	case ReplicaRoleSingle:
		if err := n.commitLocalSequence(ctx, slot, operation.Sequence); err != nil {
			slotMu.Unlock()
			return CommitResult{}, err
		}
		slotMu.Unlock()
	case ReplicaRoleHead:
		// Release slot lock before forwarding: the commit will arrive back at
		// this node's HandleCommitWrite which acquires the same slot lock.
		slotMu.Unlock()
		if record.assignment.Peers.SuccessorNodeID == "" {
			return CommitResult{}, fmt.Errorf("%w: slot %d head has no successor", ErrStateMismatch, slot)
		}
		if err := n.repl.ForwardWrite(ctx, peerTransportTarget(record.assignment.Peers.SuccessorTarget, record.assignment.Peers.SuccessorNodeID), ForwardWriteRequest{
			Operation:    cloneWriteOperation(operation),
			FromNodeID:   n.nodeID,
			ChainVersion: record.assignment.ChainVersion,
		}); err != nil {
			return CommitResult{}, fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
		}
		if err := n.awaitWriteCompletion(ctx, slot, operation.Sequence); err != nil {
			return CommitResult{}, err
		}
	default:
		slotMu.Unlock()
	}

	n.releaseClientWrite(slot)
	releasedAdmission = true
	return submitReduction.Result, nil
}

func (n *Node) activeReplicaRecord(slot int) (replicaRecord, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	record, ok := n.replicas[slot]
	if !ok {
		return replicaRecord{}, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if record.state != ReplicaStateActive {
		return replicaRecord{}, fmt.Errorf("%w: slot %d is %q", ErrWriteRejected, slot, record.state)
	}
	return cloneReplicaRecord(record), nil
}

func (n *Node) stageOperation(operation WriteOperation) error {
	switch operation.Kind {
	case OperationKindPut:
		if err := n.backend.StagePut(operation.Slot, operation.Sequence, operation.Key, operation.Value, operation.Metadata); err != nil {
			return fmt.Errorf("err in n.backend.StagePut: %w", err)
		}
	case OperationKindDelete:
		if err := n.backend.StageDelete(operation.Slot, operation.Sequence, operation.Key, operation.Metadata); err != nil {
			return fmt.Errorf("err in n.backend.StageDelete: %w", err)
		}
	default:
		return fmt.Errorf("%w: unsupported operation kind %q", ErrInvalidConfig, operation.Kind)
	}
	return nil
}

func (n *Node) validateClientWrite(slot int, expectedChainVersion uint64) error {
	n.mu.RLock()
	defer n.mu.RUnlock()
	record, ok := n.replicas[slot]
	if !ok {
		return newRoutingMismatch(slot, expectedChainVersion, replicaRecord{}, RoutingMismatchReasonUnknownSlot)
	}
	if record.state != ReplicaStateActive {
		return newRoutingMismatch(slot, expectedChainVersion, record, RoutingMismatchReasonInactiveReplica)
	}
	if record.assignment.ChainVersion != expectedChainVersion {
		return newRoutingMismatch(slot, expectedChainVersion, record, RoutingMismatchReasonWrongVersion)
	}
	if record.assignment.Role != ReplicaRoleHead && record.assignment.Role != ReplicaRoleSingle {
		return newRoutingMismatch(slot, expectedChainVersion, record, RoutingMismatchReasonWrongRole)
	}
	return nil
}

func newRoutingMismatch(
	slot int,
	expectedChainVersion uint64,
	record replicaRecord,
	reason RoutingMismatchReason,
) error {
	return &RoutingMismatchError{
		Slot:                 slot,
		ExpectedChainVersion: expectedChainVersion,
		CurrentChainVersion:  record.assignment.ChainVersion,
		CurrentRole:          record.assignment.Role,
		CurrentState:         record.state,
		Reason:               reason,
	}
}

func newAmbiguousWriteError(
	slot int,
	kind OperationKind,
	expectedChainVersion uint64,
	cause error,
) error {
	return &AmbiguousWriteError{
		Slot:                 slot,
		Kind:                 kind,
		ExpectedChainVersion: expectedChainVersion,
		Cause:                cause,
	}
}

func newConditionFailedError(
	slot int,
	kind OperationKind,
	expectedChainVersion uint64,
	found bool,
	current CommittedObject,
) error {
	var metadata *ObjectMetadata
	if found {
		metadata = cloneObjectMetadataPtr(&current.Metadata)
	}
	return &ConditionFailedError{
		Slot:                 slot,
		Kind:                 kind,
		ExpectedChainVersion: expectedChainVersion,
		CurrentExists:        found,
		CurrentMetadata:      metadata,
	}
}

func isAmbiguousWriteCause(err error) bool {
	return errors.Is(err, ErrWriteTimeout) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

func evaluateWriteConditions(conditions WriteConditions, found bool, current CommittedObject) error {
	if conditions.Exists != nil && found != *conditions.Exists {
		return ErrConditionFailed
	}
	if conditions.Version != nil {
		if !found || !compareUint64(current.Metadata.Version, *conditions.Version) {
			return ErrConditionFailed
		}
	}
	if conditions.UpdatedAt != nil {
		if !found || !compareTime(current.Metadata.UpdatedAt, *conditions.UpdatedAt) {
			return ErrConditionFailed
		}
	}
	return nil
}

func compareUint64(current uint64, comparison VersionComparison) bool {
	switch comparison.Operator {
	case ComparisonOperatorEqual:
		return current == comparison.Value
	case ComparisonOperatorLessThan:
		return current < comparison.Value
	case ComparisonOperatorLessThanOrEqual:
		return current <= comparison.Value
	case ComparisonOperatorGreaterThan:
		return current > comparison.Value
	case ComparisonOperatorGreaterThanOrEqual:
		return current >= comparison.Value
	default:
		return false
	}
}

func compareTime(current time.Time, comparison TimeComparison) bool {
	switch comparison.Operator {
	case ComparisonOperatorEqual:
		return current.Equal(comparison.Value)
	case ComparisonOperatorLessThan:
		return current.Before(comparison.Value)
	case ComparisonOperatorLessThanOrEqual:
		return current.Before(comparison.Value) || current.Equal(comparison.Value)
	case ComparisonOperatorGreaterThan:
		return current.After(comparison.Value)
	case ComparisonOperatorGreaterThanOrEqual:
		return current.After(comparison.Value) || current.Equal(comparison.Value)
	default:
		return false
	}
}

func newWriteBackpressureError(slot int, current int, limit int) error {
	return &BackpressureError{
		Slot:     slot,
		Current:  current,
		Limit:    limit,
		Resource: BackpressureResourceClientWrite,
		Cause:    ErrWriteBackpressure,
	}
}

func newReplicaBackpressureError(slot int, current int, limit int) error {
	return &BackpressureError{
		Slot:     slot,
		Current:  current,
		Limit:    limit,
		Resource: BackpressureResourceReplicaBuffer,
		Cause:    ErrReplicaBackpressure,
	}
}

func newCatchupBackpressureError(current int, limit int) error {
	return &BackpressureError{
		Slot:     -1,
		Current:  current,
		Limit:    limit,
		Resource: BackpressureResourceCatchup,
		Cause:    ErrCatchupBackpressure,
	}
}

func (n *Node) admitClientWrite(slot int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.maxInFlightClientWritesPerNode > 0 && n.inFlightClientWrites >= n.maxInFlightClientWritesPerNode {
		return newWriteBackpressureError(slot, n.inFlightClientWrites, n.maxInFlightClientWritesPerNode)
	}
	record := n.replicas[slot]
	if n.maxInFlightClientWritesPerSlot > 0 && record.inFlightClientWrites >= n.maxInFlightClientWritesPerSlot {
		return newWriteBackpressureError(slot, record.inFlightClientWrites, n.maxInFlightClientWritesPerSlot)
	}
	record.inFlightClientWrites++
	n.replicas[slot] = record
	n.inFlightClientWrites++
	n.refreshMetricGaugesLocked()
	return nil
}

func (n *Node) releaseClientWrite(slot int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	record, ok := n.replicas[slot]
	if !ok {
		if n.inFlightClientWrites > 0 {
			n.inFlightClientWrites--
		}
		return
	}
	if record.inFlightClientWrites > 0 {
		record.inFlightClientWrites--
	}
	n.replicas[slot] = record
	if n.inFlightClientWrites > 0 {
		n.inFlightClientWrites--
	}
	n.refreshMetricGaugesLocked()
}

func (n *Node) admitCatchup() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.maxConcurrentCatchups > 0 && n.inFlightCatchups >= n.maxConcurrentCatchups {
		return newCatchupBackpressureError(n.inFlightCatchups, n.maxConcurrentCatchups)
	}
	n.inFlightCatchups++
	n.refreshMetricGaugesLocked()
	return nil
}

func (n *Node) releaseCatchup() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.inFlightCatchups > 0 {
		n.inFlightCatchups--
	}
	n.refreshMetricGaugesLocked()
}

func (n *Node) bufferedReplicaMessagesForNode() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.bufferedReplicaMessagesForNodeLocked()
}

func (n *Node) bufferedReplicaMessagesForNodeLocked() int {
	total := 0
	for _, record := range n.replicas {
		total += len(record.bufferedForwards) + len(record.bufferedCommits)
	}
	return total
}

// commitLocalSequence applies a committed write to the backend. Callers must
// hold the per-slot lock so that no concurrent handler can interleave a
// read-modify-write on the same slot. The node-wide mu is acquired only
// briefly for Go map access, NOT during backend I/O.
func (n *Node) commitLocalSequence(ctx context.Context, slot int, sequence uint64) error {
	n.mu.RLock()
	record := n.replicas[slot]
	n.mu.RUnlock()

	record = ensureProtocolReplicaState(record)
	operation, err := reduceCommittableOperation(record, sequence)
	if err != nil {
		return err
	}
	applied := record
	applied.highestCommittedSequence = sequence
	applied.localDataPresent = true
	if applied.state != ReplicaStateRecovered {
		applied.lastKnownState = applied.state
	}
	persisted := persistedReplica(applied)
	applyErr := n.backend.ApplyCommitted(ctx, n.nodeID, operation, &persisted)
	if applyErr != nil {
		highestCommitted, err := n.backend.HighestCommittedSequence(slot)
		if err != nil || highestCommitted != sequence {
			return fmt.Errorf("err in n.backend.ApplyCommitted: %w", applyErr)
		}
	}
	record = reduceApplyCommittedSequence(applied, operation, sequence, n.maxBufferedReplicaMessagesPerSlot)

	n.mu.Lock()
	n.replicas[slot] = record
	n.mu.Unlock()

	if applyErr != nil {
		return fmt.Errorf("err in n.backend.ApplyCommitted: %w", applyErr)
	}
	return nil
}

// Migration shims keep existing tests and call sites pointed at the reducer
// implementation while storage moves toward slot-owned protocol state.
func (n *Node) ensureProtocolState(record replicaRecord) replicaRecord {
	return ensureProtocolReplicaState(record)
}

func (n *Node) bufferFutureForward(record replicaRecord, req ForwardWriteRequest) (replicaRecord, error) {
	return reduceBufferFutureForward(record, req, slotProtocolBufferLimits{
		perSlotLimit:    n.maxBufferedReplicaMessagesPerSlot,
		perNodeLimit:    n.maxBufferedReplicaMessagesPerNode,
		nodeBufferedNow: n.bufferedReplicaMessagesForNode(),
	})
}

func (n *Node) awaitWriteCompletion(ctx context.Context, slot int, sequence uint64) error {
	if n.writeCommitted(slot, sequence) {
		return nil
	}
	waitCtx, cancel := withDefaultTimeout(ctx, n.writeCommitTimeout)
	defer cancel()
	if waiter, ok := n.repl.(interface {
		AwaitWriteCommit(ctx context.Context, check func() bool) error
	}); ok {
		if err := waiter.AwaitWriteCommit(waitCtx, func() bool {
			return n.writeCommitted(slot, sequence)
		}); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return fmt.Errorf("%w: err in repl.AwaitWriteCommit: %w", ErrWriteTimeout, err)
			}
			return fmt.Errorf("err in repl.AwaitWriteCommit: %w", err)
		}
	}
	// Some transports commit synchronously and never need an explicit waiter,
	// while others can return from ForwardWrite before the local commit is
	// visible on the head. Polling keeps the client-facing timeout semantics
	// consistent across both transport shapes.
	if err := waitForCommitPoll(waitCtx, func() bool {
		return n.writeCommitted(slot, sequence)
	}); err != nil {
		return fmt.Errorf("%w: slot %d sequence %d: %w", ErrWriteTimeout, slot, sequence, err)
	}
	return nil
}

func waitForCommitPoll(ctx context.Context, check func() bool) error {
	if check() {
		return nil
	}
	ticker := time.NewTicker(writeCommitPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if check() {
				return nil
			}
		}
	}
}

func withDefaultTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
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

func (n *Node) writeCommitted(slot int, sequence uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	record, ok := n.replicas[slot]
	if !ok {
		return false
	}
	return record.highestCommittedSequence >= sequence
}

// applyForwardLocked dispatches side effects for an in-order forward. Most
// callers pass a reducer-updated record, but this helper also preserves the
// older direct-call contract by staging the forward first when needed.
func (n *Node) applyForwardLocked(ctx context.Context, slotMu *sync.Mutex, record replicaRecord, req ForwardWriteRequest) error {
	slot := req.Operation.Slot
	record = ensureProtocolReplicaState(record)
	if _, ok := record.stagedForwards[req.Operation.Sequence]; !ok {
		record = reduceStageForward(record, req)
		n.setReplicaRecord(slot, record)
	}
	isTail := record.assignment.Peers.SuccessorNodeID == ""
	if isTail {
		if err := n.commitLocalSequence(ctx, slot, req.Operation.Sequence); err != nil {
			slotMu.Unlock()
			return err
		}
		updated, ok := n.replicaRecordSnapshot(slot)
		if !ok {
			slotMu.Unlock()
			return nil
		}
		record = updated
	}

	predecessorNodeID := record.assignment.Peers.PredecessorNodeID
	predecessorTarget := record.assignment.Peers.PredecessorTarget
	successorNodeID := record.assignment.Peers.SuccessorNodeID
	successorTarget := record.assignment.Peers.SuccessorTarget
	slotMu.Unlock()

	if isTail {
		if predecessorNodeID != "" {
			if err := n.repl.CommitWrite(ctx, peerTransportTarget(predecessorTarget, predecessorNodeID), CommitWriteRequest{
				Slot:         slot,
				Sequence:     req.Operation.Sequence,
				FromNodeID:   n.nodeID,
				ChainVersion: record.assignment.ChainVersion,
			}); err != nil {
				return fmt.Errorf("err in n.repl.CommitWrite: %w", err)
			}
		}
	} else {
		if err := n.repl.ForwardWrite(ctx, peerTransportTarget(successorTarget, successorNodeID), ForwardWriteRequest{
			Operation:    cloneWriteOperation(req.Operation),
			FromNodeID:   n.nodeID,
			ChainVersion: record.assignment.ChainVersion,
		}); err != nil {
			return fmt.Errorf("err in n.repl.ForwardWrite: %w", err)
		}
	}

	return n.drainBufferedReplicaMessages(ctx, slot)
}

// applyCommitLocked commits a sequence under the slot lock, releases the lock,
// then sends CommitWrite to the predecessor and drains any buffered messages.
// The caller must hold slotMu; it is always released before this method returns.
func (n *Node) applyCommitLocked(ctx context.Context, slotMu *sync.Mutex, record replicaRecord, req CommitWriteRequest) error {
	if err := n.commitLocalSequence(ctx, req.Slot, req.Sequence); err != nil {
		slotMu.Unlock()
		return err
	}

	updated, ok := n.replicaRecordSnapshot(req.Slot)
	if !ok {
		slotMu.Unlock()
		return nil
	}
	record = reduceRecordCommitApplied(updated, req, n.maxBufferedReplicaMessagesPerSlot)
	n.setReplicaRecord(req.Slot, record)

	predecessorNodeID := record.assignment.Peers.PredecessorNodeID
	predecessorTarget := record.assignment.Peers.PredecessorTarget
	slotMu.Unlock()

	if predecessorNodeID != "" {
		if err := n.repl.CommitWrite(ctx, peerTransportTarget(predecessorTarget, predecessorNodeID), CommitWriteRequest{
			Slot:         req.Slot,
			Sequence:     req.Sequence,
			FromNodeID:   n.nodeID,
			ChainVersion: record.assignment.ChainVersion,
		}); err != nil {
			return fmt.Errorf("err in n.repl.CommitWrite: %w", err)
		}
	}
	return n.drainBufferedReplicaMessages(ctx, req.Slot)
}

// drainBufferedReplicaMessages processes buffered out-of-order messages that
// are now ready. Each message's state update is done under the per-slot lock,
// which is released before any outbound RPCs.
func (n *Node) drainBufferedReplicaMessages(ctx context.Context, slot int) error {
	slotMu := n.getSlotMu(slot)
	for {
		slotMu.Lock()
		record, ok := n.replicaRecordSnapshot(slot)
		if !ok {
			slotMu.Unlock()
			return nil
		}
		record = ensureProtocolReplicaState(record)
		if req, ok := record.bufferedForwards[record.nextSequence]; ok {
			record = reduceStageForward(record, req)
			n.setReplicaRecord(slot, record)
			if err := n.applyForwardLocked(ctx, slotMu, record, req); err != nil {
				return err
			}
			continue
		}
		nextCommit := record.highestCommittedSequence + 1
		if req, ok := record.bufferedCommits[nextCommit]; ok && reduceHasCommittableSequence(record, nextCommit) {
			if err := n.applyCommitLocked(ctx, slotMu, record, req); err != nil {
				return err
			}
			continue
		}
		slotMu.Unlock()
		return nil
	}
}

func (n *Node) dirtyEntriesForKey(record replicaRecord, key string) []dirtyReadEntry {
	record = ensureProtocolReplicaState(record)
	entries := record.dirtyByKey[key]
	cloned := make([]dirtyReadEntry, 0, len(entries))
	for _, entry := range entries {
		cloned = append(cloned, dirtyReadEntry{
			Sequence:  entry.Sequence,
			Operation: cloneWriteOperation(entry.Operation),
		})
	}
	return cloned
}

func dirtyKeyCount(record replicaRecord) int {
	record = ensureProtocolReplicaState(record)
	return len(record.dirtyByKey)
}

func sameForwardRequest(left ForwardWriteRequest, right ForwardWriteRequest) bool {
	return left.FromNodeID == right.FromNodeID &&
		left.ChainVersion == right.ChainVersion &&
		left.Operation == right.Operation
}

func sameCommitRequest(left CommitWriteRequest, right CommitWriteRequest) bool {
	return left == right
}

func cloneForwardRequest(req ForwardWriteRequest) ForwardWriteRequest {
	return ForwardWriteRequest{
		Operation:    cloneWriteOperation(req.Operation),
		FromNodeID:   req.FromNodeID,
		ChainVersion: req.ChainVersion,
	}
}

func cloneCommitRequest(req CommitWriteRequest) CommitWriteRequest {
	return CommitWriteRequest{
		Slot:         req.Slot,
		Sequence:     req.Sequence,
		FromNodeID:   req.FromNodeID,
		ChainVersion: req.ChainVersion,
	}
}

func cloneReplicaRecord(record replicaRecord) replicaRecord {
	cloned := record
	if record.pendingWrites != nil {
		cloned.pendingWrites = make(map[uint64]pendingWrite, len(record.pendingWrites))
		for sequence, write := range record.pendingWrites {
			cloned.pendingWrites[sequence] = clonePendingWrite(write)
		}
	}
	if record.stagedForwards != nil {
		cloned.stagedForwards = make(map[uint64]ForwardWriteRequest, len(record.stagedForwards))
		for sequence, req := range record.stagedForwards {
			cloned.stagedForwards[sequence] = cloneForwardRequest(req)
		}
	}
	if record.bufferedForwards != nil {
		cloned.bufferedForwards = make(map[uint64]ForwardWriteRequest, len(record.bufferedForwards))
		for sequence, req := range record.bufferedForwards {
			cloned.bufferedForwards[sequence] = cloneForwardRequest(req)
		}
	}
	if record.bufferedCommits != nil {
		cloned.bufferedCommits = make(map[uint64]CommitWriteRequest, len(record.bufferedCommits))
		for sequence, req := range record.bufferedCommits {
			cloned.bufferedCommits[sequence] = cloneCommitRequest(req)
		}
	}
	if record.recentCommittedForwards != nil {
		cloned.recentCommittedForwards = make(map[uint64]ForwardWriteRequest, len(record.recentCommittedForwards))
		for sequence, req := range record.recentCommittedForwards {
			cloned.recentCommittedForwards[sequence] = cloneForwardRequest(req)
		}
	}
	if record.recentCommittedCommits != nil {
		cloned.recentCommittedCommits = make(map[uint64]CommitWriteRequest, len(record.recentCommittedCommits))
		for sequence, req := range record.recentCommittedCommits {
			cloned.recentCommittedCommits[sequence] = cloneCommitRequest(req)
		}
	}
	cloned.recentForwardOrder = append([]uint64(nil), record.recentForwardOrder...)
	cloned.recentCommitOrder = append([]uint64(nil), record.recentCommitOrder...)
	if record.dirtyByKey != nil {
		cloned.dirtyByKey = make(map[string][]dirtyReadEntry, len(record.dirtyByKey))
		for key, entries := range record.dirtyByKey {
			clonedEntries := make([]dirtyReadEntry, 0, len(entries))
			for _, entry := range entries {
				clonedEntries = append(clonedEntries, dirtyReadEntry{
					Sequence:  entry.Sequence,
					Operation: cloneWriteOperation(entry.Operation),
				})
			}
			cloned.dirtyByKey[key] = clonedEntries
		}
	}
	return cloned
}

func clonePendingWrite(write pendingWrite) pendingWrite {
	cloned := write
	cloned.result.Metadata = cloneObjectMetadataPtr(write.result.Metadata)
	if write.operation != nil {
		op := cloneWriteOperation(*write.operation)
		cloned.operation = &op
	}
	return cloned
}

func (n *Node) persistReplica(ctx context.Context, record replicaRecord) error {
	persisted := persistedReplica(record)
	if err := n.local.UpsertReplica(ctx, n.nodeID, persisted); err != nil {
		return fmt.Errorf("err in n.local.UpsertReplica: %w", err)
	}
	return nil
}

func persistedReplica(record replicaRecord) PersistedReplica {
	return PersistedReplica{
		Assignment:               cloneAssignment(record.assignment),
		LastKnownState:           record.lastKnownState,
		HighestCommittedSequence: record.highestCommittedSequence,
		HasCommittedData:         record.localDataPresent,
	}
}

func (n *Node) acceptCoordinatorEpoch(ctx context.Context, epoch uint64) error {
	n.mu.Lock()
	current := n.highestAcceptedCoordinatorEpoch
	if epoch == 0 || epoch == current {
		n.mu.Unlock()
		return nil
	}
	if epoch < current {
		n.mu.Unlock()
		return fmt.Errorf("%w: coordinator epoch %d regresses highest accepted epoch %d", ErrWriteRejected, epoch, current)
	}
	n.mu.Unlock()
	if err := n.local.SetHighestAcceptedCoordinatorEpoch(ctx, n.nodeID, epoch); err != nil {
		return fmt.Errorf("err in n.local.SetHighestAcceptedCoordinatorEpoch: %w", err)
	}
	n.mu.Lock()
	if epoch > n.highestAcceptedCoordinatorEpoch {
		n.highestAcceptedCoordinatorEpoch = epoch
	}
	n.mu.Unlock()
	return nil
}

func (n *Node) HighestAcceptedCoordinatorEpoch() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.highestAcceptedCoordinatorEpoch
}

func (n *Node) replicaRecordSnapshot(slot int) (replicaRecord, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	record, ok := n.replicas[slot]
	return cloneReplicaRecord(record), ok
}

func (n *Node) setReplicaRecord(slot int, record replicaRecord) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.replicas[slot] = cloneReplicaRecord(record)
}

func (n *Node) deleteReplicaRecord(slot int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.replicas, slot)
}

func (n *Node) replicaMapSnapshot() map[int]replicaRecord {
	n.mu.RLock()
	defer n.mu.RUnlock()
	cloned := make(map[int]replicaRecord, len(n.replicas))
	for slot, record := range n.replicas {
		cloned[slot] = cloneReplicaRecord(record)
	}
	return cloned
}

func (n *Node) getSlotMu(slot int) *sync.Mutex {
	n.slotMuMu.Lock()
	defer n.slotMuMu.Unlock()
	mu, ok := n.slotMu[slot]
	if !ok {
		mu = &sync.Mutex{}
		n.slotMu[slot] = mu
	}
	return mu
}

func (n *Node) ensureBackendReplica(slot int) error {
	if _, err := n.backend.HighestCommittedSequence(slot); err == nil {
		return nil
	} else if !errors.Is(err, ErrUnknownReplica) {
		return fmt.Errorf("err in n.backend.HighestCommittedSequence: %w", err)
	}
	if err := n.backend.CreateReplica(slot); err != nil && !errors.Is(err, ErrReplicaExists) {
		return fmt.Errorf("err in n.backend.CreateReplica: %w", err)
	}
	return nil
}

func (n *Node) waitForReplicaCreationReplay(ctx context.Context, assignment ReplicaAssignment) bool {
	for attempt := 0; attempt < 64; attempt++ {
		if err := ctx.Err(); err != nil {
			return false
		}
		existing, exists := n.replicaRecordSnapshot(assignment.Slot)
		if exists && reflect.DeepEqual(existing.assignment, assignment) && existing.state != ReplicaStateRemoved {
			return true
		}
		persisted, err := n.local.LoadNode(ctx, n.nodeID)
		if err == nil && persistedReplicaMatchesAssignment(persisted, assignment) {
			return true
		}
		time.Sleep(50 * time.Microsecond)
	}
	return false
}

func autoActivationReadyContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent != nil {
		if _, ok := parent.Deadline(); !ok {
			return parent, func() {}
		}
	}
	return context.WithTimeout(context.Background(), defaultAutoActivationReadyTimeout)
}

func persistedReplicaMatchesAssignment(state PersistedNodeState, assignment ReplicaAssignment) bool {
	for _, replica := range state.Replicas {
		if replica.Assignment.Slot != assignment.Slot {
			continue
		}
		if reflect.DeepEqual(replica.Assignment, assignment) && replica.LastKnownState != ReplicaStateRemoved {
			return true
		}
		return false
	}
	return false
}

func sortedReplicaSlots(replicas map[int]replicaRecord) []int {
	slots := make([]int, 0, len(replicas))
	for slot := range replicas {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	return slots
}

func sameOwnedResource(left any, right any) bool {
	if left == nil || right == nil {
		return false
	}
	leftValue := reflect.ValueOf(left)
	rightValue := reflect.ValueOf(right)
	if !leftValue.IsValid() || !rightValue.IsValid() || leftValue.Type() != rightValue.Type() {
		return false
	}
	if leftValue.Type().Comparable() {
		return leftValue.Interface() == rightValue.Interface()
	}
	switch leftValue.Kind() {
	case reflect.Pointer, reflect.UnsafePointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func:
		return leftValue.Pointer() == rightValue.Pointer()
	default:
		return false
	}
}
