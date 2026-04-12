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
	waiter    *slotWriteWaiter
}

type dirtyReadEntry struct {
	Sequence  uint64
	Operation WriteOperation
}

type Node struct {
	mu                                sync.RWMutex
	slotOwners                        map[int]*slotOwner
	nodeID                            string
	backend                           Backend
	local                             LocalStateStore
	coord                             CoordinatorClient
	repl                              ReplicationTransport
	registration                      NodeRegistration
	publishedReplicas                 map[int]publishedReplicaSnapshot
	publishedReplicaCount             int
	publishedActiveCount              int
	publishedCatchingUpCount          int
	publishedLeavingCount             int
	publishedBufferedReplicaMessages  int
	publishedCatchingUpSlots          map[int]struct{}
	publishedLeavingSlots             map[int]struct{}
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
	done                              chan struct{}
	runtimeCtx                        context.Context
	runtimeCancel                     context.CancelFunc
	logger                            zerolog.Logger
	metrics                           *nodeMetrics
	events                            *eventRecorder
	autoActivateEmptyReplicas         bool
}

const defaultWriteCommitTimeout = 5 * time.Second
const defaultAutoActivationReadyTimeout = 30 * time.Second

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
		publishedReplicas:                 make(map[int]publishedReplicaSnapshot),
		publishedCatchingUpSlots:          make(map[int]struct{}),
		publishedLeavingSlots:             make(map[int]struct{}),
		slotOwners:                        make(map[int]*slotOwner),
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
		done:                              make(chan struct{}),
	}
	node.runtimeCtx, node.runtimeCancel = context.WithCancel(context.Background())
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

		node.publishReplicaSnapshotLocked(replica.Assignment.Slot, publishedReplicaFromRecord(record))
		node.slotOwners[replica.Assignment.Slot] = newSlotOwner(node, replica.Assignment.Slot, true, record)
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
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	err := n.addReplicaAsTailOwned(ctx, cmd)
	if err != nil {
		n.events.record(n.logger, zerolog.ErrorLevel, "add_replica_failed", "storage add replica as tail failed", ops.IntPtr(cmd.Assignment.Slot), nil, nil, cmd.Assignment.Peers.PredecessorNodeID, "", err)
	}
	return err
}

func (n *Node) ActivateReplica(ctx context.Context, cmd ActivateReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	return n.activateReplicaOwned(ctx, cmd.Slot)
}

var (
	errReplicaActivationInFlight = errors.New("storage replica activation already in flight")
	errReplicaAlreadyActive      = errors.New("storage replica already active")
)

func (n *Node) beginReplicaActivation(slot int) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	record, ok := n.publishedReplicas[slot]
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
	return n.markReplicaLeavingOwned(ctx, cmd.Slot)
}

func (n *Node) RemoveReplica(ctx context.Context, cmd RemoveReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	return n.removeReplicaOwned(ctx, cmd.Slot)
}

func (n *Node) UpdateChainPeers(ctx context.Context, cmd UpdateChainPeersCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	return n.updateChainPeersOwned(ctx, cmd.Assignment)
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
	replicas := n.publishedReplicaMapSnapshot()
	slots := sortedPublishedReplicaSlots(replicas)
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
	return n.resumeRecoveredReplicaOwned(ctx, cmd)
}

func (n *Node) RecoverReplica(ctx context.Context, cmd RecoverReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	return n.recoverReplicaOwned(ctx, cmd)
}

func (n *Node) DropRecoveredReplica(ctx context.Context, cmd DropRecoveredReplicaCommand) error {
	if err := n.acceptCoordinatorEpoch(ctx, cmd.Epoch); err != nil {
		return err
	}
	return n.dropRecoveredReplicaOwned(ctx, cmd.Slot)
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.closed = true
		if n.runtimeCancel != nil {
			n.runtimeCancel()
		}
		if n.done != nil {
			close(n.done)
		}
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
	result, err := n.handleClientGetOwned(ctx, req)
	if err != nil {
		if n.metrics != nil {
			n.metrics.clientReads.WithLabelValues(string(normalizeReadConsistency(req.Consistency)), "error").Inc()
		}
		return ReadResult{}, err
	}
	if n.metrics != nil {
		resultLabel := "miss"
		if result.Found {
			resultLabel = "hit"
		}
		n.metrics.clientReads.WithLabelValues(string(normalizeReadConsistency(req.Consistency)), resultLabel).Inc()
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
	err := n.handleForwardWriteOwned(ctx, req)
	if n.metrics != nil {
		label := "success"
		if err != nil {
			label = "error"
		}
		n.metrics.replicationForwards.WithLabelValues(label).Inc()
	}
	if err != nil {
		n.events.record(n.logger, zerolog.ErrorLevel, "replication_forward_failed", "storage forward write failed", ops.IntPtr(req.Operation.Slot), nil, ops.Uint64Ptr(req.Operation.Sequence), req.FromNodeID, "", err)
	}
	return err
}

func (n *Node) HandleCommitWrite(ctx context.Context, req CommitWriteRequest) error {
	err := n.handleCommitWriteOwned(ctx, req)
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
// committed sequence atomically through the slot owner so new commits cannot
// interleave between the two reads.
func (n *Node) CommittedSnapshotWithSequence(slot int) (Snapshot, uint64, error) {
	return n.committedSnapshotWithSequenceOwned(context.Background(), slot)
}

func (n *Node) StagedSequences(slot int) ([]uint64, error) {
	return n.stagedSequencesOwned(context.Background(), slot)
}

func (n *Node) BufferedForwardSequences(slot int) ([]uint64, error) {
	return n.bufferedForwardSequencesOwned(context.Background(), slot)
}

func (n *Node) BufferedCommitSequences(slot int) ([]uint64, error) {
	return n.bufferedCommitSequencesOwned(context.Background(), slot)
}

func (n *Node) HighestCommittedSequence(slot int) (uint64, error) {
	record, ok := n.publishedReplicaSnapshot(slot)
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
	record, ok := n.publishedReplicaSnapshot(slot)
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
	replicas := n.publishedReplicaMapSnapshot()
	state := NodeState{
		NodeID:   n.nodeID,
		Replicas: make(map[int]ReplicaStatus, len(replicas)),
	}
	for slot, record := range replicas {
		state.Replicas[slot] = ReplicaStatus{
			Assignment: cloneAssignment(record.assignment),
			State:      record.state,
		}
	}
	return state
}

func (n *Node) CatchingUpSlots() []int {
	n.mu.RLock()
	slots := make([]int, 0, len(n.publishedCatchingUpSlots))
	for slot := range n.publishedCatchingUpSlots {
		slots = append(slots, slot)
	}
	n.mu.RUnlock()
	sort.Ints(slots)
	return slots
}

func (n *Node) LeavingSlots() []int {
	n.mu.RLock()
	slots := make([]int, 0, len(n.publishedLeavingSlots))
	for slot := range n.publishedLeavingSlots {
		slots = append(slots, slot)
	}
	n.mu.RUnlock()
	sort.Ints(slots)
	return slots
}

func (n *Node) snapshotNodeStatus() NodeStatus {
	n.mu.RLock()
	status := NodeStatus{
		NodeID:          n.nodeID,
		ReplicaCount:    n.publishedReplicaCount,
		ActiveCount:     n.publishedActiveCount,
		CatchingUpCount: n.publishedCatchingUpCount,
		LeavingCount:    n.publishedLeavingCount,
	}
	n.mu.RUnlock()
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
	return n.submitWriteOwned(ctx, slot, kind, key, value, conditions)
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
	record, ok := n.publishedReplicaSnapshot(slot)
	if !ok {
		return newRoutingMismatch(slot, expectedChainVersion, ReplicaAssignment{}, ReplicaState(""), RoutingMismatchReasonUnknownSlot)
	}
	if record.state != ReplicaStateActive {
		return newRoutingMismatch(slot, expectedChainVersion, record.assignment, record.state, RoutingMismatchReasonInactiveReplica)
	}
	if record.assignment.ChainVersion != expectedChainVersion {
		return newRoutingMismatch(slot, expectedChainVersion, record.assignment, record.state, RoutingMismatchReasonWrongVersion)
	}
	if record.assignment.Role != ReplicaRoleHead && record.assignment.Role != ReplicaRoleSingle {
		return newRoutingMismatch(slot, expectedChainVersion, record.assignment, record.state, RoutingMismatchReasonWrongRole)
	}
	return nil
}

func newRoutingMismatch(
	slot int,
	expectedChainVersion uint64,
	assignment ReplicaAssignment,
	state ReplicaState,
	reason RoutingMismatchReason,
) error {
	return &RoutingMismatchError{
		Slot:                 slot,
		ExpectedChainVersion: expectedChainVersion,
		CurrentChainVersion:  assignment.ChainVersion,
		CurrentRole:          assignment.Role,
		CurrentState:         state,
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
	return n.publishedBufferedReplicaMessages
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
