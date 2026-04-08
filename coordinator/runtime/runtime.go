package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/storage"
)

var (
	ErrInvalidCommand     = errors.New("invalid coordinator runtime command")
	ErrVersionMismatch    = errors.New("coordinator runtime version mismatch")
	ErrCommandConflict    = errors.New("coordinator runtime command conflict")
	ErrAlreadyInitialized = errors.New("coordinator runtime already initialized")
	ErrNotInitialized     = errors.New("coordinator runtime not initialized")
	ErrRecovery           = errors.New("coordinator runtime recovery failed")
)

type State struct {
	Version                 uint64
	LastLogIndex            uint64
	Cluster                 coordinator.ClusterState
	SlotVersions            map[int]uint64
	CompletedProgressBySlot map[int][]CompletedProgressRecord
	NodeLivenessByID        map[string]NodeLivenessRecord
	PendingBySlot           map[int]PendingWork
	Outbox                  []OutboxEntry
	LastPolicy              coordinator.ReconfigurationPolicy
	AppliedCommands         map[string]AppliedCommand
}

type PendingKind string

const (
	PendingKindReady   PendingKind = "ready"
	PendingKindRemoved PendingKind = "removed"
)

type PendingWork struct {
	Slot        int
	NodeID      string
	Kind        PendingKind
	SlotVersion uint64
	CommandID   string
}

type OutboxCommandKind string

const (
	OutboxCommandKindAddReplicaAsTail   OutboxCommandKind = "add_replica_as_tail"
	OutboxCommandKindMarkReplicaLeaving OutboxCommandKind = "mark_replica_leaving"
	OutboxCommandKindUpdateChainPeers   OutboxCommandKind = "update_chain_peers"
)

type OutboxEntry struct {
	ID         string
	Slot       int
	NodeID     string
	Kind       OutboxCommandKind
	CommandID  string
	Assignment storage.ReplicaAssignment
}

type CompletedProgressKind string

const (
	CompletedProgressKindReady   CompletedProgressKind = "ready"
	CompletedProgressKindRemoved CompletedProgressKind = "removed"
)

type CompletedProgressRecord struct {
	NodeID      string
	Kind        CompletedProgressKind
	SlotVersion uint64
}

type NodeLivenessState string

const (
	NodeLivenessStateHealthy NodeLivenessState = "healthy"
	NodeLivenessStateSuspect NodeLivenessState = "suspect"
	NodeLivenessStateDead    NodeLivenessState = "dead"
)

type NodeLivenessRecord struct {
	LastHeartbeatUnixNano      int64
	State                      NodeLivenessState
	LastStatus                 storage.NodeStatus
	DeadActionFired            bool
	SuspectTransitionsUnixNano []int64
}

type AppliedCommand struct {
	Command                 Command
	Version                 uint64
	LastLogIndex            uint64
	Cluster                 coordinator.ClusterState
	SlotVersions            map[int]uint64
	CompletedProgressBySlot map[int][]CompletedProgressRecord
	NodeLivenessByID        map[string]NodeLivenessRecord
	PendingBySlot           map[int]PendingWork
	Outbox                  []OutboxEntry
	LastPolicy              coordinator.ReconfigurationPolicy
	Plan                    *coordinator.ReconfigurationPlan
}

type CommandKind string

const (
	CommandKindBootstrap         CommandKind = "bootstrap"
	CommandKindReconfigure       CommandKind = "reconfigure"
	CommandKindProgress          CommandKind = "progress"
	CommandKindHeartbeat         CommandKind = "heartbeat"
	CommandKindLiveness          CommandKind = "liveness"
	CommandKindAcknowledgeOutbox CommandKind = "acknowledge_outbox"
)

type Command struct {
	ID                string
	ExpectedVersion   uint64
	Kind              CommandKind
	Bootstrap         *BootstrapCommand
	Reconfigure       *ReconfigureCommand
	Progress          *ProgressCommand
	Heartbeat         *HeartbeatCommand
	Liveness          *LivenessCommand
	AcknowledgeOutbox *AcknowledgeOutboxCommand
}

type BootstrapCommand struct {
	Config coordinator.Config
	Nodes  []coordinator.Node
	Policy coordinator.ReconfigurationPolicy
}

type ReconfigureCommand struct {
	Events []coordinator.Event
	Policy coordinator.ReconfigurationPolicy
}

type ProgressCommand struct {
	Event coordinator.Event
}

type HeartbeatCommand struct {
	Status             storage.NodeStatus
	ObservedAtUnixNano int64
	FlapWindowNanos    int64
}

type LivenessCommand struct {
	NodeID              string
	State               NodeLivenessState
	EvaluatedAtUnixNano int64
	DeadActionFired     bool
	FlapWindowNanos     int64
}

type AcknowledgeOutboxCommand struct {
	EntryID string
}

type LogRecord struct {
	Index   uint64
	Command Command
}

type Checkpoint struct {
	State State
}

type Store interface {
	LoadLatestCheckpoint(ctx context.Context) (Checkpoint, bool, error)
	LoadWAL(ctx context.Context, afterIndex uint64) ([]LogRecord, error)
	AppendWAL(ctx context.Context, record LogRecord) error
	SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error
	TruncateWAL(ctx context.Context, throughIndex uint64) error
}

type Runtime struct {
	store Store
	mu    sync.RWMutex
	state State
}

type EvaluatedCommand struct {
	Plan      *coordinator.ReconfigurationPlan
	NextState State
	Duplicate *AppliedCommand
}

func Open(ctx context.Context, store Store) (*Runtime, error) {
	checkpoint, ok, err := store.LoadLatestCheckpoint(ctx)
	if err != nil {
		return nil, fmt.Errorf("err in store.LoadLatestCheckpoint: %w", err)
	}

	state := zeroState()
	if ok {
		state = cloneState(checkpoint.State)
	}

	records, err := store.LoadWAL(ctx, state.LastLogIndex)
	if err != nil {
		return nil, fmt.Errorf("err in store.LoadWAL: %w", err)
	}

	r := &Runtime{
		store: store,
		state: state,
	}
	expectedIndex := r.state.LastLogIndex
	for _, record := range records {
		if record.Index != expectedIndex+1 {
			return nil, fmt.Errorf(
				"%w: unexpected WAL index %d after %d",
				ErrRecovery,
				record.Index,
				expectedIndex,
			)
		}
		if err := r.replayRecord(record); err != nil {
			return nil, fmt.Errorf("err in r.replayRecord: %w", err)
		}
		expectedIndex = record.Index
	}

	return r, nil
}

func OpenInMemoryFromState(state State) *Runtime {
	return &Runtime{
		store: NewInMemoryStore(),
		state: cloneState(state),
	}
}

func EvaluateCommand(state State, cmd Command) (EvaluatedCommand, error) {
	r := &Runtime{state: cloneState(state)}
	cluster, plan, duplicate, err := r.executeCommand(cmd)
	if err != nil {
		return EvaluatedCommand{}, err
	}
	if duplicate != nil {
		cloned := cloneAppliedCommand(*duplicate)
		return EvaluatedCommand{
			Plan:      clonePlan(cloned.Plan),
			NextState: r.snapshotForApplied(cloned),
			Duplicate: &cloned,
		}, nil
	}
	return EvaluatedCommand{
		Plan:      clonePlan(plan),
		NextState: r.nextStateForApplied(state.LastLogIndex+1, cmd, cluster, plan),
	}, nil
}

func (r *Runtime) Current() State {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return cloneState(r.state)
}

func (r *Runtime) Bootstrap(ctx context.Context, cmd Command) (State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cluster, _, duplicate, err := r.executeBootstrap(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeBootstrap: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState

	return cloneState(r.state), nil
}

func (r *Runtime) Reconfigure(
	ctx context.Context,
	cmd Command,
) (coordinator.ReconfigurationPlan, State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cluster, plan, duplicate, err := r.executeReconfigure(cmd)
	if err != nil {
		return coordinator.ReconfigurationPlan{}, State{}, fmt.Errorf("err in r.executeReconfigure: %w", err)
	}
	if duplicate != nil {
		snapshot := r.snapshotForApplied(*duplicate)
		if duplicate.Plan == nil {
			return coordinator.ReconfigurationPlan{}, snapshot, nil
		}
		return *clonePlan(duplicate.Plan), snapshot, nil
	}

	record, nextState := r.commitCandidate(cmd, cluster, plan)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return coordinator.ReconfigurationPlan{}, State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState

	return *clonePlan(plan), cloneState(r.state), nil
}

func (r *Runtime) ApplyProgress(ctx context.Context, cmd Command) (State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cluster, _, duplicate, err := r.executeProgress(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeProgress: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState

	return cloneState(r.state), nil
}

func (r *Runtime) Heartbeat(ctx context.Context, cmd Command) (State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _, duplicate, err := r.executeHeartbeat(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeHeartbeat: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, r.state.Cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState
	return cloneState(r.state), nil
}

func (r *Runtime) ApplyLiveness(ctx context.Context, cmd Command) (State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _, duplicate, err := r.executeLiveness(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeLiveness: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, r.state.Cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState
	return cloneState(r.state), nil
}

func (r *Runtime) AcknowledgeOutbox(ctx context.Context, cmd Command) (State, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, _, duplicate, err := r.executeAcknowledgeOutbox(cmd)
	if err != nil {
		return State{}, fmt.Errorf("err in r.executeAcknowledgeOutbox: %w", err)
	}
	if duplicate != nil {
		return r.snapshotForApplied(*duplicate), nil
	}

	record, nextState := r.commitCandidate(cmd, r.state.Cluster, nil)
	if err := r.store.AppendWAL(ctx, record); err != nil {
		return State{}, fmt.Errorf("err in r.store.AppendWAL: %w", err)
	}
	r.state = nextState
	return cloneState(r.state), nil
}

func (r *Runtime) Checkpoint(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	checkpointState := cloneState(r.state)
	checkpointState.AppliedCommands = map[string]AppliedCommand{}

	if err := r.store.SaveCheckpoint(ctx, Checkpoint{State: checkpointState}); err != nil {
		return fmt.Errorf("err in r.store.SaveCheckpoint: %w", err)
	}
	if err := r.store.TruncateWAL(ctx, checkpointState.LastLogIndex); err != nil {
		return fmt.Errorf("err in r.store.TruncateWAL: %w", err)
	}

	r.state.AppliedCommands = map[string]AppliedCommand{}
	return nil
}

func (r *Runtime) replayRecord(record LogRecord) error {
	cluster, plan, duplicate, err := r.executeCommand(record.Command)
	if err != nil {
		return fmt.Errorf("%w: replay command %q: %w", ErrRecovery, record.Command.ID, err)
	}
	if duplicate != nil {
		return fmt.Errorf("%w: duplicate command %q present in WAL replay", ErrRecovery, record.Command.ID)
	}

	nextState := r.nextStateForApplied(record.Index, record.Command, cluster, plan)
	r.state = nextState
	return nil
}

func (r *Runtime) executeBootstrap(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeReconfigure(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeProgress(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeHeartbeat(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeLiveness(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeAcknowledgeOutbox(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	return r.executeCommand(cmd)
}

func (r *Runtime) executeCommand(cmd Command) (coordinator.ClusterState, *coordinator.ReconfigurationPlan, *AppliedCommand, error) {
	duplicate, err := r.validateCommand(cmd)
	if err != nil || duplicate != nil {
		if err != nil {
			return coordinator.ClusterState{}, nil, duplicate, fmt.Errorf("err in r.validateCommand: %w", err)
		}
		return coordinator.ClusterState{}, nil, duplicate, nil
	}

	switch cmd.Kind {
	case CommandKindBootstrap:
		if isInitialized(r.state) {
			return coordinator.ClusterState{}, nil, nil, ErrAlreadyInitialized
		}
		cluster, err := coordinator.BuildInitialPlacement(
			cmd.Bootstrap.Config,
			cloneNodes(cmd.Bootstrap.Nodes),
		)
		if err != nil {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("err in coordinator.BuildInitialPlacement: %w", err)
		}
		return cloneClusterState(*cluster), nil, nil, nil
	case CommandKindReconfigure:
		if !isInitialized(r.state) {
			return coordinator.ClusterState{}, nil, nil, ErrNotInitialized
		}
		plan, err := coordinator.PlanReconfiguration(
			cloneClusterState(r.state.Cluster),
			cloneEvents(cmd.Reconfigure.Events),
			cmd.Reconfigure.Policy,
		)
		if err != nil {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("err in coordinator.PlanReconfiguration: %w", err)
		}
		return cloneClusterState(plan.UpdatedState), clonePlan(plan), nil, nil
	case CommandKindProgress:
		if !isInitialized(r.state) {
			return coordinator.ClusterState{}, nil, nil, ErrNotInitialized
		}
		cluster, err := coordinator.ApplyProgress(
			cloneClusterState(r.state.Cluster),
			cloneEvent(cmd.Progress.Event),
		)
		if err != nil {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("err in coordinator.ApplyProgress: %w", err)
		}
		return cloneClusterState(*cluster), nil, nil, nil
	case CommandKindHeartbeat:
		return cloneClusterState(r.state.Cluster), nil, nil, nil
	case CommandKindLiveness:
		if cmd.Liveness.NodeID == "" {
			return coordinator.ClusterState{}, nil, nil, fmt.Errorf("%w: liveness node ID must not be empty", ErrInvalidCommand)
		}
		return cloneClusterState(r.state.Cluster), nil, nil, nil
	case CommandKindAcknowledgeOutbox:
		if !isInitialized(r.state) {
			return coordinator.ClusterState{}, nil, nil, ErrNotInitialized
		}
		if !outboxEntryExists(r.state.Outbox, cmd.AcknowledgeOutbox.EntryID) {
			return cloneClusterState(r.state.Cluster), nil, nil, nil
		}
		return cloneClusterState(r.state.Cluster), nil, nil, nil
	default:
		return coordinator.ClusterState{}, nil, nil, fmt.Errorf(
			"%w: unsupported command kind %q",
			ErrInvalidCommand,
			cmd.Kind,
		)
	}
}

func (r *Runtime) validateCommand(cmd Command) (*AppliedCommand, error) {
	if cmd.ID == "" {
		return nil, fmt.Errorf("%w: command ID must not be empty", ErrInvalidCommand)
	}
	if err := validateCommandPayload(cmd); err != nil {
		return nil, fmt.Errorf("err in validateCommandPayload: %w", err)
	}

	if applied, ok := r.state.AppliedCommands[cmd.ID]; ok {
		if !reflect.DeepEqual(applied.Command, cloneCommand(cmd)) {
			return nil, fmt.Errorf("%w: command %q was already applied with different payload", ErrCommandConflict, cmd.ID)
		}
		cloned := cloneAppliedCommand(applied)
		return &cloned, nil
	}

	if cmd.ExpectedVersion != r.state.Version {
		return nil, fmt.Errorf(
			"%w: expected version %d does not match current version %d",
			ErrVersionMismatch,
			cmd.ExpectedVersion,
			r.state.Version,
		)
	}

	return nil, nil
}

func validateCommandPayload(cmd Command) error {
	switch cmd.Kind {
	case CommandKindBootstrap:
		if cmd.Bootstrap == nil || cmd.Reconfigure != nil || cmd.Progress != nil || cmd.Heartbeat != nil || cmd.Liveness != nil || cmd.AcknowledgeOutbox != nil {
			return fmt.Errorf("%w: bootstrap command must set only bootstrap payload", ErrInvalidCommand)
		}
	case CommandKindReconfigure:
		if cmd.Reconfigure == nil || cmd.Bootstrap != nil || cmd.Progress != nil || cmd.Heartbeat != nil || cmd.Liveness != nil || cmd.AcknowledgeOutbox != nil {
			return fmt.Errorf("%w: reconfigure command must set only reconfigure payload", ErrInvalidCommand)
		}
	case CommandKindProgress:
		if cmd.Progress == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil || cmd.Heartbeat != nil || cmd.Liveness != nil || cmd.AcknowledgeOutbox != nil {
			return fmt.Errorf("%w: progress command must set only progress payload", ErrInvalidCommand)
		}
	case CommandKindHeartbeat:
		if cmd.Heartbeat == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil || cmd.Progress != nil || cmd.Liveness != nil || cmd.AcknowledgeOutbox != nil {
			return fmt.Errorf("%w: heartbeat command must set only heartbeat payload", ErrInvalidCommand)
		}
		if cmd.Heartbeat.Status.NodeID == "" {
			return fmt.Errorf("%w: heartbeat node ID must not be empty", ErrInvalidCommand)
		}
	case CommandKindLiveness:
		if cmd.Liveness == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil || cmd.Progress != nil || cmd.Heartbeat != nil || cmd.AcknowledgeOutbox != nil {
			return fmt.Errorf("%w: liveness command must set only liveness payload", ErrInvalidCommand)
		}
		if cmd.Liveness.NodeID == "" {
			return fmt.Errorf("%w: liveness node ID must not be empty", ErrInvalidCommand)
		}
	case CommandKindAcknowledgeOutbox:
		if cmd.AcknowledgeOutbox == nil || cmd.Bootstrap != nil || cmd.Reconfigure != nil || cmd.Progress != nil || cmd.Heartbeat != nil || cmd.Liveness != nil {
			return fmt.Errorf("%w: acknowledge outbox command must set only outbox payload", ErrInvalidCommand)
		}
		if cmd.AcknowledgeOutbox.EntryID == "" {
			return fmt.Errorf("%w: outbox entry id must not be empty", ErrInvalidCommand)
		}
	default:
		return fmt.Errorf("%w: unsupported command kind %q", ErrInvalidCommand, cmd.Kind)
	}
	return nil
}

func (r *Runtime) commitCandidate(
	cmd Command,
	cluster coordinator.ClusterState,
	plan *coordinator.ReconfigurationPlan,
) (LogRecord, State) {
	index := r.state.LastLogIndex + 1
	record := LogRecord{
		Index:   index,
		Command: cloneCommand(cmd),
	}
	return record, r.nextStateForApplied(index, cmd, cluster, plan)
}

func (r *Runtime) nextStateForApplied(
	logIndex uint64,
	cmd Command,
	cluster coordinator.ClusterState,
	plan *coordinator.ReconfigurationPlan,
) State {
	next := cloneState(r.state)
	next.Version++
	next.LastLogIndex = logIndex
	next.Cluster = cloneClusterState(cluster)
	next.Cluster = nextClusterState(next.Cluster, cmd)
	next.SlotVersions = nextSlotVersions(next.SlotVersions, next.Version, cmd.Kind, cluster, plan)
	next.CompletedProgressBySlot = nextCompletedProgress(next.CompletedProgressBySlot, next.SlotVersions, cmd)
	next.NodeLivenessByID = nextNodeLiveness(next.NodeLivenessByID, cmd)
	next.PendingBySlot = nextPending(next.PendingBySlot, next.Version, next.SlotVersions, cluster, cmd, plan)
	next.Outbox = nextOutbox(next.Outbox, next.Version, next.SlotVersions, cluster, cmd, plan)
	next.LastPolicy = nextLastPolicy(next.LastPolicy, cmd)
	if next.AppliedCommands == nil {
		next.AppliedCommands = make(map[string]AppliedCommand)
	}
	next.AppliedCommands[cmd.ID] = AppliedCommand{
		Command:                 cloneCommand(cmd),
		Version:                 next.Version,
		LastLogIndex:            logIndex,
		Cluster:                 cloneClusterState(next.Cluster),
		SlotVersions:            cloneSlotVersions(next.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(next.CompletedProgressBySlot),
		NodeLivenessByID:        cloneNodeLivenessMap(next.NodeLivenessByID),
		PendingBySlot:           clonePendingMap(next.PendingBySlot),
		Outbox:                  cloneOutbox(next.Outbox),
		LastPolicy:              next.LastPolicy,
		Plan:                    clonePlan(plan),
	}
	return next
}

func nextClusterState(current coordinator.ClusterState, cmd Command) coordinator.ClusterState {
	next := cloneClusterState(current)
	if cmd.Kind == CommandKindHeartbeat && cmd.Heartbeat != nil {
		nodeID := cmd.Heartbeat.Status.NodeID
		if next.NodeHealthByID[nodeID] != coordinator.NodeHealthDead {
			if next.ReadyNodeIDs == nil {
				next.ReadyNodeIDs = map[string]bool{}
			}
			next.ReadyNodeIDs[nodeID] = true
		}
	}
	return next
}

func (r *Runtime) snapshotForApplied(applied AppliedCommand) State {
	snapshot := State{
		Version:                 applied.Version,
		LastLogIndex:            applied.LastLogIndex,
		Cluster:                 cloneClusterState(applied.Cluster),
		SlotVersions:            cloneSlotVersions(applied.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(applied.CompletedProgressBySlot),
		NodeLivenessByID:        cloneNodeLivenessMap(applied.NodeLivenessByID),
		PendingBySlot:           clonePendingMap(applied.PendingBySlot),
		Outbox:                  cloneOutbox(applied.Outbox),
		LastPolicy:              applied.LastPolicy,
		AppliedCommands:         make(map[string]AppliedCommand),
	}
	for id, existing := range r.state.AppliedCommands {
		if existing.Version <= applied.Version {
			snapshot.AppliedCommands[id] = cloneAppliedCommand(existing)
		}
	}
	return snapshot
}

func zeroState() State {
	return State{
		SlotVersions:            map[int]uint64{},
		CompletedProgressBySlot: map[int][]CompletedProgressRecord{},
		NodeLivenessByID:        map[string]NodeLivenessRecord{},
		PendingBySlot:           map[int]PendingWork{},
		Outbox:                  []OutboxEntry{},
		AppliedCommands:         map[string]AppliedCommand{},
	}
}

func isInitialized(state State) bool {
	return state.Cluster.SlotCount > 0
}

func cloneState(state State) State {
	cloned := State{
		Version:                 state.Version,
		LastLogIndex:            state.LastLogIndex,
		Cluster:                 cloneClusterState(state.Cluster),
		SlotVersions:            cloneSlotVersions(state.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(state.CompletedProgressBySlot),
		NodeLivenessByID:        cloneNodeLivenessMap(state.NodeLivenessByID),
		PendingBySlot:           clonePendingMap(state.PendingBySlot),
		Outbox:                  cloneOutbox(state.Outbox),
		LastPolicy:              state.LastPolicy,
		AppliedCommands:         make(map[string]AppliedCommand, len(state.AppliedCommands)),
	}
	for id, applied := range state.AppliedCommands {
		cloned.AppliedCommands[id] = cloneAppliedCommand(applied)
	}
	return cloned
}

func cloneAppliedCommand(applied AppliedCommand) AppliedCommand {
	return AppliedCommand{
		Command:                 cloneCommand(applied.Command),
		Version:                 applied.Version,
		LastLogIndex:            applied.LastLogIndex,
		Cluster:                 cloneClusterState(applied.Cluster),
		SlotVersions:            cloneSlotVersions(applied.SlotVersions),
		CompletedProgressBySlot: cloneCompletedProgressMap(applied.CompletedProgressBySlot),
		NodeLivenessByID:        cloneNodeLivenessMap(applied.NodeLivenessByID),
		PendingBySlot:           clonePendingMap(applied.PendingBySlot),
		Outbox:                  cloneOutbox(applied.Outbox),
		LastPolicy:              applied.LastPolicy,
		Plan:                    clonePlan(applied.Plan),
	}
}

func nextSlotVersions(
	current map[int]uint64,
	_ uint64,
	kind CommandKind,
	cluster coordinator.ClusterState,
	plan *coordinator.ReconfigurationPlan,
) map[int]uint64 {
	next := cloneSlotVersions(current)
	switch kind {
	case CommandKindBootstrap:
		next = make(map[int]uint64, len(cluster.Chains))
		for _, chain := range cluster.Chains {
			next[chain.Slot] = current[chain.Slot] + 1
		}
	case CommandKindReconfigure:
		if plan == nil {
			return next
		}
		for _, slotPlan := range plan.ChangedSlots {
			next[slotPlan.Slot] = current[slotPlan.Slot] + 1
		}
	}
	return next
}

func cloneSlotVersions(slotVersions map[int]uint64) map[int]uint64 {
	cloned := make(map[int]uint64, len(slotVersions))
	for slot, version := range slotVersions {
		cloned[slot] = version
	}
	return cloned
}

const completedProgressHistoryLimit = 8

func nextCompletedProgress(
	current map[int][]CompletedProgressRecord,
	slotVersions map[int]uint64,
	cmd Command,
) map[int][]CompletedProgressRecord {
	next := cloneCompletedProgressMap(current)
	if cmd.Kind != CommandKindProgress || cmd.Progress == nil {
		return next
	}

	var kind CompletedProgressKind
	switch cmd.Progress.Event.Kind {
	case coordinator.EventKindReplicaBecameActive:
		kind = CompletedProgressKindReady
	case coordinator.EventKindReplicaRemoved:
		kind = CompletedProgressKindRemoved
	default:
		return next
	}

	slot := cmd.Progress.Event.Slot
	record := CompletedProgressRecord{
		NodeID:      cmd.Progress.Event.NodeID,
		Kind:        kind,
		SlotVersion: slotVersions[slot],
	}
	next[slot] = append(next[slot], record)
	if len(next[slot]) > completedProgressHistoryLimit {
		next[slot] = append([]CompletedProgressRecord(nil), next[slot][len(next[slot])-completedProgressHistoryLimit:]...)
	}
	return next
}

func cloneCompletedProgressMap(current map[int][]CompletedProgressRecord) map[int][]CompletedProgressRecord {
	cloned := make(map[int][]CompletedProgressRecord, len(current))
	for slot, records := range current {
		cloned[slot] = append([]CompletedProgressRecord(nil), records...)
	}
	return cloned
}

func cloneCommand(cmd Command) Command {
	cloned := Command{
		ID:              cmd.ID,
		ExpectedVersion: cmd.ExpectedVersion,
		Kind:            cmd.Kind,
	}
	if cmd.Bootstrap != nil {
		cloned.Bootstrap = &BootstrapCommand{
			Config: cmd.Bootstrap.Config,
			Nodes:  cloneNodes(cmd.Bootstrap.Nodes),
			Policy: cmd.Bootstrap.Policy,
		}
	}
	if cmd.Reconfigure != nil {
		cloned.Reconfigure = &ReconfigureCommand{
			Events: cloneEvents(cmd.Reconfigure.Events),
			Policy: cmd.Reconfigure.Policy,
		}
	}
	if cmd.Progress != nil {
		cloned.Progress = &ProgressCommand{
			Event: cloneEvent(cmd.Progress.Event),
		}
	}
	if cmd.Heartbeat != nil {
		cloned.Heartbeat = &HeartbeatCommand{
			Status:             cloneNodeStatus(cmd.Heartbeat.Status),
			ObservedAtUnixNano: cmd.Heartbeat.ObservedAtUnixNano,
			FlapWindowNanos:    cmd.Heartbeat.FlapWindowNanos,
		}
	}
	if cmd.Liveness != nil {
		cloned.Liveness = &LivenessCommand{
			NodeID:              cmd.Liveness.NodeID,
			State:               cmd.Liveness.State,
			EvaluatedAtUnixNano: cmd.Liveness.EvaluatedAtUnixNano,
			DeadActionFired:     cmd.Liveness.DeadActionFired,
			FlapWindowNanos:     cmd.Liveness.FlapWindowNanos,
		}
	}
	if cmd.AcknowledgeOutbox != nil {
		cloned.AcknowledgeOutbox = &AcknowledgeOutboxCommand{EntryID: cmd.AcknowledgeOutbox.EntryID}
	}
	return cloned
}

func nextNodeLiveness(
	current map[string]NodeLivenessRecord,
	cmd Command,
) map[string]NodeLivenessRecord {
	next := cloneNodeLivenessMap(current)
	switch cmd.Kind {
	case CommandKindHeartbeat:
		record := next[cmd.Heartbeat.Status.NodeID]
		record.LastHeartbeatUnixNano = cmd.Heartbeat.ObservedAtUnixNano
		record.LastStatus = cloneNodeStatus(cmd.Heartbeat.Status)
		record.SuspectTransitionsUnixNano = pruneSuspectTransitions(
			record.SuspectTransitionsUnixNano,
			cmd.Heartbeat.ObservedAtUnixNano,
			cmd.Heartbeat.FlapWindowNanos,
		)
		if record.State != NodeLivenessStateDead {
			record.State = NodeLivenessStateHealthy
			record.DeadActionFired = false
		}
		next[cmd.Heartbeat.Status.NodeID] = record
	case CommandKindLiveness:
		record := next[cmd.Liveness.NodeID]
		record.SuspectTransitionsUnixNano = pruneSuspectTransitions(
			record.SuspectTransitionsUnixNano,
			cmd.Liveness.EvaluatedAtUnixNano,
			cmd.Liveness.FlapWindowNanos,
		)
		if cmd.Liveness.State == NodeLivenessStateSuspect &&
			record.State != NodeLivenessStateSuspect &&
			cmd.Liveness.EvaluatedAtUnixNano != 0 {
			record.SuspectTransitionsUnixNano = append(
				record.SuspectTransitionsUnixNano,
				cmd.Liveness.EvaluatedAtUnixNano,
			)
		}
		record.State = cmd.Liveness.State
		record.DeadActionFired = cmd.Liveness.DeadActionFired
		if cmd.Liveness.EvaluatedAtUnixNano != 0 && record.LastHeartbeatUnixNano == 0 {
			record.LastHeartbeatUnixNano = cmd.Liveness.EvaluatedAtUnixNano
		}
		next[cmd.Liveness.NodeID] = record
	}
	return next
}

func cloneNodeLivenessMap(current map[string]NodeLivenessRecord) map[string]NodeLivenessRecord {
	cloned := make(map[string]NodeLivenessRecord, len(current))
	for nodeID, record := range current {
		cloned[nodeID] = NodeLivenessRecord{
			LastHeartbeatUnixNano:      record.LastHeartbeatUnixNano,
			State:                      record.State,
			LastStatus:                 cloneNodeStatus(record.LastStatus),
			DeadActionFired:            record.DeadActionFired,
			SuspectTransitionsUnixNano: append([]int64(nil), record.SuspectTransitionsUnixNano...),
		}
	}
	return cloned
}

func pruneSuspectTransitions(current []int64, observedAtUnixNano int64, flapWindowNanos int64) []int64 {
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

func clonePendingMap(current map[int]PendingWork) map[int]PendingWork {
	cloned := make(map[int]PendingWork, len(current))
	for slot, pending := range current {
		cloned[slot] = pending
	}
	return cloned
}

func cloneOutbox(current []OutboxEntry) []OutboxEntry {
	cloned := make([]OutboxEntry, len(current))
	for i, entry := range current {
		cloned[i] = OutboxEntry{
			ID:        entry.ID,
			Slot:      entry.Slot,
			NodeID:    entry.NodeID,
			Kind:      entry.Kind,
			CommandID: entry.CommandID,
			Assignment: storage.ReplicaAssignment{
				Slot:         entry.Assignment.Slot,
				ChainVersion: entry.Assignment.ChainVersion,
				Role:         entry.Assignment.Role,
				Peers:        entry.Assignment.Peers,
			},
		}
	}
	return cloned
}

func nextLastPolicy(current coordinator.ReconfigurationPolicy, cmd Command) coordinator.ReconfigurationPolicy {
	if cmd.Kind == CommandKindBootstrap && cmd.Bootstrap != nil {
		return cmd.Bootstrap.Policy
	}
	if cmd.Kind == CommandKindReconfigure && cmd.Reconfigure != nil {
		return cmd.Reconfigure.Policy
	}
	return current
}

func nextPending(
	current map[int]PendingWork,
	version uint64,
	slotVersions map[int]uint64,
	cluster coordinator.ClusterState,
	cmd Command,
	plan *coordinator.ReconfigurationPlan,
) map[int]PendingWork {
	next := clonePendingMap(current)
	switch cmd.Kind {
	case CommandKindBootstrap:
		return map[int]PendingWork{}
	case CommandKindReconfigure:
		if plan == nil {
			return next
		}
		return pendingFromPlan(next, version, slotVersions, cluster, cmd, plan)
	case CommandKindProgress:
		if cmd.Progress != nil {
			delete(next, cmd.Progress.Event.Slot)
		}
	case CommandKindHeartbeat, CommandKindLiveness, CommandKindAcknowledgeOutbox:
		return next
	}
	return next
}

func pendingFromPlan(
	current map[int]PendingWork,
	_ uint64,
	slotVersions map[int]uint64,
	cluster coordinator.ClusterState,
	cmd Command,
	plan *coordinator.ReconfigurationPlan,
) map[int]PendingWork {
	next := clonePendingMap(current)
	for _, slotPlan := range plan.ChangedSlots {
		delete(next, slotPlan.Slot)
		stepKinds := distinctStepKinds(slotPlan.Steps)
		switch {
		case len(stepKinds) == 1 && stepKinds[0] == coordinator.StepKindAppendTail:
			nodeID := firstStepNodeID(slotPlan.Steps, coordinator.StepKindAppendTail)
			if nodeID == "" {
				continue
			}
			next[slotPlan.Slot] = PendingWork{
				Slot:        slotPlan.Slot,
				NodeID:      nodeID,
				Kind:        PendingKindReady,
				SlotVersion: slotVersions[slotPlan.Slot],
			}
		case len(stepKinds) == 1 && stepKinds[0] == coordinator.StepKindMarkLeaving:
			nodeID := firstStepNodeID(slotPlan.Steps, coordinator.StepKindMarkLeaving)
			if nodeID == "" {
				continue
			}
			next[slotPlan.Slot] = PendingWork{
				Slot:        slotPlan.Slot,
				NodeID:      nodeID,
				Kind:        PendingKindRemoved,
				SlotVersion: slotVersions[slotPlan.Slot],
			}
		}
	}
	return next
}

func nextOutbox(
	current []OutboxEntry,
	version uint64,
	slotVersions map[int]uint64,
	cluster coordinator.ClusterState,
	cmd Command,
	plan *coordinator.ReconfigurationPlan,
) []OutboxEntry {
	switch cmd.Kind {
	case CommandKindBootstrap:
		return []OutboxEntry{}
	case CommandKindReconfigure:
		if plan == nil {
			return cloneOutbox(current)
		}
		return outboxFromPlan(current, version, slotVersions, cluster, plan)
	case CommandKindProgress:
		if cmd.Progress == nil {
			return cloneOutbox(current)
		}
		next := make([]OutboxEntry, 0, len(current))
		for _, entry := range current {
			if entry.Slot != cmd.Progress.Event.Slot {
				next = append(next, entry)
			}
		}
		return next
	case CommandKindAcknowledgeOutbox:
		if cmd.AcknowledgeOutbox == nil {
			return cloneOutbox(current)
		}
		next := make([]OutboxEntry, 0, len(current))
		for _, entry := range current {
			if entry.ID != cmd.AcknowledgeOutbox.EntryID {
				next = append(next, entry)
			}
		}
		return next
	default:
		return cloneOutbox(current)
	}
}

func outboxFromPlan(
	current []OutboxEntry,
	version uint64,
	slotVersions map[int]uint64,
	cluster coordinator.ClusterState,
	plan *coordinator.ReconfigurationPlan,
) []OutboxEntry {
	changedSlots := make(map[int]struct{}, len(plan.ChangedSlots))
	for _, slotPlan := range plan.ChangedSlots {
		changedSlots[slotPlan.Slot] = struct{}{}
	}

	outbox := make([]OutboxEntry, 0, len(current))
	for _, entry := range current {
		if _, changed := changedSlots[entry.Slot]; changed {
			continue
		}
		outbox = append(outbox, OutboxEntry{
			ID:        entry.ID,
			Slot:      entry.Slot,
			NodeID:    entry.NodeID,
			Kind:      entry.Kind,
			CommandID: entry.CommandID,
			Assignment: storage.ReplicaAssignment{
				Slot:         entry.Assignment.Slot,
				ChainVersion: entry.Assignment.ChainVersion,
				Role:         entry.Assignment.Role,
				Peers:        entry.Assignment.Peers,
			},
		})
	}
	for _, slotPlan := range plan.ChangedSlots {
		stepKinds := distinctStepKinds(slotPlan.Steps)
		switch {
		case len(stepKinds) == 1 && stepKinds[0] == coordinator.StepKindAppendTail:
			addedNodeID := firstStepNodeID(slotPlan.Steps, coordinator.StepKindAppendTail)
			if addedNodeID == "" {
				continue
			}
			assignment, err := assignmentForNode(slotPlan.After, cluster.NodesByID, addedNodeID, slotVersions[slotPlan.Slot])
			if err == nil {
				outbox = append(outbox, OutboxEntry{
					ID:         outboxEntryID(version, slotPlan.Slot, addedNodeID, OutboxCommandKindAddReplicaAsTail),
					Slot:       slotPlan.Slot,
					NodeID:     addedNodeID,
					Kind:       OutboxCommandKindAddReplicaAsTail,
					CommandID:  outboxCommandID(version, slotPlan.Slot, addedNodeID, "ready"),
					Assignment: assignment,
				})
			}
			skipped := map[string]bool{addedNodeID: true}
			servingChain := activeServingChain(slotPlan.After)
			for _, nodeID := range activeAfterNodeIDs(servingChain, skipped) {
				assignment, err := assignmentForNode(servingChain, cluster.NodesByID, nodeID, slotVersions[slotPlan.Slot])
				if err != nil {
					continue
				}
				outbox = append(outbox, OutboxEntry{
					ID:         outboxEntryID(version, slotPlan.Slot, nodeID, OutboxCommandKindUpdateChainPeers),
					Slot:       slotPlan.Slot,
					NodeID:     nodeID,
					Kind:       OutboxCommandKindUpdateChainPeers,
					CommandID:  outboxCommandID(version, slotPlan.Slot, nodeID, "update"),
					Assignment: assignment,
				})
			}
		case len(stepKinds) == 1 && stepKinds[0] == coordinator.StepKindMarkLeaving:
			leavingNodeID := firstStepNodeID(slotPlan.Steps, coordinator.StepKindMarkLeaving)
			if leavingNodeID == "" {
				continue
			}
			skipped := map[string]bool{leavingNodeID: true}
			servingChain := activeServingChain(slotPlan.After)
			for _, nodeID := range activeAfterNodeIDs(servingChain, skipped) {
				assignment, err := assignmentForNode(servingChain, cluster.NodesByID, nodeID, slotVersions[slotPlan.Slot])
				if err != nil {
					continue
				}
				outbox = append(outbox, OutboxEntry{
					ID:         outboxEntryID(version, slotPlan.Slot, nodeID, OutboxCommandKindUpdateChainPeers),
					Slot:       slotPlan.Slot,
					NodeID:     nodeID,
					Kind:       OutboxCommandKindUpdateChainPeers,
					CommandID:  outboxCommandID(version, slotPlan.Slot, nodeID, "update"),
					Assignment: assignment,
				})
			}
			outbox = append(outbox, OutboxEntry{
				ID:        outboxEntryID(version, slotPlan.Slot, leavingNodeID, OutboxCommandKindMarkReplicaLeaving),
				Slot:      slotPlan.Slot,
				NodeID:    leavingNodeID,
				Kind:      OutboxCommandKindMarkReplicaLeaving,
				CommandID: outboxCommandID(version, slotPlan.Slot, leavingNodeID, "removed"),
			})
		}
	}
	return outbox
}

func outboxEntryExists(current []OutboxEntry, entryID string) bool {
	for _, entry := range current {
		if entry.ID == entryID {
			return true
		}
	}
	return false
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

func clonePlan(plan *coordinator.ReconfigurationPlan) *coordinator.ReconfigurationPlan {
	if plan == nil {
		return nil
	}
	cloned := &coordinator.ReconfigurationPlan{
		UpdatedState:      cloneClusterState(plan.UpdatedState),
		UnassignedNodeIDs: append([]string(nil), plan.UnassignedNodeIDs...),
		ChangedSlots:      make([]coordinator.SlotPlan, len(plan.ChangedSlots)),
	}
	for i, slotPlan := range plan.ChangedSlots {
		cloned.ChangedSlots[i] = coordinator.SlotPlan{
			Slot:   slotPlan.Slot,
			Before: cloneChain(slotPlan.Before),
			After:  cloneChain(slotPlan.After),
			Steps:  append([]coordinator.ReconfigurationStep(nil), slotPlan.Steps...),
		}
	}
	return cloned
}

func cloneClusterState(state coordinator.ClusterState) coordinator.ClusterState {
	cloned := coordinator.ClusterState{
		Chains:            make([]coordinator.Chain, len(state.Chains)),
		NodesByID:         make(map[string]coordinator.Node, len(state.NodesByID)),
		NodeHealthByID:    make(map[string]coordinator.NodeHealth, len(state.NodeHealthByID)),
		ReadyNodeIDs:      make(map[string]bool, len(state.ReadyNodeIDs)),
		DrainingNodeIDs:   make(map[string]bool, len(state.DrainingNodeIDs)),
		NodeOrder:         append([]string(nil), state.NodeOrder...),
		SlotCount:         state.SlotCount,
		ReplicationFactor: state.ReplicationFactor,
	}
	for i, chain := range state.Chains {
		cloned.Chains[i] = cloneChain(chain)
	}
	for id, node := range state.NodesByID {
		cloned.NodesByID[id] = coordinator.Node{
			ID:             node.ID,
			RPCAddress:     node.RPCAddress,
			FailureDomains: cloneFailureDomains(node.FailureDomains),
		}
	}
	for id, health := range state.NodeHealthByID {
		cloned.NodeHealthByID[id] = health
	}
	for id, ready := range state.ReadyNodeIDs {
		if ready {
			cloned.ReadyNodeIDs[id] = true
		}
	}
	for id, draining := range state.DrainingNodeIDs {
		cloned.DrainingNodeIDs[id] = draining
	}
	return cloned
}

func cloneChain(chain coordinator.Chain) coordinator.Chain {
	cloned := coordinator.Chain{
		Slot:     chain.Slot,
		Replicas: make([]coordinator.Replica, len(chain.Replicas)),
	}
	copy(cloned.Replicas, chain.Replicas)
	return cloned
}

func cloneNodes(nodes []coordinator.Node) []coordinator.Node {
	cloned := make([]coordinator.Node, len(nodes))
	for i, node := range nodes {
		cloned[i] = coordinator.Node{
			ID:             node.ID,
			RPCAddress:     node.RPCAddress,
			FailureDomains: cloneFailureDomains(node.FailureDomains),
		}
	}
	return cloned
}

func cloneEvents(events []coordinator.Event) []coordinator.Event {
	cloned := make([]coordinator.Event, len(events))
	for i, event := range events {
		cloned[i] = cloneEvent(event)
	}
	return cloned
}

func cloneEvent(event coordinator.Event) coordinator.Event {
	return coordinator.Event{
		Kind: event.Kind,
		Node: coordinator.Node{
			ID:             event.Node.ID,
			RPCAddress:     event.Node.RPCAddress,
			FailureDomains: cloneFailureDomains(event.Node.FailureDomains),
		},
		NodeID: event.NodeID,
		Slot:   event.Slot,
	}
}

func cloneFailureDomains(domains map[string]string) map[string]string {
	if len(domains) == 0 {
		return map[string]string{}
	}
	cloned := make(map[string]string, len(domains))
	for key, value := range domains {
		cloned[key] = value
	}
	return cloned
}

func distinctStepKinds(steps []coordinator.ReconfigurationStep) []coordinator.StepKind {
	seen := map[coordinator.StepKind]bool{}
	kinds := make([]coordinator.StepKind, 0, len(steps))
	for _, step := range steps {
		if seen[step.Kind] {
			continue
		}
		seen[step.Kind] = true
		kinds = append(kinds, step.Kind)
	}
	return kinds
}

func firstStepNodeID(steps []coordinator.ReconfigurationStep, kind coordinator.StepKind) string {
	for _, step := range steps {
		if step.Kind == kind {
			return step.NodeID
		}
	}
	return ""
}

func outboxEntryID(version uint64, slot int, nodeID string, kind OutboxCommandKind) string {
	return fmt.Sprintf("outbox-v%d-slot%d-%s-%s", version, slot, nodeID, kind)
}

func outboxCommandID(version uint64, slot int, nodeID string, suffix string) string {
	return fmt.Sprintf("server-v%d-slot%d-%s-%s", version, slot, nodeID, suffix)
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
		return storage.ReplicaAssignment{}, fmt.Errorf("node %q not found in chain %d", nodeID, chain.Slot)
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
	return assignment, nil
}

func activeServingChain(chain coordinator.Chain) coordinator.Chain {
	serving := coordinator.Chain{Slot: chain.Slot, Replicas: make([]coordinator.Replica, 0, len(chain.Replicas))}
	for _, replica := range chain.Replicas {
		if replica.State == coordinator.ReplicaStateActive {
			serving.Replicas = append(serving.Replicas, replica)
		}
	}
	return serving
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
