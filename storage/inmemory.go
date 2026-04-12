package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type stagedOperation struct {
	kind     OperationKind
	key      string
	value    string
	metadata ObjectMetadata
}

type replicaData struct {
	committed        Snapshot
	staged           map[uint64]stagedOperation
	highestCommitted uint64
}

type InMemoryBackend struct {
	mu       sync.RWMutex
	replicas map[int]*replicaData
	local    LocalStateStore
}

func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		replicas: make(map[int]*replicaData),
	}
}

func (b *InMemoryBackend) BindLocalStateStore(local LocalStateStore) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.local = local
}

func (b *InMemoryBackend) CreateReplica(slot int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.replicas[slot]; exists {
		return fmt.Errorf("%w: slot %d", ErrReplicaExists, slot)
	}
	b.replicas[slot] = &replicaData{
		committed: Snapshot{},
		staged:    map[uint64]stagedOperation{},
	}
	return nil
}

func (b *InMemoryBackend) DeleteReplica(slot int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.replicas[slot]; !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	delete(b.replicas, slot)
	return nil
}

func (b *InMemoryBackend) Snapshot(slot int) (Snapshot, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return cloneSnapshot(replica.committed), nil
}

func (b *InMemoryBackend) InstallSnapshot(slot int, snap Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	replica.committed = cloneSnapshot(snap)
	replica.staged = map[uint64]stagedOperation{}
	return nil
}

func (b *InMemoryBackend) SetHighestCommittedSequence(slot int, sequence uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	replica.highestCommitted = sequence
	replica.staged = map[uint64]stagedOperation{}
	return nil
}

func (b *InMemoryBackend) ApplyCommitted(ctx context.Context, nodeID string, operation WriteOperation, persisted *PersistedReplica) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	replica, exists := b.replicas[operation.Slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, operation.Slot)
	}
	if operation.Sequence != replica.highestCommitted+1 {
		return fmt.Errorf(
			"%w: slot %d expected commit sequence %d, got %d",
			ErrSequenceMismatch,
			operation.Slot,
			replica.highestCommitted+1,
			operation.Sequence,
		)
	}
	switch operation.Kind {
	case OperationKindPut:
		replica.committed[operation.Key] = CommittedObject{
			Value:    operation.Value,
			Metadata: cloneObjectMetadata(operation.Metadata),
		}
	case OperationKindDelete:
		delete(replica.committed, operation.Key)
	default:
		return fmt.Errorf("%w: unsupported operation kind %q", ErrInvalidConfig, operation.Kind)
	}
	delete(replica.staged, operation.Sequence)
	replica.highestCommitted = operation.Sequence
	if persisted != nil && b.local != nil {
		if err := b.local.UpsertReplica(ctx, nodeID, *persisted); err != nil {
			return fmt.Errorf("err in b.local.UpsertReplica: %w", err)
		}
	}
	return nil
}

func (b *InMemoryBackend) Put(slot int, key string, value string, metadata ObjectMetadata) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	replica.committed[key] = CommittedObject{Value: value, Metadata: cloneObjectMetadata(metadata)}
	return nil
}

func (b *InMemoryBackend) ReplicaData(slot int) (Snapshot, error) {
	return b.CommittedSnapshot(slot)
}

func (b *InMemoryBackend) StagePut(slot int, sequence uint64, key string, value string, metadata ObjectMetadata) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if _, exists := replica.staged[sequence]; exists {
		return fmt.Errorf("%w: slot %d sequence %d already staged", ErrSequenceMismatch, slot, sequence)
	}
	replica.staged[sequence] = stagedOperation{
		kind:     OperationKindPut,
		key:      key,
		value:    value,
		metadata: cloneObjectMetadata(metadata),
	}
	return nil
}

func (b *InMemoryBackend) StageDelete(slot int, sequence uint64, key string, metadata ObjectMetadata) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if _, exists := replica.staged[sequence]; exists {
		return fmt.Errorf("%w: slot %d sequence %d already staged", ErrSequenceMismatch, slot, sequence)
	}
	replica.staged[sequence] = stagedOperation{
		kind:     OperationKindDelete,
		key:      key,
		metadata: cloneObjectMetadata(metadata),
	}
	return nil
}

func (b *InMemoryBackend) CommitSequence(slot int, sequence uint64) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	if sequence != replica.highestCommitted+1 {
		return fmt.Errorf(
			"%w: slot %d expected commit sequence %d, got %d",
			ErrSequenceMismatch,
			slot,
			replica.highestCommitted+1,
			sequence,
		)
	}
	operation, exists := replica.staged[sequence]
	if !exists {
		return fmt.Errorf("%w: slot %d sequence %d is not staged", ErrSequenceMismatch, slot, sequence)
	}
	switch operation.kind {
	case OperationKindPut:
		replica.committed[operation.key] = CommittedObject{
			Value:    operation.value,
			Metadata: cloneObjectMetadata(operation.metadata),
		}
	case OperationKindDelete:
		delete(replica.committed, operation.key)
	default:
		return fmt.Errorf("%w: unsupported operation kind %q", ErrInvalidConfig, operation.kind)
	}
	delete(replica.staged, sequence)
	replica.highestCommitted = sequence
	return nil
}

func (b *InMemoryBackend) CommittedSnapshot(slot int) (Snapshot, error) {
	return b.Snapshot(slot)
}

func (b *InMemoryBackend) GetCommitted(slot int, key string) (CommittedObject, bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return CommittedObject{}, false, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	value, ok := replica.committed[key]
	return cloneCommittedObject(value), ok, nil
}

func (b *InMemoryBackend) HighestCommittedSequence(slot int) (uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return 0, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	return replica.highestCommitted, nil
}

func (b *InMemoryBackend) StagedSequences(slot int) ([]uint64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	replica, exists := b.replicas[slot]
	if !exists {
		return nil, fmt.Errorf("%w: slot %d", ErrUnknownReplica, slot)
	}
	sequences := make([]uint64, 0, len(replica.staged))
	for sequence := range replica.staged {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(i, j int) bool {
		return sequences[i] < sequences[j]
	})
	return sequences, nil
}

func (b *InMemoryBackend) Close() error {
	return nil
}

type InMemoryCoordinatorClient struct {
	mu              sync.Mutex
	Registrations   []NodeRegistration
	ReadySlots      []int
	RemovedSlots    []int
	RecoveryReports []NodeRecoveryReport
	Heartbeats      []NodeStatus
	RegisterErr     error
	ReadyErr        error
	RemovedErr      error
	RecoveredErr    error
	HeartbeatErr    error
}

func NewInMemoryCoordinatorClient() *InMemoryCoordinatorClient {
	return &InMemoryCoordinatorClient{}
}

func (c *InMemoryCoordinatorClient) RegisterNode(_ context.Context, reg NodeRegistration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.RegisterErr != nil {
		return c.RegisterErr
	}
	c.Registrations = append(c.Registrations, NodeRegistration{
		NodeID:         reg.NodeID,
		RPCAddress:     reg.RPCAddress,
		FailureDomains: cloneFailureDomains(reg.FailureDomains),
	})
	return nil
}

func (c *InMemoryCoordinatorClient) ReportReplicaReady(_ context.Context, slot int, _ uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ReadyErr != nil {
		return c.ReadyErr
	}
	c.ReadySlots = append(c.ReadySlots, slot)
	return nil
}

func (c *InMemoryCoordinatorClient) ReportReplicaRemoved(_ context.Context, slot int, _ uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.RemovedErr != nil {
		return c.RemovedErr
	}
	c.RemovedSlots = append(c.RemovedSlots, slot)
	return nil
}

func (c *InMemoryCoordinatorClient) ReportNodeRecovered(_ context.Context, report NodeRecoveryReport) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.RecoveredErr != nil {
		return c.RecoveredErr
	}
	c.RecoveryReports = append(c.RecoveryReports, cloneRecoveryReport(report))
	return nil
}

func (c *InMemoryCoordinatorClient) ReportNodeHeartbeat(_ context.Context, status NodeStatus) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.HeartbeatErr != nil {
		return c.HeartbeatErr
	}
	c.Heartbeats = append(c.Heartbeats, status)
	return nil
}

type InMemoryLocalStateStore struct {
	mu    sync.RWMutex
	nodes map[string]PersistedNodeState
}

func NewInMemoryLocalStateStore() *InMemoryLocalStateStore {
	return &InMemoryLocalStateStore{
		nodes: map[string]PersistedNodeState{},
	}
}

func (s *InMemoryLocalStateStore) LoadNode(_ context.Context, nodeID string) (PersistedNodeState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.nodes[nodeID]
	if !ok {
		return PersistedNodeState{NodeID: nodeID}, nil
	}
	return clonePersistedNodeState(state), nil
}

func (s *InMemoryLocalStateStore) UpsertReplica(_ context.Context, nodeID string, replica PersistedReplica) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.nodes[nodeID]
	if !ok {
		state = PersistedNodeState{NodeID: nodeID}
	}
	replaced := false
	for i := range state.Replicas {
		if state.Replicas[i].Assignment.Slot == replica.Assignment.Slot {
			state.Replicas[i] = clonePersistedReplica(replica)
			replaced = true
			break
		}
	}
	if !replaced {
		state.Replicas = append(state.Replicas, clonePersistedReplica(replica))
	}
	sort.Slice(state.Replicas, func(i, j int) bool {
		return state.Replicas[i].Assignment.Slot < state.Replicas[j].Assignment.Slot
	})
	s.nodes[nodeID] = state
	return nil
}

func (s *InMemoryLocalStateStore) DeleteReplica(_ context.Context, nodeID string, slot int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.nodes[nodeID]
	if !ok {
		return nil
	}
	replicas := state.Replicas[:0]
	for _, replica := range state.Replicas {
		if replica.Assignment.Slot == slot {
			continue
		}
		replicas = append(replicas, replica)
	}
	state.Replicas = replicas
	s.nodes[nodeID] = state
	return nil
}

func (s *InMemoryLocalStateStore) SetHighestAcceptedCoordinatorEpoch(_ context.Context, nodeID string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, ok := s.nodes[nodeID]
	if !ok {
		state = PersistedNodeState{NodeID: nodeID}
	}
	state.HighestAcceptedCoordinatorEpoch = epoch
	s.nodes[nodeID] = clonePersistedNodeState(state)
	return nil
}

func (s *InMemoryLocalStateStore) Close() error {
	return nil
}

type InMemoryReplicationTransport struct {
	backends map[string]Backend
	nodes    map[string]replicationHandler
}

func NewInMemoryReplicationTransport() *InMemoryReplicationTransport {
	return &InMemoryReplicationTransport{
		backends: make(map[string]Backend),
		nodes:    make(map[string]replicationHandler),
	}
}

func (t *InMemoryReplicationTransport) Register(nodeID string, backend Backend) {
	t.backends[nodeID] = backend
}

func (t *InMemoryReplicationTransport) RegisterNode(nodeID string, node replicationHandler) {
	t.nodes[nodeID] = node
}

func (t *InMemoryReplicationTransport) FetchSnapshot(_ context.Context, fromNodeID string, slot int) (Snapshot, uint64, error) {
	backend, ok := t.backends[fromNodeID]
	if !ok {
		return nil, 0, fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, fromNodeID)
	}

	snapshot, err := backend.Snapshot(slot)
	if err != nil {
		return nil, 0, fmt.Errorf("err in backend.Snapshot: %w", err)
	}
	sequence, err := backend.HighestCommittedSequence(slot)
	if err != nil {
		return nil, 0, fmt.Errorf("err in backend.HighestCommittedSequence: %w", err)
	}
	return cloneSnapshot(snapshot), sequence, nil
}

func (t *InMemoryReplicationTransport) ForwardWrite(ctx context.Context, toNodeID string, req ForwardWriteRequest) error {
	node, ok := t.nodes[toNodeID]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, toNodeID)
	}
	if err := node.HandleForwardWrite(ctx, req); err != nil {
		return fmt.Errorf("err in node.HandleForwardWrite: %w", err)
	}
	return nil
}

func (t *InMemoryReplicationTransport) FetchCommittedSequence(_ context.Context, fromNodeID string, slot int) (uint64, error) {
	backend, ok := t.backends[fromNodeID]
	if !ok {
		return 0, fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, fromNodeID)
	}

	sequence, err := backend.HighestCommittedSequence(slot)
	if err != nil {
		return 0, fmt.Errorf("err in backend.HighestCommittedSequence: %w", err)
	}
	return sequence, nil
}

func (t *InMemoryReplicationTransport) CommitWrite(ctx context.Context, toNodeID string, req CommitWriteRequest) error {
	node, ok := t.nodes[toNodeID]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, toNodeID)
	}
	if err := node.HandleCommitWrite(ctx, req); err != nil {
		return fmt.Errorf("err in node.HandleCommitWrite: %w", err)
	}
	return nil
}

type replicationHandler interface {
	HandleForwardWrite(ctx context.Context, req ForwardWriteRequest) error
	HandleCommitWrite(ctx context.Context, req CommitWriteRequest) error
}

type QueuedReplicationMessage struct {
	ToNodeID string
	Forward  *ForwardWriteRequest
	Commit   *CommitWriteRequest
}

type QueuedInMemoryReplicationTransport struct {
	mu                  sync.Mutex
	backends            map[string]Backend
	nodes               map[string]replicationHandler
	queue               []QueuedReplicationMessage
	dropNextWrite       bool
	beforeDeliver       func(QueuedReplicationMessage)
	beforeFetchSnapshot func(fromNodeID string, slot int)
	beforeFetchSequence func(fromNodeID string, slot int)
	blockSnapshot       <-chan struct{}
	blockSequence       <-chan struct{}
}

func NewQueuedInMemoryReplicationTransport() *QueuedInMemoryReplicationTransport {
	return &QueuedInMemoryReplicationTransport{
		backends: map[string]Backend{},
		nodes:    map[string]replicationHandler{},
	}
}

func (t *QueuedInMemoryReplicationTransport) Register(nodeID string, backend Backend) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.backends[nodeID] = backend
}

func (t *QueuedInMemoryReplicationTransport) RegisterNode(nodeID string, node replicationHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[nodeID] = node
}

func (t *QueuedInMemoryReplicationTransport) FetchSnapshot(ctx context.Context, fromNodeID string, slot int) (Snapshot, uint64, error) {
	t.mu.Lock()
	hook := t.beforeFetchSnapshot
	blockSnapshot := t.blockSnapshot
	backend, ok := t.backends[fromNodeID]
	t.mu.Unlock()
	if hook != nil {
		hook(fromNodeID, slot)
	}
	if blockSnapshot != nil {
		select {
		case <-ctx.Done():
			return nil, 0, ctx.Err()
		case <-blockSnapshot:
		}
	}
	if !ok {
		return nil, 0, fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, fromNodeID)
	}
	snap, err := backend.Snapshot(slot)
	if err != nil {
		return nil, 0, fmt.Errorf("err in backend.Snapshot: %w", err)
	}
	sequence, err := backend.HighestCommittedSequence(slot)
	if err != nil {
		return nil, 0, fmt.Errorf("err in backend.HighestCommittedSequence: %w", err)
	}
	return cloneSnapshot(snap), sequence, nil
}

func (t *QueuedInMemoryReplicationTransport) FetchCommittedSequence(ctx context.Context, fromNodeID string, slot int) (uint64, error) {
	t.mu.Lock()
	hook := t.beforeFetchSequence
	blockSequence := t.blockSequence
	backend, ok := t.backends[fromNodeID]
	t.mu.Unlock()
	if hook != nil {
		hook(fromNodeID, slot)
	}
	if blockSequence != nil {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-blockSequence:
		}
	}
	if !ok {
		return 0, fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, fromNodeID)
	}
	return backend.HighestCommittedSequence(slot)
}

func (t *QueuedInMemoryReplicationTransport) ForwardWrite(_ context.Context, toNodeID string, req ForwardWriteRequest) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.nodes[toNodeID]; !ok {
		return fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, toNodeID)
	}
	if t.dropNextWrite {
		t.dropNextWrite = false
		return nil
	}
	cloned := req
	cloned.Operation = cloneWriteOperation(req.Operation)
	t.queue = append(t.queue, QueuedReplicationMessage{
		ToNodeID: toNodeID,
		Forward:  &cloned,
	})
	return nil
}

func (t *QueuedInMemoryReplicationTransport) CommitWrite(_ context.Context, toNodeID string, req CommitWriteRequest) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.nodes[toNodeID]; !ok {
		return fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, toNodeID)
	}
	if t.dropNextWrite {
		t.dropNextWrite = false
		return nil
	}
	cloned := req
	t.queue = append(t.queue, QueuedReplicationMessage{
		ToNodeID: toNodeID,
		Commit:   &cloned,
	})
	return nil
}

func (t *QueuedInMemoryReplicationTransport) AwaitWriteCommit(ctx context.Context, check func() bool) error {
	for !check() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if len(t.queue) == 0 {
			return fmt.Errorf("%w: replication queue drained before write completed", ErrStateMismatch)
		}
		if err := t.DeliverNext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *QueuedInMemoryReplicationTransport) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.queue)
}

func (t *QueuedInMemoryReplicationTransport) PendingMessages() []QueuedReplicationMessage {
	t.mu.Lock()
	defer t.mu.Unlock()
	cloned := make([]QueuedReplicationMessage, 0, len(t.queue))
	for _, msg := range t.queue {
		cloned = append(cloned, cloneQueuedReplicationMessage(msg))
	}
	return cloned
}

func (t *QueuedInMemoryReplicationTransport) DropNext() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dropNextWrite = true
}

func (t *QueuedInMemoryReplicationTransport) DropAt(index int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if index < 0 || index >= len(t.queue) {
		return fmt.Errorf("%w: queued message index %d", ErrStateMismatch, index)
	}
	t.queue = append(t.queue[:index], t.queue[index+1:]...)
	return nil
}

func (t *QueuedInMemoryReplicationTransport) DuplicateAt(index int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if index < 0 || index >= len(t.queue) {
		return fmt.Errorf("%w: queued message index %d", ErrStateMismatch, index)
	}
	t.queue = append(t.queue, cloneQueuedReplicationMessage(t.queue[index]))
	return nil
}

func (t *QueuedInMemoryReplicationTransport) MoveToFront(index int) error {
	return t.MoveTo(index, 0)
}

func (t *QueuedInMemoryReplicationTransport) MoveTo(index int, destination int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if index < 0 || index >= len(t.queue) {
		return fmt.Errorf("%w: queued message index %d", ErrStateMismatch, index)
	}
	if destination < 0 || destination >= len(t.queue) {
		return fmt.Errorf("%w: queued message destination %d", ErrStateMismatch, destination)
	}
	if index == destination {
		return nil
	}
	msg := t.queue[index]
	t.queue = append(t.queue[:index], t.queue[index+1:]...)
	if destination >= len(t.queue) {
		t.queue = append(t.queue, msg)
		return nil
	}
	t.queue = append(t.queue, QueuedReplicationMessage{})
	copy(t.queue[destination+1:], t.queue[destination:])
	t.queue[destination] = msg
	return nil
}

func (t *QueuedInMemoryReplicationTransport) SetBeforeDeliver(hook func(QueuedReplicationMessage)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.beforeDeliver = hook
}

func (t *QueuedInMemoryReplicationTransport) SetBeforeFetchSnapshot(hook func(fromNodeID string, slot int)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.beforeFetchSnapshot = hook
}

func (t *QueuedInMemoryReplicationTransport) SetBeforeFetchCommittedSequence(hook func(fromNodeID string, slot int)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.beforeFetchSequence = hook
}

func (t *QueuedInMemoryReplicationTransport) BlockFetchSnapshot(ch <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blockSnapshot = ch
}

func (t *QueuedInMemoryReplicationTransport) BlockFetchCommittedSequence(ch <-chan struct{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blockSequence = ch
}

func (t *QueuedInMemoryReplicationTransport) DeliverNext(ctx context.Context) error {
	return t.DeliverAt(ctx, 0)
}

func (t *QueuedInMemoryReplicationTransport) DeliverAt(ctx context.Context, index int) error {
	t.mu.Lock()
	if len(t.queue) == 0 {
		t.mu.Unlock()
		return nil
	}
	if index < 0 || index >= len(t.queue) {
		t.mu.Unlock()
		return fmt.Errorf("%w: queued message index %d", ErrStateMismatch, index)
	}
	msg := t.queue[index]
	t.queue = append(t.queue[:index], t.queue[index+1:]...)
	hook := t.beforeDeliver
	node, ok := t.nodes[msg.ToNodeID]
	t.mu.Unlock()
	if hook != nil {
		hook(msg)
	}
	if !ok {
		return fmt.Errorf("%w: node %q", ErrSnapshotSourceUnavailable, msg.ToNodeID)
	}
	switch {
	case msg.Forward != nil:
		if err := node.HandleForwardWrite(ctx, *msg.Forward); err != nil {
			return fmt.Errorf("err in node.HandleForwardWrite: %w", err)
		}
	case msg.Commit != nil:
		if err := node.HandleCommitWrite(ctx, *msg.Commit); err != nil {
			return fmt.Errorf("err in node.HandleCommitWrite: %w", err)
		}
	}
	return nil
}

func (t *QueuedInMemoryReplicationTransport) DeliverAll(ctx context.Context) error {
	for len(t.queue) > 0 {
		if err := t.DeliverNext(ctx); err != nil {
			return err
		}
	}
	return nil
}

func clonePersistedReplica(replica PersistedReplica) PersistedReplica {
	return PersistedReplica{
		Assignment:               cloneAssignment(replica.Assignment),
		LastKnownState:           replica.LastKnownState,
		HighestCommittedSequence: replica.HighestCommittedSequence,
		HasCommittedData:         replica.HasCommittedData,
	}
}

func cloneQueuedReplicationMessage(msg QueuedReplicationMessage) QueuedReplicationMessage {
	cloned := QueuedReplicationMessage{ToNodeID: msg.ToNodeID}
	if msg.Forward != nil {
		req := cloneForwardRequest(*msg.Forward)
		cloned.Forward = &req
	}
	if msg.Commit != nil {
		req := cloneCommitRequest(*msg.Commit)
		cloned.Commit = &req
	}
	return cloned
}

func clonePersistedNodeState(state PersistedNodeState) PersistedNodeState {
	cloned := PersistedNodeState{
		NodeID:                          state.NodeID,
		HighestAcceptedCoordinatorEpoch: state.HighestAcceptedCoordinatorEpoch,
		Replicas:                        make([]PersistedReplica, 0, len(state.Replicas)),
	}
	for _, replica := range state.Replicas {
		cloned.Replicas = append(cloned.Replicas, clonePersistedReplica(replica))
	}
	return cloned
}

func cloneRecoveryReport(report NodeRecoveryReport) NodeRecoveryReport {
	cloned := NodeRecoveryReport{
		NodeID:   report.NodeID,
		Replicas: make([]RecoveredReplica, 0, len(report.Replicas)),
	}
	for _, replica := range report.Replicas {
		cloned.Replicas = append(cloned.Replicas, RecoveredReplica{
			Assignment:               cloneAssignment(replica.Assignment),
			LastKnownState:           replica.LastKnownState,
			HighestCommittedSequence: replica.HighestCommittedSequence,
			HasCommittedData:         replica.HasCommittedData,
		})
	}
	return cloned
}
