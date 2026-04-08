package coordserver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

type HAConfig struct {
	CoordinatorID          string
	AdvertiseAddress       string
	Store                  HAStore
	LeaseTTL               time.Duration
	RenewInterval          time.Duration
	DisableBackgroundLoops bool
}

type haController struct {
	cfg      HAConfig
	mu       sync.RWMutex
	lease    LeaderLease
	isLeader bool
	stop     chan struct{}
	done     chan struct{}
}

const defaultHALeaseTTL = 2 * time.Second
const defaultHARenewInterval = 250 * time.Millisecond

func (s *Server) enableHA(cfg HAConfig) error {
	if cfg.CoordinatorID == "" {
		return fmt.Errorf("%w: ha coordinator ID must not be empty", ErrInvalidServerConfig)
	}
	if cfg.Store == nil {
		return fmt.Errorf("%w: ha store must not be nil", ErrInvalidServerConfig)
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultHALeaseTTL
	}
	if cfg.RenewInterval <= 0 {
		cfg.RenewInterval = defaultHARenewInterval
	}
	s.ha = &haController{
		cfg:  cfg,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	s.syncFromHASnapshot(zeroHASnapshot())
	if _, err := s.StepHA(context.Background()); err != nil && !isNotLeader(err) {
		return err
	}
	if !cfg.DisableBackgroundLoops {
		go s.runHALoop()
	} else {
		close(s.ha.done)
	}
	return nil
}

func (s *Server) runHALoop() {
	defer close(s.ha.done)
	ticker := time.NewTicker(s.ha.cfg.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ha.stop:
			return
		case <-s.closeCh:
			return
		case <-ticker.C:
			_, _ = s.StepHA(context.Background())
		}
	}
}

func (s *Server) StepHA(ctx context.Context) (bool, error) {
	if s.ha == nil {
		return true, nil
	}
	now := s.clock.Now()
	lease, isLeader, err := s.ha.cfg.Store.AcquireOrRenew(ctx, s.ha.cfg.CoordinatorID, s.ha.cfg.AdvertiseAddress, now, s.ha.cfg.LeaseTTL)
	if err != nil {
		return false, fmt.Errorf("err in ha store AcquireOrRenew: %w", err)
	}
	s.ha.setLease(lease, isLeader)
	snapshot, err := s.ha.cfg.Store.LoadSnapshot(ctx)
	if err != nil {
		return false, fmt.Errorf("err in ha store LoadSnapshot: %w", err)
	}
	if isLeader && lease.Epoch != 0 {
		if updated, changed := rewriteSnapshotEpoch(snapshot, lease.Epoch); changed {
			if err := s.saveHASnapshot(ctx, snapshot.SnapshotVersion, updated); err != nil {
				if !errors.Is(err, ErrHASnapshotConflict) && !errors.Is(err, ErrNotLeader) {
					return true, err
				}
			} else {
				snapshot = updated
			}
		}
	}
	s.syncFromHASnapshot(snapshot)
	if !isLeader {
		return false, nil
	}
	if err := s.dispatchOutbox(ctx); err != nil {
		if isNotLeader(err) {
			s.ha.setIsLeader(false)
			return false, nil
		}
		if errors.Is(err, ErrHASnapshotConflict) {
			if _, loadErr := s.loadCurrentHASnapshot(ctx); loadErr != nil {
				return true, loadErr
			}
			return true, nil
		}
		return true, err
	}
	return true, nil
}

func (s *Server) syncFromHASnapshot(snapshot HASnapshot) {
	s.replaceRuntime(coordruntime.OpenInMemoryFromState(snapshot.State))
	s.syncViewsFromRuntime()
	s.activePeerRefreshMu.Lock()
	s.activePeerRefresh = map[int]activePeerRefreshState{}
	s.activePeerRefreshMu.Unlock()
	s.viewMu.Lock()
	heartbeats := make(map[string]storage.NodeStatus, len(s.heartbeats)+len(snapshot.Heartbeats))
	for nodeID, status := range s.heartbeats {
		heartbeats[nodeID] = cloneNodeStatus(status)
	}
	for nodeID, status := range snapshot.Heartbeats {
		heartbeats[nodeID] = cloneNodeStatus(status)
	}
	liveness := make(map[string]coordruntime.NodeLivenessRecord, len(s.liveness)+len(snapshot.Liveness))
	for nodeID, record := range s.liveness {
		liveness[nodeID] = cloneLivenessRecord(record)
	}
	for nodeID, record := range snapshot.Liveness {
		liveness[nodeID] = cloneLivenessRecord(record)
	}
	s.heartbeats = heartbeats
	s.liveness = liveness
	s.pending = make(map[int]PendingWork, len(snapshot.Pending))
	for slot, pending := range snapshot.Pending {
		s.pending[slot] = pending
	}
	s.lastPolicy = snapshot.LastPolicy
	s.unavailableReplicas = make(map[string]map[int]bool, len(snapshot.UnavailableReplicas))
	for nodeID, slots := range snapshot.UnavailableReplicas {
		cloned := make(map[int]bool, len(slots))
		for slot, unavailable := range slots {
			cloned[slot] = unavailable
		}
		s.unavailableReplicas[nodeID] = cloned
	}
	s.lastRecoveryReports = make(map[string]storage.NodeRecoveryReport, len(snapshot.LastRecoveryReports))
	for nodeID, report := range snapshot.LastRecoveryReports {
		s.lastRecoveryReports[nodeID] = cloneRecoveryReport(report)
	}
	s.viewMu.Unlock()
	s.rebuildRoutingSnapshot()
}

func (s *Server) currentHASnapshot() HASnapshot {
	snapshot := zeroHASnapshot()
	snapshot.State = s.currentState()
	s.viewMu.RLock()
	defer s.viewMu.RUnlock()
	snapshot.Pending = make(map[int]PendingWork, len(s.pending))
	for slot, pending := range s.pending {
		snapshot.Pending[slot] = pending
	}
	snapshot.Heartbeats = make(map[string]storage.NodeStatus, len(s.heartbeats))
	for nodeID, status := range s.heartbeats {
		snapshot.Heartbeats[nodeID] = cloneNodeStatus(status)
	}
	snapshot.Liveness = make(map[string]coordruntime.NodeLivenessRecord, len(s.liveness))
	for nodeID, record := range s.liveness {
		snapshot.Liveness[nodeID] = cloneLivenessRecord(record)
	}
	snapshot.LastPolicy = s.lastPolicy
	snapshot.UnavailableReplicas = make(map[string]map[int]bool, len(s.unavailableReplicas))
	for nodeID, slots := range s.unavailableReplicas {
		cloned := make(map[int]bool, len(slots))
		for slot, unavailable := range slots {
			cloned[slot] = unavailable
		}
		snapshot.UnavailableReplicas[nodeID] = cloned
	}
	snapshot.LastRecoveryReports = make(map[string]storage.NodeRecoveryReport, len(s.lastRecoveryReports))
	for nodeID, report := range s.lastRecoveryReports {
		snapshot.LastRecoveryReports[nodeID] = cloneRecoveryReport(report)
	}
	return snapshot
}

func (s *Server) ensureLeader(ctx context.Context) error {
	if s.ha == nil {
		return nil
	}
	lease, isLeader := s.ha.currentLease()
	if isLeader && s.clock.Now().UnixNano() < lease.ExpiresAtUnixNano {
		return nil
	}
	isLeader, err := s.StepHA(ctx)
	if err != nil {
		return err
	}
	if !isLeader {
		return s.notLeaderError()
	}
	return nil
}

func (s *Server) CurrentLeaderLease() (LeaderLease, bool) {
	if s.ha == nil {
		return LeaderLease{}, false
	}
	return s.ha.currentLease()
}

func isNotLeader(err error) bool {
	return errors.Is(err, ErrNotLeader)
}

func (s *Server) notLeaderError() error {
	if s.ha == nil {
		return ErrNotLeader
	}
	lease, _ := s.ha.currentLease()
	return &NotLeaderError{LeaderEndpoint: lease.HolderEndpoint}
}

func (s *Server) saveHASnapshot(ctx context.Context, expectedSnapshotVersion uint64, snapshot HASnapshot) error {
	lease, _ := s.ha.currentLease()
	version, err := s.ha.cfg.Store.SaveSnapshot(ctx, lease, s.clock.Now(), expectedSnapshotVersion, snapshot)
	if err != nil {
		return err
	}
	snapshot.SnapshotVersion = version
	s.syncFromHASnapshot(snapshot)
	return nil
}

func (s *Server) loadCurrentHASnapshot(ctx context.Context) (HASnapshot, error) {
	snapshot, err := s.ha.cfg.Store.LoadSnapshot(ctx)
	if err != nil {
		return HASnapshot{}, err
	}
	s.syncFromHASnapshot(snapshot)
	return snapshot, nil
}

func commandIDForOutbox(kind OutboxCommandKind, nodeID string, slot int, epoch uint64) string {
	return fmt.Sprintf("%s-%s-%d-e%d", kind, nodeID, slot, epoch)
}

func plannerNodeClients(state coordruntime.State) map[string]*haPlanningNodeClient {
	clients := make(map[string]*haPlanningNodeClient, len(state.Cluster.NodesByID))
	for nodeID := range state.Cluster.NodesByID {
		clients[nodeID] = &haPlanningNodeClient{nodeID: nodeID}
	}
	return clients
}

func plannerNodeMap(clients map[string]*haPlanningNodeClient) map[string]StorageNodeClient {
	out := make(map[string]StorageNodeClient, len(clients))
	for nodeID, client := range clients {
		out[nodeID] = client
	}
	return out
}

func (s *Server) newPlannerServer(snapshot HASnapshot) *Server {
	planner := &Server{
		rt:                     coordruntime.OpenInMemoryFromState(snapshot.State),
		nodes:                  map[string]StorageNodeClient{},
		heartbeats:             map[string]storage.NodeStatus{},
		liveness:               map[string]coordruntime.NodeLivenessRecord{},
		pending:                map[int]PendingWork{},
		completed:              map[int][]coordruntime.CompletedProgressRecord{},
		activePeerRefresh:      map[int]activePeerRefreshState{},
		unavailableReplicas:    map[string]map[int]bool{},
		lastRecoveryReports:    map[string]storage.NodeRecoveryReport{},
		livenessPolicy:         s.livenessPolicy,
		clock:                  s.clock,
		dispatchTimeout:        s.dispatchTimeout,
		recoveryCommandTimeout: s.recoveryCommandTimeout,
		logger:                 s.logger,
		metrics:                nil,
		events:                 newServerEventRecorder(),
		closeCh:                make(chan struct{}),
	}
	for slot, pending := range snapshot.Pending {
		planner.pending[slot] = pending
	}
	for nodeID, slots := range snapshot.UnavailableReplicas {
		cloned := make(map[int]bool, len(slots))
		for slot, unavailable := range slots {
			cloned[slot] = unavailable
		}
		planner.unavailableReplicas[nodeID] = cloned
	}
	for nodeID, report := range snapshot.LastRecoveryReports {
		planner.lastRecoveryReports[nodeID] = cloneRecoveryReport(report)
	}
	planner.lastPolicy = snapshot.LastPolicy
	planner.syncViewsFromRuntime()
	planner.viewMu.Lock()
	heartbeats := make(map[string]storage.NodeStatus, len(planner.heartbeats)+len(snapshot.Heartbeats))
	for nodeID, status := range planner.heartbeats {
		heartbeats[nodeID] = cloneNodeStatus(status)
	}
	for nodeID, status := range snapshot.Heartbeats {
		heartbeats[nodeID] = cloneNodeStatus(status)
	}
	liveness := make(map[string]coordruntime.NodeLivenessRecord, len(planner.liveness)+len(snapshot.Liveness))
	for nodeID, record := range planner.liveness {
		liveness[nodeID] = cloneLivenessRecord(record)
	}
	for nodeID, record := range snapshot.Liveness {
		liveness[nodeID] = cloneLivenessRecord(record)
	}
	planner.heartbeats = heartbeats
	planner.liveness = liveness
	planner.viewMu.Unlock()
	planner.rebuildRoutingSnapshot()
	return planner
}

type haPlanningNodeClient struct {
	nodeID  string
	mu      sync.Mutex
	entries []OutboxEntry
}

func (c *haPlanningNodeClient) AddReplicaAsTail(_ context.Context, cmd storage.AddReplicaAsTailCommand) error {
	assignment := cloneReplicaAssignment(cmd.Assignment)
	c.mu.Lock()
	c.entries = append(c.entries, OutboxEntry{
		NodeID:      c.nodeID,
		Slot:        cmd.Assignment.Slot,
		SlotVersion: cmd.Assignment.ChainVersion,
		Kind:        OutboxCommandAddReplicaAsTail,
		Assignment:  &assignment,
	})
	c.mu.Unlock()
	return nil
}

func (c *haPlanningNodeClient) ActivateReplica(_ context.Context, cmd storage.ActivateReplicaCommand) error {
	return nil
}

func (c *haPlanningNodeClient) MarkReplicaLeaving(_ context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	c.mu.Lock()
	c.entries = append(c.entries, OutboxEntry{
		NodeID: c.nodeID,
		Slot:   cmd.Slot,
		Kind:   OutboxCommandMarkReplicaLeaving,
	})
	c.mu.Unlock()
	return nil
}

func (c *haPlanningNodeClient) RemoveReplica(_ context.Context, cmd storage.RemoveReplicaCommand) error {
	return nil
}

func (c *haPlanningNodeClient) UpdateChainPeers(_ context.Context, cmd storage.UpdateChainPeersCommand) error {
	assignment := cloneReplicaAssignment(cmd.Assignment)
	c.mu.Lock()
	c.entries = append(c.entries, OutboxEntry{
		NodeID:      c.nodeID,
		Slot:        cmd.Assignment.Slot,
		SlotVersion: cmd.Assignment.ChainVersion,
		Kind:        OutboxCommandUpdateChainPeers,
		Assignment:  &assignment,
	})
	c.mu.Unlock()
	return nil
}

func (c *haPlanningNodeClient) ResumeRecoveredReplica(_ context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	assignment := cloneReplicaAssignment(cmd.Assignment)
	c.mu.Lock()
	c.entries = append(c.entries, OutboxEntry{
		NodeID:      c.nodeID,
		Slot:        cmd.Assignment.Slot,
		SlotVersion: cmd.Assignment.ChainVersion,
		Kind:        OutboxCommandResumeRecovered,
		Assignment:  &assignment,
	})
	c.mu.Unlock()
	return nil
}

func (c *haPlanningNodeClient) RecoverReplica(_ context.Context, cmd storage.RecoverReplicaCommand) error {
	assignment := cloneReplicaAssignment(cmd.Assignment)
	c.mu.Lock()
	c.entries = append(c.entries, OutboxEntry{
		NodeID:       c.nodeID,
		Slot:         cmd.Assignment.Slot,
		SlotVersion:  cmd.Assignment.ChainVersion,
		Kind:         OutboxCommandRecoverReplica,
		Assignment:   &assignment,
		SourceNodeID: cmd.SourceNodeID,
	})
	c.mu.Unlock()
	return nil
}

func (c *haPlanningNodeClient) DropRecoveredReplica(_ context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	c.mu.Lock()
	c.entries = append(c.entries, OutboxEntry{
		NodeID: c.nodeID,
		Slot:   cmd.Slot,
		Kind:   OutboxCommandDropRecovered,
	})
	c.mu.Unlock()
	return nil
}

func rewriteSnapshotEpoch(snapshot HASnapshot, epoch uint64) (HASnapshot, bool) {
	changed := false
	cloned := cloneHASnapshot(snapshot)
	for i := range cloned.Outbox {
		if cloned.Outbox[i].Epoch != epoch {
			cloned.Outbox[i].Epoch = epoch
			changed = true
		}
	}
	return cloned, changed
}

func (s *Server) plannerEntries(clients map[string]*haPlanningNodeClient, epoch uint64) []OutboxEntry {
	var entries []OutboxEntry
	for _, client := range clients {
		client.mu.Lock()
		clientEntries := append([]OutboxEntry(nil), client.entries...)
		client.mu.Unlock()
		for _, entry := range clientEntries {
			cloned := cloneOutboxEntry(entry)
			cloned.Epoch = epoch
			if cloned.CommandID == "" {
				cloned.CommandID = commandIDForOutbox(cloned.Kind, cloned.NodeID, cloned.Slot, epoch)
			}
			if cloned.ID == "" {
				cloned.ID = fmt.Sprintf("%s-%s", cloned.CommandID, cloned.Kind)
			}
			entries = append(entries, cloned)
		}
	}
	return entries
}

func appendOutbox(base []OutboxEntry, added []OutboxEntry) []OutboxEntry {
	out := make([]OutboxEntry, 0, len(base)+len(added))
	for _, entry := range base {
		out = append(out, cloneOutboxEntry(entry))
	}
	for _, entry := range added {
		out = append(out, cloneOutboxEntry(entry))
	}
	return out
}

func removeOutboxEntry(entries []OutboxEntry, id string) []OutboxEntry {
	out := make([]OutboxEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.ID == id {
			continue
		}
		out = append(out, cloneOutboxEntry(entry))
	}
	return out
}

func isRecoveryOutboxKind(kind OutboxCommandKind) bool {
	switch kind {
	case OutboxCommandResumeRecovered, OutboxCommandRecoverReplica, OutboxCommandDropRecovered:
		return true
	default:
		return false
	}
}

func (s *Server) applyHAWithPlanner(
	ctx context.Context,
	seedNodes []string,
	apply func(*Server) (coordruntime.State, error),
) (coordruntime.State, error) {
	if err := s.ensureLeader(ctx); err != nil {
		return coordruntime.State{}, err
	}
	snapshot, err := s.loadCurrentHASnapshot(ctx)
	if err != nil {
		return coordruntime.State{}, fmt.Errorf("err in s.loadCurrentHASnapshot: %w", err)
	}
	planner := s.newPlannerServer(snapshot)
	clients := plannerNodeClients(snapshot.State)
	for _, nodeID := range seedNodes {
		if nodeID == "" {
			continue
		}
		if _, ok := clients[nodeID]; !ok {
			clients[nodeID] = &haPlanningNodeClient{nodeID: nodeID}
		}
	}
	planner.nodes = plannerNodeMap(clients)
	state, err := apply(planner)
	if err != nil {
		return coordruntime.State{}, err
	}
	next := planner.currentHASnapshot()
	lease, _ := s.ha.currentLease()
	planned := s.plannerEntries(clients, lease.Epoch)
	next.Outbox = appendOutbox(snapshot.Outbox, planned)
	for slot, pending := range next.Pending {
		if pending.Epoch != 0 {
			continue
		}
		for _, entry := range planned {
			if entry.Slot != slot {
				continue
			}
			switch {
			case pending.Kind == pendingKindReady && entry.Kind == OutboxCommandAddReplicaAsTail:
				pending.Epoch = entry.Epoch
			case pending.Kind == pendingKindRemoved && entry.Kind == OutboxCommandMarkReplicaLeaving:
				pending.Epoch = entry.Epoch
			}
		}
		next.Pending[slot] = pending
	}
	for _, entry := range planned {
		if isRecoveryOutboxKind(entry.Kind) {
			slots := next.UnavailableReplicas[entry.NodeID]
			if slots == nil {
				slots = map[int]bool{}
				next.UnavailableReplicas[entry.NodeID] = slots
			}
			slots[entry.Slot] = true
		}
	}
	if err := s.saveHASnapshot(ctx, snapshot.SnapshotVersion, next); err != nil {
		return coordruntime.State{}, err
	}
	return state, nil
}

func (s *Server) dispatchOutbox(ctx context.Context) error {
	if s.ha == nil {
		return nil
	}
	if _, isLeader := s.ha.currentLease(); !isLeader {
		return nil
	}
	snapshot, err := s.loadCurrentHASnapshot(ctx)
	if err != nil {
		return fmt.Errorf("err in s.loadCurrentHASnapshot: %w", err)
	}
	for _, entry := range snapshot.Outbox {
		if err := s.dispatchOutboxEntry(ctx, entry); err != nil {
			return err
		}
		current, err := s.loadCurrentHASnapshot(ctx)
		if err != nil {
			return fmt.Errorf("err in s.loadCurrentHASnapshot(ack): %w", err)
		}
		next := cloneHASnapshot(current)
		next.Outbox = removeOutboxEntry(current.Outbox, entry.ID)
		if isRecoveryOutboxKind(entry.Kind) {
			s.clearUnavailable(entry.NodeID, entry.Slot)
			s.viewMu.RLock()
			next.UnavailableReplicas = make(map[string]map[int]bool, len(s.unavailableReplicas))
			for nodeID, slots := range s.unavailableReplicas {
				cloned := make(map[int]bool, len(slots))
				for slot, unavailable := range slots {
					cloned[slot] = unavailable
				}
				next.UnavailableReplicas[nodeID] = cloned
			}
			s.viewMu.RUnlock()
		}
		if err := s.saveHASnapshot(ctx, current.SnapshotVersion, next); err != nil {
			return err
		}
	}
	return nil
}

func (c *haController) setLease(lease LeaderLease, isLeader bool) {
	c.mu.Lock()
	c.lease = cloneLeaderLease(lease)
	c.isLeader = isLeader
	c.mu.Unlock()
}

func (c *haController) setIsLeader(isLeader bool) {
	c.mu.Lock()
	c.isLeader = isLeader
	c.mu.Unlock()
}

func (c *haController) currentLease() (LeaderLease, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneLeaderLease(c.lease), c.isLeader
}

func (s *Server) dispatchOutboxEntry(ctx context.Context, entry OutboxEntry) error {
	client, err := s.clientForNodeID(entry.NodeID)
	if err != nil {
		return err
	}
	switch entry.Kind {
	case OutboxCommandAddReplicaAsTail:
		return client.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
			Assignment: cloneReplicaAssignment(*entry.Assignment),
			Epoch:      entry.Epoch,
		})
	case OutboxCommandUpdateChainPeers:
		return client.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{
			Assignment: cloneReplicaAssignment(*entry.Assignment),
			Epoch:      entry.Epoch,
		})
	case OutboxCommandMarkReplicaLeaving:
		return client.MarkReplicaLeaving(ctx, storage.MarkReplicaLeavingCommand{
			Slot:  entry.Slot,
			Epoch: entry.Epoch,
		})
	case OutboxCommandResumeRecovered:
		return client.ResumeRecoveredReplica(ctx, storage.ResumeRecoveredReplicaCommand{
			Assignment: cloneReplicaAssignment(*entry.Assignment),
			Epoch:      entry.Epoch,
		})
	case OutboxCommandRecoverReplica:
		return client.RecoverReplica(ctx, storage.RecoverReplicaCommand{
			Assignment:   cloneReplicaAssignment(*entry.Assignment),
			SourceNodeID: entry.SourceNodeID,
			Epoch:        entry.Epoch,
		})
	case OutboxCommandDropRecovered:
		return client.DropRecoveredReplica(ctx, storage.DropRecoveredReplicaCommand{
			Slot:  entry.Slot,
			Epoch: entry.Epoch,
		})
	default:
		return ErrUnknownOutboxCommand
	}
}
