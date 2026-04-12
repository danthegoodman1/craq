package coordserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

func (s *Server) enableHA(ctx context.Context, cfg HAConfig) error {
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
	if _, err := s.StepHA(ctx); err != nil && !isNotLeader(err) {
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
			_, _ = s.StepHA(s.backgroundContext())
		}
	}
}

func (s *Server) StepHA(ctx context.Context) (bool, error) {
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		return submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (bool, error) {
			return s.StepHA(engineCtx)
		})
	}
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
	normalized := normalizeHASnapshot(snapshot)
	s.replaceRuntime(coordruntime.OpenInMemoryFromState(normalized.State))
	current := s.currentStateView()
	heartbeats := make(map[string]storage.NodeStatus, len(normalized.Heartbeats))
	for nodeID, status := range normalized.Heartbeats {
		heartbeats[nodeID] = cloneNodeStatus(status)
	}
	for nodeID, record := range current.NodeLivenessByID {
		if _, ok := heartbeats[nodeID]; ok || isZeroNodeStatus(record.LastStatus) {
			continue
		}
		heartbeats[nodeID] = cloneNodeStatus(record.LastStatus)
	}
	activePeerRefresh := make(map[int]activePeerRefreshState, len(normalized.ActivePeerRefresh))
	for slot, state := range normalized.ActivePeerRefresh {
		if isZeroHAActivePeerRefreshState(state) && !haOutboxHasActivePeerRefreshForSlot(normalized.Outbox, slot) {
			continue
		}
		activePeerRefresh[slot] = activePeerRefreshStateFromSnapshot(state)
	}
	s.activePeerRefreshMu.Lock()
	s.activePeerRefresh = activePeerRefresh
	s.activePeerRefreshMu.Unlock()
	s.viewMu.Lock()
	s.heartbeats = heartbeats
	s.liveness = mergeLivenessRecords(nil, current.NodeLivenessByID)
	s.pending = pendingFromHASnapshot(normalized)
	s.completed = make(map[int][]coordruntime.CompletedProgressRecord, len(current.CompletedProgressBySlot))
	for slot, records := range current.CompletedProgressBySlot {
		s.completed[slot] = append([]coordruntime.CompletedProgressRecord(nil), records...)
	}
	s.runtimeVersion = current.Version
	s.runtimeOutbox = cloneRuntimeOutbox(current.Outbox)
	s.lastPolicy = current.LastPolicy
	s.startupBudgetActive = normalized.StartupBudgetActive
	if normalized.SnapshotVersion == 0 && !isRuntimeInitialized(normalized.State) && s.startupMaxChangedChains > 0 {
		s.startupBudgetActive = true
	}
	s.unavailableReplicas = cloneUnavailableReplicasMap(normalized.UnavailableReplicas)
	s.lastRecoveryReports = make(map[string]storage.NodeRecoveryReport, len(normalized.LastRecoveryReports))
	for nodeID, report := range normalized.LastRecoveryReports {
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
	snapshot.PendingEpochBySlot = make(map[int]uint64, len(s.pending))
	for slot, pending := range s.pending {
		if pending.Epoch != 0 {
			snapshot.PendingEpochBySlot[slot] = pending.Epoch
		}
	}
	snapshot.Heartbeats = make(map[string]storage.NodeStatus, len(s.heartbeats))
	for nodeID, status := range s.heartbeats {
		snapshot.Heartbeats[nodeID] = cloneNodeStatus(status)
	}
	snapshot.ActivePeerRefresh = make(map[int]HAActivePeerRefreshState, len(s.activePeerRefresh))
	for slot, state := range s.activePeerRefresh {
		snapshot.ActivePeerRefresh[slot] = snapshotHAActivePeerRefreshState(state)
	}
	snapshot.StartupBudgetActive = s.startupBudgetActive
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

func (s *Server) readHASnapshot(ctx context.Context) (HASnapshot, error) {
	if s.ha == nil {
		return HASnapshot{}, fmt.Errorf("%w: ha is not enabled", ErrInvalidServerConfig)
	}
	snapshot, err := s.ha.cfg.Store.LoadSnapshot(ctx)
	if err != nil {
		return HASnapshot{}, err
	}
	return cloneHASnapshot(snapshot), nil
}

func (s *Server) loadCurrentHASnapshot(ctx context.Context) (HASnapshot, error) {
	snapshot, err := s.readHASnapshot(ctx)
	if err != nil {
		return HASnapshot{}, err
	}
	s.syncFromHASnapshot(snapshot)
	return snapshot, nil
}

func commandIDForOutbox(kind OutboxCommandKind, nodeID string, slot int, epoch uint64) string {
	return fmt.Sprintf("%s-%s-%d-e%d", kind, nodeID, slot, epoch)
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

func isZeroHAActivePeerRefreshState(state HAActivePeerRefreshState) bool {
	return !state.UseFallbackRoute &&
		!state.AllowWhilePending &&
		len(state.FallbackServingChain.Replicas) == 0 &&
		len(state.AssignmentChain.Replicas) == 0 &&
		len(state.RemainingNodeIDs) == 0
}

func haOutboxHasActivePeerRefreshForSlot(entries []OutboxEntry, slot int) bool {
	prefix := fmt.Sprintf("ha-peer-refresh-slot%d-", slot)
	for _, entry := range entries {
		if entry.Kind != OutboxCommandUpdateChainPeers {
			continue
		}
		if strings.HasPrefix(entry.ID, prefix) {
			return true
		}
	}
	return false
}

func (s *Server) haDispatchTimeout(kind OutboxCommandKind) time.Duration {
	if isRecoveryOutboxKind(kind) {
		return s.recoveryCommandTimeout
	}
	return s.dispatchTimeout
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
