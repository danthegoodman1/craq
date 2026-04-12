package coordserver

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func (s *Server) applyHABootstrap(ctx context.Context, cmd coordruntime.Command) (coordruntime.State, error) {
	state, _, err := s.applyHARuntimeCommand(ctx, cmd, func(snapshot HASnapshot, lease LeaderLease, evaluated coordruntime.EvaluatedCommand) (HASnapshot, error) {
		next := cloneHASnapshot(snapshot)
		next.State = evaluated.NextState
		next.Heartbeats = map[string]storage.NodeStatus{}
		next.ActivePeerRefresh = map[int]HAActivePeerRefreshState{}
		next.UnavailableReplicas = map[string]map[int]bool{}
		next.LastRecoveryReports = map[string]storage.NodeRecoveryReport{}
		next.Outbox = rebuildHAOutbox(next, lease.Epoch)
		next.PendingEpochBySlot = nextHAPendingEpochs(snapshot.PendingEpochBySlot, snapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
		next.StartupBudgetActive = initialHAStartupBudgetActive(snapshot, s.startupMaxChangedChains > 0, next.State)
		return next, nil
	})
	return state, err
}

func (s *Server) applyHAMembershipMutation(
	ctx context.Context,
	cmd coordruntime.Command,
	expectedEvent coordinator.EventKind,
) (coordruntime.State, error) {
	if cmd.Kind != coordruntime.CommandKindReconfigure || cmd.Reconfigure == nil {
		err := fmt.Errorf("%w: mutation requires reconfigure command payload", ErrInvalidServerCommand)
		s.observeCommandResult(string(expectedEvent), err)
		return coordruntime.State{}, err
	}
	if len(cmd.Reconfigure.Events) != 1 || cmd.Reconfigure.Events[0].Kind != expectedEvent {
		err := fmt.Errorf("%w: expected exactly one %q event", ErrInvalidServerCommand, expectedEvent)
		s.observeCommandResult(string(expectedEvent), err)
		return coordruntime.State{}, err
	}

	state, plan, err := s.applyHARuntimeCommand(ctx, cmd, func(snapshot HASnapshot, lease LeaderLease, evaluated coordruntime.EvaluatedCommand) (HASnapshot, error) {
		next := cloneHASnapshot(snapshot)
		next.State = evaluated.NextState
		next.Heartbeats = nextHAHeartbeats(snapshot.Heartbeats, cmd, next.State.NodeLivenessByID)
		next.ActivePeerRefresh = nextHAActivePeerRefreshFromPlan(snapshot.ActivePeerRefresh, evaluated.Plan)
		next.StartupBudgetActive = nextHAStartupBudgetActive(snapshot, s.startupMaxChangedChains > 0, next.State)
		next.Outbox = rebuildHAOutbox(next, lease.Epoch)
		next.PendingEpochBySlot = nextHAPendingEpochs(snapshot.PendingEpochBySlot, snapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
		return next, nil
	})
	if err != nil {
		s.observeCommandResult(string(expectedEvent), err)
		return coordruntime.State{}, err
	}
	_ = plan
	s.observeCommandResult(string(expectedEvent), nil)
	return state, nil
}

func (s *Server) applyHAReplicaReady(
	ctx context.Context,
	nodeID string,
	slot int,
	_ uint64,
	commandID string,
) (coordruntime.State, error) {
	for attempt := 0; attempt < 8; attempt++ {
		if err := s.ensureLeader(ctx); err != nil {
			return coordruntime.State{}, err
		}
		snapshot, err := s.readHASnapshot(ctx)
		if err != nil {
			return coordruntime.State{}, err
		}
		current := slotProgressViewFromState(snapshot.State, slot)
		reduction, err := reduceReplicaReadyProgress(current, nodeID, commandID, attempt)
		if err != nil {
			return coordruntime.State{}, err
		}
		if reduction.duplicateCompleted {
			return snapshot.State, nil
		}
		readyRefreshState := reduction.peerRefreshState
		if reduction.enqueuePeerRefresh &&
			!readyRefreshState.useFallbackRoute &&
			len(readyRefreshState.assignmentChain.Replicas) == 0 {
			fallback := activeServingChain(current.Chain)
			if len(fallback.Replicas) > 0 {
				readyRefreshState = activePeerRefreshState{
					fallbackServingChain: fallback,
					useFallbackRoute:     true,
				}
			}
		}
		cmd := coordruntime.Command{
			ID:              reduction.progressCommandID,
			ExpectedVersion: snapshot.State.Version,
			Kind:            coordruntime.CommandKindProgress,
			Progress: &coordruntime.ProgressCommand{
				Event: coordinator.Event{
					Kind:   coordinator.EventKindReplicaBecameActive,
					NodeID: nodeID,
					Slot:   slot,
				},
			},
		}
		state, _, err := s.applyHARuntimeCommand(ctx, cmd, func(currentSnapshot HASnapshot, lease LeaderLease, evaluated coordruntime.EvaluatedCommand) (HASnapshot, error) {
			next := cloneHASnapshot(currentSnapshot)
			next.State = evaluated.NextState
			next.Heartbeats = nextHAHeartbeats(currentSnapshot.Heartbeats, cmd, next.State.NodeLivenessByID)
			next.ActivePeerRefresh = cloneHAActivePeerRefreshMap(currentSnapshot.ActivePeerRefresh)
			if reduction.enqueuePeerRefresh {
				next.ActivePeerRefresh[slot] = snapshotHAActivePeerRefreshState(readyRefreshState)
			}
			next.StartupBudgetActive = nextHAStartupBudgetActive(currentSnapshot, s.startupMaxChangedChains > 0, next.State)
			next.Outbox = rebuildHAOutbox(next, lease.Epoch)
			next.PendingEpochBySlot = nextHAPendingEpochs(currentSnapshot.PendingEpochBySlot, currentSnapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
			return next, nil
		})
		if err == nil {
			if err := s.applyHAReconcile(ctx); err != nil {
				if errors.Is(err, coordruntime.ErrVersionMismatch) || errors.Is(err, ErrHASnapshotConflict) {
					continue
				}
				return coordruntime.State{}, err
			}
			return s.currentState(), nil
		}
		if errors.Is(err, coordruntime.ErrVersionMismatch) || errors.Is(err, ErrHASnapshotConflict) {
			continue
		}
		return state, err
	}
	return coordruntime.State{}, coordruntime.ErrVersionMismatch
}

func (s *Server) applyHAReplicaRemoved(
	ctx context.Context,
	nodeID string,
	slot int,
	_ uint64,
	commandID string,
) (coordruntime.State, error) {
	for attempt := 0; attempt < 8; attempt++ {
		if err := s.ensureLeader(ctx); err != nil {
			return coordruntime.State{}, err
		}
		snapshot, err := s.readHASnapshot(ctx)
		if err != nil {
			return coordruntime.State{}, err
		}
		current := slotProgressViewFromState(snapshot.State, slot)
		reduction, err := reduceReplicaRemovedProgress(current, nodeID, commandID, attempt)
		if err != nil {
			return coordruntime.State{}, err
		}
		if reduction.duplicateCompleted {
			return snapshot.State, nil
		}
		cmd := coordruntime.Command{
			ID:              reduction.progressCommandID,
			ExpectedVersion: snapshot.State.Version,
			Kind:            coordruntime.CommandKindProgress,
			Progress: &coordruntime.ProgressCommand{
				Event: coordinator.Event{
					Kind:   coordinator.EventKindReplicaRemoved,
					NodeID: nodeID,
					Slot:   slot,
				},
			},
		}
		state, _, err := s.applyHARuntimeCommand(ctx, cmd, func(currentSnapshot HASnapshot, lease LeaderLease, evaluated coordruntime.EvaluatedCommand) (HASnapshot, error) {
			next := cloneHASnapshot(currentSnapshot)
			next.State = evaluated.NextState
			next.Heartbeats = nextHAHeartbeats(currentSnapshot.Heartbeats, cmd, next.State.NodeLivenessByID)
			next.ActivePeerRefresh = cloneHAActivePeerRefreshMap(currentSnapshot.ActivePeerRefresh)
			next.ActivePeerRefresh[slot] = snapshotHAActivePeerRefreshState(activePeerRefreshState{})
			next.StartupBudgetActive = nextHAStartupBudgetActive(currentSnapshot, s.startupMaxChangedChains > 0, next.State)
			next.Outbox = rebuildHAOutbox(next, lease.Epoch)
			next.PendingEpochBySlot = nextHAPendingEpochs(currentSnapshot.PendingEpochBySlot, currentSnapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
			return next, nil
		})
		if err == nil {
			if err := s.applyHAReconcile(ctx); err != nil {
				if errors.Is(err, coordruntime.ErrVersionMismatch) || errors.Is(err, ErrHASnapshotConflict) {
					continue
				}
				return coordruntime.State{}, err
			}
			if !s.asyncHotPathDispatch {
				if err := s.dispatchHAOutboxEntries(ctx, func(entry OutboxEntry) bool {
					return entry.Slot == slot && entry.Kind == OutboxCommandUpdateChainPeers
				}); err != nil {
					return coordruntime.State{}, err
				}
			}
			return s.currentState(), nil
		}
		if errors.Is(err, coordruntime.ErrVersionMismatch) || errors.Is(err, ErrHASnapshotConflict) {
			continue
		}
		return state, err
	}
	return coordruntime.State{}, coordruntime.ErrVersionMismatch
}

func (s *Server) applyHAHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	if err := s.ensureLeader(ctx); err != nil {
		return err
	}
	snapshot, err := s.readHASnapshot(ctx)
	if err != nil {
		return err
	}
	if _, ok := snapshot.State.Cluster.NodesByID[status.NodeID]; !ok {
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	if nodeMarkedDead(snapshot.State.Cluster, status.NodeID) {
		return fmt.Errorf("%w: %q", ErrUnknownNode, status.NodeID)
	}
	observedAt := s.clock.Now().UnixNano()
	next := cloneHASnapshot(snapshot)
	if next.Heartbeats == nil {
		next.Heartbeats = map[string]storage.NodeStatus{}
	}
	next.Heartbeats[status.NodeID] = cloneNodeStatus(status)
	record := next.State.NodeLivenessByID[status.NodeID]
	record.LastHeartbeatUnixNano = observedAt
	record.UpdatedAtUnixNano = observedAt
	record.LastStatus = cloneNodeStatus(status)
	record.SuspectTransitionsUnixNano = pruneLivenessTransitions(
		record.SuspectTransitionsUnixNano,
		observedAt,
		s.livenessPolicy.FlapWindow.Nanoseconds(),
	)
	if record.State != coordruntime.NodeLivenessStateDead {
		record.State = coordruntime.NodeLivenessStateHealthy
		record.DeadActionFired = false
	}
	next.State.NodeLivenessByID[status.NodeID] = record
	if next.State.Cluster.ReadyNodeIDs == nil {
		next.State.Cluster.ReadyNodeIDs = map[string]bool{}
	}
	becameReady := !next.State.Cluster.ReadyNodeIDs[status.NodeID]
	if becameReady {
		next.State.Cluster.ReadyNodeIDs[status.NodeID] = true
	}
	next.StartupBudgetActive = nextHAStartupBudgetActive(snapshot, s.startupMaxChangedChains > 0, next.State)
	lease, _ := s.ha.currentLease()
	next.Outbox = rebuildHAOutbox(next, lease.Epoch)
	next.PendingEpochBySlot = nextHAPendingEpochs(snapshot.PendingEpochBySlot, snapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
	if err := s.saveHASnapshot(ctx, snapshot.SnapshotVersion, next); err != nil {
		return err
	}
	if becameReady {
		if err := s.applyHAReconcile(ctx); err != nil {
			return err
		}
		if !s.asyncHotPathDispatch {
			if err := s.dispatchOutbox(ctx); err != nil {
				if errors.Is(err, ErrUnknownNode) || errors.Is(err, ErrDispatchFailed) || errors.Is(err, ErrDispatchTimeout) {
					s.logger.Warn().Err(err).Str("component", "coordserver").Str("node_id", status.NodeID).Msg("ha heartbeat triggered durable repair work that will retry later")
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (s *Server) applyHALivenessTransition(
	ctx context.Context,
	nodeID string,
	state coordruntime.NodeLivenessState,
	evaluatedAtUnixNano int64,
	deadActionFired bool,
) (coordruntime.NodeLivenessRecord, error) {
	snapshot, err := s.readHASnapshot(ctx)
	if err != nil {
		return coordruntime.NodeLivenessRecord{}, err
	}
	cmd := coordruntime.Command{
		ID:              fmt.Sprintf("server-liveness-%s-%s-%d-%t-v%d", nodeID, state, evaluatedAtUnixNano, deadActionFired, snapshot.State.Version),
		ExpectedVersion: snapshot.State.Version,
		Kind:            coordruntime.CommandKindLiveness,
		Liveness: &coordruntime.LivenessCommand{
			NodeID:              nodeID,
			State:               state,
			EvaluatedAtUnixNano: evaluatedAtUnixNano,
			DeadActionFired:     deadActionFired,
			FlapWindowNanos:     s.livenessPolicy.FlapWindow.Nanoseconds(),
		},
	}
	nextState, _, err := s.applyHARuntimeCommand(ctx, cmd, func(currentSnapshot HASnapshot, lease LeaderLease, evaluated coordruntime.EvaluatedCommand) (HASnapshot, error) {
		next := cloneHASnapshot(currentSnapshot)
		next.State = evaluated.NextState
		next.Heartbeats = nextHAHeartbeats(currentSnapshot.Heartbeats, cmd, next.State.NodeLivenessByID)
		next.StartupBudgetActive = nextHAStartupBudgetActive(currentSnapshot, s.startupMaxChangedChains > 0, next.State)
		next.Outbox = rebuildHAOutbox(next, lease.Epoch)
		next.PendingEpochBySlot = nextHAPendingEpochs(currentSnapshot.PendingEpochBySlot, currentSnapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
		return next, nil
	})
	if err != nil {
		return coordruntime.NodeLivenessRecord{}, err
	}
	record, ok := nextState.NodeLivenessByID[nodeID]
	if !ok {
		return coordruntime.NodeLivenessRecord{}, nil
	}
	return cloneLivenessRecord(record), nil
}

func (s *Server) applyHARecoveryReport(ctx context.Context, report storage.NodeRecoveryReport) error {
	if err := s.ensureLeader(ctx); err != nil {
		return err
	}
	snapshot, err := s.readHASnapshot(ctx)
	if err != nil {
		return err
	}
	if _, ok := snapshot.State.Cluster.NodesByID[report.NodeID]; !ok {
		s.nodesMu.RLock()
		_, fallbackOK := s.nodes[report.NodeID]
		s.nodesMu.RUnlock()
		if !fallbackOK {
			return fmt.Errorf("%w: %q", ErrUnknownNode, report.NodeID)
		}
	}
	if prior, ok := snapshot.LastRecoveryReports[report.NodeID]; ok &&
		reflect.DeepEqual(prior, report) &&
		!haSnapshotHasUnavailableSlots(snapshot, report.NodeID) {
		return nil
	}
	lease, _ := s.ha.currentLease()
	next := cloneHASnapshot(snapshot)
	next.LastRecoveryReports[report.NodeID] = cloneRecoveryReport(report)
	markUnavailableReplicasInSnapshot(&next, report)
	recoveryEntries, err := s.recoveryOutboxEntriesForReport(snapshot.State, report, lease.Epoch)
	if err != nil {
		return err
	}
	next.Outbox = replaceHARecoveryOutboxEntries(snapshot.Outbox, recoveryEntries)
	next.Outbox = rebuildHAOutbox(next, lease.Epoch)
	next.PendingEpochBySlot = nextHAPendingEpochs(snapshot.PendingEpochBySlot, snapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
	return s.saveHASnapshot(ctx, snapshot.SnapshotVersion, next)
}

func (s *Server) applyHARegisterNode(ctx context.Context, reg storage.NodeRegistration) (coordruntime.State, error) {
	if err := s.ensureLeader(ctx); err != nil {
		return coordruntime.State{}, err
	}
	for attempt := 0; attempt < runtimeVersionRetryLimit; attempt++ {
		snapshot, err := s.readHASnapshot(ctx)
		if err != nil {
			return coordruntime.State{}, err
		}
		if existing, ok := snapshot.State.Cluster.NodesByID[reg.NodeID]; ok &&
			snapshot.State.Cluster.NodeHealthByID[reg.NodeID] != coordinator.NodeHealthDead &&
			existing.RPCAddress == reg.RPCAddress &&
			reflect.DeepEqual(existing.FailureDomains, reg.FailureDomains) {
			return snapshot.State, nil
		}
		state, err := s.applyHAMembershipMutation(ctx, coordruntime.Command{
			ID:              fmt.Sprintf("server-register-%s-r%d-v%d", reg.NodeID, attempt, snapshot.State.Version),
			ExpectedVersion: snapshot.State.Version,
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
		if err == nil {
			return state, nil
		}
		if errors.Is(err, coordruntime.ErrVersionMismatch) || errors.Is(err, ErrHASnapshotConflict) {
			continue
		}
		return coordruntime.State{}, err
	}
	return coordruntime.State{}, coordruntime.ErrVersionMismatch
}

func (s *Server) applyHAReconcile(ctx context.Context) error {
	snapshot, err := s.readHASnapshot(ctx)
	if err != nil {
		return err
	}
	policy := s.reconfigurationPolicy()
	cmd := coordruntime.Command{
		ID:              fmt.Sprintf("server-reconcile-v%d", snapshot.State.Version),
		ExpectedVersion: snapshot.State.Version,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: nil,
			Policy: policy,
		},
	}
	_, _, err = s.applyHARuntimeCommand(ctx, cmd, func(currentSnapshot HASnapshot, lease LeaderLease, evaluated coordruntime.EvaluatedCommand) (HASnapshot, error) {
		next := cloneHASnapshot(currentSnapshot)
		next.State = evaluated.NextState
		next.Heartbeats = nextHAHeartbeats(currentSnapshot.Heartbeats, cmd, next.State.NodeLivenessByID)
		next.ActivePeerRefresh = cloneHAActivePeerRefreshMap(currentSnapshot.ActivePeerRefresh)
		next.StartupBudgetActive = nextHAStartupBudgetActive(currentSnapshot, s.startupMaxChangedChains > 0, next.State)
		next.Outbox = rebuildHAOutbox(next, lease.Epoch)
		next.PendingEpochBySlot = nextHAPendingEpochs(currentSnapshot.PendingEpochBySlot, currentSnapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
		return next, nil
	})
	return err
}

func (s *Server) applyHARuntimeCommand(
	ctx context.Context,
	cmd coordruntime.Command,
	mutate func(HASnapshot, LeaderLease, coordruntime.EvaluatedCommand) (HASnapshot, error),
) (coordruntime.State, *coordinator.ReconfigurationPlan, error) {
	if err := s.ensureLeader(ctx); err != nil {
		return coordruntime.State{}, nil, err
	}
	snapshot, err := s.readHASnapshot(ctx)
	if err != nil {
		return coordruntime.State{}, nil, err
	}
	evaluated, err := coordruntime.EvaluateCommand(snapshot.State, cmd)
	if err != nil {
		return coordruntime.State{}, nil, err
	}
	lease, _ := s.ha.currentLease()
	next := cloneHASnapshot(snapshot)
	next.State = evaluated.NextState
	next.Heartbeats = nextHAHeartbeats(snapshot.Heartbeats, cmd, next.State.NodeLivenessByID)
	next.StartupBudgetActive = nextHAStartupBudgetActive(snapshot, s.startupMaxChangedChains > 0, next.State)
	if mutate != nil {
		next, err = mutate(next, lease, evaluated)
		if err != nil {
			return coordruntime.State{}, nil, err
		}
	}
	next.Outbox = rebuildHAOutbox(next, lease.Epoch)
	next.PendingEpochBySlot = nextHAPendingEpochs(snapshot.PendingEpochBySlot, snapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
	if err := s.saveHASnapshot(ctx, snapshot.SnapshotVersion, next); err != nil {
		return coordruntime.State{}, nil, err
	}
	return next.State, evaluated.Plan, nil
}

func slotProgressViewFromState(state coordruntime.State, slot int) coordruntime.SlotProgressView {
	view := coordruntime.SlotProgressView{
		Version:           state.Version,
		ReplicationFactor: state.Cluster.ReplicationFactor,
		SlotVersion:       state.SlotVersions[slot],
		Completed:         append([]coordruntime.CompletedProgressRecord(nil), state.CompletedProgressBySlot[slot]...),
	}
	if pending, ok := state.PendingBySlot[slot]; ok {
		cloned := pending
		view.Pending = &cloned
	}
	if slot >= 0 && slot < len(state.Cluster.Chains) {
		view.Chain = cloneCoordinatorChain(state.Cluster.Chains[slot])
	}
	return view
}

func nextHAHeartbeats(
	current map[string]storage.NodeStatus,
	cmd coordruntime.Command,
	liveness map[string]coordruntime.NodeLivenessRecord,
) map[string]storage.NodeStatus {
	next := make(map[string]storage.NodeStatus, len(current))
	for nodeID, status := range current {
		next[nodeID] = cloneNodeStatus(status)
	}
	switch {
	case cmd.NodeReady != nil:
		next[cmd.NodeReady.Status.NodeID] = cloneNodeStatus(cmd.NodeReady.Status)
	case cmd.Heartbeat != nil:
		next[cmd.Heartbeat.Status.NodeID] = cloneNodeStatus(cmd.Heartbeat.Status)
	}
	for nodeID, record := range liveness {
		if isZeroNodeStatus(record.LastStatus) {
			continue
		}
		if _, ok := next[nodeID]; !ok {
			next[nodeID] = cloneNodeStatus(record.LastStatus)
		}
	}
	return next
}

func initialHAStartupBudgetActive(snapshot HASnapshot, defaultActive bool, state coordruntime.State) bool {
	active := snapshot.StartupBudgetActive
	if snapshot.SnapshotVersion == 0 && !isRuntimeInitialized(snapshot.State) {
		active = defaultActive
	}
	if clusterFullySettledForStartup(state.Cluster) {
		return false
	}
	return active
}

func nextHAStartupBudgetActive(snapshot HASnapshot, defaultActive bool, state coordruntime.State) bool {
	return initialHAStartupBudgetActive(snapshot, defaultActive, state)
}

func nextHAActivePeerRefreshFromPlan(
	current map[int]HAActivePeerRefreshState,
	plan *coordinator.ReconfigurationPlan,
) map[int]HAActivePeerRefreshState {
	next := cloneHAActivePeerRefreshMap(current)
	if plan == nil {
		return next
	}
	for _, slotPlan := range plan.ChangedSlots {
		refresh := activePeerRefreshState{}
		if slotPlanHasOnlyStepKind(slotPlan, coordinator.StepKindAppendTail) {
			refresh = activePeerRefreshState{
				assignmentChain:   cloneCoordinatorChain(slotPlan.After),
				allowWhilePending: true,
			}
		}
		next[slotPlan.Slot] = snapshotHAActivePeerRefreshState(refresh)
	}
	return next
}

func nextHAPendingEpochs(
	current map[int]uint64,
	currentPending map[int]coordruntime.PendingWork,
	nextPending map[int]coordruntime.PendingWork,
	outbox []OutboxEntry,
) map[int]uint64 {
	epochs := make(map[int]uint64, len(nextPending))
	for slot, pending := range nextPending {
		if existing, ok := currentPending[slot]; ok && existing == pending {
			if epoch, ok := current[slot]; ok {
				epochs[slot] = epoch
				continue
			}
		}
		if epoch, ok := pendingEpochFromHAOutbox(slot, pending, outbox); ok {
			epochs[slot] = epoch
			continue
		}
		if epoch, ok := current[slot]; ok {
			epochs[slot] = epoch
		}
	}
	return epochs
}

func pendingEpochFromHAOutbox(slot int, pending coordruntime.PendingWork, outbox []OutboxEntry) (uint64, bool) {
	for _, entry := range outbox {
		if entry.Slot != slot || entry.NodeID != pending.NodeID {
			continue
		}
		switch {
		case pending.Kind == coordruntime.PendingKindReady && entry.Kind == OutboxCommandAddReplicaAsTail:
			return entry.Epoch, true
		case pending.Kind == coordruntime.PendingKindRemoved && entry.Kind == OutboxCommandMarkReplicaLeaving:
			return entry.Epoch, true
		}
	}
	return 0, false
}

func rebuildHAOutbox(snapshot HASnapshot, epoch uint64) []OutboxEntry {
	outbox := filterRecoveryOutboxEntries(snapshot.Outbox)
	outbox = append(outbox, runtimeHAOutboxEntries(snapshot.State.Outbox, epoch)...)
	outbox = append(outbox, activePeerRefreshOutboxEntries(snapshot.State, snapshot.ActivePeerRefresh, epoch)...)
	sortHAOutbox(outbox)
	return outbox
}

func runtimeHAOutboxEntries(entries []coordruntime.OutboxEntry, epoch uint64) []OutboxEntry {
	out := make([]OutboxEntry, 0, len(entries))
	for _, entry := range entries {
		assignment := cloneReplicaAssignment(entry.Assignment)
		out = append(out, OutboxEntry{
			ID:          entry.ID,
			Epoch:       epoch,
			NodeID:      entry.NodeID,
			Slot:        entry.Slot,
			SlotVersion: entry.Assignment.ChainVersion,
			CommandID:   entry.CommandID,
			Kind:        haOutboxKindFromRuntime(entry.Kind),
			Assignment:  &assignment,
		})
	}
	return out
}

func haOutboxKindFromRuntime(kind coordruntime.OutboxCommandKind) OutboxCommandKind {
	switch kind {
	case coordruntime.OutboxCommandKindAddReplicaAsTail:
		return OutboxCommandAddReplicaAsTail
	case coordruntime.OutboxCommandKindUpdateChainPeers:
		return OutboxCommandUpdateChainPeers
	case coordruntime.OutboxCommandKindMarkReplicaLeaving:
		return OutboxCommandMarkReplicaLeaving
	default:
		return OutboxCommandKind(kind)
	}
}

func activePeerRefreshOutboxEntries(
	state coordruntime.State,
	persisted map[int]HAActivePeerRefreshState,
	epoch uint64,
) []OutboxEntry {
	if len(persisted) == 0 {
		return nil
	}
	out := make([]OutboxEntry, 0, len(persisted))
	slots := make([]int, 0, len(persisted))
	for slot := range persisted {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	for _, slot := range slots {
		refresh := activePeerRefreshStateFromSnapshot(persisted[slot])
		if !shouldDispatchActivePeerRefreshForState(state, slot, refresh) {
			continue
		}
		initialized, ok := refreshStateWithDefaultRemainingNodes(state, slot, refresh)
		if !ok {
			continue
		}
		chain, ok := chainForActivePeerRefresh(state, slot, initialized)
		if !ok {
			continue
		}
		nodeIDs := make([]string, 0, len(initialized.remainingNodeIDs))
		for nodeID, remaining := range initialized.remainingNodeIDs {
			if remaining {
				nodeIDs = append(nodeIDs, nodeID)
			}
		}
		sort.Strings(nodeIDs)
		for _, nodeID := range nodeIDs {
			assignment, err := assignmentForNode(chain, state.Cluster.NodesByID, nodeID, state.SlotVersions[slot])
			if err != nil {
				continue
			}
			assignmentCopy := cloneReplicaAssignment(assignment)
			out = append(out, OutboxEntry{
				ID:          activePeerRefreshOutboxID(slot, nodeID),
				Epoch:       epoch,
				NodeID:      nodeID,
				Slot:        slot,
				SlotVersion: assignment.ChainVersion,
				CommandID:   commandIDForOutbox(OutboxCommandUpdateChainPeers, nodeID, slot, epoch),
				Kind:        OutboxCommandUpdateChainPeers,
				Assignment:  &assignmentCopy,
			})
		}
	}
	return out
}

func shouldDispatchActivePeerRefreshForState(
	state coordruntime.State,
	slot int,
	refresh activePeerRefreshState,
) bool {
	if refresh.allowWhilePending {
		return true
	}
	if _, pending := state.PendingBySlot[slot]; pending {
		return false
	}
	return !runtimeOutboxHasSlot(state.Outbox, slot)
}

func refreshStateWithDefaultRemainingNodes(
	state coordruntime.State,
	slot int,
	refresh activePeerRefreshState,
) (activePeerRefreshState, bool) {
	next := cloneActivePeerRefreshState(refresh)
	if len(next.remainingNodeIDs) > 0 {
		return next, true
	}
	chain, ok := chainForActivePeerRefresh(state, slot, next)
	if !ok {
		return activePeerRefreshState{}, false
	}
	next.remainingNodeIDs = make(map[string]bool)
	for _, replica := range chain.Replicas {
		if replica.State == coordinator.ReplicaStateActive {
			next.remainingNodeIDs[replica.NodeID] = true
		}
	}
	return next, true
}

func chainForActivePeerRefresh(
	state coordruntime.State,
	slot int,
	refresh activePeerRefreshState,
) (coordinator.Chain, bool) {
	if slot < 0 || slot >= len(state.Cluster.Chains) {
		return coordinator.Chain{}, false
	}
	chain := activeServingChain(state.Cluster.Chains[slot])
	if len(refresh.assignmentChain.Replicas) > 0 && refresh.assignmentChain.Slot == slot {
		return cloneCoordinatorChain(refresh.assignmentChain), true
	}
	return chain, true
}

func activePeerRefreshOutboxID(slot int, nodeID string) string {
	return fmt.Sprintf("ha-peer-refresh-slot%d-%s", slot, nodeID)
}

func filterRecoveryOutboxEntries(entries []OutboxEntry) []OutboxEntry {
	out := make([]OutboxEntry, 0, len(entries))
	for _, entry := range entries {
		if !isRecoveryOutboxKind(entry.Kind) {
			continue
		}
		out = append(out, cloneOutboxEntry(entry))
	}
	return out
}

func sortHAOutbox(entries []OutboxEntry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Slot != entries[j].Slot {
			return entries[i].Slot < entries[j].Slot
		}
		if entries[i].Kind != entries[j].Kind {
			return entries[i].Kind < entries[j].Kind
		}
		if entries[i].NodeID != entries[j].NodeID {
			return entries[i].NodeID < entries[j].NodeID
		}
		return entries[i].ID < entries[j].ID
	})
}

func snapshotHAActivePeerRefreshState(state activePeerRefreshState) HAActivePeerRefreshState {
	return HAActivePeerRefreshState{
		FallbackServingChain: cloneCoordinatorChain(state.fallbackServingChain),
		AssignmentChain:      cloneCoordinatorChain(state.assignmentChain),
		UseFallbackRoute:     state.useFallbackRoute,
		AllowWhilePending:    state.allowWhilePending,
		RemainingNodeIDs:     cloneRemainingNodeIDs(state.remainingNodeIDs),
	}
}

func activePeerRefreshStateFromSnapshot(state HAActivePeerRefreshState) activePeerRefreshState {
	return activePeerRefreshState{
		fallbackServingChain: cloneCoordinatorChain(state.FallbackServingChain),
		assignmentChain:      cloneCoordinatorChain(state.AssignmentChain),
		useFallbackRoute:     state.UseFallbackRoute,
		allowWhilePending:    state.AllowWhilePending,
		remainingNodeIDs:     cloneRemainingNodeIDs(state.RemainingNodeIDs),
	}
}

func cloneHAActivePeerRefreshMap(current map[int]HAActivePeerRefreshState) map[int]HAActivePeerRefreshState {
	cloned := make(map[int]HAActivePeerRefreshState, len(current))
	for slot, state := range current {
		cloned[slot] = cloneHAActivePeerRefreshState(state)
	}
	return cloned
}

func cloneRemainingNodeIDs(current map[string]bool) map[string]bool {
	if len(current) == 0 {
		return nil
	}
	cloned := make(map[string]bool, len(current))
	for nodeID, value := range current {
		cloned[nodeID] = value
	}
	return cloned
}

func pendingFromHASnapshot(snapshot HASnapshot) map[int]PendingWork {
	pending := make(map[int]PendingWork, len(snapshot.State.PendingBySlot))
	for slot, runtimePending := range snapshot.State.PendingBySlot {
		pending[slot] = PendingWork{
			Slot:        runtimePending.Slot,
			NodeID:      runtimePending.NodeID,
			Kind:        pendingKind(runtimePending.Kind),
			SlotVersion: runtimePending.SlotVersion,
			Epoch:       snapshot.PendingEpochBySlot[slot],
			CommandID:   runtimePending.CommandID,
		}
	}
	return pending
}

func markUnavailableReplicasInSnapshot(snapshot *HASnapshot, report storage.NodeRecoveryReport) {
	if snapshot.UnavailableReplicas == nil {
		snapshot.UnavailableReplicas = map[string]map[int]bool{}
	}
	slots := snapshot.UnavailableReplicas[report.NodeID]
	if slots == nil {
		slots = map[int]bool{}
		snapshot.UnavailableReplicas[report.NodeID] = slots
	}
	for _, replica := range report.Replicas {
		slots[replica.Assignment.Slot] = true
	}
}

func clearUnavailableInHASnapshot(snapshot *HASnapshot, nodeID string, slot int) {
	slots, ok := snapshot.UnavailableReplicas[nodeID]
	if !ok {
		return
	}
	delete(slots, slot)
	if len(slots) == 0 {
		delete(snapshot.UnavailableReplicas, nodeID)
	}
}

func haSnapshotHasUnavailableSlots(snapshot HASnapshot, nodeID string) bool {
	slots, ok := snapshot.UnavailableReplicas[nodeID]
	return ok && len(slots) > 0
}

func replaceHARecoveryOutboxEntries(base []OutboxEntry, added []OutboxEntry) []OutboxEntry {
	out := make([]OutboxEntry, 0, len(base)+len(added))
	replaceKey := make(map[string]struct{}, len(added))
	for _, entry := range added {
		replaceKey[haRecoveryOutboxReplaceKey(entry)] = struct{}{}
	}
	for _, entry := range base {
		if !isRecoveryOutboxKind(entry.Kind) {
			continue
		}
		if _, ok := replaceKey[haRecoveryOutboxReplaceKey(entry)]; ok {
			continue
		}
		out = append(out, cloneOutboxEntry(entry))
	}
	for _, entry := range added {
		out = append(out, cloneOutboxEntry(entry))
	}
	return out
}

func haRecoveryOutboxReplaceKey(entry OutboxEntry) string {
	return fmt.Sprintf("%s-%d", entry.NodeID, entry.Slot)
}

func recoveryOutboxID(kind OutboxCommandKind, nodeID string, slot int) string {
	return fmt.Sprintf("ha-recovery-%s-%s-%d", kind, nodeID, slot)
}

func (s *Server) recoveryOutboxEntriesForReport(
	state coordruntime.State,
	report storage.NodeRecoveryReport,
	epoch uint64,
) ([]OutboxEntry, error) {
	entries := make([]OutboxEntry, 0, len(report.Replicas))
	for _, recovered := range report.Replicas {
		currentAssignment, ok := currentAssignmentForNode(state, report.NodeID, recovered.Assignment.Slot)
		switch {
		case !ok:
			entries = append(entries, OutboxEntry{
				ID:        recoveryOutboxID(OutboxCommandDropRecovered, report.NodeID, recovered.Assignment.Slot),
				Epoch:     epoch,
				NodeID:    report.NodeID,
				Slot:      recovered.Assignment.Slot,
				CommandID: commandIDForOutbox(OutboxCommandDropRecovered, report.NodeID, recovered.Assignment.Slot, epoch),
				Kind:      OutboxCommandDropRecovered,
			})
		case canResumeRecoveredReplica(recovered, currentAssignment):
			assignment := cloneReplicaAssignment(currentAssignment)
			entries = append(entries, OutboxEntry{
				ID:          recoveryOutboxID(OutboxCommandResumeRecovered, report.NodeID, recovered.Assignment.Slot),
				Epoch:       epoch,
				NodeID:      report.NodeID,
				Slot:        recovered.Assignment.Slot,
				SlotVersion: currentAssignment.ChainVersion,
				CommandID:   commandIDForOutbox(OutboxCommandResumeRecovered, report.NodeID, recovered.Assignment.Slot, epoch),
				Kind:        OutboxCommandResumeRecovered,
				Assignment:  &assignment,
			})
		default:
			sourceNodeID, ok := recoverySourceNodeID(state.Cluster.Chains[recovered.Assignment.Slot], report.NodeID)
			if !ok {
				return nil, fmt.Errorf("%w: slot %d node %q has no valid recovery source", ErrRecoveryFailed, recovered.Assignment.Slot, report.NodeID)
			}
			assignment := cloneReplicaAssignment(currentAssignment)
			entries = append(entries, OutboxEntry{
				ID:           recoveryOutboxID(OutboxCommandRecoverReplica, report.NodeID, recovered.Assignment.Slot),
				Epoch:        epoch,
				NodeID:       report.NodeID,
				Slot:         recovered.Assignment.Slot,
				SlotVersion:  currentAssignment.ChainVersion,
				CommandID:    commandIDForOutbox(OutboxCommandRecoverReplica, report.NodeID, recovered.Assignment.Slot, epoch),
				Kind:         OutboxCommandRecoverReplica,
				Assignment:   &assignment,
				SourceNodeID: sourceNodeID,
			})
		}
	}
	return entries, nil
}

func (s *Server) dispatchOutbox(ctx context.Context) error {
	if s.dispatchEngine != nil && !contextInCoordinatorEngine(ctx) {
		_, err := submitEngineCall(ctx, s.dispatchEngine, func(engineCtx context.Context) (struct{}, error) {
			return struct{}{}, s.dispatchOutbox(engineCtx)
		})
		return err
	}
	if s.ha == nil {
		return nil
	}
	if _, isLeader := s.ha.currentLease(); !isLeader {
		return nil
	}
	return s.dispatchHAOutboxEntries(ctx, nil)
}

func (s *Server) ackHAOutboxEntry(snapshot HASnapshot, entry OutboxEntry) (HASnapshot, error) {
	next := cloneHASnapshot(snapshot)
	switch {
	case isRecoveryOutboxKind(entry.Kind):
		next.Outbox = removeOutboxEntry(next.Outbox, entry.ID)
		clearUnavailableInHASnapshot(&next, entry.NodeID, entry.Slot)
		lease, _ := s.ha.currentLease()
		next.Outbox = rebuildHAOutbox(next, lease.Epoch)
		return next, nil
	case runtimeOutboxHasEntryID(snapshot.State.Outbox, entry.ID):
		evaluated, err := coordruntime.EvaluateCommand(snapshot.State, coordruntime.Command{
			ID:              outboxAckCommandID(snapshot.State.Version, []string{entry.ID}),
			ExpectedVersion: snapshot.State.Version,
			Kind:            coordruntime.CommandKindAcknowledgeOutbox,
			AcknowledgeOutbox: &coordruntime.AcknowledgeOutboxCommand{
				EntryIDs: []string{entry.ID},
			},
		})
		if err != nil {
			return HASnapshot{}, err
		}
		next.State = evaluated.NextState
	case entry.Kind == OutboxCommandUpdateChainPeers:
		refresh, ok := next.ActivePeerRefresh[entry.Slot]
		if !ok {
			return next, nil
		}
		active := activePeerRefreshStateFromSnapshot(refresh)
		active, ok = refreshStateWithDefaultRemainingNodes(next.State, entry.Slot, active)
		if ok && len(active.remainingNodeIDs) > 0 {
			delete(active.remainingNodeIDs, entry.NodeID)
			if len(active.remainingNodeIDs) == 0 {
				delete(next.ActivePeerRefresh, entry.Slot)
			} else {
				next.ActivePeerRefresh[entry.Slot] = snapshotHAActivePeerRefreshState(active)
			}
		}
	}
	lease, _ := s.ha.currentLease()
	next.Outbox = rebuildHAOutbox(next, lease.Epoch)
	next.PendingEpochBySlot = nextHAPendingEpochs(snapshot.PendingEpochBySlot, snapshot.State.PendingBySlot, next.State.PendingBySlot, next.Outbox)
	return next, nil
}

func runtimeOutboxHasEntryID(entries []coordruntime.OutboxEntry, id string) bool {
	for _, entry := range entries {
		if entry.ID == id {
			return true
		}
	}
	return false
}

func (s *Server) dispatchHAOutboxEntries(ctx context.Context, allow func(OutboxEntry) bool) error {
	for pass := 0; pass < runtimeVersionRetryLimit; pass++ {
		snapshot, err := s.readHASnapshot(ctx)
		if err != nil {
			return fmt.Errorf("err in s.readHASnapshot: %w", err)
		}
		if len(snapshot.Outbox) == 0 {
			return nil
		}
		processedAny := false
		for _, entry := range snapshot.Outbox {
			if allow != nil && !allow(entry) {
				continue
			}
			processedAny = true
			dispatchCtx, cancel := deriveDeadlineContext(ctx, s.haDispatchTimeout(entry.Kind))
			err := s.dispatchOutboxEntry(dispatchCtx, entry)
			cancel()
			if err != nil {
				return err
			}
			current, err := s.readHASnapshot(ctx)
			if err != nil {
				return fmt.Errorf("err in s.readHASnapshot(ack): %w", err)
			}
			next, err := s.ackHAOutboxEntry(current, entry)
			if err != nil {
				return err
			}
			if err := s.saveHASnapshot(ctx, current.SnapshotVersion, next); err != nil {
				return err
			}
		}
		if allow != nil || !processedAny {
			return nil
		}
	}
	return nil
}
