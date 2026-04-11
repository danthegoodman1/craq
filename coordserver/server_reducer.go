package coordserver

import (
	"fmt"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
)

type replicaProgressReduction struct {
	duplicateCompleted bool
	enqueuePeerRefresh bool
	peerRefreshState   activePeerRefreshState
	progressCommandID  string
	slotVersion        uint64
}

func reduceReplicaReadyProgress(
	current coordruntime.SlotProgressView,
	nodeID string,
	commandID string,
	attempt int,
) (replicaProgressReduction, error) {
	reduction := replicaProgressReduction{
		slotVersion: current.SlotVersion,
	}
	pending := current.Pending
	if pending == nil || pending.Kind != coordruntime.PendingKindReady || pending.NodeID != nodeID {
		if matchesCompletedSlice(current.Completed, nodeID, pendingKindReady, current.SlotVersion) {
			reduction.duplicateCompleted = true
			return reduction, nil
		}
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: unexpected ready report for node %q slot %d",
			ErrUnexpectedProgress,
			nodeID,
			current.Chain.Slot,
		)
	}
	if commandID != "" && pending.CommandID != "" && pending.CommandID != commandID {
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: ready report command %q does not match pending %q",
			ErrUnexpectedProgress,
			commandID,
			pending.CommandID,
		)
	}
	if pending.SlotVersion != current.SlotVersion {
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: ready report slot version %d does not match pending version %d",
			ErrUnexpectedProgress,
			current.SlotVersion,
			pending.SlotVersion,
		)
	}
	if !chainContainsReplicaInState(current.Chain, nodeID, coordinator.ReplicaStateJoining) {
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: node %q slot %d is not joining in current coordinator state",
			ErrStateMismatch,
			nodeID,
			current.Chain.Slot,
		)
	}
	reduction.enqueuePeerRefresh = activeReplicaCount(current.Chain) > 0 || hasLeavingReplica(current.Chain)
	if fallback, ok := readyProgressFallbackServingChainForChain(current.Chain, current.ReplicationFactor); ok {
		reduction.peerRefreshState = activePeerRefreshState{
			fallbackServingChain: fallback,
			useFallbackRoute:     true,
		}
	}
	reduction.progressCommandID = commandID
	if reduction.progressCommandID == "" {
		reduction.progressCommandID = fmt.Sprintf(
			"server-progress-ready-%s-%d-r%d-v%d",
			nodeID,
			current.Chain.Slot,
			attempt,
			current.Version,
		)
	}
	return reduction, nil
}

func reduceReplicaRemovedProgress(
	current coordruntime.SlotProgressView,
	nodeID string,
	commandID string,
	attempt int,
) (replicaProgressReduction, error) {
	reduction := replicaProgressReduction{
		enqueuePeerRefresh: true,
		slotVersion:        current.SlotVersion,
	}
	pending := current.Pending
	if pending == nil || pending.Kind != coordruntime.PendingKindRemoved || pending.NodeID != nodeID {
		if matchesCompletedSlice(current.Completed, nodeID, pendingKindRemoved, current.SlotVersion) {
			reduction.duplicateCompleted = true
			return reduction, nil
		}
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: unexpected removed report for node %q slot %d",
			ErrUnexpectedProgress,
			nodeID,
			current.Chain.Slot,
		)
	}
	if commandID != "" && pending.CommandID != "" && pending.CommandID != commandID {
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: removed report command %q does not match pending %q",
			ErrUnexpectedProgress,
			commandID,
			pending.CommandID,
		)
	}
	if pending.SlotVersion != current.SlotVersion {
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: removed report slot version %d does not match pending version %d",
			ErrUnexpectedProgress,
			current.SlotVersion,
			pending.SlotVersion,
		)
	}
	if !chainContainsReplicaInState(current.Chain, nodeID, coordinator.ReplicaStateLeaving) {
		return replicaProgressReduction{}, fmt.Errorf(
			"%w: node %q slot %d is not leaving in current coordinator state",
			ErrStateMismatch,
			nodeID,
			current.Chain.Slot,
		)
	}
	reduction.progressCommandID = commandID
	if reduction.progressCommandID == "" {
		reduction.progressCommandID = fmt.Sprintf(
			"server-progress-removed-%s-%d-r%d-v%d",
			nodeID,
			current.Chain.Slot,
			attempt,
			current.Version,
		)
	}
	return reduction, nil
}

func reduceNodeLivenessTarget(
	record coordruntime.NodeLivenessRecord,
	nowUnixNano int64,
	policy LivenessPolicy,
) coordruntime.NodeLivenessState {
	if policy.SuspectAfter <= 0 || policy.DeadAfter <= 0 {
		return record.State
	}
	if nowUnixNano <= record.LastHeartbeatUnixNano {
		return coordruntime.NodeLivenessStateHealthy
	}
	age := time.Duration(nowUnixNano - record.LastHeartbeatUnixNano)
	switch {
	case age >= policy.DeadAfter:
		return coordruntime.NodeLivenessStateDead
	case age >= policy.SuspectAfter:
		return coordruntime.NodeLivenessStateSuspect
	default:
		return coordruntime.NodeLivenessStateHealthy
	}
}

func reduceRoutingSnapshot(
	state coordruntime.View,
	unavailable map[string]map[int]bool,
	queuedPeerRefresh map[int]activePeerRefreshState,
) RoutingSnapshot {
	snapshot := RoutingSnapshot{
		Version:   state.Version,
		SlotCount: state.Cluster.SlotCount,
		Slots:     make([]SlotRoute, 0, len(state.Cluster.Chains)),
	}
	for _, chain := range state.Cluster.Chains {
		routeChain := chain
		refresh, hasRefresh := queuedPeerRefresh[chain.Slot]
		if refresh.useFallbackRoute {
			routeChain = refresh.fallbackServingChain
		}
		route := buildSlotRoute(routeChain, state.Cluster.NodesByID, state.SlotVersions[chain.Slot], unavailable)
		if !hasRefresh && chainHasReplicaState(chain, coordinator.ReplicaStateJoining) {
			route.Writable = false
		}
		if hasRefresh && !refresh.useFallbackRoute {
			route.Writable = false
			route.Readable = false
		}
		snapshot.Slots = append(snapshot.Slots, route)
	}
	return snapshot
}
