package coordinator

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const DefaultSlotCount = 4096

var (
	ErrInvalidConfig          = errors.New("invalid coordinator config")
	ErrInvalidNode            = errors.New("invalid coordinator node")
	ErrInvalidEvent           = errors.New("invalid coordinator event")
	ErrIllegalStateTransition = errors.New("illegal coordinator state transition")
	ErrPlacementImpossible    = errors.New("placement impossible")
	ErrBrokenChain            = errors.New("broken chain")
)

type Config struct {
	SlotCount         int
	ReplicationFactor int
}

type Node struct {
	ID             string
	RPCAddress     string
	FailureDomains map[string]string
}

type ReplicaState string

const (
	ReplicaStateJoining ReplicaState = "joining"
	ReplicaStateActive  ReplicaState = "active"
	ReplicaStateLeaving ReplicaState = "leaving"
)

type Replica struct {
	NodeID string
	State  ReplicaState
}

type Chain struct {
	Slot     int
	Replicas []Replica
}

type NodeHealth string

const (
	NodeHealthAlive NodeHealth = "alive"
	NodeHealthDead  NodeHealth = "dead"
)

type ClusterState struct {
	Chains            []Chain
	NodesByID         map[string]Node
	NodeHealthByID    map[string]NodeHealth
	ReadyNodeIDs      map[string]bool
	DrainingNodeIDs   map[string]bool
	NodeOrder         []string
	SlotCount         int
	ReplicationFactor int
}

type PlacementError struct {
	Slot            int
	ReplicaPosition int
}

func (e *PlacementError) Error() string {
	return fmt.Sprintf(
		"coordinator: no eligible node for slot %d replica %d",
		e.Slot,
		e.ReplicaPosition,
	)
}

func (e *PlacementError) Is(target error) bool {
	return target == ErrPlacementImpossible
}

type EventKind string

const (
	EventKindRegisterNode        EventKind = "register_node"
	EventKindAddNode             EventKind = "add_node"
	EventKindBeginDrainNode      EventKind = "begin_drain_node"
	EventKindMarkNodeDead        EventKind = "mark_node_dead"
	EventKindReplicaBecameActive EventKind = "replica_became_active"
	EventKindReplicaRemoved      EventKind = "replica_removed"
)

type Event struct {
	Kind   EventKind
	Node   Node
	NodeID string
	Slot   int
}

type ReconfigurationPolicy struct {
	MaxChangedChains int
}

type StepKind string

const (
	StepKindAppendTail  StepKind = "append_tail"
	StepKindMarkLeaving StepKind = "mark_leaving"
)

type ReconfigurationStep struct {
	Kind           StepKind
	Slot           int
	NodeID         string
	ReplacedNodeID string
}

type SlotPlan struct {
	Slot   int
	Before Chain
	After  Chain
	Steps  []ReconfigurationStep
}

type ReconfigurationPlan struct {
	UpdatedState      ClusterState
	UnassignedNodeIDs []string
	ChangedSlots      []SlotPlan
}

type assignmentCounts struct {
	headCount    int
	tailCount    int
	replicaCount int
}

func BuildInitialPlacement(cfg Config, nodes []Node) (*ClusterState, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, fmt.Errorf("err in validateConfig: %w", err)
	}

	if len(nodes) == 0 {
		return &ClusterState{
			Chains:            emptyChains(cfg.SlotCount),
			NodesByID:         map[string]Node{},
			NodeHealthByID:    map[string]NodeHealth{},
			ReadyNodeIDs:      map[string]bool{},
			DrainingNodeIDs:   map[string]bool{},
			NodeOrder:         []string{},
			SlotCount:         cfg.SlotCount,
			ReplicationFactor: cfg.ReplicationFactor,
		}, nil
	}

	normalizedNodes, nodesByID, nodeOrder, err := normalizeNodes(nodes)
	if err != nil {
		return nil, fmt.Errorf("err in normalizeNodes: %w", err)
	}
	if cfg.ReplicationFactor > len(normalizedNodes) {
		return nil, fmt.Errorf(
			"%w: replication factor %d exceeds node count %d",
			ErrInvalidConfig,
			cfg.ReplicationFactor,
			len(normalizedNodes),
		)
	}

	chains, err := buildSteadyStateChains(cfg.SlotCount, cfg.ReplicationFactor, normalizedNodes, nodesByID)
	if err != nil {
		return nil, fmt.Errorf("err in buildSteadyStateChains: %w", err)
	}

	nodeHealthByID := make(map[string]NodeHealth, len(normalizedNodes))
	readyNodeIDs := make(map[string]bool, len(normalizedNodes))
	for _, node := range normalizedNodes {
		nodeHealthByID[node.ID] = NodeHealthAlive
		readyNodeIDs[node.ID] = true
	}

	return &ClusterState{
		Chains:            chains,
		NodesByID:         nodesByID,
		NodeHealthByID:    nodeHealthByID,
		ReadyNodeIDs:      readyNodeIDs,
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         nodeOrder,
		SlotCount:         cfg.SlotCount,
		ReplicationFactor: cfg.ReplicationFactor,
	}, nil
}

func PlanReconfiguration(
	state ClusterState,
	events []Event,
	policy ReconfigurationPolicy,
) (*ReconfigurationPlan, error) {
	if policy.MaxChangedChains < 0 {
		return nil, fmt.Errorf("%w: max changed chains must be >= 0", ErrInvalidConfig)
	}

	working, err := cloneAndValidateState(state)
	if err != nil {
		return nil, fmt.Errorf("err in cloneAndValidateState: %w", err)
	}

	var changedSlots []SlotPlan
	if len(events) == 0 {
		slotPlans, err := reconcileState(&working, policy)
		if err != nil {
			return nil, fmt.Errorf("err in reconcileState: %w", err)
		}
		changedSlots = append(changedSlots, slotPlans...)
	} else {
		for _, event := range events {
			if err := applyEvent(&working, event); err != nil {
				return nil, fmt.Errorf("err in applyEvent: %w", err)
			}

			slotPlans, err := reconcileState(&working, policy)
			if err != nil {
				return nil, fmt.Errorf("err in reconcileState: %w", err)
			}
			changedSlots = append(changedSlots, slotPlans...)
		}
	}

	return &ReconfigurationPlan{
		UpdatedState:      working,
		UnassignedNodeIDs: collectUnassignedNodeIDs(working),
		ChangedSlots:      changedSlots,
	}, nil
}

func ApplyProgress(state ClusterState, event Event) (*ClusterState, error) {
	working, err := cloneAndValidateState(state)
	if err != nil {
		return nil, fmt.Errorf("err in cloneAndValidateState: %w", err)
	}
	if event.Kind != EventKindReplicaBecameActive && event.Kind != EventKindReplicaRemoved {
		return nil, fmt.Errorf("%w: unsupported progress event %q", ErrInvalidEvent, event.Kind)
	}
	if err := applyProgressEvent(&working, event); err != nil {
		return nil, fmt.Errorf("err in applyProgressEvent: %w", err)
	}
	return &working, nil
}

func validateConfig(cfg Config) error {
	if cfg.SlotCount <= 0 {
		return fmt.Errorf("%w: slot count must be > 0", ErrInvalidConfig)
	}
	if cfg.ReplicationFactor <= 0 {
		return fmt.Errorf("%w: replication factor must be > 0", ErrInvalidConfig)
	}
	return nil
}

func normalizeNodes(nodes []Node) ([]Node, map[string]Node, []string, error) {
	if len(nodes) == 0 {
		return nil, nil, nil, fmt.Errorf("%w: node list must not be empty", ErrInvalidConfig)
	}

	normalized := make([]Node, 0, len(nodes))
	nodesByID := make(map[string]Node, len(nodes))
	nodeOrder := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node.ID == "" {
			return nil, nil, nil, fmt.Errorf("%w: node ID must not be empty", ErrInvalidNode)
		}
		if _, exists := nodesByID[node.ID]; exists {
			return nil, nil, nil, fmt.Errorf("%w: duplicate node ID %q", ErrInvalidNode, node.ID)
		}

		failureDomains := make(map[string]string, len(node.FailureDomains))
		for key, value := range node.FailureDomains {
			if strings.TrimSpace(key) == "" {
				return nil, nil, nil, fmt.Errorf("%w: node %q has empty failure-domain key", ErrInvalidNode, node.ID)
			}
			if strings.TrimSpace(value) == "" {
				return nil, nil, nil, fmt.Errorf(
					"%w: node %q has empty failure-domain value for key %q",
					ErrInvalidNode,
					node.ID,
					key,
				)
			}
			failureDomains[key] = value
		}

		normalizedNode := Node{
			ID:             node.ID,
			RPCAddress:     strings.TrimSpace(node.RPCAddress),
			FailureDomains: failureDomains,
		}
		normalized = append(normalized, normalizedNode)
		nodesByID[node.ID] = normalizedNode
		nodeOrder = append(nodeOrder, node.ID)
	}

	return normalized, nodesByID, nodeOrder, nil
}

func cloneAndValidateState(state ClusterState) (ClusterState, error) {
	if err := validateConfig(Config{
		SlotCount:         state.SlotCount,
		ReplicationFactor: state.ReplicationFactor,
	}); err != nil {
		return ClusterState{}, err
	}
	if len(state.Chains) != state.SlotCount {
		return ClusterState{}, fmt.Errorf(
			"%w: chain count %d does not match slot count %d",
			ErrInvalidConfig,
			len(state.Chains),
			state.SlotCount,
		)
	}

	cloned := ClusterState{
		Chains:            make([]Chain, len(state.Chains)),
		NodesByID:         make(map[string]Node, len(state.NodesByID)),
		NodeHealthByID:    make(map[string]NodeHealth, len(state.NodeHealthByID)),
		ReadyNodeIDs:      make(map[string]bool, len(state.ReadyNodeIDs)),
		DrainingNodeIDs:   make(map[string]bool, len(state.DrainingNodeIDs)),
		NodeOrder:         append([]string(nil), state.NodeOrder...),
		SlotCount:         state.SlotCount,
		ReplicationFactor: state.ReplicationFactor,
	}

	for i, chain := range state.Chains {
		cloned.Chains[i] = cloneChain(chain)
		if cloned.Chains[i].Slot != i {
			cloned.Chains[i].Slot = i
		}
	}
	for id, node := range state.NodesByID {
		cloned.NodesByID[id] = Node{
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
		if draining {
			cloned.DrainingNodeIDs[id] = true
		}
	}

	if len(cloned.NodeOrder) == 0 {
		for id := range cloned.NodesByID {
			cloned.NodeOrder = append(cloned.NodeOrder, id)
		}
		sort.Strings(cloned.NodeOrder)
	}

	seen := make(map[string]struct{}, len(cloned.NodeOrder))
	for _, id := range cloned.NodeOrder {
		if _, ok := cloned.NodesByID[id]; !ok {
			return ClusterState{}, fmt.Errorf("%w: node order references unknown node %q", ErrInvalidConfig, id)
		}
		if _, exists := seen[id]; exists {
			return ClusterState{}, fmt.Errorf("%w: duplicate node order entry %q", ErrInvalidConfig, id)
		}
		seen[id] = struct{}{}
		if _, ok := cloned.NodeHealthByID[id]; !ok {
			cloned.NodeHealthByID[id] = NodeHealthAlive
		}
	}
	for id := range cloned.NodesByID {
		if _, ok := cloned.NodeHealthByID[id]; !ok {
			cloned.NodeHealthByID[id] = NodeHealthAlive
		}
	}

	for _, chain := range cloned.Chains {
		for _, replica := range chain.Replicas {
			if _, ok := cloned.NodesByID[replica.NodeID]; !ok {
				return ClusterState{}, fmt.Errorf("%w: chain references unknown node %q", ErrInvalidConfig, replica.NodeID)
			}
		}
	}

	return cloned, nil
}

func emptyChains(slotCount int) []Chain {
	chains := make([]Chain, slotCount)
	for slot := 0; slot < slotCount; slot++ {
		chains[slot] = Chain{Slot: slot, Replicas: []Replica{}}
	}
	return chains
}

func buildSteadyStateChains(
	slotCount int,
	replicationFactor int,
	nodes []Node,
	nodesByID map[string]Node,
) ([]Chain, error) {
	counts := make(map[string]assignmentCounts, len(nodes))
	chains := make([]Chain, slotCount)
	for slot := 0; slot < slotCount; slot++ {
		tentativeCounts := cloneAssignmentCounts(counts)
		tentative := Chain{
			Slot:     slot,
			Replicas: make([]Replica, 0, replicationFactor),
		}
		for replicaPosition := 0; replicaPosition < replicationFactor; replicaPosition++ {
			candidate, ok := selectReplicaCandidate(nodes, tentative, nodesByID, tentativeCounts)
			if !ok {
				return nil, &PlacementError{
					Slot:            slot,
					ReplicaPosition: replicaPosition,
				}
			}
			appendReplicaWithState(&tentative, candidate.ID, ReplicaStateActive, tentativeCounts)
		}
		chain := tentative
		if len(nodes) == replicationFactor {
			chain = rebalanceSteadyStateChain(slot, tentative)
		}
		recordChainAssignmentCounts(counts, chain)
		chains[slot] = chain
	}
	return chains, nil
}

func cloneAssignmentCounts(current map[string]assignmentCounts) map[string]assignmentCounts {
	cloned := make(map[string]assignmentCounts, len(current))
	for nodeID, counts := range current {
		cloned[nodeID] = counts
	}
	return cloned
}

func rebalanceSteadyStateChain(slot int, chain Chain) Chain {
	if len(chain.Replicas) <= 1 {
		return cloneChain(chain)
	}
	nodeIDs := replicaIDs(chain.Replicas)
	sort.Strings(nodeIDs)
	offset := slot % len(nodeIDs)
	rotated := append(append([]string(nil), nodeIDs[offset:]...), nodeIDs[:offset]...)
	rebalanced := Chain{
		Slot:     chain.Slot,
		Replicas: make([]Replica, 0, len(rotated)),
	}
	for _, nodeID := range rotated {
		rebalanced.Replicas = append(rebalanced.Replicas, Replica{
			NodeID: nodeID,
			State:  ReplicaStateActive,
		})
	}
	return rebalanced
}

func recordChainAssignmentCounts(counts map[string]assignmentCounts, chain Chain) {
	for i, replica := range chain.Replicas {
		nodeCounts := counts[replica.NodeID]
		nodeCounts.replicaCount++
		if i == 0 {
			nodeCounts.headCount++
		}
		if i == len(chain.Replicas)-1 {
			nodeCounts.tailCount++
		}
		counts[replica.NodeID] = nodeCounts
	}
}

func applyEvent(state *ClusterState, event Event) error {
	switch event.Kind {
	case EventKindRegisterNode:
		return addNode(state, event.Node, false)
	case EventKindAddNode:
		return addNode(state, event.Node, true)
	case EventKindBeginDrainNode:
		return beginDrainNode(state, event.NodeID)
	case EventKindMarkNodeDead:
		return markNodeDead(state, event.NodeID)
	case EventKindReplicaBecameActive, EventKindReplicaRemoved:
		return applyProgressEvent(state, event)
	default:
		return fmt.Errorf("%w: unsupported event %q", ErrInvalidEvent, event.Kind)
	}
}

func addNode(state *ClusterState, node Node, ready bool) error {
	normalizedNodes, nodesByID, nodeOrder, err := normalizeNodes([]Node{node})
	if err != nil {
		return err
	}
	normalizedNode := normalizedNodes[0]
	nodeID := nodeOrder[0]
	if existing, exists := state.NodesByID[nodeID]; exists {
		if state.NodeHealthByID[nodeID] == NodeHealthDead {
			return fmt.Errorf("%w: node %q is permanently tombstoned", ErrInvalidEvent, nodeID)
		}
		if nodeIdentityConflict(existing, normalizedNode) {
			return fmt.Errorf("%w: node %q already exists with conflicting identity", ErrInvalidEvent, nodeID)
		}
		if ready {
			state.ReadyNodeIDs[nodeID] = true
		}
		return nil
	}

	state.NodesByID[nodeID] = nodesByID[nodeID]
	state.NodeHealthByID[nodeID] = NodeHealthAlive
	if ready {
		state.ReadyNodeIDs[nodeID] = true
	} else {
		delete(state.ReadyNodeIDs, nodeID)
	}
	state.NodeOrder = append(state.NodeOrder, nodeID)
	delete(state.DrainingNodeIDs, normalizedNode.ID)
	return nil
}

func beginDrainNode(state *ClusterState, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("%w: node ID must not be empty", ErrInvalidEvent)
	}
	if _, ok := state.NodesByID[nodeID]; !ok {
		return fmt.Errorf("%w: unknown node %q", ErrInvalidEvent, nodeID)
	}
	if state.NodeHealthByID[nodeID] == NodeHealthDead {
		return fmt.Errorf("%w: cannot drain dead node %q", ErrInvalidEvent, nodeID)
	}
	state.DrainingNodeIDs[nodeID] = true
	return nil
}

func markNodeDead(state *ClusterState, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("%w: node ID must not be empty", ErrInvalidEvent)
	}
	if _, ok := state.NodesByID[nodeID]; !ok {
		return fmt.Errorf("%w: unknown node %q", ErrInvalidEvent, nodeID)
	}
	state.NodeHealthByID[nodeID] = NodeHealthDead
	delete(state.ReadyNodeIDs, nodeID)
	delete(state.DrainingNodeIDs, nodeID)
	return nil
}

func applyProgressEvent(state *ClusterState, event Event) error {
	if event.Slot < 0 || event.Slot >= state.SlotCount {
		return fmt.Errorf("%w: invalid slot %d", ErrInvalidEvent, event.Slot)
	}
	if event.NodeID == "" {
		return fmt.Errorf("%w: node ID must not be empty", ErrInvalidEvent)
	}

	chain := &state.Chains[event.Slot]
	replicaIndex := findReplicaIndex(*chain, event.NodeID)
	if replicaIndex < 0 {
		return fmt.Errorf(
			"%w: slot %d does not contain node %q",
			ErrIllegalStateTransition,
			event.Slot,
			event.NodeID,
		)
	}

	switch event.Kind {
	case EventKindReplicaBecameActive:
		if chain.Replicas[replicaIndex].State != ReplicaStateJoining {
			return fmt.Errorf(
				"%w: node %q in slot %d is not joining",
				ErrIllegalStateTransition,
				event.NodeID,
				event.Slot,
			)
		}
		chain.Replicas[replicaIndex].State = ReplicaStateActive
		return nil
	case EventKindReplicaRemoved:
		if chain.Replicas[replicaIndex].State != ReplicaStateLeaving &&
			state.NodeHealthByID[event.NodeID] != NodeHealthDead {
			return fmt.Errorf(
				"%w: node %q in slot %d is not removable",
				ErrIllegalStateTransition,
				event.NodeID,
				event.Slot,
			)
		}
		chain.Replicas = append(chain.Replicas[:replicaIndex], chain.Replicas[replicaIndex+1:]...)
		if !state.DrainingNodeIDs[event.NodeID] && state.NodeHealthByID[event.NodeID] != NodeHealthDead {
			if countReplicasForNode(*state, event.NodeID) == 0 {
				delete(state.DrainingNodeIDs, event.NodeID)
			}
		}
		return nil
	default:
		return fmt.Errorf("%w: unsupported progress event %q", ErrInvalidEvent, event.Kind)
	}
}

func reconcileState(state *ClusterState, policy ReconfigurationPolicy) ([]SlotPlan, error) {
	var changedSlots []SlotPlan

	for slot := 0; slot < state.SlotCount; slot++ {
		slotPlan, changed, err := repairDeadReplicas(state, slot)
		if err != nil {
			return nil, err
		}
		if changed {
			changedSlots = append(changedSlots, slotPlan)
		}
	}

	for slot := 0; slot < state.SlotCount; slot++ {
		slotPlan, changed := finalizeDrainReplacement(state, slot)
		if changed {
			changedSlots = append(changedSlots, slotPlan)
		}
	}

	desiredChains, haveDesired, err := buildDesiredPlacementFromState(*state)
	if err != nil {
		return nil, err
	}
	if haveDesired {
		for slot := 0; slot < state.SlotCount; slot++ {
			slotPlan, changed := finalizeJoinRebalance(state, slot, desiredChains[slot])
			if changed {
				changedSlots = append(changedSlots, slotPlan)
			}
		}
	}
	if len(changedSlots) > 0 {
		return changedSlots, nil
	}

	if haveDesired {
		remaining := remainingChangedChainBudget(policy, len(changedSlots))
		for _, slot := range appendPrioritySlots(*state) {
			if remaining == 0 {
				break
			}
			slotPlan, changed, err := appendMissingDesiredReplicas(state, slot, desiredChains[slot])
			if err != nil {
				return nil, err
			}
			if changed {
				changedSlots = append(changedSlots, slotPlan)
				if remaining > 0 {
					remaining--
				}
			}
		}
	}
	if len(changedSlots) > 0 {
		return changedSlots, nil
	}

	for slot := 0; slot < state.SlotCount; slot++ {
		slotPlan, changed, err := appendDrainReplacement(state, slot)
		if err != nil {
			return nil, err
		}
		if changed {
			changedSlots = append(changedSlots, slotPlan)
		}
	}

	if haveDesired && policy.MaxChangedChains > 0 {
		remaining := policy.MaxChangedChains
		for slot := 0; slot < state.SlotCount && remaining > 0; slot++ {
			slotPlan, changed, err := appendJoinRebalance(state, slot, desiredChains[slot])
			if err != nil {
				return nil, err
			}
			if changed {
				changedSlots = append(changedSlots, slotPlan)
				remaining--
			}
		}
	}

	return changedSlots, nil
}

func remainingChangedChainBudget(policy ReconfigurationPolicy, alreadyChanged int) int {
	if policy.MaxChangedChains <= 0 {
		return -1
	}
	remaining := policy.MaxChangedChains - alreadyChanged
	if remaining < 0 {
		return 0
	}
	return remaining
}

func appendPrioritySlots(state ClusterState) []int {
	slots := make([]int, 0, len(state.Chains))
	for _, chain := range state.Chains {
		slots = append(slots, chain.Slot)
	}
	sort.Slice(slots, func(i, j int) bool {
		left := state.Chains[slots[i]]
		right := state.Chains[slots[j]]
		if len(left.Replicas) != len(right.Replicas) {
			return len(left.Replicas) < len(right.Replicas)
		}
		return left.Slot < right.Slot
	})
	return slots
}

func appendMissingDesiredReplicas(state *ClusterState, slot int, desired Chain) (SlotPlan, bool, error) {
	chain := &state.Chains[slot]
	if hasPendingReplicaState(*chain) {
		return SlotPlan{}, false, nil
	}
	if len(chain.Replicas) >= state.ReplicationFactor {
		return SlotPlan{}, false, nil
	}
	if len(desired.Replicas) == 0 {
		return SlotPlan{}, false, nil
	}

	currentIDs := replicaIDs(chain.Replicas)
	desiredIDs := replicaIDs(desired.Replicas)
	if len(currentIDs) > len(desiredIDs) || !isPrefix(currentIDs, desiredIDs[:len(currentIDs)]) {
		return SlotPlan{}, false, nil
	}

	nextNodeID := desiredIDs[len(currentIDs)]
	if state.NodeHealthByID[nextNodeID] != NodeHealthAlive || !state.ReadyNodeIDs[nextNodeID] || state.DrainingNodeIDs[nextNodeID] {
		return SlotPlan{}, false, nil
	}
	if containsNodeID(currentIDs, nextNodeID) {
		return SlotPlan{}, false, nil
	}

	before := cloneChain(*chain)
	replacedNodeID := ""
	if len(desiredIDs) >= state.ReplicationFactor {
		replacedNodeID = desiredIDs[state.ReplicationFactor-1]
	}
	chain.Replicas = append(chain.Replicas, Replica{NodeID: nextNodeID, State: ReplicaStateJoining})
	return SlotPlan{
		Slot:   slot,
		Before: before,
		After:  cloneChain(*chain),
		Steps: []ReconfigurationStep{{
			Kind:           StepKindAppendTail,
			Slot:           slot,
			NodeID:         nextNodeID,
			ReplacedNodeID: replacedNodeID,
		}},
	}, true, nil
}

func repairDeadReplicas(state *ClusterState, slot int) (SlotPlan, bool, error) {
	chain := &state.Chains[slot]
	if !chainContainsDeadReplica(*chain, state.NodeHealthByID) {
		return SlotPlan{}, false, nil
	}

	before := cloneChain(*chain)
	survivors := make([]Replica, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if state.NodeHealthByID[replica.NodeID] != NodeHealthDead {
			survivors = append(survivors, replica)
		}
	}
	if countActiveReplicas(survivors) == 0 {
		return SlotPlan{}, false, fmt.Errorf("%w: slot %d has no active anchor after dead-node removal", ErrBrokenChain, slot)
	}

	chain.Replicas = survivors
	var steps []ReconfigurationStep
	for len(chain.Replicas) < state.ReplicationFactor {
		candidate, ok := selectTailRepairCandidate(*state, *chain)
		if !ok {
			break
		}
		chain.Replicas = append(chain.Replicas, Replica{
			NodeID: candidate.ID,
			State:  ReplicaStateJoining,
		})
		steps = append(steps, ReconfigurationStep{
			Kind:   StepKindAppendTail,
			Slot:   slot,
			NodeID: candidate.ID,
		})
	}

	return SlotPlan{
		Slot:   slot,
		Before: before,
		After:  cloneChain(*chain),
		Steps:  steps,
	}, !reflect.DeepEqual(before, *chain), nil
}

func appendDrainReplacement(state *ClusterState, slot int) (SlotPlan, bool, error) {
	chain := &state.Chains[slot]
	if hasPendingReplicaState(*chain) {
		return SlotPlan{}, false, nil
	}

	replacedNodeID := firstActiveDrainingReplicaID(*chain, state.DrainingNodeIDs)
	if replacedNodeID == "" {
		return SlotPlan{}, false, nil
	}

	before := cloneChain(*chain)
	candidate, ok := selectTailRepairCandidate(*state, *chain)
	if !ok {
		return SlotPlan{}, false, &PlacementError{
			Slot:            slot,
			ReplicaPosition: len(chain.Replicas),
		}
	}
	chain.Replicas = append(chain.Replicas, Replica{
		NodeID: candidate.ID,
		State:  ReplicaStateJoining,
	})

	return SlotPlan{
		Slot:   slot,
		Before: before,
		After:  cloneChain(*chain),
		Steps: []ReconfigurationStep{
			{
				Kind:           StepKindAppendTail,
				Slot:           slot,
				NodeID:         candidate.ID,
				ReplacedNodeID: replacedNodeID,
			},
		},
	}, true, nil
}

func finalizeDrainReplacement(state *ClusterState, slot int) (SlotPlan, bool) {
	chain := &state.Chains[slot]
	if hasJoiningReplica(*chain) {
		return SlotPlan{}, false
	}

	drainingActiveIDs := activeReplicaIDsWithDrain(*chain, state.DrainingNodeIDs)
	if len(drainingActiveIDs) == 0 {
		return SlotPlan{}, false
	}

	targetActives := activeReplicaIDsExcludingDraining(*chain, state.DrainingNodeIDs)
	if len(targetActives) != state.ReplicationFactor {
		return SlotPlan{}, false
	}

	before := cloneChain(*chain)
	newReplicas := make([]Replica, 0, len(targetActives)+len(drainingActiveIDs))
	for _, nodeID := range targetActives {
		newReplicas = append(newReplicas, Replica{NodeID: nodeID, State: ReplicaStateActive})
	}
	steps := make([]ReconfigurationStep, 0, len(drainingActiveIDs))
	for _, nodeID := range drainingActiveIDs {
		newReplicas = append(newReplicas, Replica{NodeID: nodeID, State: ReplicaStateLeaving})
		steps = append(steps, ReconfigurationStep{
			Kind:   StepKindMarkLeaving,
			Slot:   slot,
			NodeID: nodeID,
		})
	}

	chain.Replicas = newReplicas
	return SlotPlan{
		Slot:   slot,
		Before: before,
		After:  cloneChain(*chain),
		Steps:  steps,
	}, true
}

func finalizeJoinRebalance(state *ClusterState, slot int, desired Chain) (SlotPlan, bool) {
	chain := &state.Chains[slot]
	if hasJoiningReplica(*chain) || hasLeavingReplica(*chain) {
		return SlotPlan{}, false
	}
	if firstActiveDrainingReplicaID(*chain, state.DrainingNodeIDs) != "" {
		return SlotPlan{}, false
	}

	currentActives := activeReplicaIDs(*chain)
	desiredActives := replicaIDs(desired.Replicas)
	if len(currentActives) != state.ReplicationFactor+1 {
		return SlotPlan{}, false
	}

	extraNodeID, ok := extraNode(currentActives, desiredActives)
	if !ok {
		return SlotPlan{}, false
	}
	newNodeID, ok := lastActiveReplicaID(*chain)
	if !ok || newNodeID == extraNodeID || !containsNodeID(desiredActives, newNodeID) {
		return SlotPlan{}, false
	}
	if !sameRelativeOrderForJoinFinalization(currentActives, extraNodeID, newNodeID, desiredActives) {
		return SlotPlan{}, false
	}

	before := cloneChain(*chain)
	newReplicas := make([]Replica, 0, len(desiredActives)+1)
	for _, nodeID := range desiredActives {
		newReplicas = append(newReplicas, Replica{NodeID: nodeID, State: ReplicaStateActive})
	}
	newReplicas = append(newReplicas, Replica{NodeID: extraNodeID, State: ReplicaStateLeaving})
	chain.Replicas = newReplicas

	return SlotPlan{
		Slot:   slot,
		Before: before,
		After:  cloneChain(*chain),
		Steps: []ReconfigurationStep{
			{
				Kind:   StepKindMarkLeaving,
				Slot:   slot,
				NodeID: extraNodeID,
			},
		},
	}, true
}

func appendJoinRebalance(state *ClusterState, slot int, desired Chain) (SlotPlan, bool, error) {
	chain := &state.Chains[slot]
	if hasPendingReplicaState(*chain) {
		return SlotPlan{}, false, nil
	}
	if firstActiveDrainingReplicaID(*chain, state.DrainingNodeIDs) != "" {
		return SlotPlan{}, false, nil
	}
	if chainContainsDeadReplica(*chain, state.NodeHealthByID) {
		return SlotPlan{}, false, nil
	}

	currentActives := activeReplicaIDs(*chain)
	desiredActives := replicaIDs(desired.Replicas)
	if len(currentActives) != state.ReplicationFactor || len(desiredActives) != state.ReplicationFactor {
		return SlotPlan{}, false, nil
	}

	addedNodeID, removedNodeID, ok := oneMemberReplacement(currentActives, desiredActives)
	if !ok {
		if disjointChain(currentActives, desiredActives) {
			return SlotPlan{}, false, fmt.Errorf(
				"%w: slot %d would require a fully disjoint remap",
				ErrBrokenChain,
				slot,
			)
		}
		return SlotPlan{}, false, nil
	}
	if !sameRelativeOrderWithout(currentActives, removedNodeID, desiredActives, addedNodeID) {
		return SlotPlan{}, false, nil
	}

	before := cloneChain(*chain)
	chain.Replicas = append(chain.Replicas, Replica{
		NodeID: addedNodeID,
		State:  ReplicaStateJoining,
	})

	return SlotPlan{
		Slot:   slot,
		Before: before,
		After:  cloneChain(*chain),
		Steps: []ReconfigurationStep{
			{
				Kind:           StepKindAppendTail,
				Slot:           slot,
				NodeID:         addedNodeID,
				ReplacedNodeID: removedNodeID,
			},
		},
	}, true, nil
}

func buildDesiredPlacementFromState(state ClusterState) ([]Chain, bool, error) {
	eligibleNodes := orderedNodes(state, func(nodeID string) bool {
		return state.NodeHealthByID[nodeID] == NodeHealthAlive && state.ReadyNodeIDs[nodeID] && !state.DrainingNodeIDs[nodeID]
	})
	if len(eligibleNodes) < state.ReplicationFactor {
		return nil, false, nil
	}

	nodesByID := make(map[string]Node, len(eligibleNodes))
	for _, node := range eligibleNodes {
		nodesByID[node.ID] = node
	}
	chains, err := buildSteadyStateChains(state.SlotCount, state.ReplicationFactor, eligibleNodes, nodesByID)
	if err != nil {
		return nil, false, err
	}
	return chains, true, nil
}

func orderedNodes(state ClusterState, include func(nodeID string) bool) []Node {
	nodes := make([]Node, 0, len(state.NodeOrder))
	for _, nodeID := range state.NodeOrder {
		if include != nil && !include(nodeID) {
			continue
		}
		node, ok := state.NodesByID[nodeID]
		if !ok {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes
}

func deriveAssignmentCounts(state ClusterState) map[string]assignmentCounts {
	counts := make(map[string]assignmentCounts, len(state.NodesByID))
	for _, chain := range state.Chains {
		lastAliveIndex := -1
		for i, replica := range chain.Replicas {
			if state.NodeHealthByID[replica.NodeID] == NodeHealthDead {
				continue
			}
			lastAliveIndex = i
		}
		for i, replica := range chain.Replicas {
			if state.NodeHealthByID[replica.NodeID] == NodeHealthDead {
				continue
			}
			nodeCounts := counts[replica.NodeID]
			nodeCounts.replicaCount++
			if i == 0 {
				nodeCounts.headCount++
			}
			if i == lastAliveIndex {
				nodeCounts.tailCount++
			}
			counts[replica.NodeID] = nodeCounts
		}
	}
	return counts
}

func selectTailRepairCandidate(state ClusterState, chain Chain) (Node, bool) {
	counts := deriveAssignmentCounts(state)
	nodes := orderedNodes(state, func(nodeID string) bool {
		return state.NodeHealthByID[nodeID] == NodeHealthAlive && state.ReadyNodeIDs[nodeID] && !state.DrainingNodeIDs[nodeID]
	})
	return selectReplicaCandidate(nodes, chain, state.NodesByID, counts)
}

func selectReplicaCandidate(
	nodes []Node,
	chain Chain,
	nodesByID map[string]Node,
	counts map[string]assignmentCounts,
) (Node, bool) {
	var best Node
	found := false

	for _, node := range nodes {
		if !isEligible(node, chain, nodesByID) {
			continue
		}
		if !found || compareNodes(node, best, counts) < 0 {
			best = node
			found = true
		}
	}

	return best, found
}

func isEligible(candidate Node, chain Chain, nodesByID map[string]Node) bool {
	for _, replica := range chain.Replicas {
		if replica.NodeID == candidate.ID {
			return false
		}
		if hasFailureDomainConflict(candidate, nodesByID[replica.NodeID]) {
			return false
		}
	}
	return true
}

func hasFailureDomainConflict(candidate Node, existing Node) bool {
	for key, candidateValue := range candidate.FailureDomains {
		existingValue, ok := existing.FailureDomains[key]
		if ok && existingValue == candidateValue {
			return true
		}
	}
	return false
}

func compareNodes(a Node, b Node, counts map[string]assignmentCounts) int {
	aCounts := counts[a.ID]
	bCounts := counts[b.ID]

	if aCounts.tailCount != bCounts.tailCount {
		if aCounts.tailCount < bCounts.tailCount {
			return -1
		}
		return 1
	}
	if aCounts.headCount != bCounts.headCount {
		if aCounts.headCount < bCounts.headCount {
			return -1
		}
		return 1
	}
	if aCounts.replicaCount != bCounts.replicaCount {
		if aCounts.replicaCount < bCounts.replicaCount {
			return -1
		}
		return 1
	}
	return strings.Compare(a.ID, b.ID)
}

func appendReplicaWithState(
	chain *Chain,
	nodeID string,
	state ReplicaState,
	counts map[string]assignmentCounts,
) {
	if len(chain.Replicas) == 0 {
		chain.Replicas = append(chain.Replicas, Replica{NodeID: nodeID, State: state})
		nodeCounts := counts[nodeID]
		nodeCounts.headCount++
		nodeCounts.tailCount++
		nodeCounts.replicaCount++
		counts[nodeID] = nodeCounts
		return
	}

	previousTailID := chain.Replicas[len(chain.Replicas)-1].NodeID
	previousTailCounts := counts[previousTailID]
	previousTailCounts.tailCount--
	counts[previousTailID] = previousTailCounts

	chain.Replicas = append(chain.Replicas, Replica{NodeID: nodeID, State: state})
	nodeCounts := counts[nodeID]
	nodeCounts.tailCount++
	nodeCounts.replicaCount++
	counts[nodeID] = nodeCounts
}

func cloneChain(chain Chain) Chain {
	return Chain{
		Slot:     chain.Slot,
		Replicas: append([]Replica(nil), chain.Replicas...),
	}
}

func cloneFailureDomains(failureDomains map[string]string) map[string]string {
	cloned := make(map[string]string, len(failureDomains))
	for key, value := range failureDomains {
		cloned[key] = value
	}
	return cloned
}

func chainContainsDeadReplica(chain Chain, healthByID map[string]NodeHealth) bool {
	for _, replica := range chain.Replicas {
		if healthByID[replica.NodeID] == NodeHealthDead {
			return true
		}
	}
	return false
}

func activeReplicaIDs(chain Chain) []string {
	ids := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if replica.State == ReplicaStateActive {
			ids = append(ids, replica.NodeID)
		}
	}
	return ids
}

func activeReplicaIDsExcludingDraining(chain Chain, draining map[string]bool) []string {
	ids := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if replica.State == ReplicaStateActive && !draining[replica.NodeID] {
			ids = append(ids, replica.NodeID)
		}
	}
	return ids
}

func activeReplicaIDsWithDrain(chain Chain, draining map[string]bool) []string {
	ids := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if replica.State == ReplicaStateActive && draining[replica.NodeID] {
			ids = append(ids, replica.NodeID)
		}
	}
	return ids
}

func hasJoiningReplica(chain Chain) bool {
	for _, replica := range chain.Replicas {
		if replica.State == ReplicaStateJoining {
			return true
		}
	}
	return false
}

func hasLeavingReplica(chain Chain) bool {
	for _, replica := range chain.Replicas {
		if replica.State == ReplicaStateLeaving {
			return true
		}
	}
	return false
}

func hasPendingReplicaState(chain Chain) bool {
	return hasJoiningReplica(chain) || hasLeavingReplica(chain)
}

func firstActiveDrainingReplicaID(chain Chain, draining map[string]bool) string {
	for _, replica := range chain.Replicas {
		if replica.State == ReplicaStateActive && draining[replica.NodeID] {
			return replica.NodeID
		}
	}
	return ""
}

func countActiveReplicas(replicas []Replica) int {
	count := 0
	for _, replica := range replicas {
		if replica.State == ReplicaStateActive {
			count++
		}
	}
	return count
}

func replicaIDs(replicas []Replica) []string {
	ids := make([]string, 0, len(replicas))
	for _, replica := range replicas {
		ids = append(ids, replica.NodeID)
	}
	return ids
}

func extraNode(current []string, target []string) (string, bool) {
	targetSet := make(map[string]struct{}, len(target))
	for _, nodeID := range target {
		targetSet[nodeID] = struct{}{}
	}
	extra := ""
	for _, nodeID := range current {
		if _, ok := targetSet[nodeID]; !ok {
			if extra != "" {
				return "", false
			}
			extra = nodeID
		}
	}
	if extra == "" {
		return "", false
	}
	return extra, true
}

func oneMemberReplacement(current []string, target []string) (string, string, bool) {
	currentSet := make(map[string]struct{}, len(current))
	targetSet := make(map[string]struct{}, len(target))
	for _, nodeID := range current {
		currentSet[nodeID] = struct{}{}
	}
	for _, nodeID := range target {
		targetSet[nodeID] = struct{}{}
	}

	added := ""
	removed := ""
	for _, nodeID := range target {
		if _, ok := currentSet[nodeID]; !ok {
			if added != "" {
				return "", "", false
			}
			added = nodeID
		}
	}
	for _, nodeID := range current {
		if _, ok := targetSet[nodeID]; !ok {
			if removed != "" {
				return "", "", false
			}
			removed = nodeID
		}
	}
	if added == "" || removed == "" {
		return "", "", false
	}
	return added, removed, true
}

func disjointChain(left []string, right []string) bool {
	rightSet := make(map[string]struct{}, len(right))
	for _, nodeID := range right {
		rightSet[nodeID] = struct{}{}
	}
	for _, nodeID := range left {
		if _, ok := rightSet[nodeID]; ok {
			return false
		}
	}
	return true
}

func sameRelativeOrderWithout(current []string, removed string, target []string, added string) bool {
	currentFiltered := make([]string, 0, len(current)-1)
	for _, nodeID := range current {
		if nodeID != removed {
			currentFiltered = append(currentFiltered, nodeID)
		}
	}
	targetFiltered := make([]string, 0, len(target)-1)
	for _, nodeID := range target {
		if nodeID != added {
			targetFiltered = append(targetFiltered, nodeID)
		}
	}
	if len(currentFiltered) != len(targetFiltered) {
		return false
	}
	for i := range currentFiltered {
		if currentFiltered[i] != targetFiltered[i] {
			return false
		}
	}
	return true
}

func sameRelativeOrderForJoinFinalization(
	current []string,
	removed string,
	newNode string,
	target []string,
) bool {
	currentFiltered := make([]string, 0, len(current)-2)
	for _, nodeID := range current {
		if nodeID != removed && nodeID != newNode {
			currentFiltered = append(currentFiltered, nodeID)
		}
	}

	targetFiltered := make([]string, 0, len(target)-1)
	for _, nodeID := range target {
		if nodeID != newNode {
			targetFiltered = append(targetFiltered, nodeID)
		}
	}

	if len(currentFiltered) != len(targetFiltered) {
		return false
	}
	for i := range currentFiltered {
		if currentFiltered[i] != targetFiltered[i] {
			return false
		}
	}
	return true
}

func findReplicaIndex(chain Chain, nodeID string) int {
	for i, replica := range chain.Replicas {
		if replica.NodeID == nodeID {
			return i
		}
	}
	return -1
}

func countReplicasForNode(state ClusterState, nodeID string) int {
	count := 0
	for _, chain := range state.Chains {
		for _, replica := range chain.Replicas {
			if replica.NodeID == nodeID {
				count++
			}
		}
	}
	return count
}

func collectUnassignedNodeIDs(state ClusterState) []string {
	assigned := make(map[string]bool, len(state.NodesByID))
	for _, chain := range state.Chains {
		for _, replica := range chain.Replicas {
			assigned[replica.NodeID] = true
		}
	}

	unassigned := make([]string, 0)
	for _, nodeID := range state.NodeOrder {
		if state.NodeHealthByID[nodeID] != NodeHealthAlive {
			continue
		}
		if !state.ReadyNodeIDs[nodeID] {
			continue
		}
		if state.DrainingNodeIDs[nodeID] {
			continue
		}
		if !assigned[nodeID] {
			unassigned = append(unassigned, nodeID)
		}
	}
	return unassigned
}

func lastActiveReplicaID(chain Chain) (string, bool) {
	for i := len(chain.Replicas) - 1; i >= 0; i-- {
		if chain.Replicas[i].State == ReplicaStateActive {
			return chain.Replicas[i].NodeID, true
		}
	}
	return "", false
}

func containsNodeID(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func isPrefix(values []string, prefix []string) bool {
	if len(values) != len(prefix) {
		return false
	}
	for i := range values {
		if values[i] != prefix[i] {
			return false
		}
	}
	return true
}

func nodeIdentityConflict(left Node, right Node) bool {
	if left.ID != right.ID {
		return true
	}
	if left.RPCAddress != "" && right.RPCAddress != "" && left.RPCAddress != right.RPCAddress {
		return true
	}
	for key, leftValue := range left.FailureDomains {
		if rightValue, ok := right.FailureDomains[key]; ok && leftValue != rightValue {
			return true
		}
	}
	for key, rightValue := range right.FailureDomains {
		if leftValue, ok := left.FailureDomains[key]; ok && leftValue != rightValue {
			return true
		}
	}
	return false
}
