package coordinator

import (
	"errors"
	"reflect"
	"testing"
)

func TestBuildInitialPlacementBuildsActiveChains(t *testing.T) {
	state, err := BuildInitialPlacement(Config{
		SlotCount:         DefaultSlotCount,
		ReplicationFactor: 3,
	}, uniqueNodes("a", "b", "c", "d", "e"))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}

	if got, want := len(state.Chains), DefaultSlotCount; got != want {
		t.Fatalf("chain count = %d, want %d", got, want)
	}
	if got, want := len(state.NodeOrder), 5; got != want {
		t.Fatalf("node order length = %d, want %d", got, want)
	}

	for i, chain := range state.Chains {
		if chain.Slot != i {
			t.Fatalf("chain[%d].Slot = %d, want %d", i, chain.Slot, i)
		}
		if got, want := len(chain.Replicas), 3; got != want {
			t.Fatalf("chain[%d] replica count = %d, want %d", i, got, want)
		}
		for _, replica := range chain.Replicas {
			if replica.State != ReplicaStateActive {
				t.Fatalf("chain[%d] replica %q state = %q, want active", i, replica.NodeID, replica.State)
			}
		}
	}
}

func TestBuildInitialPlacementRespectsFailureDomains(t *testing.T) {
	nodes := []Node{
		{ID: "a", FailureDomains: map[string]string{"host": "h1", "rack": "r1", "az": "az1"}},
		{ID: "b", FailureDomains: map[string]string{"host": "h2", "rack": "r2", "az": "az2"}},
		{ID: "c", FailureDomains: map[string]string{"host": "h3", "rack": "r3", "az": "az3"}},
		{ID: "d", FailureDomains: map[string]string{"host": "h4", "rack": "r4", "az": "az4"}},
	}

	state, err := BuildInitialPlacement(Config{
		SlotCount:         32,
		ReplicationFactor: 3,
	}, nodes)
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}

	for _, chain := range state.Chains {
		seen := make(map[string]struct{}, len(chain.Replicas))
		for _, replica := range chain.Replicas {
			if _, exists := seen[replica.NodeID]; exists {
				t.Fatalf("chain %d contains duplicate replica ID %q", chain.Slot, replica.NodeID)
			}
			seen[replica.NodeID] = struct{}{}
		}

		for i := 0; i < len(chain.Replicas); i++ {
			left := state.NodesByID[chain.Replicas[i].NodeID]
			for j := i + 1; j < len(chain.Replicas); j++ {
				right := state.NodesByID[chain.Replicas[j].NodeID]
				if hasFailureDomainConflict(left, right) {
					t.Fatalf("chain %d has failure-domain conflict between %q and %q", chain.Slot, left.ID, right.ID)
				}
			}
		}
	}
}

func TestPlanReconfigurationAddNodeUsesExpectedChainAndTailAppend(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))

	plan, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindAddNode, Node: uniqueNode("d")},
	}, ReconfigurationPolicy{MaxChangedChains: 1})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}

	if got, want := len(plan.ChangedSlots), 1; got != want {
		t.Fatalf("changed slot count = %d, want %d", got, want)
	}

	slotPlan := plan.ChangedSlots[0]
	if got, want := slotPlan.Slot, 1; got != want {
		t.Fatalf("changed slot = %d, want %d", got, want)
	}
	if got, want := replicaNodeStates(slotPlan.Before), []string{"b:active", "c:active", "a:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("before = %v, want %v", got, want)
	}
	if got, want := replicaNodeStates(slotPlan.After), []string{"b:active", "c:active", "a:active", "d:joining"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after = %v, want %v", got, want)
	}
	if got, want := slotPlan.Steps, []ReconfigurationStep{{
		Kind:           StepKindAppendTail,
		Slot:           1,
		NodeID:         "d",
		ReplacedNodeID: "c",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("steps = %#v, want %#v", got, want)
	}
}

func TestPlanReconfigurationAddNodeCanConvergeToNonTailSteadyState(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))

	plan, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindAddNode, Node: uniqueNode("d")},
	}, ReconfigurationPolicy{MaxChangedChains: 1})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}

	progressed, err := ApplyProgress(plan.UpdatedState, Event{
		Kind:   EventKindReplicaBecameActive,
		Slot:   1,
		NodeID: "d",
	})
	if err != nil {
		t.Fatalf("ApplyProgress activate returned error: %v", err)
	}

	nextPlan, err := PlanReconfiguration(*progressed, nil, ReconfigurationPolicy{MaxChangedChains: 1})
	if err != nil {
		t.Fatalf("PlanReconfiguration after activate returned error: %v", err)
	}
	if got, want := len(nextPlan.ChangedSlots), 1; got != want {
		t.Fatalf("changed slot count after activate = %d, want %d", got, want)
	}

	slotPlan := nextPlan.ChangedSlots[0]
	if got, want := replicaNodeStates(slotPlan.After), []string{"d:active", "b:active", "a:active", "c:leaving"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after activate plan = %v, want %v", got, want)
	}

	finalState, err := ApplyProgress(nextPlan.UpdatedState, Event{
		Kind:   EventKindReplicaRemoved,
		Slot:   1,
		NodeID: "c",
	})
	if err != nil {
		t.Fatalf("ApplyProgress remove returned error: %v", err)
	}

	if got, want := replicaNodeStates(finalState.Chains[1]), []string{"d:active", "b:active", "a:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain = %v, want %v", got, want)
	}
}

func TestPlanReconfigurationHonorsDisruptionBudgetAndLeavesJoinerUnused(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))

	plan, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindAddNode, Node: uniqueNode("d")},
	}, ReconfigurationPolicy{MaxChangedChains: 0})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}

	if len(plan.ChangedSlots) != 0 {
		t.Fatalf("changed slot count = %d, want 0", len(plan.ChangedSlots))
	}
	if got, want := plan.UnassignedNodeIDs, []string{"d"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("unassigned nodes = %v, want %v", got, want)
	}
}

func TestPlanReconfigurationDrainMiddleReplicaPromotesAndAppendsTail(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d"))

	plan, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindBeginDrainNode, NodeID: "b"},
	}, ReconfigurationPolicy{})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}
	if got, want := replicaNodeStates(plan.ChangedSlots[0].After), []string{"a:active", "b:active", "c:active", "d:joining"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after drain append = %v, want %v", got, want)
	}

	progressed, err := ApplyProgress(plan.UpdatedState, Event{
		Kind:   EventKindReplicaBecameActive,
		Slot:   0,
		NodeID: "d",
	})
	if err != nil {
		t.Fatalf("ApplyProgress activate returned error: %v", err)
	}

	nextPlan, err := PlanReconfiguration(*progressed, nil, ReconfigurationPolicy{})
	if err != nil {
		t.Fatalf("PlanReconfiguration after activate returned error: %v", err)
	}
	if got, want := replicaNodeStates(nextPlan.ChangedSlots[0].After), []string{"a:active", "c:active", "d:active", "b:leaving"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after drain finalize = %v, want %v", got, want)
	}

	finalState, err := ApplyProgress(nextPlan.UpdatedState, Event{
		Kind:   EventKindReplicaRemoved,
		Slot:   0,
		NodeID: "b",
	})
	if err != nil {
		t.Fatalf("ApplyProgress remove returned error: %v", err)
	}
	if got, want := replicaNodeStates(finalState.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain = %v, want %v", got, want)
	}
}

func TestPlanReconfigurationMarkNodeDeadRepairsByAppendingTail(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d"))

	plan, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindMarkNodeDead, NodeID: "b"},
	}, ReconfigurationPolicy{})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}
	if got, want := replicaNodeStates(plan.ChangedSlots[0].After), []string{"a:active", "c:active", "d:joining"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after dead repair = %v, want %v", got, want)
	}
	if got, want := plan.UpdatedState.NodeHealthByID["b"], NodeHealthDead; got != want {
		t.Fatalf("node health = %q, want %q", got, want)
	}
}

func TestPlanReconfigurationRejectsBrokenChainWithoutActiveAnchor(t *testing.T) {
	state := ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
					{NodeID: "b", State: ReplicaStateJoining},
				},
			},
		},
		NodesByID: map[string]Node{
			"a": uniqueNode("a"),
			"b": uniqueNode("b"),
			"c": uniqueNode("c"),
		},
		NodeHealthByID: map[string]NodeHealth{
			"a": NodeHealthAlive,
			"b": NodeHealthAlive,
			"c": NodeHealthAlive,
		},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b", "c"},
		SlotCount:         1,
		ReplicationFactor: 2,
	}

	_, err := PlanReconfiguration(state, []Event{
		{Kind: EventKindMarkNodeDead, NodeID: "a"},
	}, ReconfigurationPolicy{})
	if err == nil {
		t.Fatal("PlanReconfiguration unexpectedly succeeded")
	}
	if !errors.Is(err, ErrBrokenChain) {
		t.Fatalf("error = %v, want broken chain", err)
	}
}

func TestPlanReconfigurationDoesNotAdvanceUnderReplicatedChainWithoutActiveReplacement(t *testing.T) {
	state := ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
					{NodeID: "b", State: ReplicaStateJoining},
					{NodeID: "c", State: ReplicaStateJoining},
				},
			},
		},
		NodesByID: map[string]Node{
			"a": uniqueNode("a"),
			"b": uniqueNode("b"),
			"c": uniqueNode("c"),
		},
		NodeHealthByID: map[string]NodeHealth{
			"a": NodeHealthAlive,
			"b": NodeHealthAlive,
			"c": NodeHealthAlive,
		},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b", "c"},
		SlotCount:         1,
		ReplicationFactor: 3,
	}

	t.Run("drain does not mark last active leaving while joiners pending", func(t *testing.T) {
		plan, err := PlanReconfiguration(state, []Event{
			{Kind: EventKindBeginDrainNode, NodeID: "a"},
		}, ReconfigurationPolicy{})
		if err != nil {
			t.Fatalf("PlanReconfiguration returned error: %v", err)
		}
		if got, want := len(plan.ChangedSlots), 0; got != want {
			t.Fatalf("changed slots = %d, want %d", got, want)
		}
		if got, want := replicaNodeStates(plan.UpdatedState.Chains[0]), []string{"a:active", "b:joining", "c:joining"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("updated chain = %v, want %v", got, want)
		}
		if !plan.UpdatedState.DrainingNodeIDs["a"] {
			t.Fatal("draining node a not recorded")
		}
	})

	t.Run("healthy add does not remap under replicated pending chain", func(t *testing.T) {
		plan, err := PlanReconfiguration(state, []Event{
			{Kind: EventKindAddNode, Node: uniqueNode("d")},
		}, ReconfigurationPolicy{MaxChangedChains: 1})
		if err != nil {
			t.Fatalf("PlanReconfiguration returned error: %v", err)
		}
		if got, want := len(plan.ChangedSlots), 0; got != want {
			t.Fatalf("changed slots = %d, want %d", got, want)
		}
		if got, want := replicaNodeStates(plan.UpdatedState.Chains[0]), []string{"a:active", "b:joining", "c:joining"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("updated chain = %v, want %v", got, want)
		}
	})

	t.Run("dead last active anchor with multiple joiners still breaks chain", func(t *testing.T) {
		_, err := PlanReconfiguration(state, []Event{
			{Kind: EventKindMarkNodeDead, NodeID: "a"},
		}, ReconfigurationPolicy{})
		if err == nil {
			t.Fatal("PlanReconfiguration unexpectedly succeeded")
		}
		if !errors.Is(err, ErrBrokenChain) {
			t.Fatalf("error = %v, want broken chain", err)
		}
	})
}

func TestPlanReconfigurationIsDeterministicForSameInput(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))
	events := []Event{{Kind: EventKindAddNode, Node: uniqueNode("d")}}
	policy := ReconfigurationPolicy{MaxChangedChains: 2}

	left, err := PlanReconfiguration(*state, events, policy)
	if err != nil {
		t.Fatalf("first PlanReconfiguration returned error: %v", err)
	}

	right, err := PlanReconfiguration(*state, events, policy)
	if err != nil {
		t.Fatalf("second PlanReconfiguration returned error: %v", err)
	}

	if !reflect.DeepEqual(left, right) {
		t.Fatal("reconfiguration plans differ for identical input")
	}
}

func TestCompareNodesPrefersTailThenHeadThenReplicaThenID(t *testing.T) {
	nodes := map[string]Node{
		"a": {ID: "a"},
		"b": {ID: "b"},
		"c": {ID: "c"},
		"d": {ID: "d"},
		"e": {ID: "e"},
		"f": {ID: "f"},
		"g": {ID: "g"},
		"h": {ID: "h"},
	}

	t.Run("tail count wins first", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"a": {tailCount: 0, headCount: 3, replicaCount: 5},
			"b": {tailCount: 1, headCount: 0, replicaCount: 0},
		}
		if got := compareNodes(nodes["a"], nodes["b"], counts); got >= 0 {
			t.Fatalf("compareNodes(a, b) = %d, want a to win on lower tail count", got)
		}
	})

	t.Run("head count breaks tail tie", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"c": {tailCount: 1, headCount: 0, replicaCount: 5},
			"d": {tailCount: 1, headCount: 1, replicaCount: 0},
		}
		if got := compareNodes(nodes["c"], nodes["d"], counts); got >= 0 {
			t.Fatalf("compareNodes(c, d) = %d, want c to win on lower head count", got)
		}
	})

	t.Run("replica count breaks head tie", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"e": {tailCount: 1, headCount: 1, replicaCount: 0},
			"f": {tailCount: 1, headCount: 1, replicaCount: 1},
		}
		if got := compareNodes(nodes["e"], nodes["f"], counts); got >= 0 {
			t.Fatalf("compareNodes(e, f) = %d, want e to win on lower replica count", got)
		}
	})

	t.Run("node ID breaks full tie", func(t *testing.T) {
		counts := map[string]assignmentCounts{
			"g": {},
			"h": {},
		}
		if got := compareNodes(nodes["g"], nodes["h"], counts); got >= 0 {
			t.Fatalf("compareNodes(g, h) = %d, want g to win on lower node ID", got)
		}
	})
}

func TestValidationAndBootstrapFailures(t *testing.T) {
	testCases := []struct {
		name  string
		cfg   Config
		nodes []Node
		want  error
	}{
		{
			name:  "invalid slot count",
			cfg:   Config{SlotCount: 0, ReplicationFactor: 1},
			nodes: uniqueNodes("a"),
			want:  ErrInvalidConfig,
		},
		{
			name:  "invalid replication factor",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 0},
			nodes: uniqueNodes("a"),
			want:  ErrInvalidConfig,
		},
		{
			name:  "empty node list",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: nil,
			want:  nil,
		},
		{
			name:  "empty node ID",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: []Node{{ID: "", FailureDomains: map[string]string{"host": "h1"}}},
			want:  ErrInvalidNode,
		},
		{
			name: "duplicate node ID",
			cfg:  Config{SlotCount: 1, ReplicationFactor: 1},
			nodes: []Node{
				uniqueNode("a"),
				uniqueNode("a"),
			},
			want: ErrInvalidNode,
		},
		{
			name:  "replication factor exceeds nodes",
			cfg:   Config{SlotCount: 1, ReplicationFactor: 2},
			nodes: uniqueNodes("a"),
			want:  ErrInvalidConfig,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := BuildInitialPlacement(tc.cfg, tc.nodes)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("BuildInitialPlacement returned error: %v", err)
				}
				if got, want := len(state.Chains), tc.cfg.SlotCount; got != want {
					t.Fatalf("chain count = %d, want %d", got, want)
				}
				return
			}
			if err == nil {
				t.Fatal("BuildInitialPlacement unexpectedly succeeded")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func mustBuildInitialState(t *testing.T, cfg Config, nodes []Node) *ClusterState {
	t.Helper()

	state, err := BuildInitialPlacement(cfg, nodes)
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	return state
}

func replicaNodeStates(chain Chain) []string {
	values := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		values = append(values, replica.NodeID+":"+string(replica.State))
	}
	return values
}

func uniqueNodes(ids ...string) []Node {
	nodes := make([]Node, 0, len(ids))
	for _, id := range ids {
		nodes = append(nodes, uniqueNode(id))
	}
	return nodes
}

func uniqueNode(id string) Node {
	return Node{
		ID: id,
		FailureDomains: map[string]string{
			"host": "host-" + id,
			"rack": "rack-" + id,
			"az":   "az-" + id,
		},
	}
}
