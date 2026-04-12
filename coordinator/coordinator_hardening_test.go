package coordinator

import (
	"errors"
	"reflect"
	"testing"
)

func TestBuildInitialPlacementReplicationFactorOne(t *testing.T) {
	state, err := BuildInitialPlacement(Config{
		SlotCount:         4,
		ReplicationFactor: 1,
	}, uniqueNodes("a", "b"))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}

	assertChainInvariants(t, state)
	for _, chain := range state.Chains {
		if got, want := len(chain.Replicas), 1; got != want {
			t.Fatalf("chain %d replica count = %d, want %d", chain.Slot, got, want)
		}
	}
}

func TestBuildInitialPlacementFailsWhenFailureDomainsExhausted(t *testing.T) {
	nodes := []Node{
		{ID: "a", FailureDomains: map[string]string{"rack": "r1", "az": "az1"}},
		{ID: "b", FailureDomains: map[string]string{"rack": "r1", "az": "az2"}},
		{ID: "c", FailureDomains: map[string]string{"rack": "r2", "az": "az1"}},
	}

	_, err := BuildInitialPlacement(Config{
		SlotCount:         1,
		ReplicationFactor: 3,
	}, nodes)
	if err == nil {
		t.Fatal("BuildInitialPlacement unexpectedly succeeded")
	}
	if !errors.Is(err, ErrPlacementImpossible) {
		t.Fatalf("error = %v, want placement impossible", err)
	}
}

func TestBuildInitialPlacementIsDeterministicAcrossReorderedInput(t *testing.T) {
	cfg := Config{SlotCount: 8, ReplicationFactor: 2}
	ordered := []Node{
		uniqueNode("b"),
		uniqueNode("a"),
		uniqueNode("d"),
		uniqueNode("c"),
	}
	reordered := []Node{
		ordered[3],
		ordered[2],
		ordered[1],
		ordered[0],
	}

	left, err := BuildInitialPlacement(cfg, ordered)
	if err != nil {
		t.Fatalf("BuildInitialPlacement(ordered) returned error: %v", err)
	}
	right, err := BuildInitialPlacement(cfg, reordered)
	if err != nil {
		t.Fatalf("BuildInitialPlacement(reordered) returned error: %v", err)
	}

	if !reflect.DeepEqual(left.Chains, right.Chains) {
		t.Fatalf("chains differ across reordered input\nleft=%v\nright=%v", left.Chains, right.Chains)
	}
}

func TestBuildInitialPlacementMaintainsInvariantsAcrossTopologies(t *testing.T) {
	testCases := []struct {
		name  string
		cfg   Config
		nodes []Node
	}{
		{
			name:  "four nodes rf2",
			cfg:   Config{SlotCount: 16, ReplicationFactor: 2},
			nodes: uniqueNodes("a", "b", "c", "d"),
		},
		{
			name: "five nodes rf3",
			cfg:  Config{SlotCount: 16, ReplicationFactor: 3},
			nodes: []Node{
				{ID: "a", FailureDomains: map[string]string{"host": "h1", "rack": "r1", "az": "az1"}},
				{ID: "b", FailureDomains: map[string]string{"host": "h2", "rack": "r2", "az": "az2"}},
				{ID: "c", FailureDomains: map[string]string{"host": "h3", "rack": "r3", "az": "az3"}},
				{ID: "d", FailureDomains: map[string]string{"host": "h4", "rack": "r4", "az": "az4"}},
				{ID: "e", FailureDomains: map[string]string{"host": "h5", "rack": "r5", "az": "az5"}},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := BuildInitialPlacement(tc.cfg, tc.nodes)
			if err != nil {
				t.Fatalf("BuildInitialPlacement returned error: %v", err)
			}
			assertChainInvariants(t, state)
		})
	}
}

func TestPlanReconfigurationStateValidation(t *testing.T) {
	t.Run("negative max changed chains", func(t *testing.T) {
		state := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 1}, uniqueNodes("a"))
		_, err := PlanReconfiguration(*state, nil, ReconfigurationPolicy{MaxChangedChains: -1})
		if err == nil {
			t.Fatal("PlanReconfiguration unexpectedly succeeded")
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want invalid config", err)
		}
	})

	t.Run("mismatched slot count", func(t *testing.T) {
		state := validTestState()
		state.SlotCount = 2
		_, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{})
		if err == nil {
			t.Fatal("PlanReconfiguration unexpectedly succeeded")
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want invalid config", err)
		}
	})

	t.Run("chain references unknown node", func(t *testing.T) {
		state := validTestState()
		state.Chains[0].Replicas[0].NodeID = "missing"
		_, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{})
		if err == nil {
			t.Fatal("PlanReconfiguration unexpectedly succeeded")
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want invalid config", err)
		}
	})

	t.Run("duplicate node order entry", func(t *testing.T) {
		state := validTestState()
		state.NodeOrder = []string{"a", "a", "b"}
		_, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{})
		if err == nil {
			t.Fatal("PlanReconfiguration unexpectedly succeeded")
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want invalid config", err)
		}
	})

	t.Run("node order references unknown node", func(t *testing.T) {
		state := validTestState()
		state.NodeOrder = []string{"a", "ghost", "b"}
		_, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{})
		if err == nil {
			t.Fatal("PlanReconfiguration unexpectedly succeeded")
		}
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want invalid config", err)
		}
	})

	t.Run("missing node health defaults to alive", func(t *testing.T) {
		state := validTestState()
		state.NodeHealthByID = map[string]NodeHealth{}
		plan, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{})
		if err != nil {
			t.Fatalf("PlanReconfiguration returned error: %v", err)
		}
		if got, want := plan.UpdatedState.NodeHealthByID["a"], NodeHealthAlive; got != want {
			t.Fatalf("node health = %q, want %q", got, want)
		}
		if got, want := plan.UpdatedState.NodeHealthByID["b"], NodeHealthAlive; got != want {
			t.Fatalf("node health = %q, want %q", got, want)
		}
	})
}

func TestPlanReconfigurationEventValidation(t *testing.T) {
	base := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 1}, uniqueNodes("a"))

	testCases := []struct {
		name  string
		state ClusterState
		event Event
		want  error
	}{
		{
			name:  "unsupported event kind",
			state: *base,
			event: Event{Kind: EventKind("bad_event")},
			want:  ErrInvalidEvent,
		},
		{
			name:  "duplicate add node",
			state: *base,
			event: Event{Kind: EventKindAddNode, Node: uniqueNode("a")},
			want:  ErrInvalidEvent,
		},
		{
			name:  "invalid add node metadata",
			state: *base,
			event: Event{Kind: EventKindAddNode, Node: Node{ID: "", FailureDomains: map[string]string{"host": "h1"}}},
			want:  ErrInvalidNode,
		},
		{
			name:  "drain unknown node",
			state: *base,
			event: Event{Kind: EventKindBeginDrainNode, NodeID: "missing"},
			want:  ErrInvalidEvent,
		},
		{
			name: "drain dead node",
			state: func() ClusterState {
				state := *base
				state.NodeHealthByID["a"] = NodeHealthDead
				return state
			}(),
			event: Event{Kind: EventKindBeginDrainNode, NodeID: "a"},
			want:  ErrInvalidEvent,
		},
		{
			name:  "mark dead unknown node",
			state: *base,
			event: Event{Kind: EventKindMarkNodeDead, NodeID: "missing"},
			want:  ErrInvalidEvent,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := PlanReconfiguration(tc.state, []Event{tc.event}, ReconfigurationPolicy{})
			if err == nil {
				t.Fatal("PlanReconfiguration unexpectedly succeeded")
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestPlanReconfigurationCallerOrderedEventsCanChangeOutcome(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 2, ReplicationFactor: 2}, uniqueNodes("a", "b", "c"))
	policy := ReconfigurationPolicy{MaxChangedChains: 1}

	addThenDrain, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindAddNode, Node: uniqueNode("d")},
		{Kind: EventKindBeginDrainNode, NodeID: "a"},
	}, policy)
	if err != nil {
		t.Fatalf("PlanReconfiguration(add then drain) returned error: %v", err)
	}

	drainThenAdd, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindBeginDrainNode, NodeID: "a"},
		{Kind: EventKindAddNode, Node: uniqueNode("d")},
	}, policy)
	if err != nil {
		t.Fatalf("PlanReconfiguration(drain then add) returned error: %v", err)
	}

	if reflect.DeepEqual(addThenDrain, drainThenAdd) {
		t.Fatal("plans unexpectedly match across different event order")
	}
	if got, want := addThenDrain.UnassignedNodeIDs, []string{}; !reflect.DeepEqual(got, want) {
		t.Fatalf("addThenDrain unassigned = %v, want %v", got, want)
	}
	if got, want := drainThenAdd.UnassignedNodeIDs, []string{"d"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("drainThenAdd unassigned = %v, want %v", got, want)
	}
}

func TestApplyProgressValidation(t *testing.T) {
	base := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 1}, uniqueNodes("a"))

	t.Run("activate non joining fails", func(t *testing.T) {
		_, err := ApplyProgress(*base, Event{
			Kind:   EventKindReplicaBecameActive,
			Slot:   0,
			NodeID: "a",
		})
		if err == nil {
			t.Fatal("ApplyProgress unexpectedly succeeded")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Fatalf("error = %v, want illegal state transition", err)
		}
	})

	t.Run("remove active alive replica fails", func(t *testing.T) {
		_, err := ApplyProgress(*base, Event{
			Kind:   EventKindReplicaRemoved,
			Slot:   0,
			NodeID: "a",
		})
		if err == nil {
			t.Fatal("ApplyProgress unexpectedly succeeded")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Fatalf("error = %v, want illegal state transition", err)
		}
	})

	t.Run("invalid slot fails", func(t *testing.T) {
		_, err := ApplyProgress(*base, Event{
			Kind:   EventKindReplicaRemoved,
			Slot:   5,
			NodeID: "a",
		})
		if err == nil {
			t.Fatal("ApplyProgress unexpectedly succeeded")
		}
		if !errors.Is(err, ErrInvalidEvent) {
			t.Fatalf("error = %v, want invalid event", err)
		}
	})

	t.Run("unknown node in slot fails", func(t *testing.T) {
		_, err := ApplyProgress(*base, Event{
			Kind:   EventKindReplicaRemoved,
			Slot:   0,
			NodeID: "missing",
		})
		if err == nil {
			t.Fatal("ApplyProgress unexpectedly succeeded")
		}
		if !errors.Is(err, ErrIllegalStateTransition) {
			t.Fatalf("error = %v, want illegal state transition", err)
		}
	})
}

func TestApplyProgressOnlyMutatesTargetedSlot(t *testing.T) {
	state := ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
					{NodeID: "b", State: ReplicaStateJoining},
				},
			},
			{
				Slot: 1,
				Replicas: []Replica{
					{NodeID: "c", State: ReplicaStateActive},
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
		SlotCount:         2,
		ReplicationFactor: 1,
	}

	updated, err := ApplyProgress(state, Event{
		Kind:   EventKindReplicaBecameActive,
		Slot:   0,
		NodeID: "b",
	})
	if err != nil {
		t.Fatalf("ApplyProgress returned error: %v", err)
	}

	if got, want := replicaNodeStates(updated.Chains[0]), []string{"a:active", "b:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slot 0 = %v, want %v", got, want)
	}
	if got, want := replicaNodeStates(updated.Chains[1]), []string{"c:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("slot 1 = %v, want %v", got, want)
	}
}

func TestPlanReconfigurationDrainHeadAndTail(t *testing.T) {
	testCases := []struct {
		name          string
		drainNodeID   string
		wantAfterJoin []string
		wantAfterMark []string
	}{
		{
			name:          "head",
			drainNodeID:   "a",
			wantAfterJoin: []string{"a:active", "b:active", "c:active", "d:joining"},
			wantAfterMark: []string{"b:active", "c:active", "d:active", "a:leaving"},
		},
		{
			name:          "tail",
			drainNodeID:   "c",
			wantAfterJoin: []string{"a:active", "b:active", "c:active", "d:joining"},
			wantAfterMark: []string{"a:active", "b:active", "d:active", "c:leaving"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d"))

			plan, err := PlanReconfiguration(*state, []Event{
				{Kind: EventKindBeginDrainNode, NodeID: tc.drainNodeID},
			}, ReconfigurationPolicy{})
			if err != nil {
				t.Fatalf("PlanReconfiguration returned error: %v", err)
			}
			if got := replicaNodeStates(plan.ChangedSlots[0].After); !reflect.DeepEqual(got, tc.wantAfterJoin) {
				t.Fatalf("after append = %v, want %v", got, tc.wantAfterJoin)
			}

			progressed, err := ApplyProgress(plan.UpdatedState, Event{
				Kind:   EventKindReplicaBecameActive,
				Slot:   0,
				NodeID: "d",
			})
			if err != nil {
				t.Fatalf("ApplyProgress returned error: %v", err)
			}

			nextPlan, err := PlanReconfiguration(*progressed, nil, ReconfigurationPolicy{})
			if err != nil {
				t.Fatalf("PlanReconfiguration after activate returned error: %v", err)
			}
			if got := replicaNodeStates(nextPlan.ChangedSlots[0].After); !reflect.DeepEqual(got, tc.wantAfterMark) {
				t.Fatalf("after mark leaving = %v, want %v", got, tc.wantAfterMark)
			}
		})
	}
}

func TestPlanReconfigurationDeadHeadAndTailRepair(t *testing.T) {
	testCases := []struct {
		name      string
		deadNode  string
		wantAfter []string
	}{
		{
			name:      "head",
			deadNode:  "a",
			wantAfter: []string{"b:active", "c:active", "d:joining"},
		},
		{
			name:      "tail",
			deadNode:  "c",
			wantAfter: []string{"a:active", "b:active", "d:joining"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			state := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d"))
			plan, err := PlanReconfiguration(*state, []Event{
				{Kind: EventKindMarkNodeDead, NodeID: tc.deadNode},
			}, ReconfigurationPolicy{MaxChangedChains: 0})
			if err != nil {
				t.Fatalf("PlanReconfiguration returned error: %v", err)
			}
			if got := replicaNodeStates(plan.ChangedSlots[0].After); !reflect.DeepEqual(got, tc.wantAfter) {
				t.Fatalf("after repair = %v, want %v", got, tc.wantAfter)
			}
		})
	}
}

func TestPlanReconfigurationRepairImpossibleWhenNoEligibleReplacementExists(t *testing.T) {
	state := ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
					{NodeID: "b", State: ReplicaStateActive},
					{NodeID: "c", State: ReplicaStateActive},
				},
			},
		},
		NodesByID: map[string]Node{
			"a": {ID: "a", FailureDomains: map[string]string{"rack": "r1", "az": "az1"}},
			"b": {ID: "b", FailureDomains: map[string]string{"rack": "r2", "az": "az2"}},
			"c": {ID: "c", FailureDomains: map[string]string{"rack": "r3", "az": "az3"}},
			"d": {ID: "d", FailureDomains: map[string]string{"rack": "r1", "az": "az4"}},
		},
		NodeHealthByID: map[string]NodeHealth{
			"a": NodeHealthAlive,
			"b": NodeHealthAlive,
			"c": NodeHealthAlive,
			"d": NodeHealthAlive,
		},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b", "c", "d"},
		SlotCount:         1,
		ReplicationFactor: 3,
	}

	plan, err := PlanReconfiguration(state, []Event{
		{Kind: EventKindMarkNodeDead, NodeID: "b"},
	}, ReconfigurationPolicy{})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}
	if got, want := replicaNodeStates(plan.UpdatedState.Chains[0]), []string{"a:active", "c:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("updated chain = %v, want %v", got, want)
	}
	if len(plan.ChangedSlots) != 1 {
		t.Fatalf("changed slot count = %d, want 1", len(plan.ChangedSlots))
	}
	if len(plan.ChangedSlots[0].Steps) != 0 {
		t.Fatalf("repair steps = %#v, want none until a replacement becomes eligible later", plan.ChangedSlots[0].Steps)
	}
}

func TestPlanReconfigurationJoinUsesDeterministicSlotSelection(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))

	plan, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindAddNode, Node: uniqueNode("d")},
	}, ReconfigurationPolicy{MaxChangedChains: 2})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}

	if got, want := changedSlotIDs(plan.ChangedSlots), []int{1, 4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("changed slots = %v, want %v", got, want)
	}
}

func TestPlanReconfigurationMandatoryRepairIgnoresDisruptionBudget(t *testing.T) {
	state := mustBuildInitialState(t, Config{SlotCount: 1, ReplicationFactor: 3}, uniqueNodes("a", "b", "c", "d"))

	plan, err := PlanReconfiguration(*state, []Event{
		{Kind: EventKindMarkNodeDead, NodeID: "b"},
	}, ReconfigurationPolicy{MaxChangedChains: 0})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}
	if len(plan.ChangedSlots) == 0 {
		t.Fatal("expected mandatory repair even with zero disruption budget")
	}
}

func TestPlanReconfigurationStartupAppendHonorsChangedChainBudget(t *testing.T) {
	state := ClusterState{
		Chains:            emptyChains(8),
		NodesByID:         map[string]Node{"a": uniqueNode("a"), "b": uniqueNode("b"), "c": uniqueNode("c")},
		NodeHealthByID:    map[string]NodeHealth{"a": NodeHealthAlive, "b": NodeHealthAlive, "c": NodeHealthAlive},
		ReadyNodeIDs:      map[string]bool{"a": true, "b": true, "c": true},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b", "c"},
		SlotCount:         8,
		ReplicationFactor: 3,
	}

	plan, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{MaxChangedChains: 2})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}
	if got, want := len(plan.ChangedSlots), 2; got != want {
		t.Fatalf("changed slot count = %d, want %d", got, want)
	}
	for _, slotPlan := range plan.ChangedSlots {
		if got, want := len(slotPlan.After.Replicas), 1; got != want {
			t.Fatalf("slot %d replica count after startup append = %d, want %d", slotPlan.Slot, got, want)
		}
		if got, want := slotPlan.After.Replicas[0].State, ReplicaStateJoining; got != want {
			t.Fatalf("slot %d replica state = %q, want %q", slotPlan.Slot, got, want)
		}
	}
}

func TestPlanReconfigurationSecondWaveAppendHonorsChangedChainBudget(t *testing.T) {
	desired := mustBuildInitialState(t, Config{SlotCount: 8, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))
	state := ClusterState{
		Chains:            make([]Chain, len(desired.Chains)),
		NodesByID:         map[string]Node{},
		NodeHealthByID:    map[string]NodeHealth{},
		ReadyNodeIDs:      map[string]bool{},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         append([]string(nil), desired.NodeOrder...),
		SlotCount:         desired.SlotCount,
		ReplicationFactor: desired.ReplicationFactor,
	}
	for nodeID, node := range desired.NodesByID {
		state.NodesByID[nodeID] = node
	}
	for nodeID, health := range desired.NodeHealthByID {
		state.NodeHealthByID[nodeID] = health
	}
	for nodeID, ready := range desired.ReadyNodeIDs {
		state.ReadyNodeIDs[nodeID] = ready
	}
	for nodeID, draining := range desired.DrainingNodeIDs {
		state.DrainingNodeIDs[nodeID] = draining
	}
	for i, chain := range desired.Chains {
		state.Chains[i] = Chain{
			Slot:     chain.Slot,
			Replicas: append([]Replica(nil), chain.Replicas...),
		}
	}
	for slot := range state.Chains {
		state.Chains[slot].Replicas = []Replica{state.Chains[slot].Replicas[0]}
	}

	plan, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{MaxChangedChains: 2})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}
	if got, want := len(plan.ChangedSlots), 2; got != want {
		t.Fatalf("changed slot count = %d, want %d", got, want)
	}
	for _, slotPlan := range plan.ChangedSlots {
		if got, want := len(slotPlan.After.Replicas), 2; got != want {
			t.Fatalf("slot %d replica count after second-wave append = %d, want %d", slotPlan.Slot, got, want)
		}
		if got, want := slotPlan.After.Replicas[1].State, ReplicaStateJoining; got != want {
			t.Fatalf("slot %d tail replica state = %q, want %q", slotPlan.Slot, got, want)
		}
	}
}

func TestPlanReconfigurationStartupAppendPrioritizesEmptyChainsBeforeDeepening(t *testing.T) {
	desired := mustBuildInitialState(t, Config{SlotCount: 4, ReplicationFactor: 3}, uniqueNodes("a", "b", "c"))
	state := ClusterState{
		Chains:            make([]Chain, len(desired.Chains)),
		NodesByID:         map[string]Node{},
		NodeHealthByID:    map[string]NodeHealth{},
		ReadyNodeIDs:      map[string]bool{},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         append([]string(nil), desired.NodeOrder...),
		SlotCount:         desired.SlotCount,
		ReplicationFactor: desired.ReplicationFactor,
	}
	for nodeID, node := range desired.NodesByID {
		state.NodesByID[nodeID] = node
	}
	for nodeID, health := range desired.NodeHealthByID {
		state.NodeHealthByID[nodeID] = health
	}
	for nodeID, ready := range desired.ReadyNodeIDs {
		state.ReadyNodeIDs[nodeID] = ready
	}
	for nodeID, draining := range desired.DrainingNodeIDs {
		state.DrainingNodeIDs[nodeID] = draining
	}
	for i, chain := range desired.Chains {
		state.Chains[i] = Chain{
			Slot:     chain.Slot,
			Replicas: nil,
		}
	}
	state.Chains[0].Replicas = []Replica{{NodeID: desired.Chains[0].Replicas[0].NodeID, State: ReplicaStateActive}}
	state.Chains[1].Replicas = []Replica{{NodeID: desired.Chains[1].Replicas[0].NodeID, State: ReplicaStateActive}}

	plan, err := PlanReconfiguration(state, nil, ReconfigurationPolicy{MaxChangedChains: 2})
	if err != nil {
		t.Fatalf("PlanReconfiguration returned error: %v", err)
	}
	if got, want := changedSlotIDs(plan.ChangedSlots), []int{2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("changed slots = %v, want %v", got, want)
	}
}

func TestAppendJoinRebalanceSkipsWhenContinuityCheckFails(t *testing.T) {
	state := ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
					{NodeID: "b", State: ReplicaStateActive},
					{NodeID: "c", State: ReplicaStateActive},
				},
			},
		},
		NodesByID: map[string]Node{
			"a": uniqueNode("a"),
			"b": uniqueNode("b"),
			"c": uniqueNode("c"),
			"d": uniqueNode("d"),
		},
		NodeHealthByID: map[string]NodeHealth{
			"a": NodeHealthAlive,
			"b": NodeHealthAlive,
			"c": NodeHealthAlive,
			"d": NodeHealthAlive,
		},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b", "c", "d"},
		SlotCount:         1,
		ReplicationFactor: 3,
	}

	slotPlan, changed, err := appendJoinRebalance(&state, 0, Chain{
		Slot: 0,
		Replicas: []Replica{
			{NodeID: "c", State: ReplicaStateActive},
			{NodeID: "a", State: ReplicaStateActive},
			{NodeID: "d", State: ReplicaStateActive},
		},
	})
	if err != nil {
		t.Fatalf("appendJoinRebalance returned error: %v", err)
	}
	if changed {
		t.Fatalf("appendJoinRebalance unexpectedly changed slot: %+v", slotPlan)
	}
}

func TestAppendJoinRebalanceRejectsDisjointRemap(t *testing.T) {
	state := ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
					{NodeID: "b", State: ReplicaStateActive},
					{NodeID: "c", State: ReplicaStateActive},
				},
			},
		},
		NodesByID: map[string]Node{
			"a": uniqueNode("a"),
			"b": uniqueNode("b"),
			"c": uniqueNode("c"),
			"d": uniqueNode("d"),
			"e": uniqueNode("e"),
			"f": uniqueNode("f"),
		},
		NodeHealthByID: map[string]NodeHealth{
			"a": NodeHealthAlive,
			"b": NodeHealthAlive,
			"c": NodeHealthAlive,
			"d": NodeHealthAlive,
			"e": NodeHealthAlive,
			"f": NodeHealthAlive,
		},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b", "c", "d", "e", "f"},
		SlotCount:         1,
		ReplicationFactor: 3,
	}

	_, changed, err := appendJoinRebalance(&state, 0, Chain{
		Slot: 0,
		Replicas: []Replica{
			{NodeID: "d", State: ReplicaStateActive},
			{NodeID: "e", State: ReplicaStateActive},
			{NodeID: "f", State: ReplicaStateActive},
		},
	})
	if changed {
		t.Fatal("appendJoinRebalance unexpectedly changed slot")
	}
	if err == nil {
		t.Fatal("appendJoinRebalance unexpectedly succeeded")
	}
	if !errors.Is(err, ErrBrokenChain) {
		t.Fatalf("error = %v, want broken chain", err)
	}
}

func TestHelperEdgeCases(t *testing.T) {
	t.Run("sameRelativeOrderWithout", func(t *testing.T) {
		if !sameRelativeOrderWithout([]string{"a", "b", "c"}, "b", []string{"a", "c", "d"}, "d") {
			t.Fatal("sameRelativeOrderWithout unexpectedly false")
		}
		if sameRelativeOrderWithout([]string{"a", "b", "c"}, "b", []string{"c", "a", "d"}, "d") {
			t.Fatal("sameRelativeOrderWithout unexpectedly true")
		}
	})

	t.Run("sameRelativeOrderForJoinFinalization", func(t *testing.T) {
		if !sameRelativeOrderForJoinFinalization([]string{"a", "b", "c", "d"}, "c", "d", []string{"d", "a", "b"}) {
			t.Fatal("sameRelativeOrderForJoinFinalization unexpectedly false")
		}
		if sameRelativeOrderForJoinFinalization([]string{"a", "b", "c", "d"}, "c", "d", []string{"b", "d", "a"}) {
			t.Fatal("sameRelativeOrderForJoinFinalization unexpectedly true")
		}
	})

	t.Run("oneMemberReplacement", func(t *testing.T) {
		added, removed, ok := oneMemberReplacement([]string{"a", "b", "c"}, []string{"a", "b", "d"})
		if !ok || added != "d" || removed != "c" {
			t.Fatalf("oneMemberReplacement = (%q, %q, %v), want (%q, %q, true)", added, removed, ok, "d", "c")
		}
		if _, _, ok := oneMemberReplacement([]string{"a", "b", "c"}, []string{"d", "e", "f"}); ok {
			t.Fatal("oneMemberReplacement unexpectedly succeeded")
		}
	})

	t.Run("extraNode", func(t *testing.T) {
		if nodeID, ok := extraNode([]string{"a", "b", "c", "d"}, []string{"a", "b", "d"}); !ok || nodeID != "c" {
			t.Fatalf("extraNode = (%q, %v), want (%q, true)", nodeID, ok, "c")
		}
		if _, ok := extraNode([]string{"a", "b", "c"}, []string{"a", "b", "c"}); ok {
			t.Fatal("extraNode unexpectedly succeeded")
		}
	})

	t.Run("disjointChain", func(t *testing.T) {
		if !disjointChain([]string{"a", "b"}, []string{"c", "d"}) {
			t.Fatal("disjointChain unexpectedly false")
		}
		if disjointChain([]string{"a", "b"}, []string{"b", "c"}) {
			t.Fatal("disjointChain unexpectedly true")
		}
	})

	t.Run("pending state helpers", func(t *testing.T) {
		chain := Chain{
			Replicas: []Replica{
				{NodeID: "a", State: ReplicaStateActive},
				{NodeID: "b", State: ReplicaStateJoining},
				{NodeID: "c", State: ReplicaStateLeaving},
			},
		}
		if !hasJoiningReplica(chain) {
			t.Fatal("hasJoiningReplica unexpectedly false")
		}
		if !hasLeavingReplica(chain) {
			t.Fatal("hasLeavingReplica unexpectedly false")
		}
		if !hasPendingReplicaState(chain) {
			t.Fatal("hasPendingReplicaState unexpectedly false")
		}
		if got, want := firstActiveDrainingReplicaID(chain, map[string]bool{"a": true}), "a"; got != want {
			t.Fatalf("firstActiveDrainingReplicaID = %q, want %q", got, want)
		}
	})
}

func assertChainInvariants(t *testing.T, state *ClusterState) {
	t.Helper()

	if got, want := len(state.Chains), state.SlotCount; got != want {
		t.Fatalf("chain count = %d, want %d", got, want)
	}
	for _, chain := range state.Chains {
		if got, want := len(chain.Replicas), state.ReplicationFactor; got != want {
			t.Fatalf("chain %d replica count = %d, want %d", chain.Slot, got, want)
		}
		seen := make(map[string]struct{}, len(chain.Replicas))
		for _, replica := range chain.Replicas {
			if _, exists := seen[replica.NodeID]; exists {
				t.Fatalf("chain %d contains duplicate node %q", chain.Slot, replica.NodeID)
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

func validTestState() ClusterState {
	return ClusterState{
		Chains: []Chain{
			{
				Slot: 0,
				Replicas: []Replica{
					{NodeID: "a", State: ReplicaStateActive},
				},
			},
		},
		NodesByID: map[string]Node{
			"a": uniqueNode("a"),
			"b": uniqueNode("b"),
		},
		NodeHealthByID: map[string]NodeHealth{
			"a": NodeHealthAlive,
			"b": NodeHealthAlive,
		},
		DrainingNodeIDs:   map[string]bool{},
		NodeOrder:         []string{"a", "b"},
		SlotCount:         1,
		ReplicationFactor: 1,
	}
}

func changedSlotIDs(slotPlans []SlotPlan) []int {
	ids := make([]int, 0, len(slotPlans))
	for _, slotPlan := range slotPlans {
		ids = append(ids, slotPlan.Slot)
	}
	return ids
}
