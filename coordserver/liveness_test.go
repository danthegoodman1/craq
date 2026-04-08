package coordserver

import (
	"context"
	"reflect"
	"testing"
	"time"

	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/ops"
	"github.com/danthegoodman1/craq/storage"
	"github.com/prometheus/client_golang/prometheus"
)

func TestHeartbeatPersistsHealthyLiveness(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*recordingNodeClient{"a": newRecordingNodeClient("a")}
	clock := &fakeClock{now: time.Unix(0, 100)}
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), mapToClient(nodes), ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if _, err := server.RegisterNode(ctx, storage.NodeRegistration{NodeID: "a"}); err != nil {
		t.Fatalf("RegisterNode returned error: %v", err)
	}

	status := storage.NodeStatus{NodeID: "a", ReplicaCount: 2, ActiveCount: 1}
	if err := server.ReportNodeHeartbeat(ctx, status); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	if got, want := server.Heartbeats()["a"], status; !reflect.DeepEqual(got, want) {
		t.Fatalf("heartbeat = %#v, want %#v", got, want)
	}
	record := server.Liveness()["a"]
	if got, want := record.State, coordruntime.NodeLivenessStateHealthy; got != want {
		t.Fatalf("liveness state = %q, want %q", got, want)
	}
	if got, want := record.LastHeartbeatUnixNano, clock.now.UnixNano(); got != want {
		t.Fatalf("last heartbeat = %d, want %d", got, want)
	}
}

func TestRepeatedHeartbeatsDoNotAdvanceDurableStateAfterReady(t *testing.T) {
	ctx := context.Background()
	store := coordruntime.NewInMemoryStore()
	repl := storage.NewInMemoryReplicationTransport()
	backend := storage.NewInMemoryBackend()
	repl.Register("d", backend)
	adapter, err := NewInMemoryNodeAdapter(ctx, "d", backend, repl)
	if err != nil {
		t.Fatalf("NewInMemoryNodeAdapter returned error: %v", err)
	}
	repl.RegisterNode("d", adapter.Node())

	server := mustOpenServerWithConfig(t, store, map[string]StorageNodeClient{"d": adapter}, ServerConfig{})
	adapter.BindServer(server)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := adapter.Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("initial ReportHeartbeat returned error: %v", err)
	}
	first := server.Current()
	if err := adapter.Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("second ReportHeartbeat returned error: %v", err)
	}
	second := server.Current()
	if got, want := second.Version, first.Version; got != want {
		t.Fatalf("version after repeated heartbeat = %d, want %d", got, want)
	}
	if got, want := second.LastLogIndex, first.LastLogIndex; got != want {
		t.Fatalf("last log index after repeated heartbeat = %d, want %d", got, want)
	}
	if got, want := len(second.AppliedCommands), len(first.AppliedCommands); got != want {
		t.Fatalf("applied command count after repeated heartbeat = %d, want %d", got, want)
	}
}

func TestRecoveryHeartbeatPersistsHealthyTransitionOnce(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	server := mustBootstrappedServerWithConfig(
		t,
		ctx,
		mapToClient(map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
		}),
		ServerConfig{
			LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
			Clock:          clock,
		},
		4,
		3,
		"a", "b", "c",
	)
	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("initial ReportNodeHeartbeat returned error: %v", err)
	}
	initial := server.Current()

	clock.Advance(6 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	suspect := server.Current()
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateSuspect; got != want {
		t.Fatalf("suspect state = %q, want %q", got, want)
	}
	if suspect.Version <= initial.Version {
		t.Fatalf("version after suspect = %d, want > %d", suspect.Version, initial.Version)
	}

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("recovery ReportNodeHeartbeat returned error: %v", err)
	}
	recovered := server.Current()
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateHealthy; got != want {
		t.Fatalf("recovered state = %q, want %q", got, want)
	}
	if got, want := recovered.Version, suspect.Version+1; got != want {
		t.Fatalf("version after recovery heartbeat = %d, want %d", got, want)
	}

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("steady-state ReportNodeHeartbeat returned error: %v", err)
	}
	steady := server.Current()
	if got, want := steady.Version, recovered.Version; got != want {
		t.Fatalf("version after steady-state heartbeat = %d, want %d", got, want)
	}
}

func TestEvaluateLivenessTransitionsSuspectThenHealthyWithoutRoutingChange(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	server := mustBootstrappedServerWithConfig(
		t,
		ctx,
		mapToClient(map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
		}),
		ServerConfig{
			LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
			Clock:          clock,
		},
		4,
		3,
		"a", "b", "c",
	)
	before, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}

	clock.Advance(6 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateSuspect; got != want {
		t.Fatalf("suspect state = %q, want %q", got, want)
	}
	afterSuspect, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot after suspect returned error: %v", err)
	}
	if !reflect.DeepEqual(before.Slots, afterSuspect.Slots) {
		t.Fatalf("routing slots changed on suspect\nbefore=%#v\nafter=%#v", before.Slots, afterSuspect.Slots)
	}

	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat after suspect returned error: %v", err)
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateHealthy; got != want {
		t.Fatalf("state after recovery heartbeat = %q, want %q", got, want)
	}
}

func TestDeadTransitionTriggersRepairAndDoesNotDuplicate(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})
	if err := h.adapters["b"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}

	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	if got, want := server.Liveness()["b"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("dead state = %q, want %q", got, want)
	}
	if got, want := server.Pending()[0], (PendingWork{
		Slot:        0,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[0],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending repair = %#v, want %#v", got, want)
	}

	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("second EvaluateLiveness returned error: %v", err)
	}
	if err := h.adapters["b"].Node().ReportHeartbeat(ctx); err == nil {
		t.Fatal("ReportHeartbeat after dead unexpectedly succeeded")
	}
	if !nodeMarkedDead(server.Current().Cluster, "b") {
		t.Fatal("node b was unexpectedly allowed back into cluster membership")
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain = %v, want %v", got, want)
	}
}

func TestDeadTransitionRepairCompletesWithDelayedQueuedProgress(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	h.adapters["d"].EnableQueuedProgress()
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c", "d"})
	if err := h.adapters["b"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}

	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if got, want := h.adapters["d"].PendingProgress(), 1; got != want {
		t.Fatalf("queued progress = %d, want %d", got, want)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:joining"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("chain before delayed ready delivery = %v, want %v", got, want)
	}
	if err := h.adapters["d"].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress returned error: %v", err)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain after delayed ready delivery = %v, want %v", got, want)
	}
}

func TestFlapDetectionEvictsAfterConfiguredSuspectEpisodes(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    30 * time.Second,
			FlapThreshold: 3,
		},
		Clock: clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
	}

	for i := 0; i < 2; i++ {
		clock.Advance(6 * time.Second)
		if err := server.EvaluateLiveness(ctx); err != nil {
			t.Fatalf("EvaluateLiveness cycle %d returned error: %v", i+1, err)
		}
		if got, want := server.Liveness()["c"].State, coordruntime.NodeLivenessStateSuspect; got != want {
			t.Fatalf("state after suspect cycle %d = %q, want %q", i+1, got, want)
		}
		if nodeMarkedDead(server.Current().Cluster, "c") {
			t.Fatal("node c was marked dead before reaching flap threshold")
		}
		if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(c) recovery cycle %d returned error: %v", i+1, err)
		}
	}

	clock.Advance(6 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness(third suspect) returned error: %v", err)
	}
	if got, want := server.Liveness()["c"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("state after flap eviction = %q, want %q", got, want)
	}
	if !nodeMarkedDead(server.Current().Cluster, "c") {
		t.Fatal("node c was not tombstoned after flap eviction")
	}
	if got, want := server.Pending()[0].NodeID, "d"; got != want {
		t.Fatalf("pending replacement node = %q, want %q", got, want)
	}
}

func TestFlapDetectionCountsRepeatedSuspectOnlyOnceUntilRecovery(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    30 * time.Second,
			FlapThreshold: 2,
		},
		Clock: clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 2, []string{"a", "b"})
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
	}
	if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(a) returned error: %v", err)
	}

	clock.Advance(6 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness(first suspect) returned error: %v", err)
	}
	record := server.Liveness()["a"]
	if got, want := len(record.SuspectTransitionsUnixNano), 1; got != want {
		t.Fatalf("suspect transition count after first suspect = %d, want %d", got, want)
	}
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness(repeated suspect) returned error: %v", err)
	}
	record = server.Liveness()["a"]
	if got, want := len(record.SuspectTransitionsUnixNano), 1; got != want {
		t.Fatalf("suspect transition count after repeated suspect = %d, want %d", got, want)
	}
	if nodeMarkedDead(server.Current().Cluster, "a") {
		t.Fatal("node a was marked dead without a second suspect episode")
	}

	if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(a) recovery returned error: %v", err)
	}
	clock.Advance(6 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness(second suspect) returned error: %v", err)
	}
	if got, want := server.Liveness()["a"].State, coordruntime.NodeLivenessStateDead; got != want {
		t.Fatalf("state after second suspect = %q, want %q", got, want)
	}
}

func TestFlapDetectionCanBeDisabledAndUsesCustomThreshold(t *testing.T) {
	t.Run("default threshold and window", func(t *testing.T) {
		ctx := context.Background()
		clock := &fakeClock{now: time.Unix(0, 0)}
		h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
			LivenessPolicy: LivenessPolicy{
				SuspectAfter: 5 * time.Second,
				DeadAfter:    20 * time.Second,
			},
			Clock: clock,
		})
		server := h.server
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})
		if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
		}
		if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
		}
		for i := 0; i < 3; i++ {
			clock.Advance(6 * time.Second)
			if err := server.EvaluateLiveness(ctx); err != nil {
				t.Fatalf("EvaluateLiveness cycle %d returned error: %v", i+1, err)
			}
			if i < 2 {
				if nodeMarkedDead(server.Current().Cluster, "c") {
					t.Fatal("node c was evicted before the default flap threshold")
				}
				if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
					t.Fatalf("ReportHeartbeat(c) recovery cycle %d returned error: %v", i+1, err)
				}
			}
		}
		if !nodeMarkedDead(server.Current().Cluster, "c") {
			t.Fatal("node c was not evicted at the default flap threshold")
		}
	})

	t.Run("custom threshold", func(t *testing.T) {
		ctx := context.Background()
		clock := &fakeClock{now: time.Unix(0, 0)}
		h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
			LivenessPolicy: LivenessPolicy{
				SuspectAfter:  5 * time.Second,
				DeadAfter:     20 * time.Second,
				FlapWindow:    10 * time.Second,
				FlapThreshold: 2,
			},
			Clock: clock,
		})
		server := h.server
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, 1, 2, []string{"a", "b"})
		if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
		}
		if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(a) returned error: %v", err)
		}
		for i := 0; i < 2; i++ {
			clock.Advance(6 * time.Second)
			if err := server.EvaluateLiveness(ctx); err != nil {
				t.Fatalf("EvaluateLiveness cycle %d returned error: %v", i+1, err)
			}
			if i == 0 {
				if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
					t.Fatalf("ReportHeartbeat(a) recovery returned error: %v", err)
				}
			}
		}
		if !nodeMarkedDead(server.Current().Cluster, "a") {
			t.Fatal("node a was not evicted at custom flap threshold")
		}
	})

	t.Run("disabled", func(t *testing.T) {
		ctx := context.Background()
		clock := &fakeClock{now: time.Unix(0, 0)}
		h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
			LivenessPolicy: LivenessPolicy{
				SuspectAfter:  5 * time.Second,
				DeadAfter:     20 * time.Second,
				FlapWindow:    10 * time.Second,
				FlapThreshold: -1,
			},
			Clock: clock,
		})
		server := h.server
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		h.seedBootstrap(t, 1, 2, []string{"a", "b"})
		if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
		}
		if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(a) returned error: %v", err)
		}
		for i := 0; i < 3; i++ {
			clock.Advance(6 * time.Second)
			if err := server.EvaluateLiveness(ctx); err != nil {
				t.Fatalf("EvaluateLiveness cycle %d returned error: %v", i+1, err)
			}
			if nodeMarkedDead(server.Current().Cluster, "a") {
				t.Fatal("node a was marked dead with flap detection disabled")
			}
			if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
				t.Fatalf("ReportHeartbeat(a) recovery cycle %d returned error: %v", i+1, err)
			}
		}
	})
}

func TestFlapDetectionRejectsInvalidThresholdWhenEnabled(t *testing.T) {
	_, err := OpenWithConfig(context.Background(), coordruntime.NewInMemoryStore(), nil, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    10 * time.Second,
			FlapThreshold: 1,
		},
	})
	if err == nil {
		t.Fatal("OpenWithConfig unexpectedly succeeded with invalid flap threshold")
	}
}

func TestFlapHistoryAgesOutOutsideConfiguredWindow(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    10 * time.Second,
			FlapThreshold: 2,
		},
		Clock: clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 2, []string{"a", "b"})
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
	}
	if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(a) returned error: %v", err)
	}

	clock.Advance(6 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness(first suspect) returned error: %v", err)
	}
	if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(a) recovery returned error: %v", err)
	}

	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness(after window) returned error: %v", err)
	}
	if nodeMarkedDead(server.Current().Cluster, "a") {
		t.Fatal("node a was evicted even though prior flap aged out of the window")
	}
}

func TestFlapEvictionRepairsFlappingTailAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    10 * time.Second,
			FlapThreshold: 2,
		},
		Clock: clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
	}

	if _, err := h.adapters["a"].Node().HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 0,
		Key:                  "flap-tail",
		Value:                "v1",
		ExpectedChainVersion: server.Current().SlotVersions[0],
	}); err != nil {
		t.Fatalf("initial HandleClientPut returned error: %v", err)
	}

	for i := 0; i < 2; i++ {
		clock.Advance(6 * time.Second)
		if err := server.EvaluateLiveness(ctx); err != nil {
			t.Fatalf("EvaluateLiveness cycle %d returned error: %v", i+1, err)
		}
		if i == 0 {
			if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
				t.Fatalf("ReportHeartbeat(c) recovery returned error: %v", err)
			}
		}
	}
	if !nodeMarkedDead(server.Current().Cluster, "c") {
		t.Fatal("node c was not evicted after flapping")
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "b:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain after tail flap repair = %v, want %v", got, want)
	}
	currentVersion := server.Current().SlotVersions[0]
	if _, err := h.adapters["a"].Node().HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 0,
		Key:                  "flap-tail",
		Value:                "v2",
		ExpectedChainVersion: currentVersion,
	}); err != nil {
		t.Fatalf("post-repair HandleClientPut returned error: %v", err)
	}
	read, err := h.adapters["d"].Node().HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 0,
		Key:                  "flap-tail",
		ExpectedChainVersion: currentVersion,
	})
	if err != nil {
		t.Fatalf("post-repair HandleClientGet returned error: %v", err)
	}
	if got, want := read.Value, "v2"; got != want {
		t.Fatalf("post-repair read value = %q, want %q", got, want)
	}
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err == nil {
		t.Fatal("flap-evicted tail unexpectedly rejoined")
	}
}

func TestFlapEvictionRepairsFlappingHeadAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    10 * time.Second,
			FlapThreshold: 2,
		},
		Clock: clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(a) returned error: %v", err)
	}

	if _, err := h.adapters["a"].Node().HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 0,
		Key:                  "flap-head",
		Value:                "v1",
		ExpectedChainVersion: server.Current().SlotVersions[0],
	}); err != nil {
		t.Fatalf("initial HandleClientPut returned error: %v", err)
	}

	for i := 0; i < 2; i++ {
		clock.Advance(6 * time.Second)
		if err := server.EvaluateLiveness(ctx); err != nil {
			t.Fatalf("EvaluateLiveness cycle %d returned error: %v", i+1, err)
		}
		if i == 0 {
			if err := h.adapters["a"].Node().ReportHeartbeat(ctx); err != nil {
				t.Fatalf("ReportHeartbeat(a) recovery returned error: %v", err)
			}
		}
	}
	if !nodeMarkedDead(server.Current().Cluster, "a") {
		t.Fatal("node a was not evicted after flapping")
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"b:active", "c:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final chain after head flap repair = %v, want %v", got, want)
	}
	currentVersion := server.Current().SlotVersions[0]
	if _, err := h.adapters["b"].Node().HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 0,
		Key:                  "flap-head",
		Value:                "v2",
		ExpectedChainVersion: currentVersion,
	}); err != nil {
		t.Fatalf("post-repair HandleClientPut returned error: %v", err)
	}
	read, err := h.adapters["d"].Node().HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 0,
		Key:                  "flap-head",
		ExpectedChainVersion: currentVersion,
	})
	if err != nil {
		t.Fatalf("post-repair HandleClientGet returned error: %v", err)
	}
	if got, want := read.Value, "v2"; got != want {
		t.Fatalf("post-repair read value = %q, want %q", got, want)
	}
}

func TestFlapEvictionUpdatesMetricsAndAdminState(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{now: time.Unix(0, 0)}
	registry := prometheus.NewRegistry()
	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		LivenessPolicy: LivenessPolicy{
			SuspectAfter:  5 * time.Second,
			DeadAfter:     20 * time.Second,
			FlapWindow:    10 * time.Second,
			FlapThreshold: 2,
		},
		MetricsRegistry: registry,
		Clock:           clock,
	})
	server := h.server
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(c) returned error: %v", err)
	}

	for i := 0; i < 2; i++ {
		clock.Advance(6 * time.Second)
		if err := server.EvaluateLiveness(ctx); err != nil {
			t.Fatalf("EvaluateLiveness cycle %d returned error: %v", i+1, err)
		}
		if i == 0 {
			if err := h.adapters["c"].Node().ReportHeartbeat(ctx); err != nil {
				t.Fatalf("ReportHeartbeat(c) recovery returned error: %v", err)
			}
		}
	}

	if got, want := metricCounterValue(t, registry, "craq_coordserver_flap_detections_total"), 1.0; got != want {
		t.Fatalf("flap detections metric = %v, want %v", got, want)
	}
	state := server.AdminState(ctx)
	record := state.Liveness["c"]
	if got, want := len(record.SuspectTransitionsUnixNano), 2; got != want {
		t.Fatalf("admin liveness suspect transition count = %d, want %d", got, want)
	}
	if got, want := state.Pending[0].NodeID, "d"; got != want {
		t.Fatalf("admin pending replacement = %q, want %q", got, want)
	}
	if !containsEventKind(state.Recent, "flap_eviction") {
		t.Fatalf("recent events = %#v, want flap_eviction event", state.Recent)
	}
}

func TestLivenessRecoveryAfterCoordinatorReopenDoesNotDuplicateDeadAction(t *testing.T) {
	ctx := context.Background()
	store := coordruntime.NewInMemoryStore()
	clock := &fakeClock{now: time.Unix(0, 0)}
	nodes := map[string]*recordingNodeClient{
		"a": newRecordingNodeClient("a"),
		"b": newRecordingNodeClient("b"),
		"c": newRecordingNodeClient("c"),
		"d": newRecordingNodeClient("d"),
	}
	server := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c", "d")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "b", ReplicaCount: 1, ActiveCount: 1}); err != nil {
		t.Fatalf("ReportNodeHeartbeat returned error: %v", err)
	}
	clock.Advance(11 * time.Second)
	if err := server.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("EvaluateLiveness returned error: %v", err)
	}
	initialCalls := append([]string(nil), nodes["d"].calls...)

	reopened := mustOpenServerWithConfig(t, store, mapToClient(nodes), ServerConfig{
		LivenessPolicy: LivenessPolicy{SuspectAfter: 5 * time.Second, DeadAfter: 10 * time.Second},
		Clock:          clock,
	})
	clock.Advance(1 * time.Second)
	if err := reopened.EvaluateLiveness(ctx); err != nil {
		t.Fatalf("reopened EvaluateLiveness returned error: %v", err)
	}
	if got, want := nodes["d"].calls, initialCalls; !reflect.DeepEqual(got, want) {
		t.Fatalf("calls after reopen = %v, want %v", got, want)
	}
	if got, want := reopened.Liveness()["b"].DeadActionFired, true; got != want {
		t.Fatalf("dead action fired = %t, want %t", got, want)
	}
}

type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.now = c.now.Add(d)
}

func metricCounterValue(t *testing.T, registry *prometheus.Registry, name string) float64 {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatalf("registry.Gather returned error: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if metric.Counter != nil {
				return metric.Counter.GetValue()
			}
			if metric.Gauge != nil {
				return metric.Gauge.GetValue()
			}
		}
	}
	t.Fatalf("metric %q not found", name)
	return 0
}

func containsEventKind(events []ops.Event, kind string) bool {
	for _, event := range events {
		if event.Kind == kind {
			return true
		}
	}
	return false
}

func mustOpenServerWithConfig(
	t *testing.T,
	store coordruntime.Store,
	nodes map[string]StorageNodeClient,
	cfg ServerConfig,
) *Server {
	t.Helper()
	server, err := OpenWithConfig(context.Background(), store, nodes, cfg)
	if err != nil {
		t.Fatalf("OpenWithConfig returned error: %v", err)
	}
	return server
}

func mustBootstrappedServerWithConfig(
	t *testing.T,
	ctx context.Context,
	nodes map[string]StorageNodeClient,
	cfg ServerConfig,
	slotCount int,
	replicationFactor int,
	nodeIDs ...string,
) *Server {
	t.Helper()
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nodes, cfg)
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor, nodeIDs...)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	return server
}

func newInMemoryHarnessWithConfig(t *testing.T, nodeIDs []string, cfg ServerConfig) *inMemoryHarness {
	t.Helper()
	repl := storage.NewInMemoryReplicationTransport()
	adapters := make(map[string]*InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
	}
	server := mustOpenServerWithConfig(t, coordruntime.NewInMemoryStore(), nodeClients, cfg)
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	for _, adapter := range adapters {
		adapter.BindServer(nil)
	}
	return &inMemoryHarness{
		server:   server,
		adapters: adapters,
		backends: backends,
	}
}
