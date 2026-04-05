package coordserver

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

func TestDynamicNodeRegistrationAndReadyGatingBuildsCluster(t *testing.T) {
	ctx := context.Background()
	h := newInMemoryHarness(t, []string{"a", "b", "c", "d"})
	server := h.server
	for _, adapter := range h.adapters {
		adapter.BindServer(server)
	}

	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	if got := server.Current().Cluster.Chains[0].Replicas; len(got) != 0 {
		t.Fatalf("initial chain replicas = %#v, want empty", got)
	}

	for _, nodeID := range []string{"a", "b", "c"} {
		if err := h.adapters[nodeID].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(%q) returned error: %v", nodeID, err)
		}
	}
	if !server.Current().Cluster.ReadyNodeIDs["a"] || !server.Current().Cluster.ReadyNodeIDs["b"] || !server.Current().Cluster.ReadyNodeIDs["c"] {
		t.Fatalf("ready node set = %#v, want a/b/c ready", server.Current().Cluster.ReadyNodeIDs)
	}

	for _, nodeID := range []string{"a", "b", "c"} {
		if err := h.adapters[nodeID].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("ActivateReplica(%q) returned error: %v", nodeID, err)
		}
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "b:active", "c:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("active chain = %v, want %v", got, want)
	}

	current := server.Current()
	if _, err := server.MarkNodeDead(ctx, reconfigureCommand("dead-c", current.Version, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "c",
	}, coordinator.ReconfigurationPolicy{})); err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if !nodeMarkedDead(server.Current().Cluster, "c") {
		t.Fatal("node c was not marked dead")
	}

	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if got, want := server.Pending()[0], (PendingWork{
		Slot:        0,
		NodeID:      "d",
		Kind:        pendingKindReady,
		SlotVersion: server.Current().SlotVersions[0],
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("pending repair after d join = %#v, want %#v", got, want)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := replicaNodeStates(server.Current().Cluster.Chains[0]), []string{"a:active", "b:active", "d:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final repaired chain = %v, want %v", got, want)
	}
}

func TestDynamicMembershipContinuityAfterReopen(t *testing.T) {
	t.Run("tombstone_survives_reopen", func(t *testing.T) {
		ctx := context.Background()
		path := t.TempDir()
		store, err := coordruntime.OpenBadgerStore(path)
		if err != nil {
			t.Fatalf("OpenBadgerStore returned error: %v", err)
		}
		server := mustOpenServerWithConfig(t, store, mapToClient(map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
		}), ServerConfig{})
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 2, "a", "b")); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}
		current := server.Current()
		if _, err := server.MarkNodeDead(ctx, reconfigureCommand("dead-a", current.Version, coordinator.Event{
			Kind:   coordinator.EventKindMarkNodeDead,
			NodeID: "a",
		}, coordinator.ReconfigurationPolicy{})); err != nil {
			t.Fatalf("MarkNodeDead returned error: %v", err)
		}
		if err := server.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("store.Close returned error: %v", err)
		}

		reopenedStore, err := coordruntime.OpenBadgerStore(path)
		if err != nil {
			t.Fatalf("OpenBadgerStore(reopen) returned error: %v", err)
		}
		defer func() { _ = reopenedStore.Close() }()
		reopened := mustOpenServerWithConfig(t, reopenedStore, nil, ServerConfig{})
		if !nodeMarkedDead(reopened.Current().Cluster, "a") {
			t.Fatal("dead node tombstone did not survive reopen")
		}
		if _, err := reopened.RegisterNode(ctx, storage.NodeRegistration{NodeID: "a"}); err == nil {
			t.Fatal("RegisterNode(a) unexpectedly succeeded after tombstone")
		}
		if err := reopened.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "a"}); err == nil {
			t.Fatal("ReportNodeHeartbeat(a) unexpectedly succeeded after tombstone")
		}
		if _, err := reopened.RegisterNode(ctx, storage.NodeRegistration{NodeID: "c"}); err != nil {
			t.Fatalf("RegisterNode(c) returned error: %v", err)
		}
		if err := reopened.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "c"}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(c) returned error: %v", err)
		}
		if !reopened.Current().Cluster.ReadyNodeIDs["c"] {
			t.Fatal("replacement node c did not become ready after reopen")
		}
	})

	t.Run("identity_conflict_survives_reopen", func(t *testing.T) {
		ctx := context.Background()
		path := t.TempDir()
		store, err := coordruntime.OpenBadgerStore(path)
		if err != nil {
			t.Fatalf("OpenBadgerStore returned error: %v", err)
		}
		server := mustOpenServerWithConfig(t, store, nil, ServerConfig{})
		if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 1)); err != nil {
			t.Fatalf("Bootstrap returned error: %v", err)
		}

		reg := storage.NodeRegistration{
			NodeID:         "d",
			RPCAddress:     "127.0.0.1:7414",
			FailureDomains: cloneFailureDomains(uniqueNode("d").FailureDomains),
		}
		if _, err := server.RegisterNode(ctx, reg); err != nil {
			t.Fatalf("RegisterNode returned error: %v", err)
		}
		if _, err := server.RegisterNode(ctx, reg); err != nil {
			t.Fatalf("duplicate RegisterNode returned error: %v", err)
		}
		if _, err := server.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         "d",
			RPCAddress:     "127.0.0.1:7999",
			FailureDomains: cloneFailureDomains(reg.FailureDomains),
		}); err == nil {
			t.Fatal("RegisterNode with conflicting RPCAddress unexpectedly succeeded")
		}
		if _, err := server.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:     "d",
			RPCAddress: reg.RPCAddress,
			FailureDomains: map[string]string{
				"host": "host-other",
				"rack": "rack-other",
				"az":   "az-other",
			},
		}); err == nil {
			t.Fatal("RegisterNode with conflicting failure domains unexpectedly succeeded")
		}
		if err := server.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "d"}); err != nil {
			t.Fatalf("ReportNodeHeartbeat(d) returned error: %v", err)
		}

		if err := server.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("store.Close returned error: %v", err)
		}

		reopenedStore, err := coordruntime.OpenBadgerStore(path)
		if err != nil {
			t.Fatalf("OpenBadgerStore(reopen) returned error: %v", err)
		}
		defer func() { _ = reopenedStore.Close() }()
		reopened := mustOpenServerWithConfig(t, reopenedStore, nil, ServerConfig{})
		if _, err := reopened.RegisterNode(ctx, storage.NodeRegistration{
			NodeID:         "d",
			RPCAddress:     "127.0.0.1:7999",
			FailureDomains: cloneFailureDomains(reg.FailureDomains),
		}); err == nil {
			t.Fatal("reopened RegisterNode with conflicting RPCAddress unexpectedly succeeded")
		}
		if err := reopened.ReportNodeHeartbeat(ctx, storage.NodeStatus{NodeID: "d"}); err != nil {
			t.Fatalf("reopened ReportNodeHeartbeat(d) returned error: %v", err)
		}
		if got, want := len(reopened.Current().Cluster.NodesByID), 1; got != want {
			t.Fatalf("membership size after reopen = %d, want %d", got, want)
		}
	})
}

func TestNonHADurableOutboxResumesAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := coordruntime.OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}

	repl := storage.NewInMemoryReplicationTransport()
	backends := map[string]*storage.InMemoryBackend{}
	adapters := map[string]*InMemoryNodeAdapter{}
	initialClients := map[string]StorageNodeClient{}
	for _, nodeID := range []string{"a", "b", "c", "d"} {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(ctx, nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		if nodeID != "d" {
			initialClients[nodeID] = adapter
		}
	}

	server := mustOpenServerWithConfig(t, store, initialClients, ServerConfig{})
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h := &inMemoryHarness{server: server, adapters: adapters, backends: backends}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err == nil {
		t.Fatal("AddNode unexpectedly succeeded without a client for d")
	} else if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("AddNode error = %v, want dispatch failed", err)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty after failed dispatch")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close returned error: %v", err)
	}

	reopenedStore, err := coordruntime.OpenBadgerStore(path)
	if err != nil {
		t.Fatalf("OpenBadgerStore(reopen) returned error: %v", err)
	}
	defer func() { _ = reopenedStore.Close() }()
	reopenedClients := map[string]StorageNodeClient{
		"a": adapters["a"],
		"b": adapters["b"],
		"c": adapters["c"],
		"d": adapters["d"],
	}
	reopened := mustOpenServerWithConfig(t, reopenedStore, reopenedClients, ServerConfig{})
	for _, adapter := range adapters {
		adapter.BindServer(reopened)
	}
	if err := reopened.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if err := adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if err := adapters["c"].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica(c) returned error: %v", err)
	}
	if _, exists := reopened.Pending()[1]; exists {
		t.Fatal("pending work still present after restarted outbox replay")
	}
	if got, want := replicaNodeStates(reopened.Current().Cluster.Chains[1]), []string{"d:active", "b:active", "a:active"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("final reopened chain = %v, want %v", got, want)
	}
}
