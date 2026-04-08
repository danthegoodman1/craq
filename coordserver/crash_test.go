package coordserver

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

type durableCoordHarness struct {
	path     string
	repl     *storage.InMemoryReplicationTransport
	adapters map[string]*InMemoryNodeAdapter
	backends map[string]*storage.InMemoryBackend
}

type faultInjectingNodeClient struct {
	delegate StorageNodeClient

	mu                  sync.Mutex
	addTailTimeouts     int
	markLeavingTimeouts int
	updatePeersTimeouts int
	resumeTimeouts      int
	recoverTimeouts     int
	dropTimeouts        int
	addTailCalls        int
	markLeavingCalls    int
	updatePeersCalls    int
	resumeCalls         int
	recoverCalls        int
	dropCalls           int
}

func newFaultInjectingNodeClient(delegate StorageNodeClient) *faultInjectingNodeClient {
	return &faultInjectingNodeClient{delegate: delegate}
}

func (c *faultInjectingNodeClient) AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error {
	c.mu.Lock()
	c.addTailCalls++
	shouldTimeout := c.addTailTimeouts > 0
	if shouldTimeout {
		c.addTailTimeouts--
	}
	c.mu.Unlock()
	if shouldTimeout {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
		return context.DeadlineExceeded
	}
	return c.delegate.AddReplicaAsTail(ctx, cmd)
}

func (c *faultInjectingNodeClient) ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error {
	return c.delegate.ActivateReplica(ctx, cmd)
}

func (c *faultInjectingNodeClient) MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	c.mu.Lock()
	c.markLeavingCalls++
	shouldTimeout := c.markLeavingTimeouts > 0
	if shouldTimeout {
		c.markLeavingTimeouts--
	}
	c.mu.Unlock()
	if shouldTimeout {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
		return context.DeadlineExceeded
	}
	return c.delegate.MarkReplicaLeaving(ctx, cmd)
}

func (c *faultInjectingNodeClient) RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error {
	return c.delegate.RemoveReplica(ctx, cmd)
}

func (c *faultInjectingNodeClient) UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error {
	c.mu.Lock()
	c.updatePeersCalls++
	shouldTimeout := c.updatePeersTimeouts > 0
	if shouldTimeout {
		c.updatePeersTimeouts--
	}
	c.mu.Unlock()
	if shouldTimeout {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
		return context.DeadlineExceeded
	}
	return c.delegate.UpdateChainPeers(ctx, cmd)
}

func (c *faultInjectingNodeClient) ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	c.mu.Lock()
	c.resumeCalls++
	shouldTimeout := c.resumeTimeouts > 0
	if shouldTimeout {
		c.resumeTimeouts--
	}
	c.mu.Unlock()
	if shouldTimeout {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
		return context.DeadlineExceeded
	}
	return c.delegate.ResumeRecoveredReplica(ctx, cmd)
}

func (c *faultInjectingNodeClient) RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error {
	c.mu.Lock()
	c.recoverCalls++
	shouldTimeout := c.recoverTimeouts > 0
	if shouldTimeout {
		c.recoverTimeouts--
	}
	c.mu.Unlock()
	if shouldTimeout {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
		return context.DeadlineExceeded
	}
	return c.delegate.RecoverReplica(ctx, cmd)
}

func (c *faultInjectingNodeClient) DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	c.mu.Lock()
	c.dropCalls++
	shouldTimeout := c.dropTimeouts > 0
	if shouldTimeout {
		c.dropTimeouts--
	}
	c.mu.Unlock()
	if shouldTimeout {
		if _, ok := ctx.Deadline(); ok {
			<-ctx.Done()
			return ctx.Err()
		}
		return context.DeadlineExceeded
	}
	return c.delegate.DropRecoveredReplica(ctx, cmd)
}

func (c *faultInjectingNodeClient) addTailCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.addTailCalls
}

func (c *faultInjectingNodeClient) markLeavingCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.markLeavingCalls
}

func (c *faultInjectingNodeClient) updatePeersCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.updatePeersCalls
}

func (c *faultInjectingNodeClient) resumeCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resumeCalls
}

func (c *faultInjectingNodeClient) recoverCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.recoverCalls
}

func (c *faultInjectingNodeClient) dropCallCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dropCalls
}

func newDurableCoordHarness(t *testing.T, path string, nodeIDs []string) *durableCoordHarness {
	t.Helper()

	repl := storage.NewInMemoryReplicationTransport()
	adapters := make(map[string]*InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
	}
	return &durableCoordHarness{
		path:     path,
		repl:     repl,
		adapters: adapters,
		backends: backends,
	}
}

func (h *durableCoordHarness) openServer(t *testing.T, clientIDs ...string) (*coordruntime.BadgerStore, *Server) {
	t.Helper()
	store, err := coordruntime.OpenBadgerStore(h.path)
	if err != nil {
		t.Fatalf("OpenBadgerStore returned error: %v", err)
	}
	clients := make(map[string]StorageNodeClient, len(clientIDs))
	for _, nodeID := range clientIDs {
		clients[nodeID] = h.adapters[nodeID]
	}
	server := mustOpenServerWithConfig(t, store, clients, ServerConfig{
		DispatchRetryInterval: time.Hour,
	})
	for _, adapter := range h.adapters {
		adapter.BindServer(server)
	}
	return store, server
}

func (h *durableCoordHarness) closeServer(t *testing.T, server *Server, store *coordruntime.BadgerStore) {
	t.Helper()
	for _, adapter := range h.adapters {
		adapter.BindServer(nil)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("server.Close returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close returned error: %v", err)
	}
}

func (h *durableCoordHarness) seedBootstrap(t *testing.T, server *Server, slotCount int, replicationFactor int, nodeIDs []string) {
	t.Helper()
	(&inMemoryHarness{
		server:   server,
		adapters: h.adapters,
		backends: h.backends,
	}).seedBootstrap(t, slotCount, replicationFactor, nodeIDs)
}

func TestNonHACrashBeforeFirstDispatchResumesOutboxAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err == nil {
		t.Fatal("AddNode unexpectedly succeeded without dispatch client for d")
	} else if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("AddNode error = %v, want ErrDispatchFailed", err)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty after failed dispatch")
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	if err := reopened.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := reopened.Pending()[slot].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after activation = %q, want %q", got, want)
	}
	if got, want := pendingKind(reopened.Current().PendingBySlot[slot].Kind), pendingKindRemoved; got != want {
		t.Fatalf("runtime pending kind after activation = %q, want %q", got, want)
	}
	leavingNodeID := replicaNodeWithState(reopened.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after replacement activation")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending for repaired slot after remove = %#v, want removal cleared", got)
	}

	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-before-dispatch", "v1")
}

func TestNonHACrashAfterDispatchBeforeAckReplaysSafelyAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, _, err := server.rt.Reconfigure(ctx, reconfigureCommand("add-d", server.Current().Version, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("rt.Reconfigure returned error: %v", err)
	}
	server.syncViewsFromRuntime()
	server.rebuildRoutingSnapshot()
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)

	entry := mustFindRuntimeOutboxEntry(t, server, coordruntime.OutboxCommandKindAddReplicaAsTail, "d", slot)
	if err := server.dispatchRuntimeOutboxEntry(ctx, entry); err != nil {
		t.Fatalf("dispatchRuntimeOutboxEntry(add tail) returned error: %v", err)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty before ack")
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	if err := reopened.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(reopened.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after replayed add-tail repair")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}

	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending work after replayed add-tail repair = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-dispatch-before-ack", "v2")
}

func TestNonHACrashAfterReadyProgressBeforeMarkLeavingResumesRepairAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})
	h.adapters["d"].EnableQueuedProgress()

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := plannedLeavingNodeAfterReady(t, server, slot, "d")
	if leavingNodeID == "" {
		t.Fatal("failed to determine active tail before ready progress")
	}
	delete(server.nodes, leavingNodeID)

	if err := h.adapters["d"].DeliverNextProgress(ctx); err == nil {
		t.Fatal("DeliverNextProgress unexpectedly succeeded without leaving-node client")
	} else if !errors.Is(err, ErrDispatchFailed) {
		t.Fatalf("DeliverNextProgress error = %v, want ErrDispatchFailed", err)
	}
	if got, want := server.Pending()[slot].Kind, pendingKindRemoved; got != want {
		t.Fatalf("pending kind after persisted ready progress = %q, want %q", got, want)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty after failed mark-leaving dispatch")
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	if err := reopened.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error: %v", err)
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}

	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending work after resumed mark-leaving repair = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-ready-progress", "v3")
}

func TestNonHACrashAfterRemovedProgressPreservesSettledStateAndDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	leavingNodeID := replicaNodeWithState(server.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node before removed progress")
	}
	h.adapters[leavingNodeID].EnableQueuedProgress()
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	if err := h.adapters[leavingNodeID].DeliverNextProgress(ctx); err != nil {
		t.Fatalf("DeliverNextProgress(removed) returned error: %v", err)
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	if got, exists := reopened.Pending()[slot]; exists && got.Kind == pendingKindRemoved {
		t.Fatalf("pending work after removed-progress reopen = %#v, want removal cleared", got)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, slot) {
		t.Fatalf("runtime outbox still contains repaired slot %d: %#v", slot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-removed-progress", "v4")
}

func TestNonHACrashAfterAddTailAckBeforeReadyProgressDoesNotRedispatchAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})
	wrapper := newFaultInjectingNodeClient(h.adapters["d"])
	server.nodes["d"] = wrapper

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	if got, want := wrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls before crash = %d, want %d", got, want)
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	reopened.nodes["d"] = wrapper
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := wrapper.addTailCallCount(), 1; got != want {
		t.Fatalf("add-tail calls after reopen = %d, want no redispatch", got)
	}
	leavingNodeID := replicaNodeWithState(reopened.Current().Cluster.Chains[slot], coordinator.ReplicaStateLeaving)
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node after post-ack reopen")
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-add-ack", "v5")
}

func TestNonHACrashAfterMarkLeavingAckBeforeRemovedProgressDoesNotRedispatchAndRestoresDataPlane(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	leavingNodeID := plannedLeavingNodeAfterReady(t, server, slot, "d")
	if leavingNodeID == "" {
		t.Fatal("failed to find leaving node before mark-leaving ack crash")
	}
	wrapper := newFaultInjectingNodeClient(h.adapters[leavingNodeID])
	server.nodes[leavingNodeID] = wrapper
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if got, want := wrapper.markLeavingCallCount(), 1; got != want {
		t.Fatalf("mark-leaving calls before crash = %d, want %d", got, want)
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)
	reopened.nodes[leavingNodeID] = wrapper
	if got, want := wrapper.markLeavingCallCount(), 1; got != want {
		t.Fatalf("mark-leaving calls after reopen = %d, want no redispatch yet", got)
	}
	if err := h.adapters[leavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: slot}); err != nil {
		t.Fatalf("RemoveReplica(%q) returned error: %v", leavingNodeID, err)
	}
	if got, want := wrapper.markLeavingCallCount(), 1; got != want {
		t.Fatalf("mark-leaving calls after removed progress = %d, want no redispatch", got)
	}
	assertActiveReplicaSet(t, reopened.Current().Cluster.Chains[slot], "a", "b", "d")
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, slot, "crash-after-mark-leaving-ack", "v6")
}

func TestNonHACrashWithTwoPendingSlotsPreservesPerSlotRepairIsolation(t *testing.T) {
	ctx := context.Background()
	h := newDurableCoordHarness(t, filepath.Join(t.TempDir(), "coord.db"), []string{"a", "b", "c", "d"})

	store, server := h.openServer(t, "a", "b", "c", "d")
	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, server, 8, 3, []string{"a", "b", "c"})

	if _, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 2})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
	slots := pendingSlotsForNode(t, server.Pending(), "d", pendingKindReady)
	if got, want := len(slots), 2; got != want {
		t.Fatalf("pending slot count = %d, want %d", got, want)
	}

	h.closeServer(t, server, store)

	reopenedStore, reopened := h.openServer(t, "a", "b", "c", "d")
	defer h.closeServer(t, reopened, reopenedStore)

	firstSlot, secondSlot := slots[0], slots[1]
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: firstSlot}); err != nil {
		t.Fatalf("ActivateReplica(d, firstSlot) returned error: %v", err)
	}
	firstLeavingNodeID := replicaNodeWithState(reopened.Current().Cluster.Chains[firstSlot], coordinator.ReplicaStateLeaving)
	if firstLeavingNodeID == "" {
		t.Fatal("failed to find leaving node for first slot")
	}
	if err := h.adapters[firstLeavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: firstSlot}); err != nil {
		t.Fatalf("RemoveReplica(firstSlot) returned error: %v", err)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, firstSlot) {
		t.Fatalf("runtime outbox still contains first repaired slot %d: %#v", firstSlot, reopened.Current().Outbox)
	}
	if _, exists := reopened.Pending()[firstSlot]; exists {
		t.Fatalf("pending for first repaired slot after first completion = %#v, want none", reopened.Pending()[firstSlot])
	}
	if err := reopened.reconcileAndDispatch(ctx); err != nil {
		t.Fatalf("reconcileAndDispatch after first completion returned error: %v", err)
	}
	if got, want := reopened.Pending()[secondSlot].Kind, pendingKindReady; got != want {
		t.Fatalf("second slot pending kind after first completion reconcile = %q, want %q", got, want)
	}

	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: secondSlot}); err != nil {
		t.Fatalf("ActivateReplica(d, secondSlot) returned error: %v", err)
	}
	secondLeavingNodeID := replicaNodeWithState(reopened.Current().Cluster.Chains[secondSlot], coordinator.ReplicaStateLeaving)
	if secondLeavingNodeID == "" {
		t.Fatal("failed to find leaving node for second slot")
	}
	if err := h.adapters[secondLeavingNodeID].Node().RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: secondSlot}); err != nil {
		t.Fatalf("RemoveReplica(secondSlot) returned error: %v", err)
	}
	if runtimeOutboxHasSlot(reopened.Current().Outbox, secondSlot) {
		t.Fatalf("runtime outbox still contains second repaired slot %d: %#v", secondSlot, reopened.Current().Outbox)
	}
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, firstSlot, "multi-slot-first", "v1")
	assertSlotRoundTrip(t, ctx, reopened, h.adapters, secondSlot, "multi-slot-second", "v2")
}

func mustFindRuntimeOutboxEntry(
	t *testing.T,
	server *Server,
	kind coordruntime.OutboxCommandKind,
	nodeID string,
	slot int,
) coordruntime.OutboxEntry {
	t.Helper()
	for _, entry := range server.rt.Current().Outbox {
		if entry.Kind == kind && entry.NodeID == nodeID && entry.Slot == slot {
			return entry
		}
	}
	t.Fatalf("runtime outbox entry %q node=%s slot=%d not found", kind, nodeID, slot)
	return coordruntime.OutboxEntry{}
}

func replicaNodeWithState(chain coordinator.Chain, want coordinator.ReplicaState) string {
	for _, replica := range chain.Replicas {
		if replica.State == want {
			return replica.NodeID
		}
	}
	return ""
}

func lastActiveReplicaNode(chain coordinator.Chain) string {
	for i := len(chain.Replicas) - 1; i >= 0; i-- {
		if chain.Replicas[i].State == coordinator.ReplicaStateActive {
			return chain.Replicas[i].NodeID
		}
	}
	return ""
}

func mustPendingSlotForNode(t *testing.T, pending map[int]PendingWork, nodeID string, kind pendingKind) int {
	t.Helper()
	for slot, work := range pending {
		if work.NodeID == nodeID && work.Kind == kind {
			return slot
		}
	}
	t.Fatalf("pending work for node=%q kind=%q not found in %#v", nodeID, kind, pending)
	return 0
}

func pendingSlotsForNode(t *testing.T, pending map[int]PendingWork, nodeID string, kind pendingKind) []int {
	t.Helper()
	var slots []int
	for slot, work := range pending {
		if work.NodeID == nodeID && work.Kind == kind {
			slots = append(slots, slot)
		}
	}
	sort.Ints(slots)
	if len(slots) == 0 {
		t.Fatalf("pending slots for node=%q kind=%q not found in %#v", nodeID, kind, pending)
	}
	return slots
}

func assertActiveReplicaSet(t *testing.T, chain coordinator.Chain, wantNodeIDs ...string) {
	t.Helper()
	got := make([]string, 0, len(chain.Replicas))
	for _, replica := range chain.Replicas {
		if replica.State == coordinator.ReplicaStateActive {
			got = append(got, replica.NodeID)
		}
	}
	sort.Strings(got)
	want := append([]string(nil), wantNodeIDs...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("active replica set = %v, want %v", got, want)
	}
}

func assertSlotRoundTrip(
	t *testing.T,
	ctx context.Context,
	server *Server,
	adapters map[string]*InMemoryNodeAdapter,
	slot int,
	key string,
	value string,
) {
	t.Helper()
	snapshot, err := server.RoutingSnapshot(ctx)
	if err != nil {
		t.Fatalf("RoutingSnapshot returned error: %v", err)
	}
	if slot < 0 || slot >= len(snapshot.Slots) {
		t.Fatalf("routing snapshot missing slot %d", slot)
	}
	route := snapshot.Slots[slot]
	if !route.Writable || !route.Readable {
		t.Fatalf("route flags for slot %d = %#v, want readable and writable", slot, route)
	}
	if route.HeadNodeID == "" || route.TailNodeID == "" {
		t.Fatalf("route head/tail for slot %d = %#v, want non-empty nodes", slot, route)
	}

	chainVersion := server.Current().SlotVersions[slot]
	if _, err := adapters[route.HeadNodeID].Node().HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 slot,
		Key:                  key,
		Value:                value,
		ExpectedChainVersion: chainVersion,
	}); err != nil {
		t.Fatalf("HandleClientPut(slot=%d, head=%q) returned error: %v", slot, route.HeadNodeID, err)
	}
	read, err := adapters[route.TailNodeID].Node().HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 slot,
		Key:                  key,
		ExpectedChainVersion: chainVersion,
	})
	if err != nil {
		t.Fatalf("HandleClientGet(slot=%d, tail=%q) returned error: %v", slot, route.TailNodeID, err)
	}
	if !read.Found || read.Value != value {
		t.Fatalf("tail read result = %#v, want found value %q", read, value)
	}
}
