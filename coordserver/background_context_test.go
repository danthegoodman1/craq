package coordserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/storage"
)

type observingBlockingNodeClient struct {
	started  chan struct{}
	canceled chan error
}

func newObservingBlockingNodeClient() *observingBlockingNodeClient {
	return &observingBlockingNodeClient{
		started:  make(chan struct{}, 1),
		canceled: make(chan error, 1),
	}
}

func (c *observingBlockingNodeClient) AddReplicaAsTail(ctx context.Context, _ storage.AddReplicaAsTailCommand) error {
	select {
	case c.started <- struct{}{}:
	default:
	}
	<-ctx.Done()
	err := ctx.Err()
	select {
	case c.canceled <- err:
	default:
	}
	return err
}

func (c *observingBlockingNodeClient) ActivateReplica(context.Context, storage.ActivateReplicaCommand) error {
	return nil
}

func (c *observingBlockingNodeClient) MarkReplicaLeaving(context.Context, storage.MarkReplicaLeavingCommand) error {
	return nil
}

func (c *observingBlockingNodeClient) RemoveReplica(context.Context, storage.RemoveReplicaCommand) error {
	return nil
}

func (c *observingBlockingNodeClient) UpdateChainPeers(context.Context, storage.UpdateChainPeersCommand) error {
	return nil
}

func (c *observingBlockingNodeClient) ResumeRecoveredReplica(context.Context, storage.ResumeRecoveredReplicaCommand) error {
	return nil
}

func (c *observingBlockingNodeClient) RecoverReplica(context.Context, storage.RecoverReplicaCommand) error {
	return nil
}

func (c *observingBlockingNodeClient) DropRecoveredReplica(context.Context, storage.DropRecoveredReplicaCommand) error {
	return nil
}

func (c *observingBlockingNodeClient) waitForStart(t *testing.T) {
	t.Helper()
	select {
	case <-c.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for background dispatch to start")
	}
}

func (c *observingBlockingNodeClient) waitForCancel(t *testing.T) error {
	t.Helper()
	select {
	case err := <-c.canceled:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for background dispatch to cancel")
		return nil
	}
}

func TestBackgroundDispatchCancelsInFlightOutboxWorkOnServerClose(t *testing.T) {
	ctx := context.Background()
	nodes := map[string]*blockingNodeClient{
		"a": newBlockingNodeClient("a"),
		"b": newBlockingNodeClient("b"),
		"c": newBlockingNodeClient("c"),
		"d": newBlockingNodeClient("d"),
	}
	nodes["d"].blockAddTail = true
	server := mustBootstrappedBlockingServer(t, ctx, nodes, ServerConfig{
		DisableBackgroundLoops: true,
		DispatchTimeout:        time.Hour,
	}, 8, 3, "a", "b", "c")

	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	_, err := server.AddNode(requestCtx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want ErrDispatchTimeout", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want deadline exceeded", err)
	}

	// The timed-out caller should leave durable repair work behind for the
	// background dispatcher to retry after the request returns.
	slot := mustPendingSlotForNode(t, server.Pending(), "d", pendingKindReady)
	if !runtimeOutboxHasSlot(server.Current().Outbox, slot) {
		t.Fatalf("runtime outbox missing repaired slot %d after timeout", slot)
	}

	observer := newObservingBlockingNodeClient()
	server.nodes["d"] = observer

	dispatchDone := make(chan struct{})
	go func() {
		server.runBackgroundDispatchOnce()
		close(dispatchDone)
	}()

	observer.waitForStart(t)
	if err := server.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := observer.waitForCancel(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("background dispatch cancel error = %v, want context.Canceled", err)
	}
	select {
	case <-dispatchDone:
	case <-time.After(time.Second):
		t.Fatal("background dispatch did not stop after server close")
	}
}

func TestHADispatchOutboxAppliesConfiguredDispatchTimeout(t *testing.T) {
	h := newHAInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		DispatchTimeout: time.Nanosecond,
	})
	stageHAAddNodeOutbox(t, h)

	blocking := newBlockingNodeClient("d")
	blocking.blockAddTail = true
	h.leader.nodes["d"] = blocking

	errCh := make(chan error, 1)
	go func() {
		errCh <- h.leader.dispatchOutbox(context.Background())
	}()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("dispatchOutbox unexpectedly succeeded")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dispatchOutbox error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatchOutbox did not respect the configured dispatch timeout")
	}
}

func TestHAStepCancelsInFlightOutboxWorkOnServerClose(t *testing.T) {
	h := newHAInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		DispatchTimeout: time.Hour,
	})
	stageHAAddNodeOutbox(t, h)

	observer := newObservingBlockingNodeClient()
	h.leader.nodes["d"] = observer

	errCh := make(chan error, 1)
	go func() {
		_, err := h.leader.StepHA(h.leader.backgroundContext())
		errCh <- err
	}()

	observer.waitForStart(t)
	if err := h.leader.Close(); err != nil {
		t.Fatalf("leader Close returned error: %v", err)
	}
	if err := observer.waitForCancel(t); !errors.Is(err, context.Canceled) {
		t.Fatalf("HA background dispatch cancel error = %v, want context.Canceled", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("StepHA error after close = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("StepHA did not stop after server close")
	}
}

func stageHAAddNodeOutbox(t *testing.T, h *haInMemoryHarness) {
	t.Helper()
	ctx := context.Background()

	h.mustStepLeader(t)
	h.mustBind(t, h.leader)
	if _, err := h.leader.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, h.leader, 8, 3, []string{"a", "b", "c"})
	if _, err := h.leader.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}
}
