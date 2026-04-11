package coordserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/storage"
)

type slowNodeClient struct {
	delegate StorageNodeClient
	delay    time.Duration
}

func (c *slowNodeClient) AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error {
	time.Sleep(c.delay)
	return c.delegate.AddReplicaAsTail(ctx, cmd)
}

func (c *slowNodeClient) ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error {
	return c.delegate.ActivateReplica(ctx, cmd)
}

func (c *slowNodeClient) MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	return c.delegate.MarkReplicaLeaving(ctx, cmd)
}

func (c *slowNodeClient) RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error {
	return c.delegate.RemoveReplica(ctx, cmd)
}

func (c *slowNodeClient) UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error {
	time.Sleep(c.delay)
	return c.delegate.UpdateChainPeers(ctx, cmd)
}

func (c *slowNodeClient) ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	return c.delegate.ResumeRecoveredReplica(ctx, cmd)
}

func (c *slowNodeClient) RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error {
	return c.delegate.RecoverReplica(ctx, cmd)
}

func (c *slowNodeClient) DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	return c.delegate.DropRecoveredReplica(ctx, cmd)
}

func TestDispatchRuntimeOutboxAndHeartbeatsRetryRuntimeVersionMismatch(t *testing.T) {
	slotCount := 64
	maxChangedChains := 12
	dispatchDelay := 250 * time.Microsecond
	testTimeout := 15 * time.Second
	drainTimeout := 5 * time.Second
	if coordserverRaceEnabled {
		slotCount = 24
		maxChangedChains = 4
		dispatchDelay = 100 * time.Microsecond
		testTimeout = 30 * time.Second
		drainTimeout = 15 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	h := newInMemoryHarnessWithConfig(t, []string{"a", "b", "c", "d"}, ServerConfig{
		DispatchTimeout:       2 * time.Millisecond,
		DispatchRetryInterval: time.Hour,
	})
	server := h.server

	if _, err := server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, slotCount, 3, []string{"a", "b", "c"})

	timeoutWrapper := newFaultInjectingNodeClient(h.adapters["d"])
	timeoutWrapper.addTailTimeouts = 1
	server.setNodeClient("d", timeoutWrapper)

	_, err := server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: maxChangedChains}))
	if err == nil {
		t.Fatal("AddNode unexpectedly succeeded")
	}
	if !errors.Is(err, ErrDispatchTimeout) {
		t.Fatalf("error = %v, want dispatch timeout", err)
	}
	if got := len(server.Current().Outbox); got == 0 {
		t.Fatal("runtime outbox unexpectedly empty after timed-out add-node repair")
	}

	for _, nodeID := range []string{"a", "b", "c"} {
		server.setNodeClient(nodeID, &slowNodeClient{
			delegate: h.adapters[nodeID],
			delay:    dispatchDelay,
		})
	}
	server.setNodeClient("d", &slowNodeClient{
		delegate: h.adapters["d"],
		delay:    dispatchDelay,
	})

	done := make(chan struct{})
	errCh := make(chan error, 32)
	var wg sync.WaitGroup
	for _, nodeID := range []string{"a", "b", "c"} {
		nodeID := nodeID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if err := h.adapters[nodeID].Node().ReportHeartbeat(ctx); err != nil {
					if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
						errCh <- err
					}
					return
				}
				time.Sleep(50 * time.Microsecond)
			}
		}()
	}

	if err := server.dispatchRuntimeOutbox(ctx); err != nil {
		t.Fatalf("dispatchRuntimeOutbox returned error under heartbeat churn: %v", err)
	}

	close(done)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("background heartbeat returned error: %v", err)
		}
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()
	for attempt := 0; attempt < 8 && len(server.Current().Outbox) > 0; attempt++ {
		if err := server.dispatchRuntimeOutbox(drainCtx); err != nil {
			t.Fatalf("draining runtime outbox after heartbeat churn returned error: %v", err)
		}
	}
	if got := len(server.Current().Outbox); got != 0 {
		t.Fatalf("runtime outbox still has %d entries after retrying dispatch and draining", got)
	}
}
