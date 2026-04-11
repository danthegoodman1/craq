package storage

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestInMemoryBackendSnapshotIsDeepCopy(t *testing.T) {
	backend := NewInMemoryBackend()
	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}
	if err := backend.Put(1, "k1", "v1", testObjectMetadata(1)); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	snapshot, err := backend.Snapshot(1)
	if err != nil {
		t.Fatalf("Snapshot returned error: %v", err)
	}
	snapshot["k1"] = CommittedObject{Value: "mutated", Metadata: testObjectMetadata(9)}

	data, err := backend.ReplicaData(1)
	if err != nil {
		t.Fatalf("ReplicaData returned error: %v", err)
	}
	if got, want := data["k1"].Value, "v1"; got != want {
		t.Fatalf("stored value = %q, want %q", got, want)
	}
}

func waitForTestSignal(t *testing.T, ctx context.Context, ch <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-ch:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for %s: %v", name, ctx.Err())
	}
}

func TestNodeAddReplicaAsTailCopiesSnapshotAndActivates(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceCoord := NewInMemoryCoordinatorClient()
	sourceNode := mustNewNode(t, ctx, Config{NodeID: "node-a"}, sourceBackend, sourceCoord, transport)
	transport.Register("node-a", sourceBackend)

	if err := sourceNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := sourceNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if err := sourceBackend.Put(1, "alpha", "one", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put returned error: %v", err)
	}

	targetBackend := NewInMemoryBackend()
	targetCoord := NewInMemoryCoordinatorClient()
	targetNode := mustNewNode(t, ctx, Config{NodeID: "node-b"}, targetBackend, targetCoord, transport)
	transport.Register("node-b", targetBackend)

	if err := targetNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "node-a"},
		},
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}

	state := targetNode.State()
	if got, want := state.Replicas[1].State, ReplicaStateCatchingUp; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
	if len(targetCoord.ReadySlots) != 0 {
		t.Fatalf("ready slots before activate = %v, want none", targetCoord.ReadySlots)
	}

	data, err := targetBackend.ReplicaData(1)
	if err != nil {
		t.Fatalf("target ReplicaData returned error: %v", err)
	}
	if got, want := snapshotValues(data), map[string]string{"alpha": "one"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("replica data = %v, want %v", got, want)
	}

	if err := targetNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("target ActivateReplica returned error: %v", err)
	}
	if got, want := targetCoord.ReadySlots, []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ready slots = %v, want %v", got, want)
	}
	if got, want := targetNode.State().Replicas[1].State, ReplicaStateActive; got != want {
		t.Fatalf("replica state after activate = %q, want %q", got, want)
	}
}

func TestActivateReplicaPreservesAssignmentUpdatesFromReadyCallback(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	callback := &updatingCoordinatorClient{}
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, callback, transport)
	callback.node = node

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}

	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	replica := node.State().Replicas[1]
	if got, want := replica.State, ReplicaStateActive; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
	if got, want := replica.Assignment.ChainVersion, uint64(4); got != want {
		t.Fatalf("chain version = %d, want %d", got, want)
	}
	if got, want := replica.Assignment.Role, ReplicaRoleHead; got != want {
		t.Fatalf("role = %q, want %q", got, want)
	}
}

func TestAutoActivateEmptyReplicaDoesNotDuplicateReadyProgressUnderConcurrentActivation(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := &countingCoordinatorClient{inner: NewInMemoryCoordinatorClient()}
	node := mustNewNode(t, ctx, Config{
		NodeID:                    "node-a",
		AutoActivateEmptyReplicas: true,
	}, backend, coord, transport)

	const slot = 1
	errCh := make(chan error, 2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		errCh <- node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot})
	}()

	close(start)
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 1,
			Role:         ReplicaRoleSingle,
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && !errors.Is(err, ErrUnknownReplica) {
			t.Fatalf("concurrent ActivateReplica returned error: %v", err)
		}
	}

	if got, want := node.State().Replicas[slot].State, ReplicaStateActive; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
	if got, want := coord.readyCalls, 1; got != want {
		t.Fatalf("ready calls = %d, want %d", got, want)
	}
	if got, want := coord.inner.ReadySlots, []int{slot}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ready slots = %v, want %v", got, want)
	}
}

func TestAutoActivateEmptyReplicaDetachedFromShortDispatchDeadline(t *testing.T) {
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := &slowReadyCoordinatorClient{
		inner:      NewInMemoryCoordinatorClient(),
		readyDelay: 20 * time.Millisecond,
	}
	node := mustNewNode(t, context.Background(), Config{
		NodeID:                    "node-a",
		AutoActivateEmptyReplicas: true,
	}, backend, coord, transport)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleSingle,
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}

	if got, want := node.State().Replicas[1].State, ReplicaStateActive; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}
}

func TestNodeAddReplicaAsTailFailsCleanlyWhenSourceUnavailable(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-b"}, backend, coord, transport)

	err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "missing"},
		},
	})
	if err == nil {
		t.Fatal("AddReplicaAsTail unexpectedly succeeded")
	}
	if !errors.Is(err, ErrSnapshotSourceUnavailable) {
		t.Fatalf("error = %v, want snapshot source unavailable", err)
	}
	if _, exists := node.State().Replicas[1]; exists {
		t.Fatal("replica still present after failed add")
	}
	if _, err := backend.ReplicaData(1); err == nil {
		t.Fatal("backend slot still present after failed add")
	}
}

func TestNodeInvalidLifecycleTransitionsFail(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err == nil {
		t.Fatal("ActivateReplica unexpectedly succeeded")
	} else if !errors.Is(err, ErrUnknownReplica) {
		t.Fatalf("error = %v, want unknown replica", err)
	}

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 1}); err == nil {
		t.Fatal("MarkReplicaLeaving unexpectedly succeeded from catching_up")
	} else if !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("error = %v, want invalid transition", err)
	}
}

func TestNodeDrainAndRemoveLifecycle(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := backend.Put(1, "k", "v", testObjectMetadata(1)); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 1}); err != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}
	if got, want := node.State().Replicas[1].State, ReplicaStateLeaving; got != want {
		t.Fatalf("replica state = %q, want %q", got, want)
	}

	if err := node.RemoveReplica(ctx, RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if got, want := coord.RemovedSlots, []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("removed slots = %v, want %v", got, want)
	}
	if _, exists := node.State().Replicas[1]; exists {
		t.Fatal("replica still present after remove")
	}
	if _, err := backend.ReplicaData(1); err == nil {
		t.Fatal("backend data still present after remove")
	}
}

func TestNodeUpdateChainPeersTargetsOnlyOneSlot(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	for slot := 1; slot <= 2; slot++ {
		if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{Slot: slot, ChainVersion: 1, Role: ReplicaRoleSingle},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail(slot=%d) returned error: %v", slot, err)
		}
		if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(slot=%d) returned error: %v", slot, err)
		}
	}

	if err := node.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 2,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "node-b"},
		},
	}); err != nil {
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}

	state := node.State()
	if got, want := state.Replicas[1].Assignment.Peers.SuccessorNodeID, "node-b"; got != want {
		t.Fatalf("slot 1 successor = %q, want %q", got, want)
	}
	if got, want := state.Replicas[2].Assignment.Peers.SuccessorNodeID, ""; got != want {
		t.Fatalf("slot 2 successor = %q, want %q", got, want)
	}
}

func TestNodeReportHeartbeatSummarizesReplicas(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleTail},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail(second) returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := node.ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat returned error: %v", err)
	}

	if got, want := len(coord.Heartbeats), 1; got != want {
		t.Fatalf("heartbeat count = %d, want %d", got, want)
	}
	if got, want := coord.Heartbeats[0], (NodeStatus{
		NodeID:          "node-a",
		ReplicaCount:    2,
		ActiveCount:     1,
		CatchingUpCount: 1,
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("heartbeat = %#v, want %#v", got, want)
	}
}

func TestNodeReportHeartbeatOnlyDoesNotRegister(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := &countingCoordinatorClient{inner: NewInMemoryCoordinatorClient()}
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	if err := node.ReportHeartbeatOnly(ctx); err != nil {
		t.Fatalf("ReportHeartbeatOnly returned error: %v", err)
	}
	if got, want := coord.registerCalls, 0; got != want {
		t.Fatalf("registerCalls = %d, want %d", got, want)
	}
	if got, want := len(coord.inner.Heartbeats), 1; got != want {
		t.Fatalf("heartbeat count = %d, want %d", got, want)
	}
}

func TestNodeCatchingUpSlotsReturnsSortedSnapshot(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	for _, slot := range []int{3, 1, 2} {
		if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{Slot: slot, ChainVersion: 1, Role: ReplicaRoleSingle},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail(slot=%d) returned error: %v", slot, err)
		}
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}

	if got, want := node.CatchingUpSlots(), []int{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("CatchingUpSlots() = %v, want %v", got, want)
	}
}

func TestNodeLeavingSlotsReturnsSortedSnapshot(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	for _, slot := range []int{3, 1, 2} {
		if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{Slot: slot, ChainVersion: 1, Role: ReplicaRoleSingle},
		}); err != nil {
			t.Fatalf("AddReplicaAsTail(slot=%d) returned error: %v", slot, err)
		}
	}
	for _, slot := range []int{1, 2, 3} {
		if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
			t.Fatalf("ActivateReplica(slot=%d) returned error: %v", slot, err)
		}
	}
	for _, slot := range []int{3, 1} {
		if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: slot}); err != nil {
			t.Fatalf("MarkReplicaLeaving(slot=%d) returned error: %v", slot, err)
		}
	}

	if got, want := node.LeavingSlots(), []int{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LeavingSlots() = %v, want %v", got, want)
	}
}

func TestNodeConcurrentLifecycleAndStateInspection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transport := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	coord := NewInMemoryCoordinatorClient()
	node := mustNewNode(t, ctx, Config{NodeID: "node-a"}, backend, coord, transport)

	const (
		slotCount = 24
		workers   = 4
	)

	errCh := make(chan error, workers+3)
	done := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(3)
	stateReaderStarted := make(chan struct{})
	bufferReaderStarted := make(chan struct{})
	heartbeatStarted := make(chan struct{})
	var stateReaderOnce sync.Once
	var bufferReaderOnce sync.Once
	var heartbeatOnce sync.Once

	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				stateReaderOnce.Do(func() { close(stateReaderStarted) })
				_ = node.State()
			}
		}
	}()
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				bufferReaderOnce.Do(func() { close(bufferReaderStarted) })
				_ = node.CatchingUpSlots()
				_ = node.BufferedReplicaMessages()
				_ = node.CatchupCount()
			}
		}
	}()
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				if err := node.ReportHeartbeat(ctx); err != nil {
					errCh <- err
					return
				}
				heartbeatOnce.Do(func() { close(heartbeatStarted) })
			}
		}
	}()

	waitForTestSignal(t, ctx, stateReaderStarted, "state reader startup")
	waitForTestSignal(t, ctx, bufferReaderStarted, "buffer reader startup")
	waitForTestSignal(t, ctx, heartbeatStarted, "heartbeat startup")

	var writers sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		writers.Add(1)
		go func() {
			defer writers.Done()
			for slot := worker; slot < slotCount; slot += workers {
				added := ReplicaAssignment{
					Slot:         slot,
					ChainVersion: uint64(slot + 1),
					Role:         ReplicaRoleSingle,
				}
				if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{Assignment: added}); err != nil {
					errCh <- err
					return
				}
				updated := added
				updated.ChainVersion++
				if err := node.UpdateChainPeers(ctx, UpdateChainPeersCommand{Assignment: updated}); err != nil {
					errCh <- err
					return
				}
				if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
					errCh <- err
					return
				}
				if _, err := node.HighestCommittedSequence(slot); err != nil {
					errCh <- err
					return
				}
				if _, err := node.StagedSequences(slot); err != nil {
					errCh <- err
					return
				}
				if _, err := node.BufferedForwardSequences(slot); err != nil {
					errCh <- err
					return
				}
				if _, err := node.BufferedCommitSequences(slot); err != nil {
					errCh <- err
					return
				}
				if err := node.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: slot}); err != nil {
					errCh <- err
					return
				}
				if err := node.RemoveReplica(ctx, RemoveReplicaCommand{Slot: slot}); err != nil {
					errCh <- err
					return
				}
				if _, err := node.StagedSequences(slot); !errors.Is(err, ErrUnknownReplica) {
					errCh <- err
					return
				}
			}
		}()
	}

	writers.Wait()
	close(done)
	readers.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent lifecycle returned error: %v", err)
		}
	}
	if got := len(coord.Heartbeats); got == 0 {
		t.Fatal("expected at least one heartbeat during concurrent lifecycle")
	}
	if got, want := len(node.State().Replicas), 0; got != want {
		t.Fatalf("len(node.State().Replicas) = %d, want %d", got, want)
	}
}

func TestNodeConcurrentRecoveryLifecycleAndStateInspection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	repl := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceCoord := NewInMemoryCoordinatorClient()
	source := mustNewNode(t, ctx, Config{NodeID: "source"}, sourceBackend, sourceCoord, repl)
	repl.Register("source", sourceBackend)
	repl.RegisterNode("source", source)
	if err := source.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 2, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if _, err := source.SubmitPut(ctx, 2, "k", "v1"); err != nil {
		t.Fatalf("source SubmitPut returned error: %v", err)
	}

	targetBackend := NewInMemoryBackend()
	targetLocal := NewInMemoryLocalStateStore()
	targetCoord := NewInMemoryCoordinatorClient()
	target, err := OpenNode(ctx, Config{NodeID: "target"}, targetBackend, targetLocal, targetCoord, repl)
	if err != nil {
		t.Fatalf("OpenNode(target) returned error: %v", err)
	}
	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}
	if err := target.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		t.Fatalf("target ActivateReplica returned error: %v", err)
	}
	if _, err := target.SubmitPut(ctx, 2, "stale", "old"); err != nil {
		t.Fatalf("target SubmitPut returned error: %v", err)
	}
	_ = target.Close()

	recoveredTarget, err := OpenNode(ctx, Config{NodeID: "target"}, targetBackend, targetLocal, targetCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode(target) returned error: %v", err)
	}
	t.Cleanup(func() { _ = recoveredTarget.Close() })
	repl.Register("target", targetBackend)
	repl.RegisterNode("target", recoveredTarget)

	done := make(chan struct{})
	errCh := make(chan error, 8)
	var readers sync.WaitGroup
	readers.Add(3)
	stateReaderStarted := make(chan struct{})
	snapshotReaderStarted := make(chan struct{})
	heartbeatStarted := make(chan struct{})
	var stateReaderOnce sync.Once
	var snapshotReaderOnce sync.Once
	var heartbeatOnce sync.Once

	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				stateReaderOnce.Do(func() { close(stateReaderStarted) })
				_ = recoveredTarget.State()
				_ = recoveredTarget.CatchingUpSlots()
			}
		}
	}()
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				snapshotReaderOnce.Do(func() { close(snapshotReaderStarted) })
				if _, err := recoveredTarget.CommittedSnapshot(2); err != nil && !errors.Is(err, ErrUnknownReplica) {
					errCh <- err
					return
				}
				if _, err := recoveredTarget.HighestCommittedSequence(2); err != nil && !errors.Is(err, ErrUnknownReplica) {
					errCh <- err
					return
				}
			}
		}
	}()
	go func() {
		defer readers.Done()
		for {
			select {
			case <-done:
				return
			default:
				if err := recoveredTarget.ReportHeartbeat(ctx); err != nil {
					errCh <- err
					return
				}
				heartbeatOnce.Do(func() { close(heartbeatStarted) })
			}
		}
	}()

	waitForTestSignal(t, ctx, stateReaderStarted, "recovery state reader startup")
	waitForTestSignal(t, ctx, snapshotReaderStarted, "recovery snapshot reader startup")
	waitForTestSignal(t, ctx, heartbeatStarted, "recovery heartbeat startup")

	recoveredAssignment := ReplicaAssignment{
		Slot:         2,
		ChainVersion: 5,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "source"},
	}
	if err := recoveredTarget.RecoverReplica(ctx, RecoverReplicaCommand{
		Assignment:   recoveredAssignment,
		SourceNodeID: "source",
	}); err != nil {
		close(done)
		readers.Wait()
		t.Fatalf("RecoverReplica returned error: %v", err)
	}
	if err := recoveredTarget.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 6, Role: ReplicaRoleSingle},
	}); err != nil {
		close(done)
		readers.Wait()
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}
	if err := recoveredTarget.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 2}); err != nil {
		close(done)
		readers.Wait()
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}
	if err := recoveredTarget.RemoveReplica(ctx, RemoveReplicaCommand{Slot: 2}); err != nil {
		close(done)
		readers.Wait()
		t.Fatalf("RemoveReplica returned error: %v", err)
	}
	if err := recoveredTarget.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 7, Role: ReplicaRoleSingle},
	}); err != nil {
		close(done)
		readers.Wait()
		t.Fatalf("AddReplicaAsTail(re-add) returned error: %v", err)
	}
	if err := recoveredTarget.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		close(done)
		readers.Wait()
		t.Fatalf("ActivateReplica(re-add) returned error: %v", err)
	}

	close(done)
	readers.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent recovery lifecycle returned error: %v", err)
		}
	}

	if _, err := recoveredTarget.SubmitPut(ctx, 2, "final", "v2"); err != nil {
		t.Fatalf("SubmitPut after re-add returned error: %v", err)
	}
	snapshot, err := recoveredTarget.CommittedSnapshot(2)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	if got, want := snapshot["final"].Value, "v2"; got != want {
		t.Fatalf("final snapshot value = %q, want %q", got, want)
	}
	if got := len(targetCoord.Heartbeats); got == 0 {
		t.Fatal("expected heartbeat traffic during recovery lifecycle overlap")
	}
}

type updatingCoordinatorClient struct {
	node *Node
}

type countingCoordinatorClient struct {
	inner         *InMemoryCoordinatorClient
	mu            sync.Mutex
	registerCalls int
	readyCalls    int
}

type slowReadyCoordinatorClient struct {
	inner      *InMemoryCoordinatorClient
	readyDelay time.Duration
}

func (c *countingCoordinatorClient) RegisterNode(ctx context.Context, reg NodeRegistration) error {
	c.mu.Lock()
	c.registerCalls++
	c.mu.Unlock()
	return c.inner.RegisterNode(ctx, reg)
}

func (c *countingCoordinatorClient) ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error {
	c.mu.Lock()
	c.readyCalls++
	c.mu.Unlock()
	return c.inner.ReportReplicaReady(ctx, slot, epoch)
}

func (c *countingCoordinatorClient) ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error {
	return c.inner.ReportReplicaRemoved(ctx, slot, epoch)
}

func (c *countingCoordinatorClient) ReportNodeRecovered(ctx context.Context, report NodeRecoveryReport) error {
	return c.inner.ReportNodeRecovered(ctx, report)
}

func (c *countingCoordinatorClient) ReportNodeHeartbeat(ctx context.Context, status NodeStatus) error {
	return c.inner.ReportNodeHeartbeat(ctx, status)
}

func (c *slowReadyCoordinatorClient) RegisterNode(ctx context.Context, reg NodeRegistration) error {
	return c.inner.RegisterNode(ctx, reg)
}

func (c *slowReadyCoordinatorClient) ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error {
	timer := time.NewTimer(c.readyDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return c.inner.ReportReplicaReady(ctx, slot, epoch)
}

func (c *slowReadyCoordinatorClient) ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error {
	return c.inner.ReportReplicaRemoved(ctx, slot, epoch)
}

func (c *slowReadyCoordinatorClient) ReportNodeRecovered(ctx context.Context, report NodeRecoveryReport) error {
	return c.inner.ReportNodeRecovered(ctx, report)
}

func (c *slowReadyCoordinatorClient) ReportNodeHeartbeat(ctx context.Context, status NodeStatus) error {
	return c.inner.ReportNodeHeartbeat(ctx, status)
}

func (c *updatingCoordinatorClient) RegisterNode(context.Context, NodeRegistration) error {
	return nil
}

func (c *updatingCoordinatorClient) ReportReplicaReady(ctx context.Context, slot int, _ uint64) error {
	return c.node.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 4,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "node-b"},
		},
	})
}

func (c *updatingCoordinatorClient) ReportReplicaRemoved(context.Context, int, uint64) error {
	return nil
}

func (c *updatingCoordinatorClient) ReportNodeRecovered(context.Context, NodeRecoveryReport) error {
	return nil
}

func (c *updatingCoordinatorClient) ReportNodeHeartbeat(context.Context, NodeStatus) error {
	return nil
}

func TestEndToEndDrainFlowAcrossNodesWithoutNetworking(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	headBackend := NewInMemoryBackend()
	headCoord := NewInMemoryCoordinatorClient()
	headNode := mustNewNode(t, ctx, Config{NodeID: "head"}, headBackend, headCoord, transport)
	transport.Register("head", headBackend)

	tailBackend := NewInMemoryBackend()
	tailCoord := NewInMemoryCoordinatorClient()
	tailNode := mustNewNode(t, ctx, Config{NodeID: "tail"}, tailBackend, tailCoord, transport)
	transport.Register("tail", tailBackend)

	if err := headNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 7, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("head AddReplicaAsTail returned error: %v", err)
	}
	if err := headNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 7}); err != nil {
		t.Fatalf("head ActivateReplica returned error: %v", err)
	}
	if err := headBackend.Put(7, "order-1", "committed", testObjectMetadata(1)); err != nil {
		t.Fatalf("head Put returned error: %v", err)
	}

	if err := tailNode.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         7,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "head"},
		},
	}); err != nil {
		t.Fatalf("tail AddReplicaAsTail returned error: %v", err)
	}
	if err := tailNode.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 7}); err != nil {
		t.Fatalf("tail ActivateReplica returned error: %v", err)
	}
	if err := headNode.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         7,
			ChainVersion: 2,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "tail"},
		},
	}); err != nil {
		t.Fatalf("head UpdateChainPeers returned error: %v", err)
	}
	if err := headNode.MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 7}); err != nil {
		t.Fatalf("head MarkReplicaLeaving returned error: %v", err)
	}
	if err := headNode.RemoveReplica(ctx, RemoveReplicaCommand{Slot: 7}); err != nil {
		t.Fatalf("head RemoveReplica returned error: %v", err)
	}

	tailData, err := tailBackend.ReplicaData(7)
	if err != nil {
		t.Fatalf("tail ReplicaData returned error: %v", err)
	}
	if got, want := snapshotValues(tailData), map[string]string{"order-1": "committed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail data = %v, want %v", got, want)
	}
	if got, want := tailCoord.ReadySlots, []int{7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("tail ready slots = %v, want %v", got, want)
	}
	if got, want := headCoord.RemovedSlots, []int{7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("head removed slots = %v, want %v", got, want)
	}
}

func TestMultiSlotFailureIsolation(t *testing.T) {
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceCoord := NewInMemoryCoordinatorClient()
	source := mustNewNode(t, ctx, Config{NodeID: "source"}, sourceBackend, sourceCoord, transport)
	transport.Register("source", sourceBackend)

	if err := source.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if err := sourceBackend.Put(2, "k", "v", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put returned error: %v", err)
	}

	targetBackend := NewInMemoryBackend()
	targetCoord := NewInMemoryCoordinatorClient()
	target := mustNewNode(t, ctx, Config{NodeID: "target"}, targetBackend, targetCoord, transport)

	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         1,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "missing"},
		},
	}); err == nil {
		t.Fatal("AddReplicaAsTail(slot=1) unexpectedly succeeded")
	}

	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         2,
			ChainVersion: 1,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail(slot=2) returned error: %v", err)
	}
	if err := target.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 2}); err != nil {
		t.Fatalf("ActivateReplica(slot=2) returned error: %v", err)
	}

	state := target.State()
	if _, exists := state.Replicas[1]; exists {
		t.Fatal("failed slot 1 should not remain present")
	}
	if got, want := state.Replicas[2].State, ReplicaStateActive; got != want {
		t.Fatalf("slot 2 state = %q, want %q", got, want)
	}
}

func TestDeterministicRepeatedCommandStream(t *testing.T) {
	leftState, leftReady, leftRemoved, leftData := runDeterministicFlow(t)
	rightState, rightReady, rightRemoved, rightData := runDeterministicFlow(t)

	if !reflect.DeepEqual(leftState, rightState) {
		t.Fatalf("node state mismatch\nleft=%#v\nright=%#v", leftState, rightState)
	}
	if !reflect.DeepEqual(leftReady, rightReady) {
		t.Fatalf("ready reports mismatch\nleft=%v\nright=%v", leftReady, rightReady)
	}
	if !reflect.DeepEqual(leftRemoved, rightRemoved) {
		t.Fatalf("removed reports mismatch\nleft=%v\nright=%v", leftRemoved, rightRemoved)
	}
	if !reflect.DeepEqual(leftData, rightData) {
		t.Fatalf("backend data mismatch\nleft=%v\nright=%v", leftData, rightData)
	}
}

func runDeterministicFlow(t *testing.T) (NodeState, []int, []int, Snapshot) {
	t.Helper()

	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceCoord := NewInMemoryCoordinatorClient()
	source := mustNewNode(t, ctx, Config{NodeID: "source"}, sourceBackend, sourceCoord, transport)
	transport.Register("source", sourceBackend)

	targetBackend := NewInMemoryBackend()
	targetCoord := NewInMemoryCoordinatorClient()
	target := mustNewNode(t, ctx, Config{NodeID: "target"}, targetBackend, targetCoord, transport)
	transport.Register("target", targetBackend)

	if err := source.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 9, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if err := sourceBackend.Put(9, "a", "1", testObjectMetadata(1)); err != nil {
		t.Fatalf("source Put returned error: %v", err)
	}

	if err := target.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{
			Slot:         9,
			ChainVersion: 2,
			Role:         ReplicaRoleTail,
			Peers:        ChainPeers{PredecessorNodeID: "source"},
		},
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}
	if err := target.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("target ActivateReplica returned error: %v", err)
	}
	if err := target.UpdateChainPeers(ctx, UpdateChainPeersCommand{
		Assignment: ReplicaAssignment{
			Slot:         9,
			ChainVersion: 3,
			Role:         ReplicaRoleSingle,
		},
	}); err != nil {
		t.Fatalf("target UpdateChainPeers returned error: %v", err)
	}

	data, err := targetBackend.ReplicaData(9)
	if err != nil {
		t.Fatalf("target ReplicaData returned error: %v", err)
	}
	return target.State(), append([]int(nil), targetCoord.ReadySlots...), append([]int(nil), targetCoord.RemovedSlots...), data
}

func mustNewNode(t *testing.T, ctx context.Context, cfg Config, backend Backend, coord CoordinatorClient, repl ReplicationTransport) *Node {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = &fakeClock{now: time.Unix(0, 0).UTC()}
	}
	node, err := NewNode(ctx, cfg, backend, coord, repl)
	if err != nil {
		t.Fatalf("NewNode returned error: %v", err)
	}
	return node
}
