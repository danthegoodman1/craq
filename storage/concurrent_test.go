package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestConcurrentForwardAndCommitOnSameSlot verifies that a middle node can
// handle interleaved ForwardWrite (from head) and CommitWrite (from tail)
// messages concurrently on the same slot without corruption or deadlock.
//
// Concurrent goroutines call HandleForwardWrite directly on the middle node.
// Each forward is staged and synchronously forwarded to the tail, which
// commits it and sends a CommitWrite back to the middle. The slot owner on the
// middle serializes these mixed forward/commit operations. Out-of-order
// forwards are buffered and drained once the in-order sequence arrives.
func TestConcurrentForwardAndCommitOnSameSlot(t *testing.T) {
	const (
		slot        = 0
		numForwards = 20
	)
	nodes, _, _ := setupActiveChain(t, slot, []string{"head", "middle", "tail"})
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, numForwards)
	for seq := 1; seq <= numForwards; seq++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", seq)
			value := fmt.Sprintf("val-%d", seq)
			err := nodes["middle"].HandleForwardWrite(ctx, ForwardWriteRequest{
				Operation: WriteOperation{
					Slot:     slot,
					Sequence: uint64(seq),
					Kind:     OperationKindPut,
					Key:      key,
					Value:    value,
					Metadata: testObjectMetadata(uint64(seq)),
				},
				FromNodeID:   "head",
				ChainVersion: 1,
			})
			if err != nil {
				errs <- fmt.Errorf("HandleForwardWrite(seq=%d): %w", seq, err)
			}
		}(seq)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	want := make(map[string]string, numForwards)
	for seq := 1; seq <= numForwards; seq++ {
		want[fmt.Sprintf("key-%d", seq)] = fmt.Sprintf("val-%d", seq)
	}

	// Middle and tail must agree on the committed data and have no leftovers.
	// The head is excluded: it receives CommitWrite acks that get buffered
	// (it never staged these writes via SubmitPut).
	replicatingNodes := map[string]*Node{
		"middle": nodes["middle"],
		"tail":   nodes["tail"],
	}
	assertCommittedStateEqual(t, replicatingNodes, slot, want, uint64(numForwards))
}

// TestConcurrentWriteAndMarkLeavingOnSameSlot verifies that a SubmitPut and
// MarkReplicaLeaving racing on the same slot do not deadlock. The write may
// succeed or be rejected depending on lock ordering, but the node must end in
// a consistent leaving state.
func TestConcurrentWriteAndMarkLeavingOnSameSlot(t *testing.T) {
	const slot = 0
	nodes, _, _ := setupActiveChain(t, slot, []string{"head", "tail"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		wg       sync.WaitGroup
		writeErr error
		leaveErr error
	)

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, writeErr = nodes["head"].SubmitPut(ctx, slot, "k", "v")
	}()
	go func() {
		defer wg.Done()
		// Brief yield so both goroutines are in-flight around the same time.
		time.Sleep(time.Millisecond)
		leaveErr = nodes["head"].MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: slot})
	}()
	wg.Wait()

	// Reaching this point proves no deadlock occurred within the timeout.

	// MarkReplicaLeaving on an active replica always succeeds.
	if leaveErr != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", leaveErr)
	}

	state := nodes["head"].State()
	replica, ok := state.Replicas[slot]
	if !ok {
		t.Fatal("replica missing from node state after concurrent operations")
	}
	if replica.State != ReplicaStateLeaving {
		t.Fatalf("replica state = %q, want %q", replica.State, ReplicaStateLeaving)
	}

	// If the write won the race and committed, the tail must have the data.
	if writeErr == nil {
		snap := mustNodeCommittedSnapshot(t, nodes["tail"], slot)
		if got, ok := snap["k"]; !ok || got != "v" {
			t.Fatalf("write succeeded but tail snapshot missing key: %v", snap)
		}
	}
}

// TestConcurrentForwardsOnSameSlot fires concurrent HandleForwardWrite calls
// directly at a tail node for the same slot with different sequence numbers.
// Forwards that arrive out-of-order are buffered by the node; the per-slot
// lock guarantees every sequence is eventually drained and committed without
// corruption.
func TestConcurrentForwardsOnSameSlot(t *testing.T) {
	const (
		slot        = 0
		numForwards = 10
	)
	nodes, _, _ := setupActiveChain(t, slot, []string{"head", "tail"})
	ctx := context.Background()

	var wg sync.WaitGroup
	errs := make(chan error, numForwards)
	for seq := 1; seq <= numForwards; seq++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			key := fmt.Sprintf("fwd-%d", seq)
			value := fmt.Sprintf("val-%d", seq)
			err := nodes["tail"].HandleForwardWrite(ctx, ForwardWriteRequest{
				Operation: WriteOperation{
					Slot:     slot,
					Sequence: uint64(seq),
					Kind:     OperationKindPut,
					Key:      key,
					Value:    value,
					Metadata: testObjectMetadata(uint64(seq)),
				},
				FromNodeID:   "head",
				ChainVersion: 1,
			})
			if err != nil {
				errs <- fmt.Errorf("HandleForwardWrite(seq=%d): %w", seq, err)
			}
		}(seq)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	tailSnapshot := mustNodeCommittedSnapshot(t, nodes["tail"], slot)
	if got, want := len(tailSnapshot), numForwards; got != want {
		t.Fatalf("tail committed %d keys, want %d", got, want)
	}
	for seq := 1; seq <= numForwards; seq++ {
		key := fmt.Sprintf("fwd-%d", seq)
		wantVal := fmt.Sprintf("val-%d", seq)
		if got := tailSnapshot[key]; got != wantVal {
			t.Fatalf("tail[%q] = %q, want %q", key, got, wantVal)
		}
	}

	// All sequences must be committed with none left staged.
	if got, want := mustHighestCommitted(t, nodes["tail"], slot), uint64(numForwards); got != want {
		t.Fatalf("tail highest committed = %d, want %d", got, want)
	}
	if staged := mustNodeStagedSequences(t, nodes["tail"], slot); len(staged) != 0 {
		t.Fatalf("tail has %d staged sequences, want 0: %v", len(staged), staged)
	}
}
