package storage

import (
	"context"
	"testing"
)

func mustWithSlotRuntime(t *testing.T, node *Node, slot int, fn func(*slotRuntime)) {
	t.Helper()
	owner := node.ensureSlotOwner(slot)
	done := make(chan struct{}, 1)
	if err := owner.dispatch(context.Background(), func(runtime *slotRuntime) {
		fn(runtime)
		done <- struct{}{}
	}); err != nil {
		t.Fatalf("slot owner dispatch returned error: %v", err)
	}
	select {
	case <-node.done:
		t.Fatal("node shut down while waiting on slot owner")
	case <-done:
	}
}

func mustSlotRecord(t *testing.T, node *Node, slot int) replicaRecord {
	t.Helper()
	var record replicaRecord
	mustWithSlotRuntime(t, node, slot, func(runtime *slotRuntime) {
		record = cloneReplicaRecord(runtime.record)
	})
	return ensureProtocolReplicaState(record)
}

func mustMutateSlotRecord(t *testing.T, node *Node, slot int, mutate func(replicaRecord) replicaRecord) {
	t.Helper()
	mustWithSlotRuntime(t, node, slot, func(runtime *slotRuntime) {
		record := ensureProtocolReplicaState(runtime.record)
		runtime.setRecord(mutate(record))
	})
}
