package storagetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/storage"
)

type BackendFactory func(t *testing.T) storage.Backend

type LocalStateStoreFactory func(t *testing.T) storage.LocalStateStore

func RunBackendSuite(t *testing.T, factory BackendFactory) {
	t.Helper()

	t.Run("create delete replica lifecycle", func(t *testing.T) {
		backend := factory(t)
		t.Cleanup(func() { _ = backend.Close() })

		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		if got, err := backend.HighestCommittedSequence(1); err != nil {
			t.Fatalf("HighestCommittedSequence returned error: %v", err)
		} else if got != 0 {
			t.Fatalf("HighestCommittedSequence = %d, want 0", got)
		}
		if err := backend.DeleteReplica(1); err != nil {
			t.Fatalf("DeleteReplica returned error: %v", err)
		}
		if _, err := backend.HighestCommittedSequence(1); !errors.Is(err, storage.ErrUnknownReplica) {
			t.Fatalf("HighestCommittedSequence error = %v, want unknown replica", err)
		}
	})

	t.Run("staged writes are isolated from committed reads", func(t *testing.T) {
		backend := factory(t)
		t.Cleanup(func() { _ = backend.Close() })
		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		if err := backend.StagePut(1, 1, "k", "v", testMetadata(1)); err != nil {
			t.Fatalf("StagePut returned error: %v", err)
		}
		if value, found, err := backend.GetCommitted(1, "k"); err != nil {
			t.Fatalf("GetCommitted returned error: %v", err)
		} else if found || value != (storage.CommittedObject{}) {
			t.Fatalf("GetCommitted = (%#v, %t), want zero,false", value, found)
		}
	})

	t.Run("commit enforces sequence order and clears staged entries", func(t *testing.T) {
		backend := factory(t)
		t.Cleanup(func() { _ = backend.Close() })
		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		putMetadata := testMetadata(1)
		deleteMetadata := testMetadata(2)
		if err := backend.StagePut(1, 1, "k", "v1", putMetadata); err != nil {
			t.Fatalf("StagePut returned error: %v", err)
		}
		if err := backend.StageDelete(1, 2, "k", deleteMetadata); err != nil {
			t.Fatalf("StageDelete returned error: %v", err)
		}
		if err := backend.CommitSequence(1, 2); !errors.Is(err, storage.ErrSequenceMismatch) {
			t.Fatalf("CommitSequence out of order error = %v, want sequence mismatch", err)
		}
		if err := backend.CommitSequence(1, 1); err != nil {
			t.Fatalf("CommitSequence(1) returned error: %v", err)
		}
		if got, found, err := backend.GetCommitted(1, "k"); err != nil {
			t.Fatalf("GetCommitted after put returned error: %v", err)
		} else if !found || got != (storage.CommittedObject{Value: "v1", Metadata: putMetadata}) {
			t.Fatalf("GetCommitted after put = (%#v, %t), want committed object", got, found)
		}
		if err := backend.CommitSequence(1, 2); err != nil {
			t.Fatalf("CommitSequence(2) returned error: %v", err)
		}
		if got, found, err := backend.GetCommitted(1, "k"); err != nil {
			t.Fatalf("GetCommitted after delete returned error: %v", err)
		} else if found || got != (storage.CommittedObject{}) {
			t.Fatalf("GetCommitted after delete = (%#v, %t), want zero,false", got, found)
		}
		if got, err := backend.StagedSequences(1); err != nil {
			t.Fatalf("StagedSequences returned error: %v", err)
		} else if len(got) != 0 {
			t.Fatalf("StagedSequences = %v, want none", got)
		}
		if got, err := backend.HighestCommittedSequence(1); err != nil {
			t.Fatalf("HighestCommittedSequence returned error: %v", err)
		} else if got != 2 {
			t.Fatalf("HighestCommittedSequence = %d, want 2", got)
		}
	})

	t.Run("committed snapshots are deep copies", func(t *testing.T) {
		backend := factory(t)
		t.Cleanup(func() { _ = backend.Close() })
		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		if err := backend.StagePut(1, 1, "k", "v", testMetadata(1)); err != nil {
			t.Fatalf("StagePut returned error: %v", err)
		}
		if err := backend.CommitSequence(1, 1); err != nil {
			t.Fatalf("CommitSequence returned error: %v", err)
		}
		snapshot, err := backend.CommittedSnapshot(1)
		if err != nil {
			t.Fatalf("CommittedSnapshot returned error: %v", err)
		}
		snapshot["k"] = storage.CommittedObject{Value: "mutated", Metadata: testMetadata(9)}
		if got, found, err := backend.GetCommitted(1, "k"); err != nil {
			t.Fatalf("GetCommitted returned error: %v", err)
		} else if !found || got != (storage.CommittedObject{Value: "v", Metadata: testMetadata(1)}) {
			t.Fatalf("GetCommitted = (%#v, %t), want committed object", got, found)
		}
	})

	t.Run("delete replica clears slot state", func(t *testing.T) {
		backend := factory(t)
		t.Cleanup(func() { _ = backend.Close() })
		if err := backend.CreateReplica(3); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		if err := backend.StagePut(3, 1, "k", "v", testMetadata(1)); err != nil {
			t.Fatalf("StagePut returned error: %v", err)
		}
		if err := backend.CommitSequence(3, 1); err != nil {
			t.Fatalf("CommitSequence returned error: %v", err)
		}
		if err := backend.DeleteReplica(3); err != nil {
			t.Fatalf("DeleteReplica returned error: %v", err)
		}
		if _, err := backend.CommittedSnapshot(3); !errors.Is(err, storage.ErrUnknownReplica) {
			t.Fatalf("CommittedSnapshot error = %v, want unknown replica", err)
		}
	})
}

func testMetadata(version uint64) storage.ObjectMetadata {
	base := time.Unix(0, 0).UTC()
	return storage.ObjectMetadata{
		Version:   version,
		CreatedAt: base.Add(time.Duration(version) * time.Second),
		UpdatedAt: base.Add(time.Duration(version) * time.Second),
	}
}

func RunLocalStateStoreSuite(t *testing.T, factory LocalStateStoreFactory) {
	t.Helper()

	t.Run("load empty node", func(t *testing.T) {
		local := factory(t)
		t.Cleanup(func() { _ = local.Close() })
		state, err := local.LoadNode(context.Background(), "node-a")
		if err != nil {
			t.Fatalf("LoadNode returned error: %v", err)
		}
		if got, want := state.NodeID, "node-a"; got != want {
			t.Fatalf("LoadNode node ID = %q, want %q", got, want)
		}
		if len(state.Replicas) != 0 {
			t.Fatalf("LoadNode replicas = %#v, want none", state.Replicas)
		}
	})

	t.Run("upsert overwrite and ordered load", func(t *testing.T) {
		local := factory(t)
		t.Cleanup(func() { _ = local.Close() })
		ctx := context.Background()
		replicaTwo := storage.PersistedReplica{
			Assignment:               storage.ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: storage.ReplicaRoleTail},
			LastKnownState:           storage.ReplicaStateActive,
			HighestCommittedSequence: 2,
			HasCommittedData:         true,
		}
		replicaOne := storage.PersistedReplica{
			Assignment:               storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleHead},
			LastKnownState:           storage.ReplicaStateLeaving,
			HighestCommittedSequence: 3,
			HasCommittedData:         true,
		}
		if err := local.UpsertReplica(ctx, "node-a", replicaTwo); err != nil {
			t.Fatalf("UpsertReplica(replicaTwo) returned error: %v", err)
		}
		if err := local.UpsertReplica(ctx, "node-a", replicaOne); err != nil {
			t.Fatalf("UpsertReplica(replicaOne) returned error: %v", err)
		}
		replicaOne.HighestCommittedSequence = 4
		if err := local.UpsertReplica(ctx, "node-a", replicaOne); err != nil {
			t.Fatalf("UpsertReplica(overwrite) returned error: %v", err)
		}

		state, err := local.LoadNode(ctx, "node-a")
		if err != nil {
			t.Fatalf("LoadNode returned error: %v", err)
		}
		if got, want := len(state.Replicas), 2; got != want {
			t.Fatalf("replica count = %d, want %d", got, want)
		}
		if got, want := state.Replicas[0].Assignment.Slot, 1; got != want {
			t.Fatalf("first slot = %d, want %d", got, want)
		}
		if got, want := state.Replicas[0].HighestCommittedSequence, uint64(4); got != want {
			t.Fatalf("overwritten sequence = %d, want %d", got, want)
		}
	})

	t.Run("delete replica", func(t *testing.T) {
		local := factory(t)
		t.Cleanup(func() { _ = local.Close() })
		ctx := context.Background()
		replica := storage.PersistedReplica{
			Assignment:               storage.ReplicaAssignment{Slot: 4, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
			LastKnownState:           storage.ReplicaStateActive,
			HighestCommittedSequence: 1,
			HasCommittedData:         true,
		}
		if err := local.UpsertReplica(ctx, "node-a", replica); err != nil {
			t.Fatalf("UpsertReplica returned error: %v", err)
		}
		if err := local.DeleteReplica(ctx, "node-a", 4); err != nil {
			t.Fatalf("DeleteReplica returned error: %v", err)
		}
		state, err := local.LoadNode(ctx, "node-a")
		if err != nil {
			t.Fatalf("LoadNode returned error: %v", err)
		}
		if len(state.Replicas) != 0 {
			t.Fatalf("replicas = %#v, want none", state.Replicas)
		}
	})
}
