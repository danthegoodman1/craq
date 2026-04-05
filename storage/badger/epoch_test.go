package badger

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/danthegoodman1/craq/storage"
)

func TestBadgerNodePersistsHighestAcceptedCoordinatorEpoch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	repl := storage.NewInMemoryReplicationTransport()
	coord := storage.NewInMemoryCoordinatorClient()

	node, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, store.Backend(), store.LocalStateStore(), coord, repl)
	if err != nil {
		t.Fatalf("storage.OpenNode returned error: %v", err)
	}
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
		Epoch:      7,
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if got, want := node.HighestAcceptedCoordinatorEpoch(), uint64(7); got != want {
		t.Fatalf("highest accepted epoch = %d, want %d", got, want)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("node.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, reopenedStore.Backend(), reopenedStore.LocalStateStore(), storage.NewInMemoryCoordinatorClient(), repl)
	if err != nil {
		t.Fatalf("reopen storage.OpenNode returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	if got, want := reopened.HighestAcceptedCoordinatorEpoch(), uint64(7); got != want {
		t.Fatalf("reopened highest accepted epoch = %d, want %d", got, want)
	}
	err = reopened.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 2, Role: storage.ReplicaRoleHead},
		Epoch:      6,
	})
	if !errors.Is(err, storage.ErrWriteRejected) {
		t.Fatalf("UpdateChainPeers stale epoch error = %v, want ErrWriteRejected", err)
	}
	if err := reopened.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 2, Role: storage.ReplicaRoleHead},
		Epoch:      8,
	}); err != nil {
		t.Fatalf("UpdateChainPeers newer epoch returned error: %v", err)
	}
	if got, want := reopened.HighestAcceptedCoordinatorEpoch(), uint64(8); got != want {
		t.Fatalf("updated highest accepted epoch = %d, want %d", got, want)
	}
}
