package badger

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/storage"
	"github.com/danthegoodman1/craq/storage/storagetest"
)

var errInjectedDurableWrite = errors.New("injected durable write failure")

func TestBadgerBackendConformance(t *testing.T) {
	storagetest.RunBackendSuite(t, func(t *testing.T) storage.Backend {
		t.Helper()
		store, err := Open(filepath.Join(t.TempDir(), "node.db"))
		if err != nil {
			t.Fatalf("Open returned error: %v", err)
		}
		return store.Backend()
	})
}

func TestBadgerLocalStateStoreConformance(t *testing.T) {
	storagetest.RunLocalStateStoreSuite(t, func(t *testing.T) storage.LocalStateStore {
		t.Helper()
		store, err := Open(filepath.Join(t.TempDir(), "node.db"))
		if err != nil {
			t.Fatalf("Open returned error: %v", err)
		}
		return store.LocalStateStore()
	})
}

func TestBadgerCompletedOperationsSurviveReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	backend := store.Backend()
	local := store.LocalStateStore()
	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after CreateReplica returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	if got, err := backend.HighestCommittedSequence(1); err != nil {
		t.Fatalf("HighestCommittedSequence after reopen returned error: %v", err)
	} else if got != 0 {
		t.Fatalf("HighestCommittedSequence after reopen = %d, want 0", got)
	}
	if err := backend.StagePut(1, 1, "k", "v", testMetadata(1)); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after StagePut returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	if got, err := backend.StagedSequences(1); err != nil {
		t.Fatalf("StagedSequences after reopen returned error: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("StagedSequences after reopen = %v, want none", got)
	}
	if err := backend.StagePut(1, 1, "k", "v", testMetadata(1)); err != nil {
		t.Fatalf("StagePut after reopen returned error: %v", err)
	}
	if err := backend.CommitSequence(1, 1); err != nil {
		t.Fatalf("CommitSequence returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after CommitSequence returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	if got, found, err := backend.GetCommitted(1, "k"); err != nil {
		t.Fatalf("GetCommitted after reopen returned error: %v", err)
	} else if !found || got.Value != "v" || got.Metadata != testMetadata(1) {
		t.Fatalf("GetCommitted after reopen = (%#v, %t), want committed object", got, found)
	}
	if got, err := backend.StagedSequences(1); err != nil {
		t.Fatalf("StagedSequences after commit reopen returned error: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("StagedSequences after commit reopen = %v, want none", got)
	}
	if err := local.UpsertReplica(ctx, "node-a", storage.PersistedReplica{
		Assignment:               storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
		LastKnownState:           storage.ReplicaStateActive,
		HighestCommittedSequence: 1,
		HasCommittedData:         true,
	}); err != nil {
		t.Fatalf("UpsertReplica returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after UpsertReplica returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	backend = store.Backend()
	local = store.LocalStateStore()
	state, err := local.LoadNode(ctx, "node-a")
	if err != nil {
		t.Fatalf("LoadNode after reopen returned error: %v", err)
	}
	if got, want := len(state.Replicas), 1; got != want {
		t.Fatalf("replica count after reopen = %d, want %d", got, want)
	}
	if err := backend.DeleteReplica(1); err != nil {
		t.Fatalf("DeleteReplica returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close after DeleteReplica returned error: %v", err)
	}

	store = mustOpenStore(t, path)
	defer func() { _ = store.Close() }()
	if _, err := store.Backend().HighestCommittedSequence(1); !errors.Is(err, storage.ErrUnknownReplica) {
		t.Fatalf("HighestCommittedSequence after delete reopen error = %v, want unknown replica", err)
	}
}

func TestBadgerNodeRestartRecoveryAndClose(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	repl := storage.NewInMemoryReplicationTransport()
	coord := storage.NewInMemoryCoordinatorClient()
	backend := store.Backend()
	local := store.LocalStateStore()

	node, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, backend, local, coord, repl)
	if err != nil {
		t.Fatalf("storage.OpenNode returned error: %v", err)
	}
	repl.Register("node-a", backend)
	repl.RegisterNode("node-a", node)
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 4, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if result, err := node.SubmitPut(ctx, 1, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	} else if result.Sequence != 1 {
		t.Fatalf("SubmitPut sequence = %d, want 1", result.Sequence)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Node.Close returned error: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("second Node.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopenedBackend := reopenedStore.Backend()
	reopenedLocal := reopenedStore.LocalStateStore()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, reopenedBackend, reopenedLocal, recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen storage.OpenNode returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	repl.Register("node-a", reopenedBackend)
	repl.RegisterNode("node-a", reopened)

	replica := reopened.State().Replicas[1]
	if got, want := replica.State, storage.ReplicaStateRecovered; got != want {
		t.Fatalf("reopened state = %q, want %q", got, want)
	}
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if err := reopened.ResumeRecoveredReplica(ctx, storage.ResumeRecoveredReplicaCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 4, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("ResumeRecoveredReplica returned error: %v", err)
	}
	if result, err := reopened.SubmitPut(ctx, 1, "beta", "v2"); err != nil {
		t.Fatalf("SubmitPut after reopen returned error: %v", err)
	} else if result.Sequence != 2 {
		t.Fatalf("SubmitPut after reopen sequence = %d, want 2", result.Sequence)
	}
	waitForBackendValue(t, reopenedBackend, 1, "beta", "v2")
}

func TestBadgerOpenCleansStagedOperations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	backend := store.Backend()
	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica(1) returned error: %v", err)
	}
	if err := backend.StagePut(1, 1, "staged", "ghost", testMetadata(1)); err != nil {
		t.Fatalf("StagePut(slot=1) returned error: %v", err)
	}
	if err := backend.CreateReplica(2); err != nil {
		t.Fatalf("CreateReplica(2) returned error: %v", err)
	}
	mustCommitValue(t, backend, 2, 1, "committed", "v1")
	if err := backend.StagePut(2, 2, "staged", "ghost", testMetadata(2)); err != nil {
		t.Fatalf("StagePut(slot=2) returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopened := mustOpenStore(t, path)
	defer func() { _ = reopened.Close() }()
	reopenedBackend := reopened.Backend()
	if got, err := reopenedBackend.StagedSequences(1); err != nil {
		t.Fatalf("StagedSequences(slot=1) returned error: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("StagedSequences(slot=1) after reopen = %v, want none", got)
	}
	if got, err := reopenedBackend.StagedSequences(2); err != nil {
		t.Fatalf("StagedSequences(slot=2) returned error: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("StagedSequences(slot=2) after reopen = %v, want none", got)
	}
	if got, err := reopenedBackend.HighestCommittedSequence(1); err != nil {
		t.Fatalf("HighestCommittedSequence(slot=1) returned error: %v", err)
	} else if got != 0 {
		t.Fatalf("HighestCommittedSequence(slot=1) = %d, want 0", got)
	}
	if got, err := reopenedBackend.CommittedSnapshot(1); err != nil {
		t.Fatalf("CommittedSnapshot(slot=1) returned error: %v", err)
	} else if !reflect.DeepEqual(snapshotValues(got), map[string]string{}) {
		t.Fatalf("CommittedSnapshot(slot=1) = %v, want empty", got)
	}
	if got, err := reopenedBackend.HighestCommittedSequence(2); err != nil {
		t.Fatalf("HighestCommittedSequence(slot=2) returned error: %v", err)
	} else if got != 1 {
		t.Fatalf("HighestCommittedSequence(slot=2) = %d, want 1", got)
	}
	if got, err := reopenedBackend.CommittedSnapshot(2); err != nil {
		t.Fatalf("CommittedSnapshot(slot=2) returned error: %v", err)
	} else if want := map[string]string{"committed": "v1"}; !reflect.DeepEqual(snapshotValues(got), want) {
		t.Fatalf("CommittedSnapshot(slot=2) = %v, want %v", got, want)
	}
}

func waitForBackendValue(t *testing.T, backend storage.Backend, slot int, key string, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		value, found, err := backend.GetCommitted(slot, key)
		if err == nil && found && value.Value == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if value, found, err := backend.GetCommitted(slot, key); err != nil {
		t.Fatalf("GetCommitted returned error: %v", err)
	} else if !found || value.Value != want {
		t.Fatalf("GetCommitted = (%#v, %t), want value %s", value, found, want)
	}
}

func TestBadgerOpenCleanupFailureIsRetryable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	backend := store.Backend()
	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}
	if err := backend.StagePut(1, 1, "k", "v", testMetadata(1)); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	restore := setBeforeDurableWriteForTest(func(op string) error {
		if op == opCleanupStagedOnOpen {
			return errInjectedDurableWrite
		}
		return nil
	})
	if _, err := Open(path); err == nil {
		t.Fatal("Open unexpectedly succeeded")
	} else if !errors.Is(err, errInjectedDurableWrite) {
		t.Fatalf("Open error = %v, want injected durable write failure", err)
	}
	restore()

	reopened := mustOpenStore(t, path)
	defer func() { _ = reopened.Close() }()
	if got, err := reopened.Backend().StagedSequences(1); err != nil {
		t.Fatalf("StagedSequences returned error: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("StagedSequences after retry reopen = %v, want none", got)
	}
}

func TestBadgerFailedDurableMutationsLeavePrefixStateAfterReopen(t *testing.T) {
	ctx := context.Background()
	type durableMutationCase struct {
		name    string
		op      string
		arrange func(t *testing.T, backend storage.Backend, local storage.LocalStateStore)
		act     func(backend storage.Backend, local storage.LocalStateStore) error
		assert  func(t *testing.T, backend storage.Backend, local storage.LocalStateStore)
	}

	replica := storage.PersistedReplica{
		Assignment:               storage.ReplicaAssignment{Slot: 3, ChainVersion: 2, Role: storage.ReplicaRoleTail},
		LastKnownState:           storage.ReplicaStateActive,
		HighestCommittedSequence: 5,
		HasCommittedData:         true,
	}

	cases := []durableMutationCase{
		{
			name: "create replica",
			op:   opCreateReplica,
			act: func(backend storage.Backend, _ storage.LocalStateStore) error {
				return backend.CreateReplica(1)
			},
			assert: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				if _, err := backend.HighestCommittedSequence(1); !errors.Is(err, storage.ErrUnknownReplica) {
					t.Fatalf("HighestCommittedSequence after failed create error = %v, want unknown replica", err)
				}
			},
		},
		{
			name: "install snapshot",
			op:   opInstallSnapshot,
			arrange: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				if err := backend.CreateReplica(1); err != nil {
					t.Fatalf("CreateReplica returned error: %v", err)
				}
				mustCommitValue(t, backend, 1, 1, "old", "v1")
			},
			act: func(backend storage.Backend, _ storage.LocalStateStore) error {
				return backend.InstallSnapshot(1, valueSnapshot(map[string]string{"new": "v2"}))
			},
			assert: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				assertCommittedSnapshot(t, backend, 1, map[string]string{"old": "v1"})
				assertHighestCommitted(t, backend, 1, 1)
			},
		},
		{
			name: "set highest committed sequence",
			op:   opSetHighestCommittedSequence,
			arrange: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				if err := backend.CreateReplica(1); err != nil {
					t.Fatalf("CreateReplica returned error: %v", err)
				}
				mustCommitValue(t, backend, 1, 1, "k", "v1")
			},
			act: func(backend storage.Backend, _ storage.LocalStateStore) error {
				return backend.SetHighestCommittedSequence(1, 9)
			},
			assert: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				assertCommittedSnapshot(t, backend, 1, map[string]string{"k": "v1"})
				assertHighestCommitted(t, backend, 1, 1)
			},
		},
		{
			name: "commit sequence",
			op:   opCommitSequence,
			arrange: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				if err := backend.CreateReplica(1); err != nil {
					t.Fatalf("CreateReplica returned error: %v", err)
				}
				if err := backend.StagePut(1, 1, "k", "v1", testMetadata(1)); err != nil {
					t.Fatalf("StagePut returned error: %v", err)
				}
			},
			act: func(backend storage.Backend, _ storage.LocalStateStore) error {
				return backend.CommitSequence(1, 1)
			},
			assert: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				assertCommittedSnapshot(t, backend, 1, map[string]string{})
				assertHighestCommitted(t, backend, 1, 0)
				if got, err := backend.StagedSequences(1); err != nil {
					t.Fatalf("StagedSequences returned error: %v", err)
				} else if len(got) != 0 {
					t.Fatalf("StagedSequences after failed commit reopen = %v, want none", got)
				}
			},
		},
		{
			name: "apply committed",
			op:   opApplyCommitted,
			arrange: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				if err := backend.CreateReplica(1); err != nil {
					t.Fatalf("CreateReplica returned error: %v", err)
				}
			},
			act: func(backend storage.Backend, _ storage.LocalStateStore) error {
				replica := storage.PersistedReplica{
					Assignment:               storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
					LastKnownState:           storage.ReplicaStateActive,
					HighestCommittedSequence: 1,
					HasCommittedData:         true,
				}
				return backend.ApplyCommitted(ctx, "node-a", storage.WriteOperation{
					Slot:     1,
					Sequence: 1,
					Kind:     storage.OperationKindPut,
					Key:      "k",
					Value:    "v1",
					Metadata: testMetadata(1),
				}, &replica)
			},
			assert: func(t *testing.T, backend storage.Backend, local storage.LocalStateStore) {
				t.Helper()
				assertCommittedSnapshot(t, backend, 1, map[string]string{})
				assertHighestCommitted(t, backend, 1, 0)
				state, err := local.LoadNode(ctx, "node-a")
				if err != nil {
					t.Fatalf("LoadNode returned error: %v", err)
				}
				if got, want := len(state.Replicas), 0; got != want {
					t.Fatalf("LoadNode replica count = %d, want %d", got, want)
				}
			},
		},
		{
			name: "delete replica",
			op:   opDeleteReplica,
			arrange: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				if err := backend.CreateReplica(1); err != nil {
					t.Fatalf("CreateReplica returned error: %v", err)
				}
				mustCommitValue(t, backend, 1, 1, "k", "v1")
			},
			act: func(backend storage.Backend, _ storage.LocalStateStore) error {
				return backend.DeleteReplica(1)
			},
			assert: func(t *testing.T, backend storage.Backend, _ storage.LocalStateStore) {
				t.Helper()
				assertCommittedSnapshot(t, backend, 1, map[string]string{"k": "v1"})
				assertHighestCommitted(t, backend, 1, 1)
			},
		},
		{
			name: "upsert local replica",
			op:   opUpsertLocalReplica,
			act: func(_ storage.Backend, local storage.LocalStateStore) error {
				return local.UpsertReplica(ctx, "node-a", replica)
			},
			assert: func(t *testing.T, _ storage.Backend, local storage.LocalStateStore) {
				t.Helper()
				state, err := local.LoadNode(ctx, "node-a")
				if err != nil {
					t.Fatalf("LoadNode returned error: %v", err)
				}
				if got, want := len(state.Replicas), 0; got != want {
					t.Fatalf("LoadNode replica count = %d, want %d", got, want)
				}
			},
		},
		{
			name: "delete local replica",
			op:   opDeleteLocalReplica,
			arrange: func(t *testing.T, _ storage.Backend, local storage.LocalStateStore) {
				t.Helper()
				if err := local.UpsertReplica(ctx, "node-a", replica); err != nil {
					t.Fatalf("UpsertReplica returned error: %v", err)
				}
			},
			act: func(_ storage.Backend, local storage.LocalStateStore) error {
				return local.DeleteReplica(ctx, "node-a", replica.Assignment.Slot)
			},
			assert: func(t *testing.T, _ storage.Backend, local storage.LocalStateStore) {
				t.Helper()
				state, err := local.LoadNode(ctx, "node-a")
				if err != nil {
					t.Fatalf("LoadNode returned error: %v", err)
				}
				if got, want := len(state.Replicas), 1; got != want {
					t.Fatalf("LoadNode replica count = %d, want %d", got, want)
				}
				if got, want := state.Replicas[0], replica; !reflect.DeepEqual(got, want) {
					t.Fatalf("LoadNode replica = %#v, want %#v", got, want)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "node.db")
			store := mustOpenStore(t, path)
			backend := store.Backend()
			local := store.LocalStateStore()
			if tc.arrange != nil {
				tc.arrange(t, backend, local)
			}

			restore := setBeforeDurableWriteForTest(func(op string) error {
				if op == tc.op {
					return errInjectedDurableWrite
				}
				return nil
			})
			err := tc.act(backend, local)
			restore()
			if !errors.Is(err, errInjectedDurableWrite) {
				t.Fatalf("act error = %v, want injected durable write failure", err)
			}
			if err := store.Close(); err != nil {
				t.Fatalf("Close returned error: %v", err)
			}

			reopened := mustOpenStore(t, path)
			defer func() { _ = reopened.Close() }()
			tc.assert(t, reopened.Backend(), reopened.LocalStateStore())
		})
	}
}

func TestBadgerNodeReopenDropsUncommittedForwardFailure(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	repl := storage.NewInMemoryReplicationTransport()
	coord := storage.NewInMemoryCoordinatorClient()
	backend := store.Backend()
	local := store.LocalStateStore()

	node, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, backend, local, coord, repl)
	if err != nil {
		t.Fatalf("storage.OpenNode returned error: %v", err)
	}
	repl.Register("node-a", backend)
	repl.RegisterNode("node-a", node)
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 7, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := node.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{
		Assignment: storage.ReplicaAssignment{
			Slot:         1,
			ChainVersion: 7,
			Role:         storage.ReplicaRoleHead,
			Peers: storage.ChainPeers{
				SuccessorNodeID: "missing",
			},
		},
	}); err != nil {
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}
	if _, err := node.HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 1,
		Key:                  "lost",
		Value:                "v1",
		ExpectedChainVersion: 7,
	}); err == nil {
		t.Fatal("HandleClientPut unexpectedly succeeded")
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Node.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	defer func() { _ = reopenedStore.Close() }()
	reopenedBackend := reopenedStore.Backend()
	reopenedLocal := reopenedStore.LocalStateStore()
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, reopenedBackend, reopenedLocal, recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen storage.OpenNode returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(0); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if got, found, err := reopenedBackend.GetCommitted(1, "lost"); err != nil {
		t.Fatalf("GetCommitted(lost) returned error: %v", err)
	} else if found {
		t.Fatalf("GetCommitted(lost) = (%#v, %t), want not found", got, found)
	}
	if err := reopened.ResumeRecoveredReplica(ctx, storage.ResumeRecoveredReplicaCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 7, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("ResumeRecoveredReplica returned error: %v", err)
	}
	if result, err := reopened.SubmitPut(ctx, 1, "next", "v2"); err != nil {
		t.Fatalf("SubmitPut after reopen returned error: %v", err)
	} else if got, want := result.Sequence, uint64(1); got != want {
		t.Fatalf("SubmitPut after reopen sequence = %d, want %d", got, want)
	}
}

func TestBadgerNodeReopenIgnoresStagedRemnantsForRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	repl := storage.NewInMemoryReplicationTransport()
	coord := storage.NewInMemoryCoordinatorClient()
	backend := store.Backend()
	local := store.LocalStateStore()

	node, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, backend, local, coord, repl)
	if err != nil {
		t.Fatalf("storage.OpenNode returned error: %v", err)
	}
	repl.Register("node-a", backend)
	repl.RegisterNode("node-a", node)
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 7, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if result, err := node.SubmitPut(ctx, 1, "committed", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	} else if got, want := result.Sequence, uint64(1); got != want {
		t.Fatalf("SubmitPut sequence = %d, want %d", got, want)
	}
	if err := backend.StagePut(1, 2, "ghost", "v2", testMetadata(2)); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Node.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	defer func() { _ = reopenedStore.Close() }()
	reopenedBackend := reopenedStore.Backend()
	reopenedLocal := reopenedStore.LocalStateStore()
	if got, err := reopenedBackend.StagedSequences(1); err != nil {
		t.Fatalf("StagedSequences after reopen returned error: %v", err)
	} else if len(got) != 0 {
		t.Fatalf("StagedSequences after reopen = %v, want none", got)
	}
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, reopenedBackend, reopenedLocal, recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen storage.OpenNode returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if err := reopened.ResumeRecoveredReplica(ctx, storage.ResumeRecoveredReplicaCommand{
		Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 7, Role: storage.ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("ResumeRecoveredReplica returned error: %v", err)
	}
	if result, err := reopened.SubmitPut(ctx, 1, "next", "v2"); err != nil {
		t.Fatalf("SubmitPut after reopen returned error: %v", err)
	} else if got, want := result.Sequence, uint64(2); got != want {
		t.Fatalf("SubmitPut after reopen sequence = %d, want %d", got, want)
	}
	if got, found, err := reopenedBackend.GetCommitted(1, "ghost"); err != nil {
		t.Fatalf("GetCommitted(ghost) returned error: %v", err)
	} else if found {
		t.Fatalf("GetCommitted(ghost) = (%#v, %t), want not found", got, found)
	}
}

func mustCommitValue(t *testing.T, backend storage.Backend, slot int, sequence uint64, key string, value string) {
	t.Helper()
	if err := backend.StagePut(slot, sequence, key, value, testMetadata(sequence)); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}
	if err := backend.CommitSequence(slot, sequence); err != nil {
		t.Fatalf("CommitSequence returned error: %v", err)
	}
}

func assertCommittedSnapshot(t *testing.T, backend storage.Backend, slot int, want map[string]string) {
	t.Helper()
	got, err := backend.CommittedSnapshot(slot)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	if !reflect.DeepEqual(snapshotValues(got), want) {
		t.Fatalf("CommittedSnapshot = %v, want %v", got, want)
	}
}

func assertHighestCommitted(t *testing.T, backend storage.Backend, slot int, want uint64) {
	t.Helper()
	got, err := backend.HighestCommittedSequence(slot)
	if err != nil {
		t.Fatalf("HighestCommittedSequence returned error: %v", err)
	}
	if got != want {
		t.Fatalf("HighestCommittedSequence = %d, want %d", got, want)
	}
}

func mustOpenStore(t *testing.T, path string) *Store {
	t.Helper()
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open returned error: %v", err)
	}
	return store
}

func testMetadata(version uint64) storage.ObjectMetadata {
	base := time.Unix(0, 0).UTC()
	return storage.ObjectMetadata{
		Version:   version,
		CreatedAt: base.Add(time.Duration(version) * time.Second),
		UpdatedAt: base.Add(time.Duration(version) * time.Second),
	}
}

func snapshotValues(snapshot storage.Snapshot) map[string]string {
	values := make(map[string]string, len(snapshot))
	for key, object := range snapshot {
		values[key] = object.Value
	}
	return values
}

func valueSnapshot(entries map[string]string) storage.Snapshot {
	snapshot := make(storage.Snapshot, len(entries))
	for key, value := range entries {
		snapshot[key] = storage.CommittedObject{Value: value, Metadata: storage.ObjectMetadata{}}
	}
	return snapshot
}
