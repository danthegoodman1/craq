package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestClientHandlersReadAndWriteAgainstServingReplicas(t *testing.T) {
	ctx := context.Background()

	t.Run("tail read succeeds", func(t *testing.T) {
		nodes, _, _ := setupActiveChain(t, 9, []string{"head", "mid", "tail"})
		if _, err := nodes["head"].SubmitPut(ctx, 9, "k", "v"); err != nil {
			t.Fatalf("SubmitPut returned error: %v", err)
		}

		result, err := nodes["tail"].HandleClientGet(ctx, ClientGetRequest{
			Slot:                 9,
			Key:                  "k",
			ExpectedChainVersion: 1,
		})
		if err != nil {
			t.Fatalf("HandleClientGet returned error: %v", err)
		}
		if got, want := result.Slot, 9; got != want {
			t.Fatalf("slot = %d, want %d", got, want)
		}
		if got, want := result.ChainVersion, uint64(1); got != want {
			t.Fatalf("chain version = %d, want %d", got, want)
		}
		if !result.Found || result.Value != "v" || result.Metadata == nil {
			t.Fatalf("read result = %#v, want found value with metadata", result)
		}
	})

	t.Run("single replica read and write succeed", func(t *testing.T) {
		transport := NewInMemoryReplicationTransport()
		node := mustNewNode(t, ctx, Config{NodeID: "single"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
		mustActivateReplica(t, node, 2, ReplicaAssignment{Slot: 2, ChainVersion: 1, Role: ReplicaRoleSingle})

		if _, err := node.HandleClientPut(ctx, ClientPutRequest{
			Slot:                 2,
			Key:                  "k",
			Value:                "v",
			ExpectedChainVersion: 1,
		}); err != nil {
			t.Fatalf("HandleClientPut returned error: %v", err)
		}
		result, err := node.HandleClientGet(ctx, ClientGetRequest{
			Slot:                 2,
			Key:                  "k",
			ExpectedChainVersion: 1,
		})
		if err != nil {
			t.Fatalf("HandleClientGet returned error: %v", err)
		}
		if got, want := result.Value, "v"; got != want {
			t.Fatalf("value = %q, want %q", got, want)
		}
		if _, err := node.HandleClientDelete(ctx, ClientDeleteRequest{
			Slot:                 2,
			Key:                  "k",
			ExpectedChainVersion: 1,
		}); err != nil {
			t.Fatalf("HandleClientDelete returned error: %v", err)
		}
		result, err = node.HandleClientGet(ctx, ClientGetRequest{
			Slot:                 2,
			Key:                  "k",
			ExpectedChainVersion: 1,
		})
		if err != nil {
			t.Fatalf("HandleClientGet after delete returned error: %v", err)
		}
		if result.Found {
			t.Fatalf("result after delete = %#v, want not found", result)
		}
	})
}

func TestClientHandlersRejectStaleOrIllegalTargets(t *testing.T) {
	ctx := context.Background()
	nodes, _, _ := setupActiveChain(t, 5, []string{"head", "mid", "tail"})

	assertRoutingMismatch := func(t *testing.T, err error, wantReason RoutingMismatchReason) {
		t.Helper()
		var mismatch *RoutingMismatchError
		if !errors.As(err, &mismatch) {
			t.Fatalf("error = %v, want routing mismatch", err)
		}
		var ambiguous *AmbiguousWriteError
		if errors.As(err, &ambiguous) {
			t.Fatalf("error = %v, unexpectedly classified as ambiguous write", err)
		}
		if mismatch.Reason != wantReason {
			t.Fatalf("mismatch reason = %q, want %q", mismatch.Reason, wantReason)
		}
	}

	if _, err := nodes["head"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 5,
		Key:                  "k",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("HandleClientGet on head returned error: %v", err)
	}

	if _, err := nodes["tail"].HandleClientPut(ctx, ClientPutRequest{
		Slot:                 5,
		Key:                  "k",
		Value:                "v",
		ExpectedChainVersion: 1,
	}); err == nil {
		t.Fatal("HandleClientPut on tail unexpectedly succeeded")
	} else {
		assertRoutingMismatch(t, err, RoutingMismatchReasonWrongRole)
	}

	if _, err := nodes["tail"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 5,
		Key:                  "k",
		ExpectedChainVersion: 99,
	}); err == nil {
		t.Fatal("HandleClientGet with wrong version unexpectedly succeeded")
	} else {
		assertRoutingMismatch(t, err, RoutingMismatchReasonWrongVersion)
	}

	if err := nodes["tail"].MarkReplicaLeaving(ctx, MarkReplicaLeavingCommand{Slot: 5}); err != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}
	if _, err := nodes["tail"].HandleClientGet(ctx, ClientGetRequest{
		Slot:                 5,
		Key:                  "k",
		ExpectedChainVersion: 1,
	}); err == nil {
		t.Fatal("HandleClientGet on leaving replica unexpectedly succeeded")
	} else {
		assertRoutingMismatch(t, err, RoutingMismatchReasonInactiveReplica)
	}

	transport := NewInMemoryReplicationTransport()
	unknown := mustNewNode(t, ctx, Config{NodeID: "x"}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
	if _, err := unknown.HandleClientPut(ctx, ClientPutRequest{
		Slot:                 99,
		Key:                  "k",
		Value:                "v",
		ExpectedChainVersion: 1,
	}); err == nil {
		t.Fatal("HandleClientPut on unknown slot unexpectedly succeeded")
	} else {
		assertRoutingMismatch(t, err, RoutingMismatchReasonUnknownSlot)
	}
}

func TestCommittedReadsDoNotExposeStagedWrites(t *testing.T) {
	ctx := context.Background()
	backend := NewInMemoryBackend()
	transport := NewInMemoryReplicationTransport()
	node := mustNewNode(t, ctx, Config{NodeID: "single"}, backend, NewInMemoryCoordinatorClient(), transport)
	mustActivateReplica(t, node, 4, ReplicaAssignment{Slot: 4, ChainVersion: 1, Role: ReplicaRoleSingle})

	if err := backend.StagePut(4, 1, "k", "v", testObjectMetadata(1)); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}
	result, err := node.HandleClientGet(ctx, ClientGetRequest{
		Slot:                 4,
		Key:                  "k",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("HandleClientGet returned error: %v", err)
	}
	if result.Found {
		t.Fatalf("read result = %#v, want not found before commit", result)
	}
}

func TestClientWriteTimeoutsBecomeAmbiguousErrors(t *testing.T) {
	ctx := context.Background()

	t.Run("put timeout returns ambiguous write and preserves staged state", func(t *testing.T) {
		transport := &blockingWriteTransport{}
		node := mustNewNode(t, ctx, Config{
			NodeID:             "head",
			WriteCommitTimeout: time.Nanosecond,
		}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), transport)
		mustActivateReplica(t, node, 7, ReplicaAssignment{
			Slot:         7,
			ChainVersion: 3,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "tail"},
		})

		_, err := node.HandleClientPut(ctx, ClientPutRequest{
			Slot:                 7,
			Key:                  "k",
			Value:                "v",
			ExpectedChainVersion: 3,
		})
		if err == nil {
			t.Fatal("HandleClientPut unexpectedly succeeded")
		}
		var ambiguous *AmbiguousWriteError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("error = %v, want ambiguous write", err)
		}
		if !errors.Is(err, ErrAmbiguousWrite) {
			t.Fatalf("error = %v, want ErrAmbiguousWrite", err)
		}
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want ErrWriteTimeout", err)
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context deadline exceeded", err)
		}
		if got, want := ambiguous.Slot, 7; got != want {
			t.Fatalf("ambiguous slot = %d, want %d", got, want)
		}
		if got, want := ambiguous.Kind, OperationKindPut; got != want {
			t.Fatalf("ambiguous kind = %q, want %q", got, want)
		}
		if got, want := ambiguous.ExpectedChainVersion, uint64(3); got != want {
			t.Fatalf("ambiguous expected version = %d, want %d", got, want)
		}
		if got, want := mustNodeStagedSequences(t, node, 7), []uint64{1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("staged sequences = %v, want %v", got, want)
		}
	})

	t.Run("delete cancellation returns ambiguous write", func(t *testing.T) {
		transport := &blockingWriteTransport{}
		backend := NewInMemoryBackend()
		node := mustNewNode(t, ctx, Config{
			NodeID:             "head",
			WriteCommitTimeout: time.Hour,
		}, backend, NewInMemoryCoordinatorClient(), transport)
		mustActivateReplica(t, node, 8, ReplicaAssignment{
			Slot:         8,
			ChainVersion: 4,
			Role:         ReplicaRoleHead,
			Peers:        ChainPeers{SuccessorNodeID: "tail"},
		})
		if err := backend.Put(8, "k", "seed", testObjectMetadata(1)); err != nil {
			t.Fatalf("backend.Put returned error: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := node.HandleClientDelete(ctx, ClientDeleteRequest{
			Slot:                 8,
			Key:                  "k",
			ExpectedChainVersion: 4,
		})
		if err == nil {
			t.Fatal("HandleClientDelete unexpectedly succeeded")
		}
		var ambiguous *AmbiguousWriteError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("error = %v, want ambiguous write", err)
		}
		if !errors.Is(err, ErrAmbiguousWrite) {
			t.Fatalf("error = %v, want ErrAmbiguousWrite", err)
		}
		if !errors.Is(err, ErrWriteTimeout) {
			t.Fatalf("error = %v, want ErrWriteTimeout", err)
		}
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", err)
		}
		if got, want := ambiguous.Kind, OperationKindDelete; got != want {
			t.Fatalf("ambiguous kind = %q, want %q", got, want)
		}
	})
}

func TestInMemoryBackendGetCommitted(t *testing.T) {
	backend := NewInMemoryBackend()
	if err := backend.CreateReplica(7); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}
	if err := backend.StagePut(7, 1, "k", "v", testObjectMetadata(1)); err != nil {
		t.Fatalf("StagePut returned error: %v", err)
	}

	if _, found, err := backend.GetCommitted(7, "k"); err != nil {
		t.Fatalf("GetCommitted before commit returned error: %v", err)
	} else if found {
		t.Fatal("GetCommitted before commit unexpectedly found value")
	}

	if err := backend.CommitSequence(7, 1); err != nil {
		t.Fatalf("CommitSequence returned error: %v", err)
	}
	if got, found, err := backend.GetCommitted(7, "k"); err != nil {
		t.Fatalf("GetCommitted after commit returned error: %v", err)
	} else if !found || got.Value != "v" || got.Metadata != testObjectMetadata(1) {
		t.Fatalf("GetCommitted after commit = (%#v, %v), want committed object", got, found)
	}
}
