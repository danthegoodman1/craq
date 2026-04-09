package badger

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/danthegoodman1/craq/storage"
)

func TestBadgerNodeReopenAfterAddReplicaAsTailBeforeActivationRecoversViaPeer(t *testing.T) {
	ctx := context.Background()
	repl := storage.NewInMemoryReplicationTransport()

	sourceBackend := storage.NewInMemoryBackend()
	sourceLocal := storage.NewInMemoryLocalStateStore()
	source, err := storage.OpenNode(ctx, storage.Config{NodeID: "source"}, sourceBackend, sourceLocal, storage.NewInMemoryCoordinatorClient(), repl)
	if err != nil {
		t.Fatalf("OpenNode(source) returned error: %v", err)
	}
	repl.Register("source", sourceBackend)
	repl.RegisterNode("source", source)
	if err := source.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: storage.ReplicaAssignment{Slot: 2, ChainVersion: 4, Role: storage.ReplicaRoleSingle},
		Epoch:      5,
	}); err != nil {
		t.Fatalf("source AddReplicaAsTail returned error: %v", err)
	}
	if err := source.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 2, Epoch: 5}); err != nil {
		t.Fatalf("source ActivateReplica returned error: %v", err)
	}
	if result, err := source.SubmitPut(ctx, 2, "alpha", "v1"); err != nil {
		t.Fatalf("source SubmitPut returned error: %v", err)
	} else if got, want := result.Sequence, uint64(1); got != want {
		t.Fatalf("source sequence = %d, want %d", got, want)
	}

	path := filepath.Join(t.TempDir(), "target.db")
	store := mustOpenStore(t, path)
	coord := storage.NewInMemoryCoordinatorClient()
	target, err := storage.OpenNode(ctx, storage.Config{NodeID: "target"}, store.Backend(), store.LocalStateStore(), coord, repl)
	if err != nil {
		t.Fatalf("OpenNode(target) returned error: %v", err)
	}
	repl.Register("target", store.Backend())
	repl.RegisterNode("target", target)

	assignment := storage.ReplicaAssignment{
		Slot:         2,
		ChainVersion: 5,
		Role:         storage.ReplicaRoleTail,
		Peers:        storage.ChainPeers{PredecessorNodeID: "source"},
	}
	if err := target.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: assignment,
		Epoch:      6,
	}); err != nil {
		t.Fatalf("target AddReplicaAsTail returned error: %v", err)
	}
	if err := target.Close(); err != nil {
		t.Fatalf("target.Close returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "target"}, reopenedStore.Backend(), reopenedStore.LocalStateStore(), recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode(target) returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	repl.Register("target", reopenedStore.Backend())
	repl.RegisterNode("target", reopened)

	replica := reopened.State().Replicas[2]
	if got, want := replica.State, storage.ReplicaStateRecovered; got != want {
		t.Fatalf("reopened target state = %q, want %q", got, want)
	}
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].LastKnownState, storage.ReplicaStateCatchingUp; got != want {
		t.Fatalf("reported last-known state = %q, want %q", got, want)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if !recoveredCoord.RecoveryReports[0].Replicas[0].HasCommittedData {
		t.Fatal("reported committed data presence = false, want true")
	}

	if err := reopened.RecoverReplica(ctx, storage.RecoverReplicaCommand{
		Assignment:   assignment,
		SourceNodeID: "source",
		Epoch:        7,
	}); err != nil {
		t.Fatalf("RecoverReplica returned error: %v", err)
	}
	if got, want := reopened.State().Replicas[2].State, storage.ReplicaStateActive; got != want {
		t.Fatalf("recovered target state = %q, want %q", got, want)
	}
	if got, err := reopened.HighestCommittedSequence(2); err != nil {
		t.Fatalf("HighestCommittedSequence returned error: %v", err)
	} else if want := uint64(1); got != want {
		t.Fatalf("recovered target highest committed sequence = %d, want %d", got, want)
	}
	if read, err := reopened.HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 2,
		Key:                  "alpha",
		ExpectedChainVersion: 5,
	}); err != nil {
		t.Fatalf("HandleClientGet after recovery returned error: %v", err)
	} else if !read.Found || read.Value != "v1" {
		t.Fatalf("HandleClientGet after recovery = %#v, want value v1", read)
	}
}

func TestBadgerNodeReopenAfterDirtyCRAQStateRecoversFromServingPeer(t *testing.T) {
	ctx := context.Background()
	repl := storage.NewQueuedInMemoryReplicationTransport()

	headPath := filepath.Join(t.TempDir(), "head.db")
	headStore := mustOpenStore(t, headPath)
	headCoord := storage.NewInMemoryCoordinatorClient()
	head, err := storage.OpenNode(ctx, storage.Config{NodeID: "head"}, headStore.Backend(), headStore.LocalStateStore(), headCoord, repl)
	if err != nil {
		t.Fatalf("OpenNode(head) returned error: %v", err)
	}
	repl.Register("head", headStore.Backend())
	repl.RegisterNode("head", head)

	tailBackend := storage.NewInMemoryBackend()
	tailLocal := storage.NewInMemoryLocalStateStore()
	tailCoord := storage.NewInMemoryCoordinatorClient()
	tail, err := storage.OpenNode(ctx, storage.Config{NodeID: "tail"}, tailBackend, tailLocal, tailCoord, repl)
	if err != nil {
		t.Fatalf("OpenNode(tail) returned error: %v", err)
	}
	repl.Register("tail", tailBackend)
	repl.RegisterNode("tail", tail)

	headAssignment := storage.ReplicaAssignment{
		Slot:         3,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleHead,
		Peers: storage.ChainPeers{
			SuccessorNodeID: "tail",
			SuccessorTarget: "tail",
			TailNodeID:      "tail",
			TailTarget:      "tail",
		},
	}
	tailAssignment := storage.ReplicaAssignment{
		Slot:         3,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleTail,
		Peers: storage.ChainPeers{
			PredecessorNodeID: "head",
			PredecessorTarget: "head",
			TailNodeID:        "tail",
			TailTarget:        "tail",
		},
	}
	// Head must be initialized first so that the tail can fetch its snapshot.
	for _, pair := range []struct {
		node       *storage.Node
		assignment storage.ReplicaAssignment
	}{
		{head, headAssignment},
		{tail, tailAssignment},
	} {
		if err := pair.node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: pair.assignment, Epoch: 5}); err != nil {
			t.Fatalf("AddReplicaAsTail returned error: %v", err)
		}
		if err := pair.node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 3, Epoch: 5}); err != nil {
			t.Fatalf("ActivateReplica returned error: %v", err)
		}
		if err := pair.node.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{Assignment: pair.assignment, Epoch: 5}); err != nil {
			t.Fatalf("UpdateChainPeers returned error: %v", err)
		}
	}

	if _, err := head.SubmitPut(ctx, 3, "alpha", "v1"); err != nil {
		t.Fatalf("head SubmitPut(v1) returned error: %v", err)
	}

	var dropped bool
	repl.SetBeforeDeliver(func(msg storage.QueuedReplicationMessage) {
		if dropped || msg.Forward == nil || msg.ToNodeID != "tail" {
			return
		}
		dropped = true
		repl.DropNext()
	})
	if _, err := head.SubmitPut(ctx, 3, "alpha", "v2"); err == nil {
		t.Fatal("head SubmitPut(v2) unexpectedly succeeded")
	} else if !errors.Is(err, storage.ErrStateMismatch) {
		t.Fatalf("head SubmitPut(v2) error = %v, want ErrStateMismatch", err)
	}

	if read, err := tail.HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 3,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("tail HandleClientGet returned error: %v", err)
	} else if !read.Found || read.Value != "v2" {
		t.Fatalf("tail HandleClientGet = %#v, want value v2", read)
	}

	if err := head.Close(); err != nil {
		t.Fatalf("head.Close returned error: %v", err)
	}
	if err := headStore.Close(); err != nil {
		t.Fatalf("headStore.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, headPath)
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "head"}, reopenedStore.Backend(), reopenedStore.LocalStateStore(), recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode(head) returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	repl.Register("head", reopenedStore.Backend())
	repl.RegisterNode("head", reopened)

	replica := reopened.State().Replicas[3]
	if got, want := replica.State, storage.ReplicaStateRecovered; got != want {
		t.Fatalf("reopened head state = %q, want %q", got, want)
	}
	if _, err := reopened.HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 3,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	}); err == nil {
		t.Fatal("HandleClientGet unexpectedly succeeded on recovered replica")
	}
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	report := recoveredCoord.RecoveryReports[0].Replicas[0]
	if got, want := report.HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if got, want := report.LastKnownState, storage.ReplicaStateActive; got != want {
		t.Fatalf("reported last-known state = %q, want %q", got, want)
	}
	if !report.HasCommittedData {
		t.Fatal("reported committed data presence = false, want true")
	}

	if err := reopened.RecoverReplica(ctx, storage.RecoverReplicaCommand{
		Assignment:   headAssignment,
		SourceNodeID: "tail",
		Epoch:        6,
	}); err != nil {
		t.Fatalf("RecoverReplica returned error: %v", err)
	}
	if read, err := reopened.HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 3,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("HandleClientGet after recovery returned error: %v", err)
	} else if !read.Found || read.Value != "v2" {
		t.Fatalf("HandleClientGet after recovery = %#v, want value v2", read)
	}
}

func TestBadgerNodeReopenAfterMarkReplicaLeavingBeforeRemoveDropsRecoveredReplica(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	repl := storage.NewInMemoryReplicationTransport()
	coord := storage.NewInMemoryCoordinatorClient()
	node, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, store.Backend(), store.LocalStateStore(), coord, repl)
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}

	assignment := storage.ReplicaAssignment{Slot: 1, ChainVersion: 4, Role: storage.ReplicaRoleSingle}
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment, Epoch: 5}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1, Epoch: 5}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if result, err := node.SubmitPut(ctx, 1, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	} else if got, want := result.Sequence, uint64(1); got != want {
		t.Fatalf("SubmitPut sequence = %d, want %d", got, want)
	}
	if err := node.MarkReplicaLeaving(ctx, storage.MarkReplicaLeavingCommand{Slot: 1, Epoch: 6}); err != nil {
		t.Fatalf("MarkReplicaLeaving returned error: %v", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("node.Close returned error: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("store.Close returned error: %v", err)
	}

	reopenedStore := mustOpenStore(t, path)
	recoveredCoord := storage.NewInMemoryCoordinatorClient()
	reopened, err := storage.OpenNode(ctx, storage.Config{NodeID: "node-a"}, reopenedStore.Backend(), reopenedStore.LocalStateStore(), recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode returned error: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	replica := reopened.State().Replicas[1]
	if got, want := replica.State, storage.ReplicaStateRecovered; got != want {
		t.Fatalf("reopened state = %q, want %q", got, want)
	}
	if err := reopened.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].LastKnownState, storage.ReplicaStateLeaving; got != want {
		t.Fatalf("reported last-known state = %q, want %q", got, want)
	}
	if got, want := recoveredCoord.RecoveryReports[0].Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}

	if err := reopened.DropRecoveredReplica(ctx, storage.DropRecoveredReplicaCommand{Slot: 1, Epoch: 7}); err != nil {
		t.Fatalf("DropRecoveredReplica returned error: %v", err)
	}
	if _, exists := reopened.State().Replicas[1]; exists {
		t.Fatal("replica still present after DropRecoveredReplica")
	}
	if _, err := reopenedStore.Backend().HighestCommittedSequence(1); !errors.Is(err, storage.ErrUnknownReplica) {
		t.Fatalf("HighestCommittedSequence error after drop = %v, want ErrUnknownReplica", err)
	}
	state, err := reopenedStore.LocalStateStore().LoadNode(ctx, "node-a")
	if err != nil {
		t.Fatalf("LoadNode returned error: %v", err)
	}
	if got := len(state.Replicas); got != 0 {
		t.Fatalf("persisted replicas after drop = %d, want 0", got)
	}
}
