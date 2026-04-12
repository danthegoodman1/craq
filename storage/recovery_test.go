package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestOpenNodeRestoresRecoveredReplicaAndResumePreservesSequence(t *testing.T) {
	ctx := context.Background()
	repl := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	local := NewInMemoryLocalStateStore()
	coord := NewInMemoryCoordinatorClient()

	node, err := OpenNode(ctx, Config{NodeID: "node-a"}, backend, local, coord, repl)
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	repl.Register("node-a", backend)
	repl.RegisterNode("node-a", node)

	assignment := ReplicaAssignment{
		Slot:         1,
		ChainVersion: 4,
		Role:         ReplicaRoleSingle,
	}
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if result, err := node.SubmitPut(ctx, 1, "alpha", "v1"); err != nil {
		t.Fatalf("SubmitPut returned error: %v", err)
	} else if got, want := result.Sequence, uint64(1); got != want {
		t.Fatalf("first write sequence = %d, want %d", got, want)
	}

	persisted, err := local.LoadNode(ctx, "node-a")
	if err != nil {
		t.Fatalf("LoadNode returned error: %v", err)
	}
	if got, want := persisted.Replicas[0].HighestCommittedSequence, uint64(0); got != want {
		t.Fatalf("persisted highest committed sequence = %d, want %d (commit sequence now rehydrates from backend metadata on reopen)", got, want)
	}
	if got, want := persisted.Replicas[0].LastKnownState, ReplicaStateActive; got != want {
		t.Fatalf("persisted last-known state = %q, want %q", got, want)
	}

	recoveredCoord := NewInMemoryCoordinatorClient()
	recovered, err := OpenNode(ctx, Config{NodeID: "node-a"}, backend, local, recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode returned error: %v", err)
	}
	repl.RegisterNode("node-a", recovered)

	replica := recovered.State().Replicas[1]
	if got, want := replica.State, ReplicaStateRecovered; got != want {
		t.Fatalf("recovered state = %q, want %q", got, want)
	}
	if _, err := recovered.HandleClientGet(ctx, ClientGetRequest{
		Slot:                 1,
		Key:                  "alpha",
		ExpectedChainVersion: 4,
	}); err == nil {
		t.Fatal("HandleClientGet unexpectedly succeeded on recovered replica")
	} else {
		var mismatch *RoutingMismatchError
		if !errors.As(err, &mismatch) || mismatch.Reason != RoutingMismatchReasonInactiveReplica {
			t.Fatalf("HandleClientGet error = %v, want inactive routing mismatch", err)
		}
	}

	if err := recovered.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	if got, want := len(recoveredCoord.RecoveryReports), 1; got != want {
		t.Fatalf("recovery reports = %d, want %d", got, want)
	}
	report := recoveredCoord.RecoveryReports[0]
	if got, want := report.Replicas[0].LastKnownState, ReplicaStateActive; got != want {
		t.Fatalf("reported last-known state = %q, want %q", got, want)
	}
	if got, want := report.Replicas[0].HighestCommittedSequence, uint64(1); got != want {
		t.Fatalf("reported highest committed sequence = %d, want %d", got, want)
	}
	if !report.Replicas[0].HasCommittedData {
		t.Fatal("reported committed data presence = false, want true")
	}

	if err := recovered.ResumeRecoveredReplica(ctx, ResumeRecoveredReplicaCommand{Assignment: assignment}); err != nil {
		t.Fatalf("ResumeRecoveredReplica returned error: %v", err)
	}
	if result, err := recovered.SubmitPut(ctx, 1, "beta", "v2"); err != nil {
		t.Fatalf("SubmitPut after resume returned error: %v", err)
	} else if got, want := result.Sequence, uint64(2); got != want {
		t.Fatalf("post-resume write sequence = %d, want %d", got, want)
	}
	if value, found, err := backend.GetCommitted(1, "beta"); err != nil {
		t.Fatalf("GetCommitted returned error: %v", err)
	} else if !found || value.Value != "v2" {
		t.Fatalf("GetCommitted = (%#v, %t), want value v2", value, found)
	}
}

func TestReportRecoveredStateMarksMissingBackendDataAsUnavailable(t *testing.T) {
	ctx := context.Background()
	repl := NewInMemoryReplicationTransport()
	backend := NewInMemoryBackend()
	local := NewInMemoryLocalStateStore()
	coord := NewInMemoryCoordinatorClient()

	node, err := OpenNode(ctx, Config{NodeID: "node-a"}, backend, local, coord, repl)
	if err != nil {
		t.Fatalf("OpenNode returned error: %v", err)
	}
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 3, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 3}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := backend.DeleteReplica(3); err != nil {
		t.Fatalf("DeleteReplica returned error: %v", err)
	}

	recoveredCoord := NewInMemoryCoordinatorClient()
	recovered, err := OpenNode(ctx, Config{NodeID: "node-a"}, backend, local, recoveredCoord, repl)
	if err != nil {
		t.Fatalf("reopen OpenNode returned error: %v", err)
	}
	if err := recovered.ReportRecoveredState(ctx); err != nil {
		t.Fatalf("ReportRecoveredState returned error: %v", err)
	}
	report := recoveredCoord.RecoveryReports[0]
	if report.Replicas[0].HasCommittedData {
		t.Fatal("HasCommittedData = true, want false")
	}
}

func TestRecoverReplicaRebuildsFromPeerAndDropRecoveredReplicaDeletesLocalState(t *testing.T) {
	ctx := context.Background()
	repl := NewInMemoryReplicationTransport()

	sourceBackend := NewInMemoryBackend()
	sourceLocal := NewInMemoryLocalStateStore()
	sourceCoord := NewInMemoryCoordinatorClient()
	source, err := OpenNode(ctx, Config{NodeID: "source"}, sourceBackend, sourceLocal, sourceCoord, repl)
	if err != nil {
		t.Fatalf("OpenNode(source) returned error: %v", err)
	}
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
	if _, err := source.SubmitPut(ctx, 2, "k", "v"); err != nil {
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

	recoveredTarget, err := OpenNode(ctx, Config{NodeID: "target"}, targetBackend, targetLocal, NewInMemoryCoordinatorClient(), repl)
	if err != nil {
		t.Fatalf("reopen OpenNode(target) returned error: %v", err)
	}
	repl.Register("target", targetBackend)
	repl.RegisterNode("target", recoveredTarget)

	newAssignment := ReplicaAssignment{
		Slot:         2,
		ChainVersion: 5,
		Role:         ReplicaRoleTail,
		Peers:        ChainPeers{PredecessorNodeID: "source"},
	}
	if err := recoveredTarget.RecoverReplica(ctx, RecoverReplicaCommand{
		Assignment:   newAssignment,
		SourceNodeID: "source",
	}); err != nil {
		t.Fatalf("RecoverReplica returned error: %v", err)
	}
	if got, want := recoveredTarget.State().Replicas[2].State, ReplicaStateActive; got != want {
		t.Fatalf("recovered target state = %q, want %q", got, want)
	}
	snapshot, err := recoveredTarget.CommittedSnapshot(2)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	wantSnapshot := map[string]string{"k": "v"}
	if got := snapshotValues(snapshot); !reflect.DeepEqual(got, wantSnapshot) {
		t.Fatalf("recovered snapshot = %#v, want %#v", got, wantSnapshot)
	}
	if got, want := recoveredTarget.State().Replicas[2].Assignment, newAssignment; !reflect.DeepEqual(got, want) {
		t.Fatalf("recovered assignment = %#v, want %#v", got, want)
	}

	staleBackend := NewInMemoryBackend()
	staleLocal := NewInMemoryLocalStateStore()
	stale, err := OpenNode(ctx, Config{NodeID: "stale"}, staleBackend, staleLocal, NewInMemoryCoordinatorClient(), repl)
	if err != nil {
		t.Fatalf("OpenNode(stale) returned error: %v", err)
	}
	if err := stale.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
		Assignment: ReplicaAssignment{Slot: 9, ChainVersion: 1, Role: ReplicaRoleSingle},
	}); err != nil {
		t.Fatalf("stale AddReplicaAsTail returned error: %v", err)
	}
	if err := stale.ActivateReplica(ctx, ActivateReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("stale ActivateReplica returned error: %v", err)
	}
	reopenedStale, err := OpenNode(ctx, Config{NodeID: "stale"}, staleBackend, staleLocal, NewInMemoryCoordinatorClient(), repl)
	if err != nil {
		t.Fatalf("reopen OpenNode(stale) returned error: %v", err)
	}
	if err := reopenedStale.DropRecoveredReplica(ctx, DropRecoveredReplicaCommand{Slot: 9}); err != nil {
		t.Fatalf("DropRecoveredReplica returned error: %v", err)
	}
	if _, err := staleLocal.LoadNode(ctx, "stale"); err != nil {
		t.Fatalf("LoadNode(stale) returned error: %v", err)
	}
	if _, exists := reopenedStale.State().Replicas[9]; exists {
		t.Fatal("replica still present after drop")
	}
	if _, err := staleBackend.HighestCommittedSequence(9); !errors.Is(err, ErrUnknownReplica) {
		t.Fatalf("HighestCommittedSequence error = %v, want unknown replica", err)
	}
}
