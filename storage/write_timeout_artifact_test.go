package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteTimeoutArtifactCapturesSlotAndJournalState(t *testing.T) {
	ctx := context.Background()
	artifactPath := filepath.Join(t.TempDir(), "timeout.jsonl")
	tracePath := filepath.Join(t.TempDir(), "trace.jsonl")
	node := mustNewNode(t, ctx, Config{
		NodeID:                         "node-a",
		WriteTraceOutputPath:           tracePath,
		WriteTraceSampleRate:           1,
		WriteTimeoutArtifactOutputPath: artifactPath,
	}, NewInMemoryBackend(), NewInMemoryCoordinatorClient(), NewInMemoryReplicationTransport())
	mustActivateReplica(t, node, 17, ReplicaAssignment{
		Slot:         17,
		ChainVersion: 3,
		Role:         ReplicaRoleHead,
		Peers: ChainPeers{
			SuccessorNodeID: "mid",
			SuccessorTarget: "mid",
		},
	})
	preparePendingCommitState(t, node, 17, []uint64{1, 2}, "mid", 3)
	mustWithSlotRuntime(t, node, 17, func(runtime *slotRuntime) {
		runtime.commitEffectInFlight = true
		runtime.commitEffectSequence = 2
		runtime.acceptedCommitEntry(2, CommitWriteRequest{
			Slot:         17,
			Sequence:     2,
			FromNodeID:   "mid",
			ChainVersion: 3,
		}).stage = acceptedCommitDurableInFlight
		runtime.parkAcceptedCommitWaiter(2, make(chan error, 1), context.Background())
		runtime.markBreadcrumb(&runtime.lastAcceptCommitReceived, 2)
		runtime.markBreadcrumb(&runtime.lastDuplicateCommitParked, 2)
		runtime.markBreadcrumb(&runtime.lastReconciledFromJournal, 1)
		runtime.markBreadcrumb(&runtime.lastAppliedLocally, 1)
		runtime.markBreadcrumb(&runtime.lastWaiterReleased, 1)
		record := ensureProtocolReplicaState(runtime.record)
		record.highestCommitTokenReceived = 2
		record.bufferedCommits[3] = CommitWriteRequest{
			Slot:         17,
			Sequence:     3,
			FromNodeID:   "mid",
			ChainVersion: 3,
		}
		runtime.setRecord(record)
	})
	mustJournalDurablyCommitSequence(t, node, 17, 1)
	node.traceWriteEvent(ReplicaAssignment{Slot: 17, ChainVersion: 3, Role: ReplicaRoleHead}, 2, "head_commit_accept_received")
	node.traceWriteEvent(ReplicaAssignment{Slot: 17, ChainVersion: 3, Role: ReplicaRoleHead}, 2, "head_commit_intent_queued")
	node.captureWriteTimeout(17, 2, ReplicaRoleHead, context.DeadlineExceeded)

	data, err := os.ReadFile(artifactPath)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	var artifacts []writeTimeoutArtifact
	for _, line := range bytesLines(data) {
		var artifact writeTimeoutArtifact
		if err := json.Unmarshal(line, &artifact); err != nil {
			t.Fatalf("Unmarshal returned error: %v", err)
		}
		artifacts = append(artifacts, artifact)
	}
	if len(artifacts) != 1 {
		t.Fatalf("len(artifacts) = %d, want 1", len(artifacts))
	}
	artifact := artifacts[0]
	if artifact.SlotState.NextSequence != 3 {
		t.Fatalf("artifact next sequence = %d, want 3", artifact.SlotState.NextSequence)
	}
	if artifact.SlotState.CommitEffectSequence != 2 {
		t.Fatalf("artifact commit effect sequence = %d, want 2", artifact.SlotState.CommitEffectSequence)
	}
	if len(artifact.SlotState.ParkedCommitAcceptWaiters) != 1 || artifact.SlotState.ParkedCommitAcceptWaiters[0].Sequence != 2 {
		t.Fatalf("artifact parked waiters = %#v, want sequence 2", artifact.SlotState.ParkedCommitAcceptWaiters)
	}
	if len(artifact.SlotState.AcceptedCommitLedger) == 0 || artifact.SlotState.AcceptedCommitLedger[0].Sequence != 2 {
		t.Fatalf("artifact accepted commit ledger = %#v, want sequence 2", artifact.SlotState.AcceptedCommitLedger)
	}
	if artifact.JournalState == nil || artifact.JournalState.DurableCommittedHighWater == 0 {
		t.Fatalf("artifact journal state = %#v, want durable high water", artifact.JournalState)
	}
	if len(artifact.RecentSlotTrace) == 0 {
		t.Fatal("artifact recent slot trace was empty")
	}
}

func bytesLines(data []byte) [][]byte {
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	out := make([][]byte, 0, len(lines))
	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		out = append(out, line)
	}
	return out
}
