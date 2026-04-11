package storage

import (
	"errors"
	"reflect"
	"testing"
)

func TestReduceSubmitWriteTracksPendingAndDirtyState(t *testing.T) {
	// A submitted client write should only mutate slot-owned bookkeeping here:
	// pending completion state, dirty-read visibility, and the next sequence.
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         7,
			ChainVersion: 1,
			Role:         ReplicaRoleHead,
		},
		state:        ReplicaStateActive,
		nextSequence: 3,
	})
	operation := WriteOperation{
		Slot:     7,
		Sequence: 3,
		Kind:     OperationKindPut,
		Key:      "alpha",
		Value:    "one",
		Metadata: testObjectMetadata(3),
	}

	reduction := reduceSubmitWrite(record, operation)

	if got, want := reduction.Record.nextSequence, uint64(4); got != want {
		t.Fatalf("nextSequence = %d, want %d", got, want)
	}
	if pending, ok := reduction.Record.pendingWrites[3]; !ok {
		t.Fatal("pending write missing")
	} else {
		if pending.operation == nil || *pending.operation != operation {
			t.Fatalf("pending operation = %#v, want %#v", pending.operation, operation)
		}
		if !reflect.DeepEqual(pending.result, reduction.Result) {
			t.Fatalf("pending result = %#v, want %#v", pending.result, reduction.Result)
		}
	}
	entries := reduction.Record.dirtyByKey["alpha"]
	if got, want := len(entries), 1; got != want {
		t.Fatalf("dirty entries len = %d, want %d", got, want)
	}
	if got := entries[0].Operation; got != operation {
		t.Fatalf("dirty operation = %#v, want %#v", got, operation)
	}
}

func TestReduceForwardWriteBuffersFutureMessage(t *testing.T) {
	// Out-of-order forwards must buffer without advancing nextSequence.
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         5,
			ChainVersion: 1,
			Role:         ReplicaRoleMiddle,
			Peers: ChainPeers{
				PredecessorNodeID: "head",
				SuccessorNodeID:   "tail",
			},
		},
		state:        ReplicaStateActive,
		nextSequence: 1,
	})
	req := ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     5,
			Sequence: 2,
			Kind:     OperationKindPut,
			Key:      "k",
			Value:    "v",
			Metadata: testObjectMetadata(2),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	}

	reduction, err := reduceForwardWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit: 4,
		perNodeLimit: 8,
	})
	if err != nil {
		t.Fatalf("reduceForwardWrite returned error: %v", err)
	}
	if got, want := reduction.Action, slotReducerActionBuffer; got != want {
		t.Fatalf("action = %q, want %q", got, want)
	}
	if got, ok := reduction.Record.bufferedForwards[2]; !ok {
		t.Fatal("buffered forward missing")
	} else if !sameForwardRequest(got, req) {
		t.Fatalf("buffered forward = %#v, want %#v", got, req)
	}
	if got, want := reduction.Record.nextSequence, uint64(1); got != want {
		t.Fatalf("nextSequence = %d, want %d", got, want)
	}
}

func TestReduceForwardWriteAppliesInOrderMessage(t *testing.T) {
	// In-order forwards should stage the operation immediately so the effect
	// runner can forward or commit it after the reducer turn.
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         9,
			ChainVersion: 1,
			Role:         ReplicaRoleMiddle,
			Peers: ChainPeers{
				PredecessorNodeID: "head",
				SuccessorNodeID:   "tail",
			},
		},
		state:        ReplicaStateActive,
		nextSequence: 4,
	})
	req := ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     9,
			Sequence: 4,
			Kind:     OperationKindPut,
			Key:      "k4",
			Value:    "v4",
			Metadata: testObjectMetadata(4),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	}

	reduction, err := reduceForwardWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit: 4,
		perNodeLimit: 8,
	})
	if err != nil {
		t.Fatalf("reduceForwardWrite returned error: %v", err)
	}
	if got, want := reduction.Action, slotReducerActionApply; got != want {
		t.Fatalf("action = %q, want %q", got, want)
	}
	if got, want := reduction.Record.nextSequence, uint64(5); got != want {
		t.Fatalf("nextSequence = %d, want %d", got, want)
	}
	if got, ok := reduction.Record.stagedForwards[4]; !ok {
		t.Fatal("staged forward missing")
	} else if !sameForwardRequest(got, req) {
		t.Fatalf("staged forward = %#v, want %#v", got, req)
	}
	if entries := reduction.Record.dirtyByKey["k4"]; len(entries) != 1 || entries[0].Sequence != 4 {
		t.Fatalf("dirty entries = %#v, want sequence 4", entries)
	}
}

func TestReduceCommitWriteBuffersUntilSequenceIsCommittable(t *testing.T) {
	// A commit cannot apply until all earlier sequences are committed locally,
	// even if the staged data for the target sequence already exists.
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         6,
			ChainVersion: 1,
			Role:         ReplicaRoleMiddle,
			Peers: ChainPeers{
				SuccessorNodeID: "tail",
			},
		},
		state:                    ReplicaStateActive,
		nextSequence:             3,
		highestCommittedSequence: 0,
		stagedForwards: map[uint64]ForwardWriteRequest{
			2: {
				Operation: WriteOperation{
					Slot:     6,
					Sequence: 2,
					Kind:     OperationKindPut,
					Key:      "late",
					Value:    "v2",
					Metadata: testObjectMetadata(2),
				},
				FromNodeID:   "head",
				ChainVersion: 1,
			},
		},
	})
	req := CommitWriteRequest{
		Slot:         6,
		Sequence:     2,
		FromNodeID:   "tail",
		ChainVersion: 1,
	}

	reduction, err := reduceCommitWrite(record, req, slotProtocolBufferLimits{
		perSlotLimit: 4,
		perNodeLimit: 8,
	})
	if err != nil {
		t.Fatalf("reduceCommitWrite returned error: %v", err)
	}
	if got, want := reduction.Action, slotReducerActionBuffer; got != want {
		t.Fatalf("action = %q, want %q", got, want)
	}
	if got, ok := reduction.Record.bufferedCommits[2]; !ok {
		t.Fatal("buffered commit missing")
	} else if got != req {
		t.Fatalf("buffered commit = %#v, want %#v", got, req)
	}
}

func TestReduceForwardWriteDetectsConflictingDuplicate(t *testing.T) {
	// Retried delivery of the same sequence is only idempotent when the payload
	// matches the retained staged or committed history exactly.
	req := ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     4,
			Sequence: 1,
			Kind:     OperationKindPut,
			Key:      "k",
			Value:    "v1",
			Metadata: testObjectMetadata(1),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	}
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         4,
			ChainVersion: 1,
			Role:         ReplicaRoleMiddle,
			Peers: ChainPeers{
				PredecessorNodeID: "head",
			},
		},
		state:        ReplicaStateActive,
		nextSequence: 2,
		stagedForwards: map[uint64]ForwardWriteRequest{
			1: req,
		},
	})
	conflicting := req
	conflicting.Operation.Value = "different"

	_, err := reduceForwardWrite(record, conflicting, slotProtocolBufferLimits{
		perSlotLimit: 4,
		perNodeLimit: 8,
	})
	if !errors.Is(err, ErrProtocolConflict) {
		t.Fatalf("error = %v, want ErrProtocolConflict", err)
	}
}

func TestReduceApplyCommittedSequenceClearsTransientState(t *testing.T) {
	// Once a sequence becomes committed, the reducer should leave only durable
	// committed-history bookkeeping behind and clear transient staged/dirty state.
	forward := ForwardWriteRequest{
		Operation: WriteOperation{
			Slot:     3,
			Sequence: 2,
			Kind:     OperationKindPut,
			Key:      "alpha",
			Value:    "v2",
			Metadata: testObjectMetadata(2),
		},
		FromNodeID:   "head",
		ChainVersion: 1,
	}
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         3,
			ChainVersion: 1,
		},
		state:                    ReplicaStateActive,
		nextSequence:             3,
		highestCommittedSequence: 1,
		stagedForwards: map[uint64]ForwardWriteRequest{
			2: forward,
		},
		pendingWrites: map[uint64]pendingWrite{
			2: {operation: &forward.Operation},
		},
		dirtyByKey: map[string][]dirtyReadEntry{
			"alpha": {{
				Sequence:  2,
				Operation: forward.Operation,
			}},
		},
	})

	reduced := reduceApplyCommittedSequence(record, forward.Operation, 2, 8)

	if got, want := reduced.highestCommittedSequence, uint64(2); got != want {
		t.Fatalf("highestCommittedSequence = %d, want %d", got, want)
	}
	if _, ok := reduced.stagedForwards[2]; ok {
		t.Fatal("staged forward for committed sequence still present")
	}
	if _, ok := reduced.pendingWrites[2]; ok {
		t.Fatal("pending write for committed sequence still present")
	}
	if entries := reduced.dirtyByKey["alpha"]; len(entries) != 0 {
		t.Fatalf("dirty entries = %#v, want empty", entries)
	}
	if got, ok := reduced.recentCommittedForwards[2]; !ok {
		t.Fatal("recent committed forward missing")
	} else if !sameForwardRequest(got, forward) {
		t.Fatalf("recent committed forward = %#v, want %#v", got, forward)
	}
}

func TestReduceStageForwardPromotesBufferedSequence(t *testing.T) {
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         8,
			ChainVersion: 2,
			Role:         ReplicaRoleMiddle,
		},
		state:        ReplicaStateActive,
		nextSequence: 5,
		bufferedForwards: map[uint64]ForwardWriteRequest{
			5: {
				Operation: WriteOperation{
					Slot:     8,
					Sequence: 5,
					Kind:     OperationKindPut,
					Key:      "beta",
					Value:    "v5",
					Metadata: testObjectMetadata(5),
				},
				FromNodeID:   "head",
				ChainVersion: 2,
			},
		},
	})

	req := record.bufferedForwards[5]
	reduced := reduceStageForward(record, req)

	if got, want := reduced.nextSequence, uint64(6); got != want {
		t.Fatalf("nextSequence = %d, want %d", got, want)
	}
	if _, ok := reduced.bufferedForwards[5]; ok {
		t.Fatal("buffered forward still present after promotion")
	}
	if got, ok := reduced.stagedForwards[5]; !ok {
		t.Fatal("staged forward missing after promotion")
	} else if !sameForwardRequest(got, req) {
		t.Fatalf("staged forward = %#v, want %#v", got, req)
	}
}

func TestReduceRecordCommitAppliedMarksPendingAndClearsBufferedCommit(t *testing.T) {
	req := CommitWriteRequest{
		Slot:         2,
		Sequence:     7,
		FromNodeID:   "tail",
		ChainVersion: 3,
	}
	record := ensureProtocolReplicaState(replicaRecord{
		assignment: ReplicaAssignment{
			Slot:         2,
			ChainVersion: 3,
			Role:         ReplicaRoleHead,
		},
		state: ReplicaStateActive,
		pendingWrites: map[uint64]pendingWrite{
			7: {
				result: CommitResult{
					Slot:     2,
					Sequence: 7,
					Applied:  true,
				},
			},
		},
		bufferedCommits: map[uint64]CommitWriteRequest{
			7: req,
		},
	})

	reduced := reduceRecordCommitApplied(record, req, 8)

	if _, ok := reduced.bufferedCommits[7]; ok {
		t.Fatal("buffered commit still present after apply")
	}
	if pending := reduced.pendingWrites[7]; !pending.completed {
		t.Fatal("pending write not marked completed")
	}
	if got, ok := reduced.recentCommittedCommits[7]; !ok {
		t.Fatal("recent committed commit missing")
	} else if got != req {
		t.Fatalf("recent committed commit = %#v, want %#v", got, req)
	}
}
