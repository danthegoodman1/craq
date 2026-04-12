package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func FuzzQueuedMiddleReplicaProtocol(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 3, 3},
		{1, 0, 3, 3, 3},
		{2, 1, 0, 3, 3, 3},
		{0, 4, 3, 5, 3, 3},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, program []byte) {
		if len(program) > 32 {
			program = program[:32]
		}
		nodes, _, repl := setupActiveChainWithQueuedTransport(t, 21, []string{"head", "mid", "tail"})
		ctx := context.Background()

		for _, raw := range program {
			switch raw % 6 {
			case 0, 1, 2:
				seq := uint64(raw%3 + 1)
				err := nodes["mid"].HandleForwardWrite(ctx, ForwardWriteRequest{
					Operation: WriteOperation{
						Slot:     21,
						Sequence: seq,
						Kind:     OperationKindPut,
						Key:      fmt.Sprintf("k%d", seq),
						Value:    fmt.Sprintf("v%d", seq),
						Metadata: testObjectMetadata(seq),
					},
					FromNodeID:   "head",
					ChainVersion: 1,
				})
				if err != nil && !isExpectedProtocolFuzzError(err) {
					t.Fatalf("HandleForwardWrite(seq=%d) returned unexpected error: %v", seq, err)
				}
			case 3:
				if repl.Pending() > 0 {
					err := repl.DeliverNext(ctx)
					if err != nil && !isExpectedProtocolFuzzError(err) {
						t.Fatalf("DeliverNext returned unexpected error: %v", err)
					}
				}
			case 4:
				if repl.Pending() > 0 {
					if err := repl.DuplicateAt(repl.Pending() - 1); err != nil {
						t.Fatalf("DuplicateAt returned error: %v", err)
					}
				}
			case 5:
				if repl.Pending() > 1 {
					if err := repl.MoveToFront(repl.Pending() - 1); err != nil {
						t.Fatalf("MoveToFront returned error: %v", err)
					}
				}
			}
			assertQueuedProtocolInvariants(t, nodes["mid"], nodes["tail"], 21)
		}

		for repl.Pending() > 0 {
			err := repl.DeliverNext(ctx)
			if err != nil && !isExpectedProtocolFuzzError(err) {
				t.Fatalf("final DeliverNext returned unexpected error: %v", err)
			}
			assertQueuedProtocolInvariants(t, nodes["mid"], nodes["tail"], 21)
		}
	})
}

func isExpectedProtocolFuzzError(err error) bool {
	return errors.Is(err, ErrSequenceMismatch) ||
		errors.Is(err, ErrProtocolConflict) ||
		errors.Is(err, ErrBufferedMessageLimit) ||
		errors.Is(err, ErrReplicaBackpressure) ||
		errors.Is(err, ErrPeerMismatch)
}

func assertQueuedProtocolInvariants(t *testing.T, mid *Node, tail *Node, slot int) {
	t.Helper()

	midHighest := mustHighestCommitted(t, mid, slot)
	tailHighest := mustHighestCommitted(t, tail, slot)
	if midHighest > tailHighest {
		t.Fatalf("middle highest committed = %d, tail = %d", midHighest, tailHighest)
	}

	for _, seq := range mustNodeStagedSequences(t, mid, slot) {
		if seq <= midHighest {
			t.Fatalf("middle staged sequence %d is <= highest committed %d", seq, midHighest)
		}
	}
	for _, seq := range mustNodeStagedSequences(t, tail, slot) {
		if seq <= tailHighest {
			t.Fatalf("tail staged sequence %d is <= highest committed %d", seq, tailHighest)
		}
	}
	for _, seq := range mustBufferedForwardSequences(t, mid, slot) {
		if seq <= midHighest {
			t.Fatalf("middle buffered forward %d is <= highest committed %d", seq, midHighest)
		}
	}
	for _, seq := range mustBufferedCommitSequences(t, mid, slot) {
		if seq <= midHighest {
			t.Fatalf("middle buffered commit %d is <= highest committed %d", seq, midHighest)
		}
	}

	midSnapshot := mustNodeCommittedSnapshot(t, mid, slot)
	tailSnapshot := mustNodeCommittedSnapshot(t, tail, slot)
	for key, value := range midSnapshot {
		if tailSnapshot[key] != value {
			t.Fatalf("tail snapshot[%q] = %q, want %q", key, tailSnapshot[key], value)
		}
	}
}
