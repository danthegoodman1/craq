package grpcx_test

import (
	"context"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/storage"
)

type queuedCommitOutcome struct {
	result storage.CommitResult
	err    error
}

func runQueuedCommitResultWithDelivery(
	ctx context.Context,
	repl *storage.QueuedInMemoryReplicationTransport,
	run func() (storage.CommitResult, error),
) (storage.CommitResult, error) {
	resultCh := make(chan queuedCommitOutcome, 1)
	go func() {
		result, err := run()
		resultCh <- queuedCommitOutcome{result: result, err: err}
	}()
	for {
		select {
		case outcome := <-resultCh:
			return outcome.result, outcome.err
		default:
		}
		if repl.Pending() > 0 {
			if err := repl.DeliverNext(ctx); err != nil {
				return storage.CommitResult{}, err
			}
			select {
			case outcome := <-resultCh:
				return outcome.result, outcome.err
			case <-time.After(100 * time.Microsecond):
			}
			continue
		}
		select {
		case outcome := <-resultCh:
			return outcome.result, outcome.err
		case <-time.After(100 * time.Microsecond):
		}
	}
}

func submitPutWithQueuedDelivery(
	t *testing.T,
	ctx context.Context,
	node *storage.Node,
	repl *storage.QueuedInMemoryReplicationTransport,
	slot int,
	key string,
	value string,
) (storage.CommitResult, error) {
	t.Helper()
	return runQueuedCommitResultWithDelivery(ctx, repl, func() (storage.CommitResult, error) {
		return node.SubmitPut(ctx, slot, key, value)
	})
}
