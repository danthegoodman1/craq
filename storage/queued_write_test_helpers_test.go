package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

type queuedCommitResult struct {
	result CommitResult
	err    error
}

func runQueuedCommitResultWithDelivery(
	ctx context.Context,
	repl *QueuedInMemoryReplicationTransport,
	run func() (CommitResult, error),
) (CommitResult, error) {
	resultCh := make(chan queuedCommitResult, 1)
	go func() {
		result, err := run()
		resultCh <- queuedCommitResult{result: result, err: err}
	}()
	for {
		select {
		case outcome := <-resultCh:
			return outcome.result, outcome.err
		default:
		}
		if repl.Pending() > 0 {
			if err := repl.DeliverNext(ctx); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					time.Sleep(100 * time.Microsecond)
					continue
				}
				return CommitResult{}, err
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

func runQueuedCommitResult(
	t *testing.T,
	ctx context.Context,
	repl *QueuedInMemoryReplicationTransport,
	run func() (CommitResult, error),
) (CommitResult, error) {
	t.Helper()
	return runQueuedCommitResultWithDelivery(ctx, repl, run)
}

func submitPutWithQueuedDelivery(
	t *testing.T,
	ctx context.Context,
	node *Node,
	repl *QueuedInMemoryReplicationTransport,
	slot int,
	key string,
	value string,
) (CommitResult, error) {
	t.Helper()
	return runQueuedCommitResult(t, ctx, repl, func() (CommitResult, error) {
		return node.SubmitPut(ctx, slot, key, value)
	})
}

func handleClientPutWithQueuedDelivery(
	t *testing.T,
	ctx context.Context,
	node *Node,
	repl *QueuedInMemoryReplicationTransport,
	req ClientPutRequest,
) (CommitResult, error) {
	t.Helper()
	return runQueuedCommitResult(t, ctx, repl, func() (CommitResult, error) {
		return node.HandleClientPut(ctx, req)
	})
}
