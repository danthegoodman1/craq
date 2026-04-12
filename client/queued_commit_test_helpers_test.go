package client

import (
	"context"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/storage"
)

type commitDeliveryPump interface {
	Pending() int
	DeliverNext(context.Context) error
}

type routedCommitResult struct {
	result storage.CommitResult
	err    error
}

func drainCommitPumpUntilIdle(ctx context.Context, pump commitDeliveryPump) error {
	for {
		if pump.Pending() > 0 {
			if err := pump.DeliverNext(ctx); err != nil {
				return err
			}
			continue
		}
		select {
		case <-time.After(100 * time.Microsecond):
			if pump.Pending() == 0 {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func runRouterCommitResultWithDelivery(
	ctx context.Context,
	pump commitDeliveryPump,
	run func() (storage.CommitResult, error),
) (storage.CommitResult, error) {
	resultCh := make(chan routedCommitResult, 1)
	go func() {
		result, err := run()
		resultCh <- routedCommitResult{result: result, err: err}
	}()
	for {
		select {
		case outcome := <-resultCh:
			if outcome.err == nil {
				if err := drainCommitPumpUntilIdle(ctx, pump); err != nil {
					return storage.CommitResult{}, err
				}
			}
			return outcome.result, outcome.err
		default:
		}
		if pump.Pending() > 0 {
			if err := pump.DeliverNext(ctx); err != nil {
				return storage.CommitResult{}, err
			}
			select {
			case outcome := <-resultCh:
				if outcome.err == nil {
					if err := drainCommitPumpUntilIdle(ctx, pump); err != nil {
						return storage.CommitResult{}, err
					}
				}
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

func routerPutWithDelivery(
	t *testing.T,
	ctx context.Context,
	router *Router,
	pump commitDeliveryPump,
	key string,
	value string,
) (storage.CommitResult, error) {
	t.Helper()
	return runRouterCommitResultWithDelivery(ctx, pump, func() (storage.CommitResult, error) {
		return router.Put(ctx, key, value)
	})
}

func routerDeleteWithDelivery(
	t *testing.T,
	ctx context.Context,
	router *Router,
	pump commitDeliveryPump,
	key string,
) (storage.CommitResult, error) {
	t.Helper()
	return runRouterCommitResultWithDelivery(ctx, pump, func() (storage.CommitResult, error) {
		return router.Delete(ctx, key)
	})
}
