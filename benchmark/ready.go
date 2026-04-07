package benchmark

import (
	"context"
	"fmt"
	"time"

	"github.com/danthegoodman1/craq/client"
	"github.com/danthegoodman1/craq/quickstart"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

func WaitForClusterReady(ctx context.Context, manifestPath string, timeout time.Duration) error {
	manifest, err := quickstart.Load(manifestPath)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()
	router, err := client.NewRouter(
		grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool),
		grpcx.NewClientTransport(pool),
	)
	if err != nil {
		return fmt.Errorf("create benchmark client router: %w", err)
	}
	key := "craq-bench-ready"
	value := "ready"
	var lastErr error
	for time.Now().Before(deadline) {
		if err := router.Refresh(ctx); err != nil {
			lastErr = err
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
		reqCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_, err := router.Put(reqCtx, key, value)
		cancel()
		if err == nil {
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			result, readErr := router.Get(readCtx, key)
			readCancel()
			if readErr == nil && result.Found && result.Value == value {
				return nil
			}
			lastErr = readErr
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("timed out waiting for ready probe")
	}
	return lastErr
}
