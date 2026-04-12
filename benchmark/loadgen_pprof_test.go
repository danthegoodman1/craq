package benchmark

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/adminhttp"
	"github.com/danthegoodman1/craq/storage"
)

func TestCaptureSelfScenarioProfilesCollectsProfiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	outputDir := filepath.Join(t.TempDir(), "client")
	if err := captureSelfScenarioProfiles(ctx, outputDir, 0, time.Second); err != nil {
		t.Fatalf("captureSelfScenarioProfiles returned error: %v", err)
	}
	for _, path := range []string{
		filepath.Join(outputDir, "cpu.pprof"),
		filepath.Join(outputDir, "heap.pprof"),
		filepath.Join(outputDir, "goroutine.pprof"),
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("expected profile %s: %v", path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile %s is empty", path)
		}
	}
}

func TestCaptureRemoteScenarioProfilesCollectsProfiles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node, err := storage.NewNode(
		ctx,
		storage.Config{NodeID: "node-a"},
		storage.NewInMemoryBackend(),
		storage.NewInMemoryCoordinatorClient(),
		storage.NewInMemoryReplicationTransport(),
	)
	if err != nil {
		t.Fatalf("storage.NewNode returned error: %v", err)
	}
	t.Cleanup(func() { _ = node.Close() })

	address := reserveAddress(t)
	server := adminhttp.NewStorage(node, adminhttp.Config{Address: address, Gatherer: node.MetricsRegistry()})
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, net.ErrClosed) && !strings.Contains(err.Error(), "Server closed") && !strings.Contains(err.Error(), "closed network connection") {
			panic(err)
		}
	}()
	t.Cleanup(func() { _ = server.Close(context.Background()) })

	outputDir := filepath.Join(t.TempDir(), "remote")
	if err := captureRemoteScenarioProfiles(ctx, outputDir, address, 0, time.Second); err != nil {
		t.Fatalf("captureRemoteScenarioProfiles returned error: %v", err)
	}
	for _, path := range []string{
		filepath.Join(outputDir, "cpu.pprof"),
		filepath.Join(outputDir, "heap.pprof"),
		filepath.Join(outputDir, "goroutine.pprof"),
	} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("expected profile %s: %v", path, err)
		}
		if info.Size() == 0 {
			t.Fatalf("profile %s is empty", path)
		}
	}
}
