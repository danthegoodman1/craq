package benchmark

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareLocalStorageRootRepoDisk(t *testing.T) {
	runDir := t.TempDir()
	root, cleanup, err := prepareLocalStorageRoot(context.Background(), runDir, "run-1", LocalProfile{
		StorageLayout: "repo_disk",
	})
	if err != nil {
		t.Fatalf("prepareLocalStorageRoot returned error: %v", err)
	}
	defer cleanup()

	if got, want := root, filepath.Join(runDir, "local"); got != want {
		t.Fatalf("root = %q, want %q", got, want)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
}

func TestPrepareLocalStorageRootConfiguredRamdiskPath(t *testing.T) {
	base := t.TempDir()
	root, cleanup, err := prepareLocalStorageRoot(context.Background(), t.TempDir(), "run-2", LocalProfile{
		StorageLayout:  "ramdisk",
		RamdiskPath:    base,
		RamdiskSizeMiB: 1024,
	})
	if err != nil {
		t.Fatalf("prepareLocalStorageRoot returned error: %v", err)
	}
	if got, want := root, filepath.Join(base, "run-2"); got != want {
		t.Fatalf("root = %q, want %q", got, want)
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatalf("Stat returned error: %v", err)
	}
	cleanup()
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("cleanup left root behind, stat err = %v", err)
	}
}
