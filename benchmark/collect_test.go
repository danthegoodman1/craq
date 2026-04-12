package benchmark

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCopyTreeSkipsTerraformCacheAndPreservesMode(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()

	scriptPath := filepath.Join(src, "script.sh")
	if err := os.WriteFile(scriptPath, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("WriteFile(script) returned error: %v", err)
	}

	terraformPluginPath := filepath.Join(src, ".terraform", "providers", "plugin")
	if err := os.MkdirAll(filepath.Dir(terraformPluginPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(terraform plugin dir) returned error: %v", err)
	}
	if err := os.WriteFile(terraformPluginPath, []byte("plugin"), 0o755); err != nil {
		t.Fatalf("WriteFile(terraform plugin) returned error: %v", err)
	}

	if err := copyTree(src, dst); err != nil {
		t.Fatalf("copyTree returned error: %v", err)
	}

	info, err := os.Stat(filepath.Join(dst, "script.sh"))
	if err != nil {
		t.Fatalf("Stat(script) returned error: %v", err)
	}
	if got, want := info.Mode().Perm(), os.FileMode(0o755); got != want {
		t.Fatalf("script mode = %v, want %v", got, want)
	}

	if _, err := os.Stat(filepath.Join(dst, ".terraform")); !os.IsNotExist(err) {
		t.Fatalf(".terraform copied unexpectedly, stat err = %v", err)
	}
}
