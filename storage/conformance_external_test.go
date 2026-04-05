package storage_test

import (
	"testing"

	"github.com/danthegoodman1/craq/storage"
	"github.com/danthegoodman1/craq/storage/storagetest"
)

func TestInMemoryBackendConformance(t *testing.T) {
	storagetest.RunBackendSuite(t, func(t *testing.T) storage.Backend {
		t.Helper()
		return storage.NewInMemoryBackend()
	})
}

func TestInMemoryLocalStateStoreConformance(t *testing.T) {
	storagetest.RunLocalStateStoreSuite(t, func(t *testing.T) storage.LocalStateStore {
		t.Helper()
		return storage.NewInMemoryLocalStateStore()
	})
}
