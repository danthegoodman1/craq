package coordserver_test

import (
	"context"
	"os"
	"testing"

	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/coordserver/hastoretest"
)

func TestPostgresHAStoreConformance(t *testing.T) {
	dsn := os.Getenv("CRAQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CRAQ_TEST_POSTGRES_DSN is not set")
	}
	hastoretest.Run(t, func(t *testing.T) coordserver.HAStore {
		t.Helper()
		store, err := coordserver.OpenPostgresHAStore(context.Background(), dsn)
		if err != nil {
			t.Fatalf("OpenPostgresHAStore returned error: %v", err)
		}
		if err := store.Reset(context.Background()); err != nil {
			_ = store.Close()
			t.Fatalf("Reset returned error: %v", err)
		}
		return store
	})
}
