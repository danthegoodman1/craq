package coordserver_test

import (
	"testing"

	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/coordserver/hastoretest"
)

func TestInMemoryHAStoreConformance(t *testing.T) {
	hastoretest.Run(t, func(t *testing.T) coordserver.HAStore {
		t.Helper()
		return coordserver.NewInMemoryHAStore()
	})
}
