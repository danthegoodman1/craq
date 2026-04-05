package coordserver

import (
	"context"
	"testing"

	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
)

func BenchmarkServerRoutingSnapshot_StableState(b *testing.B) {
	server, err := Open(
		context.Background(),
		coordruntime.NewInMemoryStore(),
		mapToClient(map[string]*recordingNodeClient{
			"a": newRecordingNodeClient("a"),
			"b": newRecordingNodeClient("b"),
			"c": newRecordingNodeClient("c"),
		}),
	)
	if err != nil {
		b.Fatalf("Open returned error: %v", err)
	}
	if _, err := server.Bootstrap(context.Background(), bootstrapCommand("bootstrap-1", 0, 4096, 3, "a", "b", "c")); err != nil {
		b.Fatalf("Bootstrap returned error: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := server.RoutingSnapshot(context.Background()); err != nil {
			b.Fatalf("RoutingSnapshot returned error: %v", err)
		}
	}
}
