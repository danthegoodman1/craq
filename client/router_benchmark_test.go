package client

import (
	"context"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
)

func BenchmarkRouterGet_PreloadedSnapshot(b *testing.B) {
	router, key := benchmarkRouter(b, 128)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := router.Get(context.Background(), key); err != nil {
			b.Fatalf("Get returned error: %v", err)
		}
	}
}

func BenchmarkRouterPut_PreloadedSnapshot(b *testing.B) {
	router, key := benchmarkRouter(b, 128)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := router.Put(context.Background(), key, "value"); err != nil {
			b.Fatalf("Put returned error: %v", err)
		}
	}
}

func BenchmarkRouterDelete_PreloadedSnapshot(b *testing.B) {
	router, key := benchmarkRouter(b, 128)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := router.Delete(context.Background(), key); err != nil {
			b.Fatalf("Delete returned error: %v", err)
		}
	}
}

func benchmarkRouter(b *testing.B, slotCount int) (*Router, string) {
	b.Helper()

	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: slotCount,
		Slots:     make([]coordserver.SlotRoute, slotCount),
	}
	for slot := 0; slot < slotCount; slot++ {
		snapshot.Slots[slot] = coordserver.SlotRoute{
			Slot:         slot,
			ChainVersion: 1,
			HeadNodeID:   "head",
			TailNodeID:   "tail",
			Writable:     true,
			Readable:     true,
		}
	}

	router := mustNewRouterForBenchmark(b, &scriptedSnapshotSource{
		snapshots: []coordserver.RoutingSnapshot{snapshot},
	}, &benchmarkTransport{})
	if err := router.Refresh(context.Background()); err != nil {
		b.Fatalf("Refresh returned error: %v", err)
	}

	return router, benchmarkKeyForSlot(b, 17, slotCount, "bench")
}

func mustNewRouterForBenchmark(b *testing.B, source SnapshotSource, transport Transport) *Router {
	b.Helper()
	router, err := NewRouter(source, transport)
	if err != nil {
		b.Fatalf("NewRouter returned error: %v", err)
	}
	return router
}

func benchmarkKeyForSlot(b *testing.B, slot int, slotCount int, prefix string) string {
	b.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if int(crc32.ChecksumIEEE([]byte(key))%uint32(slotCount)) == slot {
			return key
		}
	}
	b.Fatalf("unable to find key for slot %d", slot)
	return ""
}

type benchmarkTransport struct{}

func (t *benchmarkTransport) Get(_ context.Context, _ string, req storage.ClientGetRequest) (storage.ReadResult, error) {
	return storage.ReadResult{
		Slot:         req.Slot,
		ChainVersion: req.ExpectedChainVersion,
		Found:        true,
		Value:        "value",
	}, nil
}

func (t *benchmarkTransport) Put(_ context.Context, _ string, req storage.ClientPutRequest) (storage.CommitResult, error) {
	return storage.CommitResult{
		Slot:     req.Slot,
		Sequence: 1,
	}, nil
}

func (t *benchmarkTransport) Delete(_ context.Context, _ string, req storage.ClientDeleteRequest) (storage.CommitResult, error) {
	return storage.CommitResult{
		Slot:     req.Slot,
		Sequence: 1,
	}, nil
}
