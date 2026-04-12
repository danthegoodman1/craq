package client

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
)

func TestRouterHashesKeysDeterministicallyAndRoutesToExpectedEndpoints(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 2, 4, "route")
	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{Slot: 1, ChainVersion: 1, HeadNodeID: "h1", TailNodeID: "t1", Writable: true, Readable: true},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{snapshot}}
	transport := &recordingTransport{}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if _, err := router.Get(ctx, key); err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if _, err := router.Delete(ctx, key); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}

	if got, want := transport.putNodes, []string{"h2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("put nodes = %v, want %v", got, want)
	}
	if got, want := transport.getNodes, []string{"t2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("get nodes = %v, want %v", got, want)
	}
	if got, want := transport.deleteNodes, []string{"h2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete nodes = %v, want %v", got, want)
	}
}

func TestRouterRefreshesOnceOnRoutingMismatchAndNotOnGenericFailure(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 1, 4, "refresh")
	initial := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{Slot: 1, ChainVersion: 1, HeadNodeID: "old-head", TailNodeID: "old-tail", Writable: true, Readable: true},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	refreshed := coordserver.RoutingSnapshot{
		Version:   2,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{Slot: 1, ChainVersion: 2, HeadNodeID: "new-head", TailNodeID: "new-tail", Writable: true, Readable: true},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{initial, refreshed}}
	transport := &recordingTransport{
		putErrs: []error{
			&storage.RoutingMismatchError{
				Slot:                 1,
				ExpectedChainVersion: 1,
				CurrentChainVersion:  2,
				CurrentRole:          storage.ReplicaRoleHead,
				CurrentState:         storage.ReplicaStateActive,
				Reason:               storage.RoutingMismatchReasonWrongVersion,
			},
			nil,
		},
	}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if got, want := source.calls, 2; got != want {
		t.Fatalf("snapshot source calls = %d, want %d", got, want)
	}
	if got, want := transport.putNodes, []string{"old-head", "new-head"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("put nodes = %v, want %v", got, want)
	}

	source = &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{initial}}
	transport = &recordingTransport{
		putErrs: []error{errors.New("boom")},
	}
	router = mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	if _, err := router.Put(ctx, key, "v"); err == nil {
		t.Fatal("Put unexpectedly succeeded")
	}
	if got, want := source.calls, 1; got != want {
		t.Fatalf("snapshot source calls after generic failure = %d, want %d", got, want)
	}
}

func TestRouterRoundRobinsReadsAcrossReadReplicas(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 1, 4, "round-robin")
	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{
				Slot:         1,
				ChainVersion: 3,
				HeadNodeID:   "head-1",
				TailNodeID:   "tail-1",
				Readable:     true,
				ReadReplicas: []coordserver.ReadReplicaRoute{
					{NodeID: "head-1", Role: storage.ReplicaRoleHead},
					{NodeID: "mid-1", Role: storage.ReplicaRoleMiddle},
					{NodeID: "tail-1", Role: storage.ReplicaRoleTail},
				},
			},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{snapshot}}
	transport := &recordingTransport{}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	for i := 0; i < 3; i++ {
		if _, err := router.Get(ctx, key); err != nil {
			t.Fatalf("Get #%d returned error: %v", i+1, err)
		}
	}
	if got, want := transport.getNodes, []string{"head-1", "mid-1", "tail-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("get nodes = %v, want %v", got, want)
	}
}

func TestRouterConcurrentGetsShareSingleRouterSafely(t *testing.T) {
	ctx := context.Background()
	repl := storage.NewInMemoryReplicationTransport()
	backend := storage.NewInMemoryBackend()
	node := mustNewStorageNodeForRouter(t, ctx, "node-a", backend, repl)
	mustActivateStorageAssignment(t, node, storage.ReplicaAssignment{
		Slot:         0,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleSingle,
		Peers: storage.ChainPeers{
			TailNodeID: "node-a",
			TailTarget: "node-a",
		},
	})

	transport := NewInMemoryTransport()
	transport.RegisterNode("node-a", node)
	router := mustNewRouter(t, &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{{
		Version:   1,
		SlotCount: 1,
		Slots: []coordserver.SlotRoute{{
			Slot:         0,
			ChainVersion: 1,
			HeadNodeID:   "node-a",
			TailNodeID:   "node-a",
			Writable:     true,
			Readable:     true,
			ReadReplicas: []coordserver.ReadReplicaRoute{{
				NodeID: "node-a",
				Role:   storage.ReplicaRoleSingle,
			}},
		}},
	}}}, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	if _, err := router.Put(ctx, "alpha", "one"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := router.Get(ctx, "alpha")
			if err != nil {
				errCh <- fmt.Errorf("Get returned error: %w", err)
				return
			}
			if !result.Found || result.Value != "one" {
				errCh <- fmt.Errorf("Get result = %#v, want found value one", result)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
}

func TestRouterReadFallbacksForTransportUnavailableAndReadDependency(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 1, 4, "fallbacks")
	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{
				Slot:         1,
				ChainVersion: 2,
				HeadNodeID:   "head-1",
				TailNodeID:   "tail-1",
				Readable:     true,
				ReadReplicas: []coordserver.ReadReplicaRoute{
					{NodeID: "head-1", Role: storage.ReplicaRoleHead},
					{NodeID: "mid-1", Role: storage.ReplicaRoleMiddle},
					{NodeID: "tail-1", Role: storage.ReplicaRoleTail},
				},
			},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}

	t.Run("transport unavailable retries another replica before refresh", func(t *testing.T) {
		source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{snapshot}}
		transport := &recordingTransport{
			getErrs: []error{ErrNoRoute, nil},
		}
		router := mustNewRouter(t, source, transport)
		if err := router.Refresh(ctx); err != nil {
			t.Fatalf("Refresh returned error: %v", err)
		}
		if _, err := router.Get(ctx, key); err != nil {
			t.Fatalf("Get returned error: %v", err)
		}
		if got, want := transport.getNodes, []string{"head-1", "mid-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("get nodes = %v, want %v", got, want)
		}
		if got, want := source.calls, 1; got != want {
			t.Fatalf("snapshot source calls = %d, want %d", got, want)
		}
	})

	t.Run("read dependency falls back directly to tail", func(t *testing.T) {
		source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{snapshot}}
		transport := &recordingTransport{
			getErrs: []error{
				&storage.ReadDependencyError{
					Slot:                 1,
					ExpectedChainVersion: 2,
					TailNodeID:           "tail-1",
					Cause:                errors.New("tail lookup unavailable"),
				},
				nil,
			},
		}
		router := mustNewRouter(t, source, transport)
		if err := router.Refresh(ctx); err != nil {
			t.Fatalf("Refresh returned error: %v", err)
		}
		if _, err := router.Get(ctx, key); err != nil {
			t.Fatalf("Get returned error: %v", err)
		}
		if got, want := transport.getNodes, []string{"head-1", "tail-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("get nodes = %v, want %v", got, want)
		}
	})
}

func TestRouterGetWithConsistencyPassesRequestedMode(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 1, 2, "consistency")
	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 2,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{
				Slot:         1,
				ChainVersion: 1,
				HeadNodeID:   "head-1",
				TailNodeID:   "tail-1",
				Readable:     true,
				ReadReplicas: []coordserver.ReadReplicaRoute{{NodeID: "head-1", Role: storage.ReplicaRoleHead}},
			},
		},
	}
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{snapshot}}
	transport := &recordingTransport{}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	if _, err := router.GetWithConsistency(ctx, key, storage.ReadConsistencyLocalCommitted); err != nil {
		t.Fatalf("GetWithConsistency returned error: %v", err)
	}
	if got, want := transport.getModes, []storage.ReadConsistency{storage.ReadConsistencyLocalCommitted}; !reflect.DeepEqual(got, want) {
		t.Fatalf("get modes = %v, want %v", got, want)
	}
}

func TestRouterReturnsAmbiguousWriteWithoutRefreshOrRetry(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 1, 4, "ambiguous")
	snapshot := coordserver.RoutingSnapshot{
		Version:   1,
		SlotCount: 4,
		Slots: []coordserver.SlotRoute{
			{Slot: 0, ChainVersion: 1, HeadNodeID: "h0", TailNodeID: "t0", Writable: true, Readable: true},
			{Slot: 1, ChainVersion: 2, HeadNodeID: "head-1", TailNodeID: "tail-1", Writable: true, Readable: true},
			{Slot: 2, ChainVersion: 1, HeadNodeID: "h2", TailNodeID: "t2", Writable: true, Readable: true},
			{Slot: 3, ChainVersion: 1, HeadNodeID: "h3", TailNodeID: "t3", Writable: true, Readable: true},
		},
	}
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{snapshot, snapshot}}
	putErr := &storage.AmbiguousWriteError{
		Slot:                 1,
		Kind:                 storage.OperationKindPut,
		ExpectedChainVersion: 2,
		Cause:                fmt.Errorf("%w: %w", storage.ErrWriteTimeout, context.DeadlineExceeded),
	}
	deleteErr := &storage.AmbiguousWriteError{
		Slot:                 1,
		Kind:                 storage.OperationKindDelete,
		ExpectedChainVersion: 2,
		Cause:                fmt.Errorf("%w: %w", storage.ErrWriteTimeout, context.Canceled),
	}
	transport := &recordingTransport{
		putErrs:    []error{putErr},
		deleteErrs: []error{deleteErr},
	}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v"); err == nil {
		t.Fatal("Put unexpectedly succeeded")
	} else {
		var ambiguous *storage.AmbiguousWriteError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("Put error = %v, want ambiguous write", err)
		}
		if !errors.Is(err, storage.ErrAmbiguousWrite) {
			t.Fatalf("Put error = %v, want ErrAmbiguousWrite", err)
		}
	}
	if _, err := router.Delete(ctx, key); err == nil {
		t.Fatal("Delete unexpectedly succeeded")
	} else {
		var ambiguous *storage.AmbiguousWriteError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("Delete error = %v, want ambiguous write", err)
		}
		if !errors.Is(err, storage.ErrAmbiguousWrite) {
			t.Fatalf("Delete error = %v, want ErrAmbiguousWrite", err)
		}
	}

	if got, want := source.calls, 1; got != want {
		t.Fatalf("snapshot source calls = %d, want %d", got, want)
	}
	if got, want := transport.putNodes, []string{"head-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("put nodes = %v, want %v", got, want)
	}
	if got, want := transport.deleteNodes, []string{"head-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delete nodes = %v, want %v", got, want)
	}
}

func TestRouterSnapshotReturnsDefensiveCopy(t *testing.T) {
	ctx := context.Background()
	key := keyForSlot(t, 0, 2, "copy")
	source := &scriptedSnapshotSource{snapshots: []coordserver.RoutingSnapshot{
		{
			Version:   1,
			SlotCount: 2,
			Slots: []coordserver.SlotRoute{
				{Slot: 0, ChainVersion: 1, HeadNodeID: "head-0", TailNodeID: "tail-0", Writable: true, Readable: true},
				{Slot: 1, ChainVersion: 1, HeadNodeID: "head-1", TailNodeID: "tail-1", Writable: true, Readable: true},
			},
		},
	}}
	transport := &recordingTransport{}
	router := mustNewRouter(t, source, transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	snapshot, ok := router.Snapshot()
	if !ok {
		t.Fatal("Snapshot unexpectedly not loaded")
	}
	snapshot.Slots[0].HeadNodeID = "mutated-head"
	snapshot.Slots[0].TailNodeID = "mutated-tail"
	snapshot.Slots[0].Writable = false
	snapshot.Slots[0].Readable = false

	if _, err := router.Put(ctx, key, "v"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if got, want := transport.putNodes, []string{"head-0"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("put nodes = %v, want %v", got, want)
	}

	snapshot, ok = router.Snapshot()
	if !ok {
		t.Fatal("Snapshot unexpectedly not loaded on second read")
	}
	if got, want := snapshot.Slots[0].HeadNodeID, "head-0"; got != want {
		t.Fatalf("head node after external mutation = %q, want %q", got, want)
	}
	if got, want := snapshot.Slots[0].TailNodeID, "tail-0"; got != want {
		t.Fatalf("tail node after external mutation = %q, want %q", got, want)
	}
	if !snapshot.Slots[0].Writable || !snapshot.Slots[0].Readable {
		t.Fatalf("route after external mutation = %#v, want readable and writable", snapshot.Slots[0])
	}
}

func TestEndToEndRouterPutGetDeleteAndRefreshAfterReconfiguration(t *testing.T) {
	ctx := context.Background()
	h := newRouterHarness(t, []string{"a", "b", "c", "d"})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 1, 8, "slot1")
	putResult, err := router.Put(ctx, key, "v1")
	if err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if got, want := putResult.Slot, 1; got != want {
		t.Fatalf("put slot = %d, want %d", got, want)
	}
	if got, want := putResult.Sequence, uint64(1); got != want {
		t.Fatalf("put sequence = %d, want %d", got, want)
	}
	if !putResult.Applied || putResult.Metadata == nil {
		t.Fatalf("put result = %#v, want applied metadata-bearing commit", putResult)
	}
	readResult, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got, want := readResult.Slot, 1; got != want {
		t.Fatalf("read slot = %d, want %d", got, want)
	}
	if got, want := readResult.ChainVersion, uint64(1); got != want {
		t.Fatalf("read chain version = %d, want %d", got, want)
	}
	if !readResult.Found || readResult.Value != "v1" || readResult.Metadata == nil {
		t.Fatalf("read result = %#v, want found value with metadata", readResult)
	}
	assertChainValue(t, h, 1, key, "v1")

	if _, err := h.server.AddNode(ctx, reconfigureCommand("add-d", 1, coordinator.Event{
		Kind: coordinator.EventKindAddNode,
		Node: uniqueNode("d"),
	}, coordinator.ReconfigurationPolicy{MaxChangedChains: 1})); err != nil {
		t.Fatalf("AddNode returned error: %v", err)
	}

	if _, err := router.Put(ctx, key, "v2"); err == nil {
		t.Fatal("Put during join unexpectedly succeeded")
	} else if !errors.Is(err, ErrNoRoute) {
		t.Fatalf("Put during join error = %v, want no route", err)
	}
	snapshot, ok := router.Snapshot()
	if !ok {
		t.Fatal("router snapshot not loaded after stale write refresh")
	}
	if got, want := snapshot.Slots[1].ChainVersion, uint64(2); got != want {
		t.Fatalf("router chain version after stale write refresh = %d, want %d", got, want)
	}
	if snapshot.Slots[1].Writable {
		t.Fatalf("router slot during join = %#v, want not writable", snapshot.Slots[1])
	}
	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get during join returned error: %v", err)
	}
	if got, want := readResult.Value, "v1"; got != want {
		t.Fatalf("read value during join = %q, want %q", got, want)
	}

	if err := h.adapters["d"].ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := h.adapters["c"].RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 1}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}

	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after stale tail returned error: %v", err)
	}
	if got, want := readResult.Value, "v1"; got != want {
		t.Fatalf("read value after reconfiguration = %q, want %q", got, want)
	}
	snapshot, ok = router.Snapshot()
	if !ok {
		t.Fatal("router snapshot missing after stale read refresh")
	}
	if got, want := snapshot.Slots[1].ChainVersion, h.server.Current().SlotVersions[1]; got != want {
		t.Fatalf("router chain version after stale read refresh = %d, want %d", got, want)
	}
	if !snapshot.Slots[1].Writable {
		t.Fatalf("router slot after reconfiguration = %#v, want writable", snapshot.Slots[1])
	}

	if _, err := router.Put(ctx, key, "v2"); err != nil {
		t.Fatalf("Put after repair returned error: %v", err)
	}

	if _, err := router.Delete(ctx, key); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete returned error: %v", err)
	}
	if readResult.Found {
		t.Fatalf("read after delete = %#v, want not found", readResult)
	}
	assertChainValue(t, h, 1, key, "")
}

func TestRouterServesWritesAfterDynamicAutoJoinTailActivation(t *testing.T) {
	ctx := context.Background()
	h := newRouterHarness(t, []string{"a", "b", "c"})
	bootstrapDynamicCluster(t, ctx, h, 1, 3, []string{"a", "b", "c"})

	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	snapshot, ok := router.Snapshot()
	if !ok {
		t.Fatal("router snapshot not loaded")
	}
	if got, want := snapshot.Slots[0].HeadNodeID, "a"; got != want {
		t.Fatalf("head node = %q, want %q", got, want)
	}
	if got, want := snapshot.Slots[0].TailNodeID, "c"; got != want {
		t.Fatalf("tail node = %q, want %q", got, want)
	}
	if !snapshot.Slots[0].Writable || !snapshot.Slots[0].Readable {
		t.Fatalf("slot route = %#v, want readable and writable", snapshot.Slots[0])
	}

	key := keyForSlot(t, 0, 1, "dynamic-tail")
	if _, err := router.Put(ctx, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	readResult, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got, want := readResult.Value, "v1"; got != want {
		t.Fatalf("read value = %q, want %q", got, want)
	}
	assertReplicaValueOnNodes(t, h, 0, key, "v1", "a", "b", "c")
}

func TestRouterReadAndReplacementTailStayCorrectAcrossDeadTailReconfiguration(t *testing.T) {
	ctx := context.Background()
	h := newRouterHarness(t, []string{"a", "b", "c", "d"})
	bootstrapDynamicCluster(t, ctx, h, 1, 3, []string{"a", "b", "c"})

	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 0, 1, "dead-tail")
	if _, err := router.Put(ctx, key, "v1"); err != nil {
		t.Fatalf("initial Put returned error: %v", err)
	}
	assertReplicaValueOnNodes(t, h, 0, key, "v1", "a", "b", "c")

	current := h.server.Current()
	if _, err := h.server.MarkNodeDead(ctx, reconfigureCommand("dead-c", current.Version, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "c",
	}, coordinator.ReconfigurationPolicy{})); err != nil {
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after dead tail returned error: %v", err)
	}
	snapshot, ok := router.Snapshot()
	if !ok {
		t.Fatal("router snapshot not loaded after dead tail")
	}
	if got, want := snapshot.Slots[0].TailNodeID, "b"; got != want {
		t.Fatalf("tail after dead-node shorten = %q, want %q", got, want)
	}
	readResult, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after dead tail returned error: %v", err)
	}
	if got, want := readResult.Value, "v1"; got != want {
		t.Fatalf("read value after dead tail = %q, want %q", got, want)
	}
	assertReplicaValueOnNodes(t, h, 0, key, "v1", "a", "b")

	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh after replacement tail returned error: %v", err)
	}
	snapshot, ok = router.Snapshot()
	if !ok {
		t.Fatal("router snapshot not loaded after replacement tail")
	}
	if got, want := snapshot.Slots[0].TailNodeID, "d"; got != want {
		t.Fatalf("tail after replacement join = %q, want %q", got, want)
	}

	if _, err := router.Put(ctx, key, "v2"); err != nil {
		t.Fatalf("Put after replacement returned error: %v", err)
	}
	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after replacement returned error: %v", err)
	}
	if got, want := readResult.Value, "v2"; got != want {
		t.Fatalf("read value after replacement = %q, want %q", got, want)
	}
	assertReplicaValueOnNodes(t, h, 0, key, "v2", "a", "b", "d")
}

func TestRouterConcurrentRefreshAndOperationsStayCorrectAcrossDeadTailRepair(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h := newRouterHarness(t, []string{"a", "b", "c", "d"})
	bootstrapDynamicCluster(t, ctx, h, 1, 3, []string{"a", "b", "c"})

	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	retryTransient := func(fn func() error) error {
		deadline := time.Now().Add(2 * time.Second)
		for {
			err := fn()
			if err == nil {
				return nil
			}
			var mismatch *storage.RoutingMismatchError
			if errors.Is(err, ErrNoRoute) ||
				errors.Is(err, ErrSnapshotNotLoaded) ||
				errors.Is(err, storage.ErrStateMismatch) ||
				errors.As(err, &mismatch) {
				if time.Now().After(deadline) {
					return err
				}
				time.Sleep(250 * time.Microsecond)
				continue
			}
			return err
		}
	}

	done := make(chan struct{})
	errCh := make(chan error, 8)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(250 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if err := router.Refresh(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					errCh <- err
					return
				}
			}
		}
	}()

	keys := []string{
		keyForSlot(t, 0, 1, "concurrent-router-a"),
	}
	for worker, key := range keys {
		worker := worker
		key := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iteration := 0; iteration < 16; iteration++ {
				value := fmt.Sprintf("worker-%d-%d", worker, iteration)
				if err := retryTransient(func() error {
					_, err := router.Put(ctx, key, value)
					return err
				}); err != nil {
					errCh <- fmt.Errorf("put %q: %w", key, err)
					return
				}
				if err := retryTransient(func() error {
					result, err := router.Get(ctx, key)
					if err != nil {
						return err
					}
					if !result.Found || result.Value != value {
						return fmt.Errorf("get %q = %#v, want value %q", key, result, value)
					}
					return nil
				}); err != nil {
					errCh <- err
					return
				}
				if err := retryTransient(func() error {
					_, err := router.Delete(ctx, key)
					return err
				}); err != nil {
					errCh <- fmt.Errorf("delete %q: %w", key, err)
					return
				}
				if err := retryTransient(func() error {
					result, err := router.Get(ctx, key)
					if err != nil {
						return err
					}
					if result.Found {
						return fmt.Errorf("get after delete %q = %#v, want not found", key, result)
					}
					return nil
				}); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	time.Sleep(5 * time.Millisecond)
	current := h.server.Current()
	if _, err := h.server.MarkNodeDead(ctx, reconfigureCommand("dead-c", current.Version, coordinator.Event{
		Kind:   coordinator.EventKindMarkNodeDead,
		NodeID: "c",
	}, coordinator.ReconfigurationPolicy{})); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("MarkNodeDead returned error: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if err := h.adapters["d"].Node().ReportHeartbeat(ctx); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("ReportHeartbeat(d) returned error: %v", err)
	}
	if err := h.adapters["d"].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		close(done)
		wg.Wait()
		t.Fatalf("ActivateReplica(d) returned error: %v", err)
	}

	close(done)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent router workload returned error: %v", err)
		}
	}

	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("final Refresh returned error: %v", err)
	}
	snapshot, ok := router.Snapshot()
	if !ok {
		t.Fatal("router snapshot missing after concurrent churn")
	}
	route := snapshot.Slots[0]
	if !route.Readable || !route.Writable {
		t.Fatalf("final route = %#v, want readable+writable", route)
	}
	if got, want := route.TailNodeID, "d"; got != want {
		t.Fatalf("final tail node = %q, want %q", got, want)
	}

	verifyKey := keyForSlot(t, 0, 1, "concurrent-router-verify")
	if _, err := router.Put(ctx, verifyKey, "final"); err != nil {
		t.Fatalf("final Put returned error: %v", err)
	}
	readResult, err := router.Get(ctx, verifyKey)
	if err != nil {
		t.Fatalf("final Get returned error: %v", err)
	}
	if !readResult.Found || readResult.Value != "final" {
		t.Fatalf("final Get result = %#v, want final value", readResult)
	}
	assertReplicaValueOnNodes(t, h, 0, verifyKey, "final", "a", "b", "d")
}

func TestRouterPutIfReturnsTypedConditionFailure(t *testing.T) {
	ctx := context.Background()
	h := newRouterHarness(t, []string{"a", "b", "c"})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 8, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 8, 3, []string{"a", "b", "c"})

	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 1, 8, "slot1")
	if _, err := router.Put(ctx, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	existsFalse := false
	_, err := router.PutIf(ctx, key, "v2", storage.WriteConditions{
		Exists: &existsFalse,
	})
	if err == nil {
		t.Fatal("PutIf unexpectedly succeeded")
	}
	var failed *storage.ConditionFailedError
	if !errors.As(err, &failed) {
		t.Fatalf("error = %v, want condition failed", err)
	}
	if !failed.CurrentExists || failed.CurrentMetadata == nil || failed.CurrentMetadata.Version != 1 {
		t.Fatalf("condition failure = %#v, want current version 1", failed)
	}
}

func TestEndToEndRouterPutGetDeleteWithQueuedReplicationTransport(t *testing.T) {
	ctx := context.Background()
	h := newQueuedRouterHarness(t, []string{"a", "b", "c"})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 4, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 4, 3, []string{"a", "b", "c"})

	var observedHeadStaged bool
	h.repl.SetBeforeDeliver(func(msg storage.QueuedReplicationMessage) {})
	router := mustNewRouter(t, h.server, h.transport)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	key := keyForSlot(t, 1, 4, "queued")

	h.repl.SetBeforeDeliver(func(msg storage.QueuedReplicationMessage) {
		if observedHeadStaged || msg.Forward == nil {
			return
		}
		observedHeadStaged = true
		if got, want := mustNodeStagedSequencesForRouter(t, h.adapters["b"].Node(), 1), []uint64{1}; !reflect.DeepEqual(got, want) {
			t.Fatalf("head staged before first queued delivery = %v, want %v", got, want)
		}
	})
	if _, err := routerPutWithDelivery(t, ctx, router, h.repl, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	if !observedHeadStaged {
		t.Fatal("queued replication hook did not observe staged head write")
	}
	readResult, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got, want := readResult.Value, "v1"; got != want {
		t.Fatalf("read value = %q, want %q", got, want)
	}
	if _, err := routerDeleteWithDelivery(t, ctx, router, h.repl, key); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
	readResult, err = router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get after delete returned error: %v", err)
	}
	if readResult.Found {
		t.Fatalf("read result after delete = %#v, want not found", readResult)
	}
}

func TestRouterCRAQRoundRobinUsesActiveReadReplicasWithRealHarness(t *testing.T) {
	ctx := context.Background()
	h := newRouterHarness(t, []string{"a", "b", "c"})
	bootstrapDynamicCluster(t, ctx, h, 1, 3, []string{"a", "b", "c"})

	tracing := &observingTransport{base: h.transport}
	router := mustNewRouter(t, h.server, tracing)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 0, 1, "craq-round-robin-real")
	if _, err := router.Put(ctx, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	for i := 0; i < 3; i++ {
		read, err := router.Get(ctx, key)
		if err != nil {
			t.Fatalf("Get #%d returned error: %v", i+1, err)
		}
		if !read.Found || read.Value != "v1" {
			t.Fatalf("Get #%d result = %#v, want value v1", i+1, read)
		}
	}
	if got, want := tracing.getNodes, []string{"a", "b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("get nodes = %v, want %v", got, want)
	}
}

func TestRouterCRAQLocalCommittedAndLinearizableReadsUseRealHarness(t *testing.T) {
	ctx := context.Background()
	h := newQueuedRouterHarness(t, []string{"a", "b", "c"})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})

	tracing := &observingTransport{base: h.transport}
	router := mustNewRouter(t, h.server, tracing)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 0, 1, "craq-local-real")
	if _, err := routerPutWithDelivery(t, ctx, router, h.repl, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	enqueueDirtyCommittedMiddleWrite(t, ctx, h, 0, key, "v2", 2)

	setNextReadReplicaForSlot(router, 0, 1)
	local, err := router.GetWithConsistency(ctx, key, storage.ReadConsistencyLocalCommitted)
	if err != nil {
		t.Fatalf("GetWithConsistency returned error: %v", err)
	}
	if !local.Found || local.Value != "v1" {
		t.Fatalf("local committed read = %#v, want value v1", local)
	}
	if got, want := tracing.getNodes, []string{"b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("local committed nodes = %v, want %v", got, want)
	}
	if got, want := tracing.getModes, []storage.ReadConsistency{storage.ReadConsistencyLocalCommitted}; !reflect.DeepEqual(got, want) {
		t.Fatalf("local committed modes = %v, want %v", got, want)
	}

	tracing.reset()
	setNextReadReplicaForSlot(router, 0, 1)
	linearizable, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !linearizable.Found || linearizable.Value != "v2" {
		t.Fatalf("linearizable read = %#v, want value v2", linearizable)
	}
	if got, want := tracing.getNodes, []string{"b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("linearizable nodes = %v, want %v", got, want)
	}
}

func TestRouterCRAQFallsBackDirectlyToTailWithRealHarness(t *testing.T) {
	ctx := context.Background()
	h := newQueuedRouterHarness(t, []string{"a", "b", "c"})
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, 1, 3, "a", "b", "c")); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	h.seedBootstrap(t, 1, 3, []string{"a", "b", "c"})

	tracing := &observingTransport{base: h.transport}
	router := mustNewRouter(t, h.server, tracing)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 0, 1, "craq-tail-fallback-real")
	if _, err := routerPutWithDelivery(t, ctx, router, h.repl, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	enqueueDirtyCommittedMiddleWrite(t, ctx, h, 0, key, "v2", 2)
	misconfigureReadDependency(t, h.adapters["b"].Node(), 0, "missing-tail")
	setNextReadReplicaForSlot(router, 0, 1)

	read, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !read.Found || read.Value != "v2" {
		t.Fatalf("Get result = %#v, want value v2", read)
	}
	if got, want := tracing.getNodes, []string{"b", "c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("get nodes = %v, want %v", got, want)
	}
}

func TestRouterCRAQRetriesAnotherReplicaOnTransportUnavailableWithRealHarness(t *testing.T) {
	ctx := context.Background()
	h := newRouterHarness(t, []string{"a", "b", "c"})
	bootstrapDynamicCluster(t, ctx, h, 1, 3, []string{"a", "b", "c"})

	attempts := map[string]int{}
	tracing := &observingTransport{
		base: h.transport,
		getHook: func(nodeID string, _ storage.ClientGetRequest) error {
			attempts[nodeID]++
			if nodeID == "a" && attempts[nodeID] == 1 {
				return ErrNoRoute
			}
			return nil
		},
	}
	router := mustNewRouter(t, h.server, tracing)
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	key := keyForSlot(t, 0, 1, "craq-unavailable-real")
	if _, err := router.Put(ctx, key, "v1"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	read, err := router.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !read.Found || read.Value != "v1" {
		t.Fatalf("Get result = %#v, want value v1", read)
	}
	if got, want := tracing.getNodes, []string{"a", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("get nodes = %v, want %v", got, want)
	}
}

type scriptedSnapshotSource struct {
	snapshots []coordserver.RoutingSnapshot
	calls     int
}

func (s *scriptedSnapshotSource) RoutingSnapshot(_ context.Context) (coordserver.RoutingSnapshot, error) {
	index := s.calls
	if index >= len(s.snapshots) {
		index = len(s.snapshots) - 1
	}
	s.calls++
	return s.snapshots[index], nil
}

type recordingTransport struct {
	getNodes    []string
	getErrs     []error
	getResults  []storage.ReadResult
	getModes    []storage.ReadConsistency
	putNodes    []string
	deleteNodes []string
	putErrs     []error
	putResults  []storage.CommitResult
	deleteErrs  []error
}

func (t *recordingTransport) Get(_ context.Context, nodeID string, req storage.ClientGetRequest) (storage.ReadResult, error) {
	t.getNodes = append(t.getNodes, nodeID)
	t.getModes = append(t.getModes, req.Consistency)
	if len(t.getErrs) > 0 {
		err := t.getErrs[0]
		t.getErrs = t.getErrs[1:]
		if err != nil {
			return storage.ReadResult{}, err
		}
	}
	if len(t.getResults) > 0 {
		result := t.getResults[0]
		t.getResults = t.getResults[1:]
		return result, nil
	}
	return storage.ReadResult{
		Slot:         req.Slot,
		ChainVersion: req.ExpectedChainVersion,
	}, nil
}

func (t *recordingTransport) Put(_ context.Context, nodeID string, req storage.ClientPutRequest) (storage.CommitResult, error) {
	t.putNodes = append(t.putNodes, nodeID)
	if len(t.putErrs) > 0 {
		err := t.putErrs[0]
		t.putErrs = t.putErrs[1:]
		if err != nil {
			return storage.CommitResult{}, err
		}
	}
	if len(t.putResults) > 0 {
		result := t.putResults[0]
		t.putResults = t.putResults[1:]
		return result, nil
	}
	return storage.CommitResult{Slot: req.Slot, Sequence: 1}, nil
}

func (t *recordingTransport) Delete(_ context.Context, nodeID string, req storage.ClientDeleteRequest) (storage.CommitResult, error) {
	t.deleteNodes = append(t.deleteNodes, nodeID)
	if len(t.deleteErrs) > 0 {
		err := t.deleteErrs[0]
		t.deleteErrs = t.deleteErrs[1:]
		if err != nil {
			return storage.CommitResult{}, err
		}
	}
	return storage.CommitResult{Slot: req.Slot, Sequence: 1}, nil
}

type routerHarness struct {
	server    *coordserver.Server
	transport *InMemoryTransport
	adapters  map[string]*coordserver.InMemoryNodeAdapter
	backends  map[string]*storage.InMemoryBackend
	repl      interface{}
}

func newRouterHarness(t *testing.T, nodeIDs []string) *routerHarness {
	t.Helper()
	repl := storage.NewInMemoryReplicationTransport()
	transport := NewInMemoryTransport()
	adapters := make(map[string]*coordserver.InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]coordserver.StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := coordserver.NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
		transport.RegisterNode(nodeID, adapter.Node())
	}
	server, err := coordserver.Open(context.Background(), coordruntime.NewInMemoryStore(), nodeClients)
	if err != nil {
		t.Fatalf("coordserver.Open returned error: %v", err)
	}
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	return &routerHarness{
		server:    server,
		transport: transport,
		adapters:  adapters,
		backends:  backends,
		repl:      repl,
	}
}

type queuedRouterHarness struct {
	server    *coordserver.Server
	transport *InMemoryTransport
	adapters  map[string]*coordserver.InMemoryNodeAdapter
	backends  map[string]*storage.InMemoryBackend
	repl      *storage.QueuedInMemoryReplicationTransport
}

func newQueuedRouterHarness(t *testing.T, nodeIDs []string) *queuedRouterHarness {
	t.Helper()
	repl := storage.NewQueuedInMemoryReplicationTransport()
	transport := NewInMemoryTransport()
	adapters := make(map[string]*coordserver.InMemoryNodeAdapter, len(nodeIDs))
	backends := make(map[string]*storage.InMemoryBackend, len(nodeIDs))
	nodeClients := make(map[string]coordserver.StorageNodeClient, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := storage.NewInMemoryBackend()
		backends[nodeID] = backend
		repl.Register(nodeID, backend)
		adapter, err := coordserver.NewInMemoryNodeAdapter(context.Background(), nodeID, backend, repl)
		if err != nil {
			t.Fatalf("NewInMemoryNodeAdapter(%q) returned error: %v", nodeID, err)
		}
		adapters[nodeID] = adapter
		nodeClients[nodeID] = adapter
		repl.RegisterNode(nodeID, adapter.Node())
		transport.RegisterNode(nodeID, adapter.Node())
	}
	server, err := coordserver.Open(context.Background(), coordruntime.NewInMemoryStore(), nodeClients)
	if err != nil {
		t.Fatalf("coordserver.Open returned error: %v", err)
	}
	for _, adapter := range adapters {
		adapter.BindServer(server)
	}
	return &queuedRouterHarness{
		server:    server,
		transport: transport,
		adapters:  adapters,
		backends:  backends,
		repl:      repl,
	}
}

func (h *queuedRouterHarness) seedBootstrap(t *testing.T, slotCount int, replicationFactor int, nodeIDs []string) {
	t.Helper()
	state, err := coordinator.BuildInitialPlacement(coordinator.Config{
		SlotCount:         slotCount,
		ReplicationFactor: replicationFactor,
	}, uniqueNodes(nodeIDs...))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(nil)
	}
	for _, chain := range state.Chains {
		for _, replica := range chain.Replicas {
			assignment := assignmentForChainNode(chain, replica.NodeID, 1)
			adapter := h.adapters[replica.NodeID]
			if err := adapter.Node().AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				t.Fatalf("seed AddReplicaAsTail returned error: %v", err)
			}
			if err := adapter.Node().ActivateReplica(context.Background(), storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				t.Fatalf("seed ActivateReplica returned error: %v", err)
			}
		}
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(h.server)
	}
}

func (h *routerHarness) seedBootstrap(t *testing.T, slotCount int, replicationFactor int, nodeIDs []string) {
	t.Helper()
	state, err := coordinator.BuildInitialPlacement(coordinator.Config{
		SlotCount:         slotCount,
		ReplicationFactor: replicationFactor,
	}, uniqueNodes(nodeIDs...))
	if err != nil {
		t.Fatalf("BuildInitialPlacement returned error: %v", err)
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(nil)
	}
	for _, chain := range state.Chains {
		for _, replica := range chain.Replicas {
			assignment := assignmentForChainNode(chain, replica.NodeID, 1)
			adapter := h.adapters[replica.NodeID]
			if err := adapter.Node().AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
				t.Fatalf("seed AddReplicaAsTail returned error: %v", err)
			}
			if err := adapter.Node().ActivateReplica(context.Background(), storage.ActivateReplicaCommand{Slot: chain.Slot}); err != nil {
				t.Fatalf("seed ActivateReplica returned error: %v", err)
			}
		}
	}
	for _, adapter := range h.adapters {
		adapter.BindServer(h.server)
	}
}

func assignmentForChainNode(chain coordinator.Chain, nodeID string, chainVersion uint64) storage.ReplicaAssignment {
	position := -1
	for i, replica := range chain.Replicas {
		if replica.NodeID == nodeID {
			position = i
			break
		}
	}
	if position < 0 {
		panic("node not found in chain")
	}
	role := storage.ReplicaRoleMiddle
	switch len(chain.Replicas) {
	case 1:
		role = storage.ReplicaRoleSingle
	default:
		switch position {
		case 0:
			role = storage.ReplicaRoleHead
		case len(chain.Replicas) - 1:
			role = storage.ReplicaRoleTail
		}
	}
	assignment := storage.ReplicaAssignment{
		Slot:         chain.Slot,
		ChainVersion: chainVersion,
		Role:         role,
	}
	if position > 0 {
		assignment.Peers.PredecessorNodeID = chain.Replicas[position-1].NodeID
	}
	if position+1 < len(chain.Replicas) {
		assignment.Peers.SuccessorNodeID = chain.Replicas[position+1].NodeID
	}
	if len(chain.Replicas) > 0 {
		assignment.Peers.TailNodeID = chain.Replicas[len(chain.Replicas)-1].NodeID
		assignment.Peers.TailTarget = chain.Replicas[len(chain.Replicas)-1].NodeID
	}
	return assignment
}

func bootstrapCommand(id string, expected uint64, slotCount int, replicationFactor int, nodeIDs ...string) coordruntime.Command {
	return coordruntime.Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         slotCount,
				ReplicationFactor: replicationFactor,
			},
			Nodes: uniqueNodes(nodeIDs...),
		},
	}
}

func reconfigureCommand(
	id string,
	expected uint64,
	event coordinator.Event,
	policy coordinator.ReconfigurationPolicy,
) coordruntime.Command {
	return coordruntime.Command{
		ID:              id,
		ExpectedVersion: expected,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: []coordinator.Event{event},
			Policy: policy,
		},
	}
}

func uniqueNodes(ids ...string) []coordinator.Node {
	nodes := make([]coordinator.Node, len(ids))
	for i, id := range ids {
		nodes[i] = uniqueNode(id)
	}
	return nodes
}

func uniqueNode(id string) coordinator.Node {
	return coordinator.Node{
		ID: id,
		FailureDomains: map[string]string{
			"host": "host-" + id,
			"rack": "rack-" + id,
			"az":   "az-" + id,
		},
	}
}

func keyForSlot(t *testing.T, slot int, slotCount int, prefix string) string {
	t.Helper()
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("%s-%d", prefix, i)
		if int(crc32.ChecksumIEEE([]byte(key))%uint32(slotCount)) == slot {
			return key
		}
	}
	t.Fatalf("unable to find key for slot %d", slot)
	return ""
}

func bootstrapDynamicCluster(
	t *testing.T,
	ctx context.Context,
	h *routerHarness,
	slotCount int,
	replicationFactor int,
	nodeIDs []string,
) {
	t.Helper()
	if _, err := h.server.Bootstrap(ctx, bootstrapCommand("bootstrap-1", 0, slotCount, replicationFactor)); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}
	for _, nodeID := range nodeIDs {
		if err := h.adapters[nodeID].Node().ReportHeartbeat(ctx); err != nil {
			t.Fatalf("ReportHeartbeat(%q) returned error: %v", nodeID, err)
		}
	}
	for _, nodeID := range nodeIDs {
		if err := h.adapters[nodeID].Node().ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
			t.Fatalf("ActivateReplica(%q) returned error: %v", nodeID, err)
		}
	}
}

func assertChainValue(t *testing.T, h *routerHarness, slot int, key string, want string) {
	t.Helper()
	for nodeID, adapter := range h.adapters {
		state := adapter.Node().State()
		replica, ok := state.Replicas[slot]
		if !ok {
			continue
		}
		if replica.State != storage.ReplicaStateActive {
			continue
		}
		snapshot, err := adapter.Node().CommittedSnapshot(slot)
		if err != nil {
			t.Fatalf("CommittedSnapshot(%q, %d) returned error: %v", nodeID, slot, err)
		}
		got := snapshot[key]
		if want == "" {
			if _, exists := snapshot[key]; exists {
				t.Fatalf("node %q slot %d has value %q, want missing", nodeID, slot, got.Value)
			}
			continue
		}
		if got.Value != want {
			t.Fatalf("node %q slot %d value = %q, want %q", nodeID, slot, got.Value, want)
		}
	}
}

func assertReplicaValueOnNodes(t *testing.T, h *routerHarness, slot int, key string, want string, nodeIDs ...string) {
	t.Helper()
	for _, nodeID := range nodeIDs {
		adapter, ok := h.adapters[nodeID]
		if !ok {
			t.Fatalf("unknown adapter %q", nodeID)
		}
		snapshot, err := adapter.Node().CommittedSnapshot(slot)
		if err != nil {
			t.Fatalf("CommittedSnapshot(%q, %d) returned error: %v", nodeID, slot, err)
		}
		got, ok := snapshot[key]
		if !ok {
			t.Fatalf("node %q slot %d missing key %q", nodeID, slot, key)
		}
		if got.Value != want {
			t.Fatalf("node %q slot %d value = %q, want %q", nodeID, slot, got.Value, want)
		}
	}
}

func mustNodeStagedSequencesForRouter(t *testing.T, node *storage.Node, slot int) []uint64 {
	t.Helper()
	sequences, err := node.StagedSequences(slot)
	if err != nil {
		t.Fatalf("StagedSequences returned error: %v", err)
	}
	return sequences
}

func mustNewRouter(t *testing.T, source SnapshotSource, transport Transport) *Router {
	t.Helper()
	router, err := NewRouter(source, transport)
	if err != nil {
		t.Fatalf("NewRouter returned error: %v", err)
	}
	return router
}

type observingTransport struct {
	base     Transport
	getNodes []string
	getModes []storage.ReadConsistency
	getHook  func(nodeID string, req storage.ClientGetRequest) error
}

func (t *observingTransport) Get(ctx context.Context, nodeID string, req storage.ClientGetRequest) (storage.ReadResult, error) {
	t.getNodes = append(t.getNodes, nodeID)
	t.getModes = append(t.getModes, req.Consistency)
	if t.getHook != nil {
		if err := t.getHook(nodeID, req); err != nil {
			return storage.ReadResult{}, err
		}
	}
	return t.base.Get(ctx, nodeID, req)
}

func (t *observingTransport) Put(ctx context.Context, nodeID string, req storage.ClientPutRequest) (storage.CommitResult, error) {
	return t.base.Put(ctx, nodeID, req)
}

func (t *observingTransport) Delete(ctx context.Context, nodeID string, req storage.ClientDeleteRequest) (storage.CommitResult, error) {
	return t.base.Delete(ctx, nodeID, req)
}

func (t *observingTransport) reset() {
	t.getNodes = nil
	t.getModes = nil
}

func mustNewStorageNodeForRouter(
	t *testing.T,
	ctx context.Context,
	nodeID string,
	backend storage.Backend,
	repl *storage.InMemoryReplicationTransport,
) *storage.Node {
	t.Helper()
	node, err := storage.OpenNode(
		ctx,
		storage.Config{NodeID: nodeID},
		backend,
		storage.NewInMemoryLocalStateStore(),
		storage.NewInMemoryCoordinatorClient(),
		repl,
	)
	if err != nil {
		t.Fatalf("OpenNode(%q) returned error: %v", nodeID, err)
	}
	repl.Register(nodeID, backend)
	repl.RegisterNode(nodeID, node)
	return node
}

func mustActivateStorageAssignment(t *testing.T, node *storage.Node, assignment storage.ReplicaAssignment) {
	t.Helper()
	ctx := context.Background()
	if err := node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: assignment.Slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
}

func enqueueDirtyCommittedMiddleWrite(
	t *testing.T,
	ctx context.Context,
	h *queuedRouterHarness,
	slot int,
	key string,
	value string,
	sequence uint64,
) {
	t.Helper()
	ts := time.Unix(int64(sequence), 0).UTC()
	if err := h.adapters["b"].Node().HandleForwardWrite(ctx, storage.ForwardWriteRequest{
		Operation: storage.WriteOperation{
			Slot:     slot,
			Sequence: sequence,
			Kind:     storage.OperationKindPut,
			Key:      key,
			Value:    value,
			Metadata: storage.ObjectMetadata{
				Version:   sequence,
				CreatedAt: ts,
				UpdatedAt: ts,
			},
		},
		FromNodeID:   "a",
		ChainVersion: 1,
	}); err != nil {
		t.Fatalf("HandleForwardWrite returned error: %v", err)
	}
	if err := h.repl.DeliverNext(ctx); err != nil {
		t.Fatalf("DeliverNext returned error: %v", err)
	}
}

func misconfigureReadDependency(t *testing.T, node *storage.Node, slot int, tailNodeID string) {
	t.Helper()
	state := node.State()
	replica, ok := state.Replicas[slot]
	if !ok {
		t.Fatalf("slot %d missing from node %q", slot, state.NodeID)
	}
	assignment := replica.Assignment
	assignment.Peers.TailNodeID = tailNodeID
	assignment.Peers.TailTarget = tailNodeID
	if err := node.UpdateChainPeers(context.Background(), storage.UpdateChainPeersCommand{Assignment: assignment}); err != nil {
		t.Fatalf("UpdateChainPeers returned error: %v", err)
	}
}

func setNextReadReplicaForSlot(router *Router, slot int, next int) {
	router.roundRobinMu.Lock()
	defer router.roundRobinMu.Unlock()
	router.nextReadReplica[slot] = next
}
