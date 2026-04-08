package coordserver_test

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/coordserver/hastoretest"
	"github.com/danthegoodman1/craq/storage"
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

func TestPostgresHAStoreRoundTripsComplexSnapshot(t *testing.T) {
	dsn := os.Getenv("CRAQ_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CRAQ_TEST_POSTGRES_DSN is not set")
	}

	ctx := context.Background()
	store, err := coordserver.OpenPostgresHAStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgresHAStore returned error: %v", err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Reset(ctx); err != nil {
		t.Fatalf("Reset returned error: %v", err)
	}

	now := time.Unix(100, 0).UTC()
	lease, leader, err := store.AcquireOrRenew(ctx, "coord-a", "leader-a", now, time.Second)
	if err != nil {
		t.Fatalf("AcquireOrRenew returned error: %v", err)
	}
	if !leader {
		t.Fatal("coord-a did not become leader")
	}

	snapshot := coordserver.HASnapshot{
		State: coordruntime.State{
			Version:                 7,
			LastLogIndex:            7,
			Cluster:                 coordinator.ClusterState{SlotCount: 1},
			SlotVersions:            map[int]uint64{0: 3},
			CompletedProgressBySlot: map[int][]coordruntime.CompletedProgressRecord{0: {{NodeID: "d", Kind: coordruntime.CompletedProgressKindReady, SlotVersion: 3}}},
			NodeLivenessByID:        map[string]coordruntime.NodeLivenessRecord{"d": {State: coordruntime.NodeLivenessStateHealthy, LastHeartbeatUnixNano: now.UnixNano()}},
			PendingBySlot:           map[int]coordruntime.PendingWork{0: {Slot: 0, NodeID: "d", Kind: coordruntime.PendingKindRemoved, SlotVersion: 3, CommandID: "cmd-1"}},
			AppliedCommands:         map[string]coordruntime.AppliedCommand{},
		},
		Pending: map[int]coordserver.PendingWork{
			0: {Slot: 0, NodeID: "d", Kind: "removed", SlotVersion: 3, Epoch: lease.Epoch, CommandID: "cmd-1"},
		},
		LastPolicy:          coordinator.ReconfigurationPolicy{MaxChangedChains: 4},
		UnavailableReplicas: map[string]map[int]bool{"c": {0: true}},
		LastRecoveryReports: map[string]storage.NodeRecoveryReport{
			"d": {NodeID: "d", Replicas: []storage.RecoveredReplica{{
				Assignment:               storage.ReplicaAssignment{Slot: 0, ChainVersion: 3, Role: storage.ReplicaRoleTail},
				LastKnownState:           storage.ReplicaStateActive,
				HighestCommittedSequence: 9,
				HasCommittedData:         true,
			}}},
		},
		Outbox: []coordserver.OutboxEntry{{
			ID:        "outbox-1",
			Epoch:     lease.Epoch,
			NodeID:    "d",
			Slot:      0,
			CommandID: "cmd-1",
			Kind:      coordserver.OutboxCommandAddReplicaAsTail,
		}},
	}

	version, err := store.SaveSnapshot(ctx, lease, now.Add(100*time.Millisecond), 0, snapshot)
	if err != nil {
		t.Fatalf("SaveSnapshot returned error: %v", err)
	}
	loaded, err := store.LoadSnapshot(ctx)
	if err != nil {
		t.Fatalf("LoadSnapshot returned error: %v", err)
	}

	snapshot.SnapshotVersion = version
	if !reflect.DeepEqual(loaded, snapshot) {
		t.Fatalf("loaded snapshot mismatch\nloaded=%#v\nwant=%#v", loaded, snapshot)
	}
}
