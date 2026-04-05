package hastoretest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/coordserver"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

type Factory func(t *testing.T) coordserver.HAStore

func Run(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("initial_acquire_renew_and_save", func(t *testing.T) {
		store := factory(t)
		defer func() { _ = store.Close() }()

		now := time.Unix(100, 0)
		lease, leader, err := store.AcquireOrRenew(context.Background(), "coord-a", "leader-a", now, time.Second)
		if err != nil {
			t.Fatalf("AcquireOrRenew returned error: %v", err)
		}
		if !leader {
			t.Fatal("first coordinator did not become leader")
		}
		if got, want := lease.Epoch, uint64(1); got != want {
			t.Fatalf("lease epoch = %d, want %d", got, want)
		}

		renewed, leader, err := store.AcquireOrRenew(context.Background(), "coord-a", "leader-a-2", now.Add(500*time.Millisecond), time.Second)
		if err != nil {
			t.Fatalf("renew returned error: %v", err)
		}
		if !leader {
			t.Fatal("renewing coordinator lost leadership")
		}
		if got, want := renewed.Epoch, lease.Epoch; got != want {
			t.Fatalf("renewed epoch = %d, want %d", got, want)
		}

		snapshot := coordserver.HASnapshot{
			Pending: map[int]coordserver.PendingWork{
				1: {Slot: 1, NodeID: "n1", Kind: "ready", SlotVersion: 4, Epoch: renewed.Epoch, CommandID: "cmd-1"},
			},
			Outbox: []coordserver.OutboxEntry{{
				ID:        "outbox-1",
				Epoch:     renewed.Epoch,
				NodeID:    "n1",
				Slot:      1,
				CommandID: "cmd-1",
				Kind:      coordserver.OutboxCommandAddReplicaAsTail,
			}},
		}
		version, err := store.SaveSnapshot(context.Background(), renewed, now.Add(600*time.Millisecond), 0, snapshot)
		if err != nil {
			t.Fatalf("SaveSnapshot returned error: %v", err)
		}
		if got, want := version, uint64(1); got != want {
			t.Fatalf("snapshot version = %d, want %d", got, want)
		}

		loaded, err := store.LoadSnapshot(context.Background())
		if err != nil {
			t.Fatalf("LoadSnapshot returned error: %v", err)
		}
		if got, want := loaded.SnapshotVersion, uint64(1); got != want {
			t.Fatalf("loaded snapshot version = %d, want %d", got, want)
		}
		snapshot.SnapshotVersion = 1
		if !reflect.DeepEqual(loaded, coordserverCloneSnapshot(snapshot)) {
			t.Fatalf("loaded snapshot = %#v, want %#v", loaded, snapshot)
		}
	})

	t.Run("single_winner_and_takeover", func(t *testing.T) {
		store := factory(t)
		defer func() { _ = store.Close() }()

		now := time.Unix(200, 0)
		first, firstLeader, err := store.AcquireOrRenew(context.Background(), "coord-a", "leader-a", now, time.Second)
		if err != nil {
			t.Fatalf("first AcquireOrRenew returned error: %v", err)
		}
		if !firstLeader {
			t.Fatal("first coordinator did not become leader")
		}
		second, secondLeader, err := store.AcquireOrRenew(context.Background(), "coord-b", "leader-b", now, time.Second)
		if err != nil {
			t.Fatalf("second AcquireOrRenew returned error: %v", err)
		}
		if secondLeader {
			t.Fatal("second coordinator incorrectly became leader while first lease active")
		}
		if got, want := second.Epoch, first.Epoch; got != want {
			t.Fatalf("observed epoch = %d, want %d", got, want)
		}

		takeover, takeoverLeader, err := store.AcquireOrRenew(context.Background(), "coord-b", "leader-b", now.Add(2*time.Second), time.Second)
		if err != nil {
			t.Fatalf("takeover AcquireOrRenew returned error: %v", err)
		}
		if !takeoverLeader {
			t.Fatal("second coordinator did not become leader after expiry")
		}
		if got, want := takeover.Epoch, first.Epoch+1; got != want {
			t.Fatalf("takeover epoch = %d, want %d", got, want)
		}
	})

	t.Run("stale_leader_cannot_commit_after_takeover", func(t *testing.T) {
		store := factory(t)
		defer func() { _ = store.Close() }()

		now := time.Unix(300, 0)
		leaseA, _, err := store.AcquireOrRenew(context.Background(), "coord-a", "leader-a", now, time.Second)
		if err != nil {
			t.Fatalf("AcquireOrRenew returned error: %v", err)
		}
		version, err := store.SaveSnapshot(context.Background(), leaseA, now.Add(100*time.Millisecond), 0, coordserver.HASnapshot{
			Outbox: []coordserver.OutboxEntry{{
				ID:      "outbox-1",
				Epoch:   leaseA.Epoch,
				NodeID:  "n1",
				Slot:    1,
				Kind:    coordserver.OutboxCommandMarkReplicaLeaving,
				CommandID: "cmd-1",
			}},
		})
		if err != nil {
			t.Fatalf("initial SaveSnapshot returned error: %v", err)
		}

		leaseB, leader, err := store.AcquireOrRenew(context.Background(), "coord-b", "leader-b", now.Add(2*time.Second), time.Second)
		if err != nil {
			t.Fatalf("takeover AcquireOrRenew returned error: %v", err)
		}
		if !leader {
			t.Fatal("coord-b did not become leader")
		}

		_, err = store.SaveSnapshot(context.Background(), leaseA, now.Add(2100*time.Millisecond), version, coordserver.HASnapshot{})
		if !errors.Is(err, coordserver.ErrNotLeader) {
			t.Fatalf("stale SaveSnapshot error = %v, want ErrNotLeader", err)
		}

		current, err := store.LoadSnapshot(context.Background())
		if err != nil {
			t.Fatalf("LoadSnapshot returned error: %v", err)
		}
		if got, want := len(current.Outbox), 1; got != want {
			t.Fatalf("outbox len = %d, want %d", got, want)
		}
		current.Outbox = nil
		version, err = store.SaveSnapshot(context.Background(), leaseB, now.Add(2200*time.Millisecond), version, current)
		if err != nil {
			t.Fatalf("new leader SaveSnapshot returned error: %v", err)
		}
		if got, want := version, uint64(2); got != want {
			t.Fatalf("snapshot version = %d, want %d", got, want)
		}
		loaded, err := store.LoadSnapshot(context.Background())
		if err != nil {
			t.Fatalf("LoadSnapshot returned error: %v", err)
		}
		if got := len(loaded.Outbox); got != 0 {
			t.Fatalf("outbox len after ack = %d, want 0", got)
		}
	})

	t.Run("expired_leader_cannot_commit_without_takeover", func(t *testing.T) {
		store := factory(t)
		defer func() { _ = store.Close() }()

		now := time.Unix(400, 0)
		lease, leader, err := store.AcquireOrRenew(context.Background(), "coord-a", "leader-a", now, time.Second)
		if err != nil {
			t.Fatalf("AcquireOrRenew returned error: %v", err)
		}
		if !leader {
			t.Fatal("coord-a did not become leader")
		}

		_, err = store.SaveSnapshot(context.Background(), lease, now.Add(2*time.Second), 0, coordserver.HASnapshot{})
		if !errors.Is(err, coordserver.ErrNotLeader) {
			t.Fatalf("expired SaveSnapshot error = %v, want ErrNotLeader", err)
		}
	})

	t.Run("snapshot_conflict_rejects_stale_expected_version", func(t *testing.T) {
		store := factory(t)
		defer func() { _ = store.Close() }()

		now := time.Unix(500, 0)
		lease, leader, err := store.AcquireOrRenew(context.Background(), "coord-a", "leader-a", now, time.Second)
		if err != nil {
			t.Fatalf("AcquireOrRenew returned error: %v", err)
		}
		if !leader {
			t.Fatal("coord-a did not become leader")
		}

		version, err := store.SaveSnapshot(context.Background(), lease, now.Add(100*time.Millisecond), 0, coordserver.HASnapshot{
			Outbox: []coordserver.OutboxEntry{{
				ID:        "outbox-1",
				Epoch:     lease.Epoch,
				NodeID:    "n1",
				Slot:      1,
				CommandID: "cmd-1",
				Kind:      coordserver.OutboxCommandAddReplicaAsTail,
			}},
		})
		if err != nil {
			t.Fatalf("initial SaveSnapshot returned error: %v", err)
		}
		if got, want := version, uint64(1); got != want {
			t.Fatalf("snapshot version = %d, want %d", got, want)
		}

		_, err = store.SaveSnapshot(context.Background(), lease, now.Add(200*time.Millisecond), 0, coordserver.HASnapshot{})
		if !errors.Is(err, coordserver.ErrHASnapshotConflict) {
			t.Fatalf("stale expected version SaveSnapshot error = %v, want ErrHASnapshotConflict", err)
		}
	})
}

func coordserverCloneSnapshot(snapshot coordserver.HASnapshot) coordserver.HASnapshot {
	cloned := snapshot
	if cloned.State.Cluster.Chains == nil {
		cloned.State.Cluster.Chains = []coordinator.Chain{}
	}
	if cloned.State.Cluster.NodesByID == nil {
		cloned.State.Cluster.NodesByID = map[string]coordinator.Node{}
	}
	if cloned.State.Cluster.NodeHealthByID == nil {
		cloned.State.Cluster.NodeHealthByID = map[string]coordinator.NodeHealth{}
	}
	if cloned.State.Cluster.ReadyNodeIDs == nil {
		cloned.State.Cluster.ReadyNodeIDs = map[string]bool{}
	}
	if cloned.State.Cluster.DrainingNodeIDs == nil {
		cloned.State.Cluster.DrainingNodeIDs = map[string]bool{}
	}
	if cloned.State.SlotVersions == nil {
		cloned.State.SlotVersions = map[int]uint64{}
	}
	if cloned.State.CompletedProgressBySlot == nil {
		cloned.State.CompletedProgressBySlot = map[int][]coordruntime.CompletedProgressRecord{}
	}
	if cloned.State.NodeLivenessByID == nil {
		cloned.State.NodeLivenessByID = map[string]coordruntime.NodeLivenessRecord{}
	}
	if cloned.State.PendingBySlot == nil {
		cloned.State.PendingBySlot = map[int]coordruntime.PendingWork{}
	}
	if cloned.State.Outbox == nil {
		cloned.State.Outbox = []coordruntime.OutboxEntry{}
	}
	if cloned.State.AppliedCommands == nil {
		cloned.State.AppliedCommands = map[string]coordruntime.AppliedCommand{}
	}
	if cloned.Pending == nil {
		cloned.Pending = map[int]coordserver.PendingWork{}
	}
	if cloned.UnavailableReplicas == nil {
		cloned.UnavailableReplicas = map[string]map[int]bool{}
	}
	if cloned.LastRecoveryReports == nil {
		cloned.LastRecoveryReports = map[string]storage.NodeRecoveryReport{}
	}
	if cloned.Outbox == nil {
		cloned.Outbox = []coordserver.OutboxEntry{}
	}
	return cloned
}
