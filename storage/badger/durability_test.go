package badger

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/danthegoodman1/craq/storage"
)

// Values above Badger's value-log threshold exercise the value-log code path
// rather than inline LSM storage. This constant produces values just over 1MB.
const largeValueSize = 1<<20 + 512

func largeValue(seed byte) string {
	unit := string([]byte{seed, seed + 1, seed + 2, seed + 3})
	return strings.Repeat(unit, largeValueSize/4)
}

func TestLargeValueReopenAndRecovery(t *testing.T) {
	// Verifies that values exceeding Badger's value-log threshold survive
	// a clean close/reopen cycle through both the commit and snapshot paths.

	t.Run("committed large value survives reopen", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "node.db")
		store := mustOpenStore(t, path)
		backend := store.Backend()

		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		bigVal := largeValue('A')
		mustCommitValue(t, backend, 1, 1, "big-key", bigVal)
		if err := store.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}

		reopened := mustOpenStore(t, path)
		defer func() { _ = reopened.Close() }()
		got, found, err := reopened.Backend().GetCommitted(1, "big-key")
		if err != nil {
			t.Fatalf("GetCommitted after reopen returned error: %v", err)
		}
		if !found {
			t.Fatal("GetCommitted after reopen: key not found")
		}
		if got.Value != bigVal {
			t.Fatalf("GetCommitted value length = %d, want %d", len(got.Value), len(bigVal))
		}
	})

	t.Run("install snapshot with large value survives reopen", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "node.db")
		store := mustOpenStore(t, path)
		backend := store.Backend()

		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		bigVal := largeValue('B')
		snap := storage.Snapshot{
			"snap-key": storage.CommittedObject{
				Value:    bigVal,
				Metadata: testMetadata(1),
			},
		}
		if err := backend.InstallSnapshot(1, snap); err != nil {
			t.Fatalf("InstallSnapshot returned error: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}

		reopened := mustOpenStore(t, path)
		defer func() { _ = reopened.Close() }()
		got, found, err := reopened.Backend().GetCommitted(1, "snap-key")
		if err != nil {
			t.Fatalf("GetCommitted after reopen returned error: %v", err)
		}
		if !found {
			t.Fatal("GetCommitted after reopen: key not found")
		}
		if got.Value != bigVal {
			t.Fatalf("GetCommitted value length = %d, want %d", len(got.Value), len(bigVal))
		}
	})
}

func TestLargeValueCrashBoundary(t *testing.T) {
	// Verifies that large values written through the value-log path are durable
	// even with an immediate close (no additional flushes or cleanup between the
	// write and shutdown). SyncWrites is enabled, so committed data must survive.

	t.Run("ApplyCommitted with large value survives abrupt close", func(t *testing.T) {
		ctx := context.Background()
		path := filepath.Join(t.TempDir(), "node.db")
		store := mustOpenStore(t, path)
		backend := store.Backend()

		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		bigVal := largeValue('C')
		if err := backend.ApplyCommitted(ctx, "node-a", storage.WriteOperation{
			Slot:     1,
			Sequence: 1,
			Kind:     storage.OperationKindPut,
			Key:      "big-apply",
			Value:    bigVal,
			Metadata: testMetadata(1),
		}, nil); err != nil {
			t.Fatalf("ApplyCommitted returned error: %v", err)
		}

		if err := store.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}

		reopened := mustOpenStore(t, path)
		defer func() { _ = reopened.Close() }()
		got, found, err := reopened.Backend().GetCommitted(1, "big-apply")
		if err != nil {
			t.Fatalf("GetCommitted after reopen returned error: %v", err)
		}
		if !found {
			t.Fatal("GetCommitted after reopen: key not found")
		}
		if got.Value != bigVal {
			t.Fatalf("GetCommitted value length = %d, want %d", len(got.Value), len(bigVal))
		}
		assertHighestCommitted(t, reopened.Backend(), 1, 1)
	})

	t.Run("InstallSnapshot with large value survives abrupt close", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "node.db")
		store := mustOpenStore(t, path)
		backend := store.Backend()

		if err := backend.CreateReplica(1); err != nil {
			t.Fatalf("CreateReplica returned error: %v", err)
		}
		bigVal := largeValue('D')
		if err := backend.InstallSnapshot(1, storage.Snapshot{
			"snap-crash": storage.CommittedObject{
				Value:    bigVal,
				Metadata: testMetadata(1),
			},
		}); err != nil {
			t.Fatalf("InstallSnapshot returned error: %v", err)
		}

		if err := store.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}

		reopened := mustOpenStore(t, path)
		defer func() { _ = reopened.Close() }()
		got, found, err := reopened.Backend().GetCommitted(1, "snap-crash")
		if err != nil {
			t.Fatalf("GetCommitted after reopen returned error: %v", err)
		}
		if !found {
			t.Fatal("GetCommitted after reopen: key not found")
		}
		if got.Value != bigVal {
			t.Fatalf("GetCommitted value length = %d, want %d", len(got.Value), len(bigVal))
		}
	})
}

func TestValueLogFileRotationReopen(t *testing.T) {
	// Writes a large number of unique keys with moderate-size values to stress
	// Badger's value-log and SST file creation paths, then verifies every key
	// survives close/reopen.
	path := filepath.Join(t.TempDir(), "node.db")
	store := mustOpenStore(t, path)
	backend := store.Backend()

	if err := backend.CreateReplica(1); err != nil {
		t.Fatalf("CreateReplica returned error: %v", err)
	}

	const numKeys = 200
	const valueSize = 100 * 1024 // 100KB per value, ~20MB total
	expected := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%04d", i)
		value := strings.Repeat(fmt.Sprintf("%04d", i), valueSize/4)
		expected[key] = value
		mustCommitValue(t, backend, 1, uint64(i+1), key, value)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}

	reopened := mustOpenStore(t, path)
	defer func() { _ = reopened.Close() }()
	reopenedBackend := reopened.Backend()

	assertHighestCommitted(t, reopenedBackend, 1, uint64(numKeys))

	snapshot, err := reopenedBackend.CommittedSnapshot(1)
	if err != nil {
		t.Fatalf("CommittedSnapshot returned error: %v", err)
	}
	if got, want := len(snapshot), numKeys; got != want {
		t.Fatalf("snapshot key count = %d, want %d", got, want)
	}
	for key, wantVal := range expected {
		obj, ok := snapshot[key]
		if !ok {
			t.Fatalf("key %q missing from snapshot after reopen", key)
		}
		if obj.Value != wantVal {
			t.Fatalf("key %q: value length = %d, want %d", key, len(obj.Value), len(wantVal))
		}
	}
}

func TestRandomizedBadgerInMemoryParity(t *testing.T) {
	// Drives the Badger and in-memory backends through an identical randomized
	// operation stream (with periodic close+reopen of the Badger store) and
	// asserts that every observable result matches after each operation.
	const (
		numOps = 300
		seed   = 42
	)
	rng := rand.New(rand.NewSource(seed))
	ctx := context.Background()

	path := filepath.Join(t.TempDir(), "node.db")
	bStore := mustOpenStore(t, path)
	defer func() { _ = bStore.Close() }()
	bBackend := bStore.Backend()
	mBackend := storage.NewInMemoryBackend()

	type slotState struct {
		highestCommitted uint64
		keys             map[string]string
	}
	slots := map[int]*slotState{}
	sortedActiveSlots := func() []int {
		out := make([]int, 0, len(slots))
		for s := range slots {
			out = append(out, s)
		}
		sort.Ints(out)
		return out
	}
	nextSlot := 1

	for i := 0; i < numOps; i++ {
		active := sortedActiveSlots()
		op := rng.Intn(10)

		switch {
		case len(active) == 0:
			slot := nextSlot
			nextSlot++
			err1 := bBackend.CreateReplica(slot)
			err2 := mBackend.CreateReplica(slot)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d CreateReplica(%d): badger err=%v, mem err=%v", i, slot, err1, err2)
			}
			slots[slot] = &slotState{keys: map[string]string{}}

		case op == 0 && len(active) < 5:
			slot := nextSlot
			nextSlot++
			err1 := bBackend.CreateReplica(slot)
			err2 := mBackend.CreateReplica(slot)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d CreateReplica(%d): badger err=%v, mem err=%v", i, slot, err1, err2)
			}
			if err1 == nil {
				slots[slot] = &slotState{keys: map[string]string{}}
			}

		case op <= 2:
			// StagePut followed by CommitSequence
			slot := active[rng.Intn(len(active))]
			st := slots[slot]
			seq := st.highestCommitted + 1
			key := fmt.Sprintf("k%d", rng.Intn(20))
			val := fmt.Sprintf("v%d-%d", i, rng.Intn(1000))
			md := testMetadata(seq)

			err1 := bBackend.StagePut(slot, seq, key, val, md)
			err2 := mBackend.StagePut(slot, seq, key, val, md)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d StagePut(%d,%d): badger err=%v, mem err=%v", i, slot, seq, err1, err2)
			}
			if err1 != nil {
				continue
			}
			err1 = bBackend.CommitSequence(slot, seq)
			err2 = mBackend.CommitSequence(slot, seq)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d CommitSequence(%d,%d): badger err=%v, mem err=%v", i, slot, seq, err1, err2)
			}
			if err1 == nil {
				st.highestCommitted = seq
				st.keys[key] = val
			}

		case op == 3:
			// StageDelete followed by CommitSequence (requires existing keys)
			slot := active[rng.Intn(len(active))]
			st := slots[slot]
			if len(st.keys) == 0 {
				continue
			}
			seq := st.highestCommitted + 1
			// Sort keys for deterministic selection with the seeded RNG
			sortedKeys := make([]string, 0, len(st.keys))
			for k := range st.keys {
				sortedKeys = append(sortedKeys, k)
			}
			sort.Strings(sortedKeys)
			key := sortedKeys[rng.Intn(len(sortedKeys))]
			md := testMetadata(seq)

			err1 := bBackend.StageDelete(slot, seq, key, md)
			err2 := mBackend.StageDelete(slot, seq, key, md)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d StageDelete(%d,%d): badger err=%v, mem err=%v", i, slot, seq, err1, err2)
			}
			if err1 != nil {
				continue
			}
			err1 = bBackend.CommitSequence(slot, seq)
			err2 = mBackend.CommitSequence(slot, seq)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d CommitSequence(%d,%d) delete: badger err=%v, mem err=%v", i, slot, seq, err1, err2)
			}
			if err1 == nil {
				st.highestCommitted = seq
				delete(st.keys, key)
			}

		case op == 4:
			// ApplyCommitted (atomic put without staging)
			slot := active[rng.Intn(len(active))]
			st := slots[slot]
			seq := st.highestCommitted + 1
			key := fmt.Sprintf("k%d", rng.Intn(20))
			val := fmt.Sprintf("apply-%d-%d", i, rng.Intn(1000))
			wo := storage.WriteOperation{
				Slot: slot, Sequence: seq,
				Kind: storage.OperationKindPut,
				Key: key, Value: val,
				Metadata: testMetadata(seq),
			}
			err1 := bBackend.ApplyCommitted(ctx, "node-a", wo, nil)
			err2 := mBackend.ApplyCommitted(ctx, "node-a", wo, nil)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d ApplyCommitted(%d,%d): badger err=%v, mem err=%v", i, slot, seq, err1, err2)
			}
			if err1 == nil {
				st.highestCommitted = seq
				st.keys[key] = val
			}

		case op == 5:
			// Snapshot comparison
			slot := active[rng.Intn(len(active))]
			snap1, err1 := bBackend.CommittedSnapshot(slot)
			snap2, err2 := mBackend.CommittedSnapshot(slot)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d Snapshot(%d): badger err=%v, mem err=%v", i, slot, err1, err2)
			}
			if err1 == nil && !reflect.DeepEqual(snapshotValues(snap1), snapshotValues(snap2)) {
				t.Fatalf("op %d Snapshot(%d) mismatch:\n  badger=%v\n  mem   =%v",
					i, slot, snapshotValues(snap1), snapshotValues(snap2))
			}

		case op == 6:
			// HighestCommittedSequence comparison
			slot := active[rng.Intn(len(active))]
			seq1, err1 := bBackend.HighestCommittedSequence(slot)
			seq2, err2 := mBackend.HighestCommittedSequence(slot)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d HighestCommittedSequence(%d): badger err=%v, mem err=%v", i, slot, err1, err2)
			}
			if err1 == nil && seq1 != seq2 {
				t.Fatalf("op %d HighestCommittedSequence(%d): badger=%d, mem=%d", i, slot, seq1, seq2)
			}

		case op == 7:
			// InstallSnapshot with random entries
			slot := active[rng.Intn(len(active))]
			st := slots[slot]
			numEntries := rng.Intn(5)
			snap := make(storage.Snapshot, numEntries)
			newKeys := map[string]string{}
			for j := 0; j < numEntries; j++ {
				key := fmt.Sprintf("snap-k%d", j)
				val := fmt.Sprintf("snap-v%d-%d", i, j)
				snap[key] = storage.CommittedObject{
					Value:    val,
					Metadata: testMetadata(uint64(j + 1)),
				}
				newKeys[key] = val
			}
			err1 := bBackend.InstallSnapshot(slot, snap)
			err2 := mBackend.InstallSnapshot(slot, snap)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d InstallSnapshot(%d): badger err=%v, mem err=%v", i, slot, err1, err2)
			}
			if err1 == nil {
				st.keys = newKeys
			}

		case op == 8:
			// Close + Reopen the Badger store. The in-memory backend has no
			// reopen, but Badger clears staged ops on open. Since we always
			// commit immediately after staging, there should be no staged ops
			// to diverge on.
			if err := bStore.Close(); err != nil {
				t.Fatalf("op %d Close: %v", i, err)
			}
			var err error
			bStore, err = Open(path)
			if err != nil {
				t.Fatalf("op %d Reopen: %v", i, err)
			}
			bBackend = bStore.Backend()

		default:
			// GetCommitted comparison on a random key
			slot := active[rng.Intn(len(active))]
			key := fmt.Sprintf("k%d", rng.Intn(20))
			obj1, found1, err1 := bBackend.GetCommitted(slot, key)
			obj2, found2, err2 := mBackend.GetCommitted(slot, key)
			if (err1 == nil) != (err2 == nil) {
				t.Fatalf("op %d GetCommitted(%d,%q): badger err=%v, mem err=%v", i, slot, key, err1, err2)
			}
			if found1 != found2 {
				t.Fatalf("op %d GetCommitted(%d,%q): badger found=%t, mem found=%t", i, slot, key, found1, found2)
			}
			if found1 && obj1.Value != obj2.Value {
				t.Fatalf("op %d GetCommitted(%d,%q): badger=%q, mem=%q", i, slot, key, obj1.Value, obj2.Value)
			}
		}
	}

	// Final exhaustive comparison across all slots
	for slot := range slots {
		seq1, err := bBackend.HighestCommittedSequence(slot)
		if err != nil {
			t.Fatalf("final HighestCommittedSequence(%d) badger: %v", slot, err)
		}
		seq2, err := mBackend.HighestCommittedSequence(slot)
		if err != nil {
			t.Fatalf("final HighestCommittedSequence(%d) mem: %v", slot, err)
		}
		if seq1 != seq2 {
			t.Fatalf("final HighestCommittedSequence(%d): badger=%d, mem=%d", slot, seq1, seq2)
		}

		snap1, err := bBackend.CommittedSnapshot(slot)
		if err != nil {
			t.Fatalf("final CommittedSnapshot(%d) badger: %v", slot, err)
		}
		snap2, err := mBackend.CommittedSnapshot(slot)
		if err != nil {
			t.Fatalf("final CommittedSnapshot(%d) mem: %v", slot, err)
		}
		if !reflect.DeepEqual(snapshotValues(snap1), snapshotValues(snap2)) {
			t.Fatalf("final CommittedSnapshot(%d) mismatch:\n  badger=%v\n  mem   =%v",
				slot, snapshotValues(snap1), snapshotValues(snap2))
		}
	}
}
