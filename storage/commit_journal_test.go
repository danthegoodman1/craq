package storage

import (
	"context"
	"testing"
	"time"
)

func TestCommitJournalShardFlushesCommitsFairlyAcrossSlots(t *testing.T) {
	shard, journal := newTestCommitJournalShard(t, journalConfig{
		shardCount:          1,
		batchDelayLow:       time.Hour,
		batchDelayHigh:      time.Hour,
		batchDepthThreshold: 8,
		batchMaxOps:         4,
	})
	t.Cleanup(func() { shard.close() })

	for _, commit := range []DurableCommit{
		testDurableCommit(11, 1),
		testDurableCommit(11, 2),
		testDurableCommit(22, 1),
		testDurableCommit(22, 2),
	} {
		shard.prepareCh <- &journalPrepareIntent{prepare: commit, queuedAt: time.Now()}
	}
	go shard.run()

	waitForJournalRecordCount(t, journal, 4)
	records := snapshotJournalRecords(t, journal)
	got := make([]struct {
		slot     int
		sequence uint64
	}, 0, len(records))
	for _, record := range records {
		got = append(got, struct {
			slot     int
			sequence uint64
		}{slot: record.Slot, sequence: record.Sequence})
	}
	want := []struct {
		slot     int
		sequence uint64
	}{
		{slot: 11, sequence: 1},
		{slot: 22, sequence: 1},
		{slot: 11, sequence: 2},
		{slot: 22, sequence: 2},
	}
	if len(got) != len(want) {
		t.Fatalf("len(records) = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("record[%d] = %+v, want %+v (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestCommitJournalShardCoalescesUpstreamConfirms(t *testing.T) {
	shard, journal := newTestCommitJournalShard(t, journalConfig{
		shardCount:          1,
		batchDelayLow:       time.Millisecond,
		batchDelayHigh:      time.Millisecond,
		batchDepthThreshold: 8,
		batchMaxOps:         4,
	})
	t.Cleanup(func() { shard.close() })

	for _, sequence := range []uint64{7, 8, 9} {
		shard.confirmCh <- &journalConfirmIntent{
			assignment: ReplicaAssignment{Slot: 5, ChainVersion: 3, Role: ReplicaRoleTail},
			sequence:   sequence,
		}
	}
	go shard.run()

	waitForJournalRecordCount(t, journal, 1)
	records := snapshotJournalRecords(t, journal)
	if got, want := len(records), 1; got != want {
		t.Fatalf("len(records) = %d, want %d", got, want)
	}
	record := records[0]
	if got, want := record.recordType(), journalRecordTypeUpstreamConfirm; got != want {
		t.Fatalf("record type = %q, want %q", got, want)
	}
	if got, want := record.Sequence, uint64(9); got != want {
		t.Fatalf("record sequence = %d, want %d", got, want)
	}
}

func newTestCommitJournalShard(t *testing.T, cfg journalConfig) (*commitJournalShard, *inMemoryCommitJournal) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	node := &Node{
		nodeID:            "node-a",
		runtimeCtx:        ctx,
		done:              make(chan struct{}),
		publishedReplicas: map[int]publishedReplicaSnapshot{},
	}
	journal := newInMemoryCommitJournal()
	engine := &commitJournalEngine{
		node:        node,
		highWater:   map[int]uint64{},
		prepareHigh: map[int]uint64{},
		config:      cfg,
	}
	shard := &commitJournalShard{
		engine:       engine,
		index:        0,
		journal:      journal,
		prepareCh:    make(chan *journalPrepareIntent, 32),
		watermarkCh:  make(chan *journalWatermarkIntent, 32),
		confirmCh:    make(chan *journalConfirmIntent, 32),
		closeCh:      make(chan struct{}),
		closedCh:     make(chan struct{}),
		recentBySlot: map[int][]journalRecord{},
	}
	engine.shards = []*commitJournalShard{shard}
	return shard, journal
}

func testDurableCommit(slot int, sequence uint64) DurableCommit {
	assignment := ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         ReplicaRoleHead,
	}
	metadata := ObjectMetadata{
		Version:   sequence,
		CreatedAt: time.Unix(0, 0).UTC(),
		UpdatedAt: time.Unix(0, int64(sequence)).UTC(),
	}
	return DurableCommit{
		Operation: WriteOperation{
			Slot:     slot,
			Sequence: sequence,
			Kind:     OperationKindPut,
			Key:      "key",
			Value:    "value",
			Metadata: metadata,
		},
		Persisted: persistedReplica(ensureProtocolReplicaState(replicaRecord{
			assignment:                    assignment,
			state:                         ReplicaStateActive,
			nextSequence:                  sequence + 1,
			highestCommittedSequence:      sequence,
			materializedCommittedSequence: sequence,
			localDataPresent:              true,
		})),
		UpstreamConfirmedSequence: sequence,
	}
}

func waitForJournalRecordCount(t *testing.T, journal *inMemoryCommitJournal, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := len(snapshotJournalRecords(t, journal)); got >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("journal record count did not reach %d; got %d", want, len(snapshotJournalRecords(t, journal)))
}

func snapshotJournalRecords(t *testing.T, journal *inMemoryCommitJournal) []journalRecord {
	t.Helper()
	journal.mu.Lock()
	defer journal.mu.Unlock()
	records := make([]journalRecord, len(journal.records))
	copy(records, journal.records)
	return records
}
