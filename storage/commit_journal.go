package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultJournalBatchDelayLow      = 50 * time.Microsecond
	defaultJournalBatchDelayHigh     = 250 * time.Microsecond
	defaultJournalBatchLowDepthLimit = 8
	defaultJournalBatchMaxOps        = 128
	defaultJournalBatchMaxBytes      = 1 << 20
	defaultMaterializeBatchDelay     = 250 * time.Microsecond
	defaultMaterializeRetryDelay     = 5 * time.Millisecond
	defaultJournalShardCount         = 16
)

type CommitJournalStore interface {
	AppendBatch(records []journalRecord) error
	Replay(apply func(journalRecord) error) error
	Close() error
}

type commitJournalStore = CommitJournalStore

type commitJournalProvider interface {
	CommitJournal(nodeID string) (commitJournalStore, error)
}

type commitJournalShardProvider interface {
	CommitJournalShard(nodeID string, shard int) (commitJournalStore, error)
}

type journalRecordType string

const (
	journalRecordTypeCommit          journalRecordType = "commit"
	journalRecordTypeUpstreamConfirm journalRecordType = "upstream_confirm"
)

type journalRecord struct {
	Type                      journalRecordType `json:"type,omitempty"`
	Slot                      int               `json:"slot"`
	ChainVersion              uint64            `json:"chain_version"`
	Sequence                  uint64            `json:"sequence"`
	Kind                      OperationKind     `json:"kind,omitempty"`
	Key                       string            `json:"key,omitempty"`
	Value                     string            `json:"value,omitempty"`
	Metadata                  ObjectMetadata    `json:"metadata,omitempty"`
	UpstreamConfirmedSequence uint64            `json:"upstream_confirmed_sequence,omitempty"`
}

func (r journalRecord) recordType() journalRecordType {
	if r.Type == "" {
		return journalRecordTypeCommit
	}
	return r.Type
}

func journalRecordFromCommit(commit DurableCommit) journalRecord {
	confirmed := commit.UpstreamConfirmedSequence
	if confirmed > commit.Operation.Sequence {
		confirmed = commit.Operation.Sequence
	}
	return journalRecord{
		Type:                      journalRecordTypeCommit,
		Slot:                      commit.Operation.Slot,
		ChainVersion:              commit.Persisted.Assignment.ChainVersion,
		Sequence:                  commit.Operation.Sequence,
		Kind:                      commit.Operation.Kind,
		Key:                       commit.Operation.Key,
		Value:                     commit.Operation.Value,
		Metadata:                  cloneObjectMetadata(commit.Operation.Metadata),
		UpstreamConfirmedSequence: confirmed,
	}
}

func journalRecordFromUpstreamConfirm(assignment ReplicaAssignment, sequence uint64) journalRecord {
	return journalRecord{
		Type:         journalRecordTypeUpstreamConfirm,
		Slot:         assignment.Slot,
		ChainVersion: assignment.ChainVersion,
		Sequence:     sequence,
	}
}

func (r journalRecord) operation() WriteOperation {
	return WriteOperation{
		Slot:     r.Slot,
		Sequence: r.Sequence,
		Kind:     r.Kind,
		Key:      r.Key,
		Value:    r.Value,
		Metadata: cloneObjectMetadata(r.Metadata),
	}
}

func cloneJournalRecord(record journalRecord) journalRecord {
	return journalRecord{
		Type:                      record.recordType(),
		Slot:                      record.Slot,
		ChainVersion:              record.ChainVersion,
		Sequence:                  record.Sequence,
		Kind:                      record.Kind,
		Key:                       record.Key,
		Value:                     record.Value,
		Metadata:                  cloneObjectMetadata(record.Metadata),
		UpstreamConfirmedSequence: record.UpstreamConfirmedSequence,
	}
}

type fileCommitJournal struct {
	path string
	file *os.File
	mu   sync.Mutex
}

func openFileCommitJournal(path string) (*fileCommitJournal, error) {
	if path == "" {
		return nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir commit journal dir: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open commit journal: %w", err)
	}
	return &fileCommitJournal{path: path, file: file}, nil
}

func OpenFileCommitJournalForLocalState(path string) (CommitJournalStore, error) {
	return openFileCommitJournal(path)
}

func (j *fileCommitJournal) AppendBatch(records []journalRecord) error {
	if j == nil || j.file == nil || len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	for _, record := range records {
		payload, err := json.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal journal record: %w", err)
		}
		if len(payload) > int(^uint32(0)) {
			return fmt.Errorf("%w: journal record too large", ErrInvalidConfig)
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		buf.Write(lenBuf[:])
		buf.Write(payload)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.file.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("write commit journal: %w", err)
	}
	if err := j.file.Sync(); err != nil {
		return fmt.Errorf("sync commit journal: %w", err)
	}
	return nil
}

func (j *fileCommitJournal) Replay(apply func(journalRecord) error) error {
	if j == nil || j.file == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek commit journal start: %w", err)
	}
	reader := io.NewSectionReader(j.file, 0, 1<<63-1)
	var (
		offset int64
		lenBuf [4]byte
	)
	for {
		if _, err := io.ReadFull(reader, lenBuf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				if truncateErr := j.file.Truncate(offset); truncateErr != nil {
					return fmt.Errorf("truncate partial commit journal length prefix: %w", truncateErr)
				}
				break
			}
			return fmt.Errorf("read commit journal length prefix: %w", err)
		}
		offset += int64(len(lenBuf))
		length := binary.LittleEndian.Uint32(lenBuf[:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				if truncateErr := j.file.Truncate(offset - int64(len(lenBuf))); truncateErr != nil {
					return fmt.Errorf("truncate partial commit journal payload: %w", truncateErr)
				}
				break
			}
			return fmt.Errorf("read commit journal payload: %w", err)
		}
		offset += int64(length)
		var record journalRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return fmt.Errorf("unmarshal commit journal record: %w", err)
		}
		if err := apply(record); err != nil {
			return err
		}
	}
	if _, err := j.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek commit journal end: %w", err)
	}
	return nil
}

func (j *fileCommitJournal) Close() error {
	if j == nil || j.file == nil {
		return nil
	}
	return j.file.Close()
}

type inMemoryCommitJournal struct {
	mu      sync.Mutex
	records []journalRecord
}

func newInMemoryCommitJournal() *inMemoryCommitJournal {
	return &inMemoryCommitJournal{}
}

func (j *inMemoryCommitJournal) AppendBatch(records []journalRecord) error {
	if j == nil || len(records) == 0 {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, record := range records {
		j.records = append(j.records, cloneJournalRecord(record))
	}
	return nil
}

func (j *inMemoryCommitJournal) Replay(apply func(journalRecord) error) error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	records := make([]journalRecord, len(j.records))
	copy(records, j.records)
	j.mu.Unlock()
	for _, record := range records {
		if err := apply(record); err != nil {
			return err
		}
	}
	return nil
}

func (j *inMemoryCommitJournal) Close() error {
	return nil
}

type journalCompletionHandler func(*slotRuntime, error, time.Time)

type journalCommitIntent struct {
	ctx        context.Context
	commit     DurableCommit
	owner      *slotOwner
	onComplete journalCompletionHandler
	queuedAt   time.Time
}

type journalConfirmIntent struct {
	ctx        context.Context
	assignment ReplicaAssignment
	sequence   uint64
	owner      *slotOwner
	onComplete journalCompletionHandler
}

type journalBatchItem struct {
	commit  *journalCommitIntent
	confirm *journalConfirmIntent
}

type commitJournalEngine struct {
	node *Node

	highWaterMu sync.RWMutex
	highWater   map[int]uint64
	shards      []*commitJournalShard
}

type commitJournalShard struct {
	engine       *commitJournalEngine
	index        int
	journal      commitJournalStore
	materializer *commitMaterializer
	commitCh     chan *journalCommitIntent
	confirmCh    chan *journalConfirmIntent
	closeCh      chan struct{}
	closedCh     chan struct{}

	diagMu       sync.Mutex
	lastFlushAt  time.Time
	recentBySlot map[int][]journalRecord
}

func newCommitJournalEngine(node *Node, journals []commitJournalStore, highWater map[int]uint64, backlog []DurableCommit) *commitJournalEngine {
	engine := &commitJournalEngine{
		node:      node,
		highWater: make(map[int]uint64, len(highWater)),
		shards:    make([]*commitJournalShard, 0, len(journals)),
	}
	for slot, sequence := range highWater {
		engine.highWater[slot] = sequence
	}
	backlogByShard := map[int][]DurableCommit{}
	for _, commit := range backlog {
		shard := engine.shardIndex(commit.Operation.Slot)
		backlogByShard[shard] = append(backlogByShard[shard], cloneDurableCommit(commit))
	}
	for i, journal := range journals {
		shard := &commitJournalShard{
			engine:       engine,
			index:        i,
			journal:      journal,
			commitCh:     make(chan *journalCommitIntent, 4096),
			confirmCh:    make(chan *journalConfirmIntent, 4096),
			closeCh:      make(chan struct{}),
			closedCh:     make(chan struct{}),
			recentBySlot: map[int][]journalRecord{},
		}
		shard.materializer = newCommitMaterializer(node, backlogByShard[i])
		engine.shards = append(engine.shards, shard)
		go shard.run()
	}
	return engine
}

func (e *commitJournalEngine) close() {
	if e == nil {
		return
	}
	for _, shard := range e.shards {
		shard.close()
	}
}

func (e *commitJournalEngine) committedSequence(slot int) uint64 {
	if e == nil {
		return 0
	}
	e.highWaterMu.RLock()
	defer e.highWaterMu.RUnlock()
	return e.highWater[slot]
}

func (e *commitJournalEngine) markCommitted(slot int, sequence uint64) {
	e.highWaterMu.Lock()
	defer e.highWaterMu.Unlock()
	if sequence > e.highWater[slot] {
		e.highWater[slot] = sequence
	}
}

func (e *commitJournalEngine) shardIndex(slot int) int {
	count := len(e.shards)
	if count == 0 {
		return 0
	}
	if slot < 0 {
		slot = -slot
	}
	return slot % count
}

func (e *commitJournalEngine) shardForSlot(slot int) *commitJournalShard {
	if e == nil || len(e.shards) == 0 {
		return nil
	}
	return e.shards[e.shardIndex(slot)]
}

func (e *commitJournalEngine) submitCommit(
	ctx context.Context,
	owner *slotOwner,
	commit DurableCommit,
	onComplete journalCompletionHandler,
) error {
	shard := e.shardForSlot(commit.Operation.Slot)
	if shard == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	intent := &journalCommitIntent{
		ctx:        ctx,
		commit:     cloneDurableCommit(commit),
		owner:      owner,
		onComplete: onComplete,
		queuedAt:   time.Now(),
	}
	return shard.enqueueCommit(intent)
}

func (e *commitJournalEngine) submitUpstreamConfirm(
	ctx context.Context,
	owner *slotOwner,
	assignment ReplicaAssignment,
	sequence uint64,
	onComplete journalCompletionHandler,
) error {
	shard := e.shardForSlot(assignment.Slot)
	if shard == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	intent := &journalConfirmIntent{
		ctx:        ctx,
		assignment: cloneAssignment(assignment),
		sequence:   sequence,
		owner:      owner,
		onComplete: onComplete,
	}
	return shard.enqueueConfirm(intent)
}

func (s *commitJournalShard) close() {
	select {
	case <-s.closeCh:
	default:
		close(s.closeCh)
	}
	<-s.closedCh
	if s.materializer != nil {
		s.materializer.close()
	}
	if s.journal != nil {
		_ = s.journal.Close()
	}
}

func (s *commitJournalShard) enqueueCommit(intent *journalCommitIntent) error {
	select {
	case <-s.closeCh:
		return context.Canceled
	default:
	}
	select {
	case <-s.closeCh:
		return context.Canceled
	case s.commitCh <- intent:
		s.engine.node.recordTimeoutJournalQueueDepth(len(s.commitCh) + len(s.confirmCh))
		return nil
	}
}

func (s *commitJournalShard) enqueueConfirm(intent *journalConfirmIntent) error {
	select {
	case <-s.closeCh:
		return context.Canceled
	default:
	}
	select {
	case <-s.closeCh:
		return context.Canceled
	case s.confirmCh <- intent:
		s.engine.node.recordTimeoutJournalQueueDepth(len(s.commitCh) + len(s.confirmCh))
		return nil
	}
}

func (s *commitJournalShard) run() {
	defer close(s.closedCh)

	var (
		batch     []journalBatchItem
		batchSize int
		timer     *time.Timer
		timerCh   <-chan time.Time
	)

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerCh = nil
	}

	nextBatchDelay := func(queueDepth int) time.Duration {
		if queueDepth < defaultJournalBatchLowDepthLimit {
			return defaultJournalBatchDelayLow
		}
		return defaultJournalBatchDelayHigh
	}

	resetTimer := func(queueDepth int) {
		delay := nextBatchDelay(queueDepth)
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		timerCh = timer.C
	}

	deliver := func(owner *slotOwner, handler journalCompletionHandler, err error, completedAt time.Time) {
		if owner == nil || handler == nil {
			return
		}
		_ = owner.enqueueCompletion(func(runtime *slotRuntime) {
			handler(runtime, err, completedAt)
		})
	}

	flush := func() {
		if len(batch) == 0 {
			stopTimer()
			return
		}
		items := batch
		batch = nil
		batchSize = 0
		stopTimer()
		s.engine.node.recordTimeoutJournalQueueDepth(len(s.commitCh) + len(s.confirmCh))

		records := make([]journalRecord, 0, len(items))
		commits := make([]*journalCommitIntent, 0, len(items))
		commitBytes := 0
		for _, item := range items {
			switch {
			case item.commit != nil:
				intent := item.commit
				if intent.ctx != nil {
					if err := intent.ctx.Err(); err != nil {
						deliver(intent.owner, intent.onComplete, err, time.Now())
						continue
					}
				}
				s.engine.node.observeWriteStage(writeStageCommitBatchWait, intent.commit.Persisted.Assignment.Role, writeStageResultSuccess, time.Since(intent.queuedAt))
				records = append(records, journalRecordFromCommit(intent.commit))
				commits = append(commits, intent)
				commitBytes += estimatedCommitBytes(intent.commit)
			case item.confirm != nil:
				intent := item.confirm
				if intent.ctx != nil {
					if err := intent.ctx.Err(); err != nil {
						deliver(intent.owner, intent.onComplete, err, time.Now())
						continue
					}
				}
				records = append(records, journalRecordFromUpstreamConfirm(intent.assignment, intent.sequence))
			}
		}
		if len(records) == 0 {
			return
		}
		if len(commits) > 0 {
			s.engine.node.observeCommitBatchSize(len(commits), commitBytes)
			for _, intent := range commits {
				if stage := writeTraceFlushStartStage(intent.commit.Persisted.Assignment.Role); stage != "" {
					s.engine.node.traceWriteEvent(intent.commit.Persisted.Assignment, intent.commit.Operation.Sequence, stage)
				}
			}
		}
		flushStarted := time.Now()
		appendErr := s.journal.AppendBatch(records)
		flushDuration := time.Since(flushStarted)
		completedAt := time.Now()
		if appendErr != nil {
			for _, item := range items {
				switch {
				case item.commit != nil:
					intent := item.commit
					s.engine.node.observeCommitFlush(intent.commit.Persisted.Assignment.Role, writeStageResult(appendErr), flushDuration)
					deliver(intent.owner, intent.onComplete, appendErr, completedAt)
				case item.confirm != nil:
					intent := item.confirm
					deliver(intent.owner, intent.onComplete, appendErr, completedAt)
				}
			}
			return
		}
		s.recordDurableRecords(records, completedAt)
		committed := make([]DurableCommit, 0, len(commits))
		for _, item := range items {
			switch {
			case item.commit != nil:
				intent := item.commit
				s.engine.markCommitted(intent.commit.Operation.Slot, intent.commit.Operation.Sequence)
				s.engine.node.observeCommitFlush(intent.commit.Persisted.Assignment.Role, writeStageResultSuccess, flushDuration)
				if stage := writeTraceFlushEndStage(intent.commit.Persisted.Assignment.Role); stage != "" {
					s.engine.node.traceWriteEvent(intent.commit.Persisted.Assignment, intent.commit.Operation.Sequence, stage)
				}
				committed = append(committed, cloneDurableCommit(intent.commit))
				deliver(intent.owner, intent.onComplete, nil, completedAt)
			case item.confirm != nil:
				intent := item.confirm
				deliver(intent.owner, intent.onComplete, nil, completedAt)
			}
		}
		if s.materializer != nil && len(committed) > 0 {
			s.materializer.enqueue(committed)
		}
	}

	for {
		select {
		case <-s.closeCh:
			for _, item := range batch {
				switch {
				case item.commit != nil:
					deliver(item.commit.owner, item.commit.onComplete, context.Canceled, time.Now())
				case item.confirm != nil:
					deliver(item.confirm.owner, item.confirm.onComplete, context.Canceled, time.Now())
				}
			}
			return
		case intent := <-s.commitCh:
			batch = append(batch, journalBatchItem{commit: intent})
			batchSize += estimatedCommitBytes(intent.commit)
			if len(batch) == 1 {
				resetTimer(len(s.commitCh) + len(s.confirmCh))
			}
			if len(batch) > 1 && len(s.commitCh)+len(s.confirmCh) == 0 {
				flush()
				continue
			}
			if len(batch) >= defaultJournalBatchMaxOps || batchSize >= defaultJournalBatchMaxBytes {
				flush()
			}
		case intent := <-s.confirmCh:
			batch = append(batch, journalBatchItem{confirm: intent})
			batchSize += 64
			if len(batch) == 1 {
				resetTimer(len(s.commitCh) + len(s.confirmCh))
			}
			if len(batch) > 1 && len(s.commitCh)+len(s.confirmCh) == 0 {
				flush()
				continue
			}
			if len(batch) >= defaultJournalBatchMaxOps || batchSize >= defaultJournalBatchMaxBytes {
				flush()
			}
		case <-timerCh:
			flush()
		}
	}
}

func (s *commitJournalShard) recordDurableRecords(records []journalRecord, flushedAt time.Time) {
	if s == nil {
		return
	}
	s.diagMu.Lock()
	defer s.diagMu.Unlock()
	s.lastFlushAt = flushedAt.UTC()
	for _, record := range records {
		cloned := cloneJournalRecord(record)
		slotRecords := append(s.recentBySlot[record.Slot], cloned)
		if len(slotRecords) > 32 {
			copy(slotRecords, slotRecords[len(slotRecords)-32:])
			slotRecords = slotRecords[:32]
		}
		s.recentBySlot[record.Slot] = slotRecords
	}
}

func (s *commitJournalShard) snapshot(slot int) journalSlotSnapshot {
	if s == nil {
		return journalSlotSnapshot{}
	}
	out := journalSlotSnapshot{
		Shard:      s.index,
		QueueDepth: len(s.commitCh) + len(s.confirmCh),
	}
	s.diagMu.Lock()
	out.LastFlushAt = s.lastFlushAt
	if records := s.recentBySlot[slot]; len(records) > 0 {
		out.RecentRecords = make([]journalRecordSnapshot, 0, len(records))
		for _, record := range records {
			out.RecentRecords = append(out.RecentRecords, journalRecordSnapshot{
				Type:                      string(record.recordType()),
				ChainVersion:              record.ChainVersion,
				Sequence:                  record.Sequence,
				Kind:                      record.Kind,
				Key:                       record.Key,
				UpstreamConfirmedSequence: record.UpstreamConfirmedSequence,
			})
		}
	}
	s.diagMu.Unlock()
	out.DurableCommittedHighWater = s.engine.committedSequence(slot)
	return out
}

func (e *commitJournalEngine) snapshot(slot int) *journalSlotSnapshot {
	if e == nil {
		return nil
	}
	shard := e.shardForSlot(slot)
	if shard == nil {
		return nil
	}
	snapshot := shard.snapshot(slot)
	return &snapshot
}

type commitMaterializer struct {
	node     *Node
	submitCh chan []DurableCommit
	closeCh  chan struct{}
	closedCh chan struct{}
}

func newCommitMaterializer(node *Node, backlog []DurableCommit) *commitMaterializer {
	m := &commitMaterializer{
		node:     node,
		submitCh: make(chan []DurableCommit, 1024),
		closeCh:  make(chan struct{}),
		closedCh: make(chan struct{}),
	}
	go m.run(backlog)
	return m
}

func (m *commitMaterializer) close() {
	if m == nil {
		return
	}
	select {
	case <-m.closeCh:
	default:
		close(m.closeCh)
	}
	<-m.closedCh
}

func (m *commitMaterializer) enqueue(commits []DurableCommit) {
	if m == nil || len(commits) == 0 {
		return
	}
	cloned := make([]DurableCommit, 0, len(commits))
	for _, commit := range commits {
		cloned = append(cloned, cloneDurableCommit(commit))
	}
	select {
	case <-m.closeCh:
	case m.submitCh <- cloned:
		m.node.recordTimeoutMaterializerQueueDepth(len(m.submitCh))
	}
}

func (m *commitMaterializer) run(backlog []DurableCommit) {
	defer close(m.closedCh)
	pending := make([]DurableCommit, 0, len(backlog))
	pending = append(pending, cloneDurableCommits(backlog)...)
	var (
		timer   *time.Timer
		timerCh <-chan time.Time
	)

	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerCh = nil
	}

	resetTimer := func() {
		if timer == nil {
			timer = time.NewTimer(defaultMaterializeBatchDelay)
		} else {
			timer.Reset(defaultMaterializeBatchDelay)
		}
		timerCh = timer.C
	}

	flush := func() bool {
		if len(pending) == 0 {
			stopTimer()
			return true
		}
		stopTimer()
		filtered := make([]DurableCommit, 0, len(pending))
		appliedHigh := map[int]uint64{}
		for _, commit := range pending {
			if !m.node.shouldMaterializeCommit(commit) {
				continue
			}
			filtered = append(filtered, commit)
		}
		if len(filtered) == 0 {
			pending = pending[:0]
			m.node.recordTimeoutMaterializerQueueDepth(len(m.submitCh))
			return true
		}
		err := applyMaterializedBatch(context.Background(), m.node.backend, m.node.nodeID, filtered)
		if err != nil {
			time.Sleep(defaultMaterializeRetryDelay)
			return false
		}
		for _, commit := range filtered {
			if commit.Operation.Sequence > appliedHigh[commit.Operation.Slot] {
				appliedHigh[commit.Operation.Slot] = commit.Operation.Sequence
			}
		}
		for slot, sequence := range appliedHigh {
			m.node.notifyMaterialized(slot, sequence)
		}
		pending = pending[:0]
		m.node.recordTimeoutMaterializerQueueDepth(len(m.submitCh))
		return true
	}

	if len(pending) > 0 {
		resetTimer()
	}

	for {
		select {
		case <-m.closeCh:
			return
		case commits := <-m.submitCh:
			pending = append(pending, commits...)
			m.node.recordTimeoutMaterializerQueueDepth(len(pending))
			if len(pending) == len(commits) {
				resetTimer()
			}
			if len(pending) >= defaultJournalBatchMaxOps {
				_ = flush()
			}
		case <-timerCh:
			if !flush() && len(pending) > 0 {
				resetTimer()
			}
		}
	}
}

func applyMaterializedBatch(ctx context.Context, backend Backend, nodeID string, commits []DurableCommit) error {
	if len(commits) == 0 {
		return nil
	}
	filtered, err := filterMaterializedCommits(backend, commits)
	if err != nil {
		return err
	}
	if len(filtered) == 0 {
		return nil
	}
	if batchBackend, ok := backend.(batchCommitBackend); ok {
		return batchBackend.ApplyCommittedBatch(ctx, nodeID, filtered)
	}
	for _, commit := range filtered {
		if err := backend.ApplyCommitted(ctx, nodeID, commit.Operation, &commit.Persisted); err != nil {
			return err
		}
	}
	return nil
}

func filterMaterializedCommits(backend Backend, commits []DurableCommit) ([]DurableCommit, error) {
	expected := make(map[int]uint64)
	filtered := make([]DurableCommit, 0, len(commits))
	for _, commit := range commits {
		highestCommitted, ok := expected[commit.Operation.Slot]
		if !ok {
			current, err := backend.HighestCommittedSequence(commit.Operation.Slot)
			if err != nil {
				return nil, err
			}
			highestCommitted = current
		}
		switch {
		case commit.Operation.Sequence <= highestCommitted:
			continue
		case commit.Operation.Sequence != highestCommitted+1:
			return nil, fmt.Errorf(
				"%w: slot %d expected commit sequence %d, got %d",
				ErrSequenceMismatch,
				commit.Operation.Slot,
				highestCommitted+1,
				commit.Operation.Sequence,
			)
		default:
			filtered = append(filtered, commit)
			expected[commit.Operation.Slot] = commit.Operation.Sequence
		}
	}
	return filtered, nil
}

func cloneDurableCommit(commit DurableCommit) DurableCommit {
	return DurableCommit{
		Operation:                 cloneWriteOperation(commit.Operation),
		Persisted:                 clonePersistedReplica(commit.Persisted),
		UpstreamConfirmedSequence: commit.UpstreamConfirmedSequence,
	}
}

func cloneDurableCommits(commits []DurableCommit) []DurableCommit {
	cloned := make([]DurableCommit, 0, len(commits))
	for _, commit := range commits {
		cloned = append(cloned, cloneDurableCommit(commit))
	}
	return cloned
}

func openNodeCommitJournals(local LocalStateStore, nodeID string) ([]commitJournalStore, error) {
	if provider, ok := local.(commitJournalShardProvider); ok {
		journals := make([]commitJournalStore, 0, defaultJournalShardCount)
		for shard := 0; shard < defaultJournalShardCount; shard++ {
			journal, err := provider.CommitJournalShard(nodeID, shard)
			if err != nil {
				for _, existing := range journals {
					if existing != nil {
						_ = existing.Close()
					}
				}
				return nil, err
			}
			journals = append(journals, journal)
		}
		return journals, nil
	}
	if provider, ok := local.(commitJournalProvider); ok {
		journal, err := provider.CommitJournal(nodeID)
		if err != nil {
			return nil, err
		}
		return []commitJournalStore{journal}, nil
	}
	return []commitJournalStore{newInMemoryCommitJournal()}, nil
}

func recoverJournaledReplicaState(journals []commitJournalStore, records map[int]replicaRecord) ([]DurableCommit, map[int]uint64, error) {
	highWater := make(map[int]uint64, len(records))
	for slot, record := range records {
		record = ensureProtocolReplicaState(record)
		records[slot] = record
		highWater[slot] = record.highestCommittedSequence
	}
	if len(journals) == 0 {
		return nil, highWater, nil
	}
	backlog := make([]DurableCommit, 0)
	for _, journal := range journals {
		if journal == nil {
			continue
		}
		if err := journal.Replay(func(entry journalRecord) error {
			record, ok := records[entry.Slot]
			if !ok {
				return nil
			}
			record = ensureProtocolReplicaState(record)
			if entry.ChainVersion != record.assignment.ChainVersion {
				return nil
			}
			switch entry.recordType() {
			case journalRecordTypeUpstreamConfirm:
				if entry.Sequence > record.highestUpstreamConfirmedSequence {
					record.highestUpstreamConfirmedSequence = entry.Sequence
				}
			case journalRecordTypeCommit:
				if entry.Sequence > record.highestCommittedSequence {
					record.highestCommittedSequence = entry.Sequence
					record.localDataPresent = true
					if nextSequence := entry.Sequence + 1; record.nextSequence < nextSequence {
						record.nextSequence = nextSequence
					}
				}
				if entry.UpstreamConfirmedSequence > record.highestUpstreamConfirmedSequence {
					record.highestUpstreamConfirmedSequence = entry.UpstreamConfirmedSequence
				}
				if entry.Sequence > record.materializedCommittedSequence {
					record = recordWithCommittedOverlay(record, entry.operation())
					backlog = append(backlog, DurableCommit{
						Operation:                 entry.operation(),
						Persisted:                 persistedReplica(record),
						UpstreamConfirmedSequence: entry.UpstreamConfirmedSequence,
					})
				}
			}
			record.highestUpstreamConfirmedSequence = normalizeUpstreamConfirmedSequence(record)
			records[entry.Slot] = record
			if record.highestCommittedSequence > highWater[entry.Slot] {
				highWater[entry.Slot] = record.highestCommittedSequence
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}
	return backlog, highWater, nil
}
