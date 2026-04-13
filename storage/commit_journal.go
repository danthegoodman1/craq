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
	"sort"
	"sync"
	"time"
)

const (
	defaultJournalBatchDelayLow      = 50 * time.Microsecond
	defaultJournalBatchDelayHigh     = 250 * time.Microsecond
	defaultJournalBatchLowDepthLimit = 8
	defaultJournalBatchMaxOps        = 64
	defaultJournalBatchMaxBytes      = 1 << 20
	defaultJournalPriorityWatermarks = 8
	defaultMaterializeBatchMaxOps    = 128
	defaultMaterializeBatchDelay     = 250 * time.Microsecond
	defaultMaterializeRetryDelay     = 5 * time.Millisecond
	defaultJournalShardCount         = 16
)

type journalConfig struct {
	shardCount          int
	batchDelayLow       time.Duration
	batchDelayHigh      time.Duration
	batchDepthThreshold int
	batchMaxOps         int
	experiment          JournalExperiment
}

func resolvedJournalConfig(cfg Config) journalConfig {
	journal := journalConfig{
		shardCount:          cfg.JournalShards,
		batchDelayLow:       cfg.JournalBatchDelayLow,
		batchDelayHigh:      cfg.JournalBatchDelayHigh,
		batchDepthThreshold: cfg.JournalBatchDepthThreshold,
		batchMaxOps:         cfg.JournalBatchMaxOps,
		experiment:          NormalizeJournalExperiment(cfg.JournalExperiment),
	}
	if journal.shardCount <= 0 {
		journal.shardCount = defaultJournalShardCount
	}
	if journal.batchDelayLow <= 0 {
		journal.batchDelayLow = defaultJournalBatchDelayLow
	}
	if journal.batchDelayHigh <= 0 {
		journal.batchDelayHigh = defaultJournalBatchDelayHigh
	}
	if journal.batchDepthThreshold <= 0 {
		journal.batchDepthThreshold = defaultJournalBatchLowDepthLimit
	}
	if journal.batchMaxOps <= 0 {
		journal.batchMaxOps = defaultJournalBatchMaxOps
	}
	if journal.batchDelayLow > journal.batchDelayHigh {
		journal.batchDelayLow = journal.batchDelayHigh
	}
	if journal.experiment == "" {
		journal.experiment = defaultJournalExperiment
	}
	return journal
}

type CommitJournalStore interface {
	AppendBatch(records []journalRecord) (journalAppendReport, error)
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

type commitJournalProviderWithOptions interface {
	CommitJournalWithOptions(nodeID string, opts CommitJournalOpenOptions) (commitJournalStore, error)
}

type commitJournalShardProviderWithOptions interface {
	CommitJournalShardWithOptions(nodeID string, shard int, opts CommitJournalOpenOptions) (commitJournalStore, error)
}

type journalRecordType string

const (
	journalRecordTypePrepare         journalRecordType = "prepare"
	journalRecordTypeCommitWatermark journalRecordType = "commit_watermark"
	journalRecordTypeHeadCommitRange journalRecordType = "head_commit_range"
	journalRecordTypeUpstreamConfirm journalRecordType = "upstream_confirm"
	journalRecordTypeLegacyCommit    journalRecordType = "commit"
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
		return journalRecordTypeLegacyCommit
	}
	return r.Type
}

func journalRecordFromPrepare(prepare DurableCommit) journalRecord {
	return journalRecord{
		Type:         journalRecordTypePrepare,
		Slot:         prepare.Operation.Slot,
		ChainVersion: prepare.Persisted.Assignment.ChainVersion,
		Sequence:     prepare.Operation.Sequence,
		Kind:         prepare.Operation.Kind,
		Key:          prepare.Operation.Key,
		Value:        prepare.Operation.Value,
		Metadata:     cloneObjectMetadata(prepare.Operation.Metadata),
	}
}

func journalRecordFromCommitWatermark(assignment ReplicaAssignment, sequence uint64) journalRecord {
	return journalRecord{
		Type:         journalRecordTypeCommitWatermark,
		Slot:         assignment.Slot,
		ChainVersion: assignment.ChainVersion,
		Sequence:     sequence,
	}
}

func journalRecordFromHeadCommitRange(assignment ReplicaAssignment, sequence uint64) journalRecord {
	return journalRecord{
		Type:         journalRecordTypeHeadCommitRange,
		Slot:         assignment.Slot,
		ChainVersion: assignment.ChainVersion,
		Sequence:     sequence,
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

type journalAppendReport struct {
	Experiment JournalExperiment
	BatchOps   int
	BatchBytes int
	Slots      int
	Records    map[journalRecordType]int
	Encode     time.Duration
	Write      time.Duration
	Sync       time.Duration
	Total      time.Duration
}

type fileCommitJournal struct {
	path       string
	file       *os.File
	segmentDir string
	segmentSeq int
	segmentCap int64
	mu         sync.Mutex
	opts       CommitJournalOpenOptions
}

const binarySegmentSizeBytes = 8 << 20

func openFileCommitJournal(path string, opts CommitJournalOpenOptions) (*fileCommitJournal, error) {
	if path == "" {
		return nil, nil
	}
	opts.Experiment = NormalizeJournalExperiment(opts.Experiment)
	if opts.Experiment == "" {
		opts.Experiment = defaultJournalExperiment
	}
	journal := &fileCommitJournal{
		path:       path,
		opts:       opts,
		segmentCap: binarySegmentSizeBytes,
	}
	switch opts.Experiment {
	case JournalExperimentBinarySegmentSync:
		journal.segmentDir = path + ".segments"
		if err := os.MkdirAll(journal.segmentDir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir commit journal segment dir: %w", err)
		}
		file, seq, err := openLatestSegmentFile(journal.segmentDir)
		if err != nil {
			return nil, err
		}
		journal.file = file
		journal.segmentSeq = seq
	default:
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("mkdir commit journal dir: %w", err)
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open commit journal: %w", err)
		}
		journal.file = file
	}
	return journal, nil
}

func OpenFileCommitJournalForLocalState(path string) (CommitJournalStore, error) {
	return openFileCommitJournal(path, CommitJournalOpenOptions{})
}

func OpenFileCommitJournalForLocalStateWithOptions(path string, opts CommitJournalOpenOptions) (CommitJournalStore, error) {
	return openFileCommitJournal(path, opts)
}

func (j *fileCommitJournal) AppendBatch(records []journalRecord) (journalAppendReport, error) {
	if j == nil || j.file == nil || len(records) == 0 {
		return journalAppendReport{}, nil
	}
	report := journalAppendReport{
		Experiment: j.opts.Experiment,
		Records:    countJournalRecordTypes(records),
		BatchOps:   len(records),
		Slots:      countJournalRecordSlots(records),
	}
	totalStarted := time.Now()
	encodeStarted := time.Now()
	payload, err := j.encodeRecords(records)
	report.Encode = time.Since(encodeStarted)
	if err != nil {
		return report, err
	}
	report.BatchBytes = len(payload)
	j.mu.Lock()
	defer j.mu.Unlock()
	writeStarted := time.Now()
	if err := j.appendPayload(payload); err != nil {
		return report, err
	}
	report.Write = time.Since(writeStarted)
	if j.opts.Experiment != JournalExperimentNoSyncBound {
		syncStarted := time.Now()
		if err := j.file.Sync(); err != nil {
			return report, fmt.Errorf("sync commit journal: %w", err)
		}
		report.Sync = time.Since(syncStarted)
	}
	report.Total = time.Since(totalStarted)
	return report, nil
}

func countJournalRecordTypes(records []journalRecord) map[journalRecordType]int {
	out := make(map[journalRecordType]int, len(records))
	for _, record := range records {
		out[record.recordType()]++
	}
	return out
}

func countJournalRecordSlots(records []journalRecord) int {
	if len(records) == 0 {
		return 0
	}
	seen := make(map[int]struct{}, len(records))
	for _, record := range records {
		seen[record.Slot] = struct{}{}
	}
	return len(seen)
}

func (j *fileCommitJournal) encodeRecords(records []journalRecord) ([]byte, error) {
	switch j.opts.Experiment {
	case JournalExperimentBinarySync, JournalExperimentBinarySegmentSync:
		return encodeBinaryJournalRecords(records)
	default:
		return encodeJSONJournalRecords(records)
	}
}

func encodeJSONJournalRecords(records []journalRecord) ([]byte, error) {
	var buf bytes.Buffer
	for _, record := range records {
		payload, err := json.Marshal(record)
		if err != nil {
			return nil, fmt.Errorf("marshal journal record: %w", err)
		}
		if len(payload) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: journal record too large", ErrInvalidConfig)
		}
		var lenBuf [4]byte
		binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(payload)))
		buf.Write(lenBuf[:])
		buf.Write(payload)
	}
	return buf.Bytes(), nil
}

const binaryJournalHeaderSize = 68

func encodeBinaryJournalRecords(records []journalRecord) ([]byte, error) {
	var buf bytes.Buffer
	for _, record := range records {
		keyBytes := []byte(record.Key)
		valueBytes := []byte(record.Value)
		if len(keyBytes) > int(^uint32(0)) || len(valueBytes) > int(^uint32(0)) {
			return nil, fmt.Errorf("%w: journal record payload too large", ErrInvalidConfig)
		}
		header := make([]byte, binaryJournalHeaderSize)
		header[0] = encodeJournalRecordType(record.recordType())
		header[1] = encodeOperationKind(record.Kind)
		binary.LittleEndian.PutUint64(header[4:12], uint64(record.Slot))
		binary.LittleEndian.PutUint64(header[12:20], record.ChainVersion)
		binary.LittleEndian.PutUint64(header[20:28], record.Sequence)
		binary.LittleEndian.PutUint64(header[28:36], record.Metadata.Version)
		binary.LittleEndian.PutUint64(header[36:44], uint64(record.Metadata.CreatedAt.UTC().UnixNano()))
		binary.LittleEndian.PutUint64(header[44:52], uint64(record.Metadata.UpdatedAt.UTC().UnixNano()))
		binary.LittleEndian.PutUint64(header[52:60], record.UpstreamConfirmedSequence)
		binary.LittleEndian.PutUint32(header[60:64], uint32(len(keyBytes)))
		binary.LittleEndian.PutUint32(header[64:68], uint32(len(valueBytes)))
		buf.Write(header)
		buf.Write(keyBytes)
		buf.Write(valueBytes)
	}
	return buf.Bytes(), nil
}

func encodeJournalRecordType(recordType journalRecordType) byte {
	switch recordType {
	case journalRecordTypePrepare:
		return 1
	case journalRecordTypeCommitWatermark:
		return 2
	case journalRecordTypeHeadCommitRange:
		return 3
	case journalRecordTypeUpstreamConfirm:
		return 4
	default:
		return 0
	}
}

func decodeJournalRecordType(code byte) journalRecordType {
	switch code {
	case 1:
		return journalRecordTypePrepare
	case 2:
		return journalRecordTypeCommitWatermark
	case 3:
		return journalRecordTypeHeadCommitRange
	case 4:
		return journalRecordTypeUpstreamConfirm
	default:
		return journalRecordTypeLegacyCommit
	}
}

func encodeOperationKind(kind OperationKind) byte {
	switch kind {
	case OperationKindPut:
		return 1
	case OperationKindDelete:
		return 2
	default:
		return 0
	}
}

func decodeOperationKind(code byte) OperationKind {
	switch code {
	case 1:
		return OperationKindPut
	case 2:
		return OperationKindDelete
	default:
		return ""
	}
}

func (j *fileCommitJournal) appendPayload(payload []byte) error {
	switch j.opts.Experiment {
	case JournalExperimentBinarySegmentSync:
		if err := j.rotateSegmentIfNeeded(int64(len(payload))); err != nil {
			return err
		}
	}
	if _, err := j.file.Write(payload); err != nil {
		return fmt.Errorf("write commit journal: %w", err)
	}
	return nil
}

func (j *fileCommitJournal) rotateSegmentIfNeeded(payloadBytes int64) error {
	if j == nil || j.file == nil || j.segmentDir == "" {
		return nil
	}
	info, err := j.file.Stat()
	if err != nil {
		return fmt.Errorf("stat commit journal segment: %w", err)
	}
	if info.Size()+payloadBytes <= j.segmentCap {
		return nil
	}
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("close commit journal segment: %w", err)
	}
	j.segmentSeq++
	path := filepath.Join(j.segmentDir, fmt.Sprintf("%08d.log", j.segmentSeq))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open commit journal segment: %w", err)
	}
	j.file = file
	return nil
}

func openLatestSegmentFile(dir string) (*os.File, int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("read commit journal segment dir: %w", err)
	}
	latest := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
			continue
		}
		var seq int
		if _, err := fmt.Sscanf(entry.Name(), "%08d.log", &seq); err == nil && seq > latest {
			latest = seq
		}
	}
	if latest == 0 {
		latest = 1
	}
	path := filepath.Join(dir, fmt.Sprintf("%08d.log", latest))
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, 0, fmt.Errorf("open commit journal segment: %w", err)
	}
	return file, latest, nil
}

func (j *fileCommitJournal) Replay(apply func(journalRecord) error) error {
	if j == nil || j.file == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	switch j.opts.Experiment {
	case JournalExperimentBinarySync, JournalExperimentBinarySegmentSync:
		if err := j.replayBinary(apply); err != nil {
			return err
		}
	default:
		if err := j.replayJSON(apply); err != nil {
			return err
		}
	}
	if _, err := j.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek commit journal end: %w", err)
	}
	return nil
}

func (j *fileCommitJournal) replayJSON(apply func(journalRecord) error) error {
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
	return nil
}

func (j *fileCommitJournal) replayBinary(apply func(journalRecord) error) error {
	if j.segmentDir != "" {
		entries, err := os.ReadDir(j.segmentDir)
		if err != nil {
			return fmt.Errorf("read commit journal segment dir: %w", err)
		}
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry.IsDir() || filepath.Ext(entry.Name()) != ".log" {
				continue
			}
			names = append(names, entry.Name())
		}
		sort.Strings(names)
		for _, name := range names {
			if err := replayBinaryJournalFile(filepath.Join(j.segmentDir, name), apply); err != nil {
				return err
			}
		}
		return nil
	}
	return replayBinaryJournalReader(j.file, apply)
}

func replayBinaryJournalFile(path string, apply func(journalRecord) error) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open commit journal segment replay file: %w", err)
	}
	defer file.Close()
	return replayBinaryJournalReader(file, apply)
}

func replayBinaryJournalReader(reader io.ReadSeeker, apply func(journalRecord) error) error {
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek commit journal start: %w", err)
	}
	header := make([]byte, binaryJournalHeaderSize)
	for {
		if _, err := io.ReadFull(reader, header); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read binary commit journal header: %w", err)
		}
		keyLen := binary.LittleEndian.Uint32(header[60:64])
		valueLen := binary.LittleEndian.Uint32(header[64:68])
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, key); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read binary commit journal key: %w", err)
		}
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(reader, value); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return fmt.Errorf("read binary commit journal value: %w", err)
		}
		record := journalRecord{
			Type:                      decodeJournalRecordType(header[0]),
			Kind:                      decodeOperationKind(header[1]),
			Slot:                      int(binary.LittleEndian.Uint64(header[4:12])),
			ChainVersion:              binary.LittleEndian.Uint64(header[12:20]),
			Sequence:                  binary.LittleEndian.Uint64(header[20:28]),
			Metadata:                  ObjectMetadata{Version: binary.LittleEndian.Uint64(header[28:36]), CreatedAt: time.Unix(0, int64(binary.LittleEndian.Uint64(header[36:44]))).UTC(), UpdatedAt: time.Unix(0, int64(binary.LittleEndian.Uint64(header[44:52]))).UTC()},
			UpstreamConfirmedSequence: binary.LittleEndian.Uint64(header[52:60]),
			Key:                       string(key),
			Value:                     string(value),
		}
		if record.Type == journalRecordTypeLegacyCommit {
			record.Type = ""
		}
		if err := apply(record); err != nil {
			return err
		}
	}
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

func (j *inMemoryCommitJournal) AppendBatch(records []journalRecord) (journalAppendReport, error) {
	if j == nil || len(records) == 0 {
		return journalAppendReport{}, nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, record := range records {
		j.records = append(j.records, cloneJournalRecord(record))
	}
	return journalAppendReport{
		Experiment: defaultJournalExperiment,
		BatchOps:   len(records),
		BatchBytes: 0,
		Slots:      countJournalRecordSlots(records),
		Records:    countJournalRecordTypes(records),
	}, nil
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

type journalPrepareIntent struct {
	ctx        context.Context
	prepare    DurableCommit
	owner      *slotOwner
	onComplete journalCompletionHandler
	queuedAt   time.Time
}

type journalWatermarkIntent struct {
	ctx        context.Context
	assignment ReplicaAssignment
	sequence   uint64
	recordType journalRecordType
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
	prepare      *journalPrepareIntent
	watermark    *pendingJournalWatermark
	confirmBatch *pendingJournalConfirm
}

type journalWatermarkKey struct {
	recordType   journalRecordType
	slot         int
	chainVersion uint64
}

type pendingJournalWatermark struct {
	assignment ReplicaAssignment
	sequence   uint64
	recordType journalRecordType
	intents    []*journalWatermarkIntent
}

type journalConfirmKey struct {
	slot         int
	chainVersion uint64
}

type pendingJournalConfirm struct {
	assignment ReplicaAssignment
	sequence   uint64
	intents    []*journalConfirmIntent
}

type commitJournalEngine struct {
	node *Node

	highWaterMu sync.RWMutex
	highWater   map[int]uint64
	prepareHigh map[int]uint64
	shards      []*commitJournalShard
	config      journalConfig
}

func (e *commitJournalEngine) allowSlot(slot int) {
	if e == nil {
		return
	}
	shard := e.shardForSlot(slot)
	if shard != nil && shard.materializer != nil {
		shard.materializer.allowSlot(slot)
	}
}

func (e *commitJournalEngine) dropSlot(slot int) {
	if e == nil {
		return
	}
	e.highWaterMu.Lock()
	delete(e.highWater, slot)
	delete(e.prepareHigh, slot)
	e.highWaterMu.Unlock()
	shard := e.shardForSlot(slot)
	if shard != nil && shard.materializer != nil {
		shard.materializer.dropSlot(slot)
	}
}

type commitJournalShard struct {
	engine       *commitJournalEngine
	index        int
	journal      commitJournalStore
	materializer *commitMaterializer
	prepareCh    chan *journalPrepareIntent
	watermarkCh  chan *journalWatermarkIntent
	confirmCh    chan *journalConfirmIntent
	closeCh      chan struct{}
	closedCh     chan struct{}

	diagMu       sync.Mutex
	lastFlushAt  time.Time
	recentBySlot map[int][]journalRecord
	pendingDiag  journalShardPendingSnapshot
}

type journalShardPendingSnapshot struct {
	prepareItems int
	prepareSlots int
	watermarks   int
	confirmItems int
}

func newCommitJournalEngine(node *Node, journals []commitJournalStore, highWater map[int]uint64, backlog []DurableCommit, cfg journalConfig) *commitJournalEngine {
	engine := &commitJournalEngine{
		node:        node,
		highWater:   make(map[int]uint64, len(highWater)),
		prepareHigh: make(map[int]uint64, len(highWater)),
		shards:      make([]*commitJournalShard, 0, len(journals)),
		config:      cfg,
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
			prepareCh:    make(chan *journalPrepareIntent, 4096),
			watermarkCh:  make(chan *journalWatermarkIntent, 4096),
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

func (e *commitJournalEngine) preparedSequence(slot int) uint64 {
	if e == nil {
		return 0
	}
	e.highWaterMu.RLock()
	defer e.highWaterMu.RUnlock()
	return e.prepareHigh[slot]
}

func (e *commitJournalEngine) markCommitted(slot int, sequence uint64) {
	e.highWaterMu.Lock()
	defer e.highWaterMu.Unlock()
	if sequence > e.highWater[slot] {
		e.highWater[slot] = sequence
	}
}

func (e *commitJournalEngine) markPrepared(slot int, sequence uint64) {
	e.highWaterMu.Lock()
	defer e.highWaterMu.Unlock()
	if sequence > e.prepareHigh[slot] {
		e.prepareHigh[slot] = sequence
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

func (e *commitJournalEngine) submitPrepare(
	ctx context.Context,
	owner *slotOwner,
	prepare DurableCommit,
	onComplete journalCompletionHandler,
) error {
	shard := e.shardForSlot(prepare.Operation.Slot)
	if shard == nil {
		return fmt.Errorf("%w: commit journal unavailable", ErrInvalidConfig)
	}
	intent := &journalPrepareIntent{
		ctx:        ctx,
		prepare:    cloneDurableCommit(prepare),
		owner:      owner,
		onComplete: onComplete,
		queuedAt:   time.Now(),
	}
	return shard.enqueuePrepare(intent)
}

func (e *commitJournalEngine) submitCommitWatermark(
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
	intent := &journalWatermarkIntent{
		ctx:        ctx,
		assignment: cloneAssignment(assignment),
		sequence:   sequence,
		recordType: journalRecordTypeCommitWatermark,
		owner:      owner,
		onComplete: onComplete,
		queuedAt:   time.Now(),
	}
	return shard.enqueueCommitWatermark(intent)
}

func (e *commitJournalEngine) submitHeadCommitRange(
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
	intent := &journalWatermarkIntent{
		ctx:        ctx,
		assignment: cloneAssignment(assignment),
		sequence:   sequence,
		recordType: journalRecordTypeHeadCommitRange,
		owner:      owner,
		onComplete: onComplete,
		queuedAt:   time.Now(),
	}
	return shard.enqueueCommitWatermark(intent)
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

func (e *commitJournalEngine) enqueueMaterialized(commits []DurableCommit) {
	if e == nil || len(commits) == 0 {
		return
	}
	byShard := map[int][]DurableCommit{}
	for _, commit := range commits {
		shard := e.shardIndex(commit.Operation.Slot)
		byShard[shard] = append(byShard[shard], cloneDurableCommit(commit))
	}
	for shardIdx, shardCommits := range byShard {
		shard := e.shards[shardIdx]
		if shard != nil && shard.materializer != nil {
			shard.materializer.enqueue(shardCommits)
		}
	}
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

func (s *commitJournalShard) enqueuePrepare(intent *journalPrepareIntent) error {
	select {
	case <-s.closeCh:
		return context.Canceled
	default:
	}
	select {
	case <-s.closeCh:
		return context.Canceled
	case s.prepareCh <- intent:
		s.engine.node.recordTimeoutJournalQueueDepth(len(s.prepareCh) + len(s.watermarkCh) + len(s.confirmCh))
		return nil
	}
}

func (s *commitJournalShard) enqueueCommitWatermark(intent *journalWatermarkIntent) error {
	select {
	case <-s.closeCh:
		return context.Canceled
	default:
	}
	select {
	case <-s.closeCh:
		return context.Canceled
	case s.watermarkCh <- intent:
		s.engine.node.recordTimeoutJournalQueueDepth(len(s.prepareCh) + len(s.watermarkCh) + len(s.confirmCh))
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
		s.engine.node.recordTimeoutJournalQueueDepth(len(s.prepareCh) + len(s.watermarkCh) + len(s.confirmCh))
		return nil
	}
}

func (s *commitJournalShard) deliver(owner *slotOwner, handler journalCompletionHandler, err error, completedAt time.Time) {
	if owner == nil || handler == nil {
		return
	}
	_ = owner.enqueueCompletion(func(runtime *slotRuntime) {
		handler(runtime, err, completedAt)
	})
}

func (s *commitJournalShard) nextBatchDelay(queueDepth int) time.Duration {
	if queueDepth < s.engine.config.batchDepthThreshold {
		return s.engine.config.batchDelayLow
	}
	return s.engine.config.batchDelayHigh
}

func (s *commitJournalShard) updatePendingSnapshot(prepareItems int, prepareSlots int, watermarks int, confirmItems int) {
	if s == nil {
		return
	}
	s.diagMu.Lock()
	s.pendingDiag = journalShardPendingSnapshot{
		prepareItems: prepareItems,
		prepareSlots: prepareSlots,
		watermarks:   watermarks,
		confirmItems: confirmItems,
	}
	s.diagMu.Unlock()
	s.engine.node.observeJournalShardPending(s.index, prepareItems, prepareSlots, confirmItems)
}

func (s *commitJournalShard) run() {
	defer close(s.closedCh)

	var (
		timer               *time.Timer
		timerCh             <-chan time.Time
		pendingPrepareMap   = map[int][]*journalPrepareIntent{}
		pendingPrepareSeq   []int
		pendingPrepareN     int
		pendingPrepareBytes int
		pendingWatermarks   = map[journalWatermarkKey]*pendingJournalWatermark{}
		pendingWatermarkSeq []journalWatermarkKey
		pendingConfirms     = map[journalConfirmKey]*pendingJournalConfirm{}
		pendingConfirmSeq   []journalConfirmKey
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

	resetTimer := func(queueDepth int) {
		delay := s.nextBatchDelay(queueDepth)
		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}
		timerCh = timer.C
	}

	queueDepth := func() int {
		return len(s.prepareCh) + len(s.watermarkCh) + len(s.confirmCh) + pendingPrepareN + len(pendingWatermarks) + len(pendingConfirms)
	}

	updatePending := func() {
		s.updatePendingSnapshot(pendingPrepareN, len(pendingPrepareMap), len(pendingWatermarks), len(pendingConfirms))
		s.engine.node.recordTimeoutJournalQueueDepth(queueDepth())
	}

	enqueuePendingPrepare := func(intent *journalPrepareIntent) {
		slot := intent.prepare.Operation.Slot
		queue := pendingPrepareMap[slot]
		if len(queue) == 0 {
			pendingPrepareSeq = append(pendingPrepareSeq, slot)
		}
		pendingPrepareMap[slot] = append(queue, intent)
		pendingPrepareN++
		pendingPrepareBytes += estimatedCommitBytes(intent.prepare)
		updatePending()
	}

	enqueuePendingWatermark := func(intent *journalWatermarkIntent) {
		key := journalWatermarkKey{recordType: intent.recordType, slot: intent.assignment.Slot, chainVersion: intent.assignment.ChainVersion}
		entry, ok := pendingWatermarks[key]
		if !ok {
			pendingWatermarks[key] = &pendingJournalWatermark{
				assignment: cloneAssignment(intent.assignment),
				sequence:   intent.sequence,
				recordType: intent.recordType,
				intents:    []*journalWatermarkIntent{intent},
			}
			pendingWatermarkSeq = append(pendingWatermarkSeq, key)
			updatePending()
			return
		}
		if intent.sequence > entry.sequence {
			entry.sequence = intent.sequence
			entry.assignment = cloneAssignment(intent.assignment)
		}
		entry.intents = append(entry.intents, intent)
		updatePending()
	}

	enqueuePendingConfirm := func(intent *journalConfirmIntent) {
		key := journalConfirmKey{slot: intent.assignment.Slot, chainVersion: intent.assignment.ChainVersion}
		entry, ok := pendingConfirms[key]
		if !ok {
			pendingConfirms[key] = &pendingJournalConfirm{
				assignment: cloneAssignment(intent.assignment),
				sequence:   intent.sequence,
				intents:    []*journalConfirmIntent{intent},
			}
			pendingConfirmSeq = append(pendingConfirmSeq, key)
			updatePending()
			return
		}
		if intent.sequence > entry.sequence {
			entry.sequence = intent.sequence
			entry.assignment = cloneAssignment(intent.assignment)
		}
		entry.intents = append(entry.intents, intent)
		s.engine.node.observeJournalCoalescedConfirm(1)
		updatePending()
	}

	popPrepareBatch := func(
		pendingMap map[int][]*journalPrepareIntent,
		pendingSeq *[]int,
		pendingN *int,
		pendingBytes *int,
	) ([]journalBatchItem, int, int, int) {
		if *pendingN == 0 || len(*pendingSeq) == 0 {
			return nil, 0, 0, 0
		}
		items := make([]journalBatchItem, 0, min(*pendingN, s.engine.config.batchMaxOps))
		batchBytes := 0
		prepareCount := 0
		seenSlots := map[int]struct{}{}
		order := *pendingSeq
		for len(order) > 0 && prepareCount < s.engine.config.batchMaxOps && batchBytes < defaultJournalBatchMaxBytes {
			nextOrder := make([]int, 0, len(order))
			madeProgress := false
			for idx, slot := range order {
				queue := pendingMap[slot]
				if len(queue) == 0 {
					delete(pendingMap, slot)
					continue
				}
				intent := queue[0]
				intentBytes := estimatedCommitBytes(intent.prepare)
				if prepareCount > 0 && batchBytes+intentBytes > defaultJournalBatchMaxBytes {
					nextOrder = append(nextOrder, slot)
					nextOrder = append(nextOrder, order[idx+1:]...)
					order = nextOrder
					*pendingSeq = order
					updatePending()
					return items, batchBytes, prepareCount, len(seenSlots)
				}
				queue = queue[1:]
				*pendingN = *pendingN - 1
				*pendingBytes -= intentBytes
				items = append(items, journalBatchItem{prepare: intent})
				batchBytes += intentBytes
				prepareCount++
				seenSlots[slot] = struct{}{}
				madeProgress = true
				if len(queue) > 0 {
					pendingMap[slot] = queue
					nextOrder = append(nextOrder, slot)
				} else {
					delete(pendingMap, slot)
				}
				if prepareCount >= s.engine.config.batchMaxOps || batchBytes >= defaultJournalBatchMaxBytes {
					nextOrder = append(nextOrder, order[idx+1:]...)
					order = nextOrder
					*pendingSeq = order
					updatePending()
					return items, batchBytes, prepareCount, len(seenSlots)
				}
			}
			order = nextOrder
			*pendingSeq = order
			if !madeProgress {
				break
			}
		}
		updatePending()
		return items, batchBytes, prepareCount, len(seenSlots)
	}

	popWatermarkBatch := func(remainingOps int, remainingBytes int, allow bool) ([]journalBatchItem, int) {
		if !allow || remainingOps <= 0 || remainingBytes <= 0 || len(pendingWatermarkSeq) == 0 {
			return nil, 0
		}
		items := make([]journalBatchItem, 0, min(len(pendingWatermarkSeq), remainingOps))
		count := 0
		nextOrder := make([]journalWatermarkKey, 0, len(pendingWatermarkSeq))
		for idx, key := range pendingWatermarkSeq {
			entry := pendingWatermarks[key]
			if entry == nil {
				continue
			}
			if count > 0 && (count >= remainingOps || (count+1)*64 > remainingBytes) {
				nextOrder = append(nextOrder, key)
				nextOrder = append(nextOrder, pendingWatermarkSeq[idx+1:]...)
				break
			}
			items = append(items, journalBatchItem{watermark: entry})
			delete(pendingWatermarks, key)
			count++
			if count >= remainingOps || count*64 >= remainingBytes {
				nextOrder = append(nextOrder, pendingWatermarkSeq[idx+1:]...)
				break
			}
		}
		pendingWatermarkSeq = nextOrder
		updatePending()
		return items, count
	}

	popPriorityHeadRangeBatch := func(remainingOps int, remainingBytes int) ([]journalBatchItem, int) {
		if remainingOps <= 0 || remainingBytes <= 0 || len(pendingWatermarkSeq) == 0 {
			return nil, 0
		}
		limitOps := remainingOps
		if limitOps > defaultJournalPriorityWatermarks {
			limitOps = defaultJournalPriorityWatermarks
		}
		if limitOps <= 0 {
			return nil, 0
		}
		items := make([]journalBatchItem, 0, min(len(pendingWatermarkSeq), limitOps))
		count := 0
		nextOrder := make([]journalWatermarkKey, 0, len(pendingWatermarkSeq))
		for _, key := range pendingWatermarkSeq {
			entry := pendingWatermarks[key]
			if entry == nil {
				continue
			}
			if key.recordType != journalRecordTypeHeadCommitRange {
				nextOrder = append(nextOrder, key)
				continue
			}
			if count >= limitOps || (count+1)*64 > remainingBytes {
				nextOrder = append(nextOrder, key)
				continue
			}
			items = append(items, journalBatchItem{watermark: entry})
			delete(pendingWatermarks, key)
			count++
		}
		if count == 0 {
			return nil, 0
		}
		pendingWatermarkSeq = nextOrder
		updatePending()
		return items, count
	}

	popConfirmBatch := func(remainingOps int, remainingBytes int, allow bool) ([]journalBatchItem, int) {
		if !allow || remainingOps <= 0 || remainingBytes <= 0 || len(pendingConfirmSeq) == 0 {
			return nil, 0
		}
		items := make([]journalBatchItem, 0, min(len(pendingConfirmSeq), remainingOps))
		confirmCount := 0
		nextOrder := make([]journalConfirmKey, 0, len(pendingConfirmSeq))
		for idx, key := range pendingConfirmSeq {
			entry := pendingConfirms[key]
			if entry == nil {
				continue
			}
			if confirmCount > 0 && (confirmCount >= remainingOps || (confirmCount+1)*64 > remainingBytes) {
				nextOrder = append(nextOrder, key)
				nextOrder = append(nextOrder, pendingConfirmSeq[idx+1:]...)
				break
			}
			items = append(items, journalBatchItem{confirmBatch: entry})
			delete(pendingConfirms, key)
			confirmCount++
			if confirmCount >= remainingOps || confirmCount*64 >= remainingBytes {
				nextOrder = append(nextOrder, pendingConfirmSeq[idx+1:]...)
				break
			}
		}
		pendingConfirmSeq = nextOrder
		updatePending()
		return items, confirmCount
	}

	flush := func() {
		if pendingPrepareN == 0 && len(pendingWatermarks) == 0 && len(pendingConfirms) == 0 {
			stopTimer()
			updatePending()
			return
		}
		stopTimer()
		items := make([]journalBatchItem, 0, s.engine.config.batchMaxOps)
		remainingOps := s.engine.config.batchMaxOps
		remainingBytes := defaultJournalBatchMaxBytes
		priorityWatermarkItems, priorityWatermarkCount := popPriorityHeadRangeBatch(remainingOps, remainingBytes)
		items = append(items, priorityWatermarkItems...)
		remainingOps -= priorityWatermarkCount
		remainingBytes -= priorityWatermarkCount * 64
		prepareItems, prepareBytes, prepareCount, slotsTouched := popPrepareBatch(
			pendingPrepareMap,
			&pendingPrepareSeq,
			&pendingPrepareN,
			&pendingPrepareBytes,
		)
		items = append(items, prepareItems...)
		remainingOps -= prepareCount
		remainingBytes -= prepareBytes
		watermarkItems, watermarkCount := popWatermarkBatch(remainingOps, remainingBytes, prepareCount == 0 || remainingOps > 0)
		items = append(items, watermarkItems...)
		remainingOps -= watermarkCount
		remainingBytes -= watermarkCount * 64
		confirmItems, confirmCount := popConfirmBatch(remainingOps, remainingBytes, prepareCount == 0 || remainingOps > 0)
		items = append(items, confirmItems...)
		if len(items) == 0 {
			return
		}

		records := make([]journalRecord, 0, len(items))
		prepares := make([]*journalPrepareIntent, 0, len(items))
		for _, item := range items {
			switch {
			case item.prepare != nil:
				intent := item.prepare
				if intent.ctx != nil {
					if err := intent.ctx.Err(); err != nil {
						s.deliver(intent.owner, intent.onComplete, err, time.Now())
						continue
					}
				}
				s.engine.node.observeWriteStage(writeStagePrepareQueueWait, intent.prepare.Persisted.Assignment.Role, writeStageResultSuccess, time.Since(intent.queuedAt))
				records = append(records, journalRecordFromPrepare(intent.prepare))
				prepares = append(prepares, intent)
			case item.watermark != nil:
				entry := item.watermark
				if len(entry.intents) == 0 {
					continue
				}
				canceled := 0
				for _, intent := range entry.intents {
					if intent.ctx != nil {
						if err := intent.ctx.Err(); err != nil {
							s.deliver(intent.owner, intent.onComplete, err, time.Now())
							canceled++
						}
					}
				}
				if canceled == len(entry.intents) {
					continue
				}
				switch entry.recordType {
				case journalRecordTypeHeadCommitRange:
					records = append(records, journalRecordFromHeadCommitRange(entry.assignment, entry.sequence))
				default:
					records = append(records, journalRecordFromCommitWatermark(entry.assignment, entry.sequence))
				}
			case item.confirmBatch != nil:
				entry := item.confirmBatch
				if len(entry.intents) == 0 {
					continue
				}
				canceled := 0
				for _, intent := range entry.intents {
					if intent.ctx != nil {
						if err := intent.ctx.Err(); err != nil {
							s.deliver(intent.owner, intent.onComplete, err, time.Now())
							canceled++
						}
					}
				}
				if canceled == len(entry.intents) {
					continue
				}
				records = append(records, journalRecordFromUpstreamConfirm(entry.assignment, entry.sequence))
			}
		}
		if len(records) == 0 {
			return
		}
		if len(prepares) > 0 {
			s.engine.node.observeCommitBatchSize(len(prepares), prepareBytes)
			s.engine.node.observeJournalCommitBatchSlots(slotsTouched)
			for _, intent := range prepares {
				if stage := writeTracePrepareFlushStartStage(intent.prepare.Persisted.Assignment.Role); stage != "" {
					s.engine.node.traceWriteEvent(intent.prepare.Persisted.Assignment, intent.prepare.Operation.Sequence, stage)
				}
			}
		}
		if watermarkCount > 0 {
			s.engine.node.observeJournalCommitWatermarkBatchCount(watermarkCount)
		}
		if confirmCount > 0 {
			s.engine.node.observeJournalConfirmBatchCount(confirmCount)
		}
		flushStarted := time.Now()
		appendReport, appendErr := s.journal.AppendBatch(records)
		flushDuration := time.Since(flushStarted)
		completedAt := time.Now()
		if appendReport.Total <= 0 {
			appendReport.Total = flushDuration
		}
		s.engine.node.observeJournalFlushBreakdown(s.index, appendReport, writeStageResult(appendErr))
		if appendErr != nil {
			for _, item := range items {
				switch {
				case item.prepare != nil:
					intent := item.prepare
					s.engine.node.observeWriteStage(writeStagePrepareFlush, intent.prepare.Persisted.Assignment.Role, writeStageResult(appendErr), flushDuration)
					s.deliver(intent.owner, intent.onComplete, appendErr, completedAt)
				case item.watermark != nil:
					for _, intent := range item.watermark.intents {
						s.engine.node.observeWriteStage(writeStageCommitWatermarkFlush, intent.assignment.Role, writeStageResult(appendErr), flushDuration)
						s.deliver(intent.owner, intent.onComplete, appendErr, completedAt)
					}
				case item.confirmBatch != nil:
					for _, intent := range item.confirmBatch.intents {
						s.deliver(intent.owner, intent.onComplete, appendErr, completedAt)
					}
				}
			}
			return
		}
		s.recordDurableRecords(records, completedAt)
		for _, item := range items {
			switch {
			case item.prepare != nil:
				intent := item.prepare
				s.engine.markPrepared(intent.prepare.Operation.Slot, intent.prepare.Operation.Sequence)
				s.engine.node.observeWriteStage(writeStagePrepareFlush, intent.prepare.Persisted.Assignment.Role, writeStageResultSuccess, flushDuration)
				if stage := writeTracePrepareFlushEndStage(intent.prepare.Persisted.Assignment.Role); stage != "" {
					s.engine.node.traceWriteEvent(intent.prepare.Persisted.Assignment, intent.prepare.Operation.Sequence, stage)
				}
				s.deliver(intent.owner, intent.onComplete, nil, completedAt)
			case item.watermark != nil:
				s.engine.markCommitted(item.watermark.assignment.Slot, item.watermark.sequence)
				for _, intent := range item.watermark.intents {
					s.engine.node.observeWriteStage(writeStageCommitWatermarkFlush, intent.assignment.Role, writeStageResultSuccess, flushDuration)
					s.deliver(intent.owner, intent.onComplete, nil, completedAt)
				}
			case item.confirmBatch != nil:
				for _, intent := range item.confirmBatch.intents {
					s.deliver(intent.owner, intent.onComplete, nil, completedAt)
				}
			}
		}
	}

	for {
		select {
		case <-s.closeCh:
			now := time.Now()
			for _, slot := range pendingPrepareSeq {
				for _, intent := range pendingPrepareMap[slot] {
					s.deliver(intent.owner, intent.onComplete, context.Canceled, now)
				}
			}
			for _, key := range pendingWatermarkSeq {
				entry := pendingWatermarks[key]
				if entry == nil {
					continue
				}
				for _, intent := range entry.intents {
					s.deliver(intent.owner, intent.onComplete, context.Canceled, now)
				}
			}
			for _, key := range pendingConfirmSeq {
				entry := pendingConfirms[key]
				if entry == nil {
					continue
				}
				for _, intent := range entry.intents {
					s.deliver(intent.owner, intent.onComplete, context.Canceled, now)
				}
			}
			return
		case intent := <-s.prepareCh:
			enqueuePendingPrepare(intent)
			if queueDepth() == 1 {
				resetTimer(queueDepth())
			}
			if queueDepth() > 1 && len(s.prepareCh)+len(s.watermarkCh)+len(s.confirmCh) == 0 {
				flush()
				continue
			}
			if pendingPrepareN >= s.engine.config.batchMaxOps || pendingPrepareBytes >= defaultJournalBatchMaxBytes {
				flush()
			}
		case intent := <-s.watermarkCh:
			enqueuePendingWatermark(intent)
			if queueDepth() == 1 {
				resetTimer(queueDepth())
			}
			if queueDepth() > 1 && len(s.prepareCh)+len(s.watermarkCh)+len(s.confirmCh) == 0 {
				flush()
				continue
			}
			if pendingPrepareN >= s.engine.config.batchMaxOps || pendingPrepareBytes >= defaultJournalBatchMaxBytes {
				flush()
			}
		case intent := <-s.confirmCh:
			enqueuePendingConfirm(intent)
			if queueDepth() == 1 {
				resetTimer(queueDepth())
			}
			if queueDepth() > 1 && len(s.prepareCh)+len(s.watermarkCh)+len(s.confirmCh) == 0 {
				flush()
				continue
			}
			if pendingPrepareN >= s.engine.config.batchMaxOps || pendingPrepareBytes >= defaultJournalBatchMaxBytes {
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
		QueueDepth: len(s.prepareCh) + len(s.watermarkCh) + len(s.confirmCh),
	}
	s.diagMu.Lock()
	out.LastFlushAt = s.lastFlushAt
	out.QueueDepth += s.pendingDiag.prepareItems + s.pendingDiag.watermarks + s.pendingDiag.confirmItems
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
	node      *Node
	submitCh  chan []DurableCommit
	controlCh chan materializerSlotControl
	closeCh   chan struct{}
	closedCh  chan struct{}
}

type materializerSlotControl struct {
	slot  int
	allow bool
	done  chan struct{}
}

func newCommitMaterializer(node *Node, backlog []DurableCommit) *commitMaterializer {
	m := &commitMaterializer{
		node:      node,
		submitCh:  make(chan []DurableCommit, 1024),
		controlCh: make(chan materializerSlotControl, 128),
		closeCh:   make(chan struct{}),
		closedCh:  make(chan struct{}),
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

func (m *commitMaterializer) allowSlot(slot int) {
	m.controlSlot(slot, true)
}

func (m *commitMaterializer) dropSlot(slot int) {
	m.controlSlot(slot, false)
}

func (m *commitMaterializer) controlSlot(slot int, allow bool) {
	if m == nil {
		return
	}
	done := make(chan struct{})
	control := materializerSlotControl{slot: slot, allow: allow, done: done}
	select {
	case <-m.closeCh:
		return
	case m.controlCh <- control:
	}
	select {
	case <-m.closeCh:
	case <-done:
	}
}

func (m *commitMaterializer) run(backlog []DurableCommit) {
	defer close(m.closedCh)
	pending := make([]DurableCommit, 0, len(backlog))
	pending = append(pending, cloneDurableCommits(backlog)...)
	suppressedSlots := map[int]struct{}{}
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
			if _, suppressed := suppressedSlots[commit.Operation.Slot]; suppressed {
				continue
			}
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
		case control := <-m.controlCh:
			if control.allow {
				delete(suppressedSlots, control.slot)
			} else {
				suppressedSlots[control.slot] = struct{}{}
				filtered := pending[:0]
				for _, commit := range pending {
					if commit.Operation.Slot != control.slot {
						filtered = append(filtered, commit)
					}
				}
				pending = filtered
				m.node.recordTimeoutMaterializerQueueDepth(len(pending))
			}
			close(control.done)
		case commits := <-m.submitCh:
			for _, commit := range commits {
				if _, suppressed := suppressedSlots[commit.Operation.Slot]; suppressed {
					continue
				}
				pending = append(pending, commit)
			}
			m.node.recordTimeoutMaterializerQueueDepth(len(pending))
			if len(pending) > 0 && timer == nil {
				resetTimer()
			}
			if len(pending) >= defaultMaterializeBatchMaxOps {
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

func openNodeCommitJournals(local LocalStateStore, nodeID string, cfg journalConfig) ([]commitJournalStore, error) {
	shardCount := cfg.shardCount
	if shardCount <= 0 {
		shardCount = defaultJournalShardCount
	}
	opts := CommitJournalOpenOptions{Experiment: cfg.experiment}
	if provider, ok := local.(commitJournalShardProviderWithOptions); ok {
		journals := make([]commitJournalStore, 0, shardCount)
		for shard := 0; shard < shardCount; shard++ {
			journal, err := provider.CommitJournalShardWithOptions(nodeID, shard, opts)
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
	if provider, ok := local.(commitJournalShardProvider); ok {
		journals := make([]commitJournalStore, 0, shardCount)
		for shard := 0; shard < shardCount; shard++ {
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
	if provider, ok := local.(commitJournalProviderWithOptions); ok {
		journal, err := provider.CommitJournalWithOptions(nodeID, opts)
		if err != nil {
			return nil, err
		}
		return []commitJournalStore{journal}, nil
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
			case journalRecordTypePrepare:
				if entry.Sequence > record.highestPreparedDurable {
					record.highestPreparedDurable = entry.Sequence
				}
				record.preparedEntries[entry.Sequence] = entry.operation()
				if nextSequence := entry.Sequence + 1; record.nextSequence < nextSequence {
					record.nextSequence = nextSequence
				}
			case journalRecordTypeCommitWatermark, journalRecordTypeHeadCommitRange:
				if entry.Sequence > record.highestCommittedSequence {
					record.highestCommittedSequence = entry.Sequence
					record.localDataPresent = true
				}
			case journalRecordTypeLegacyCommit:
				if entry.Sequence > record.highestPreparedDurable {
					record.highestPreparedDurable = entry.Sequence
				}
				record.preparedEntries[entry.Sequence] = entry.operation()
				if entry.Sequence > record.highestCommittedSequence {
					record.highestCommittedSequence = entry.Sequence
					record.localDataPresent = true
				}
				if entry.UpstreamConfirmedSequence > record.highestUpstreamConfirmedSequence {
					record.highestUpstreamConfirmedSequence = entry.UpstreamConfirmedSequence
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
	backlog := make([]DurableCommit, 0)
	for slot, record := range records {
		if record.assignment.Peers.SuccessorNodeID == "" {
			for sequence := record.highestCommittedSequence + 1; ; sequence++ {
				if _, ok := record.preparedEntries[sequence]; !ok {
					break
				}
				record.highestCommittedSequence = sequence
				record.localDataPresent = true
			}
		}
		for sequence := record.materializedCommittedSequence + 1; sequence <= record.highestCommittedSequence; sequence++ {
			operation, ok := record.preparedEntries[sequence]
			if !ok {
				continue
			}
			record = recordWithCommittedOverlay(record, operation)
			backlog = append(backlog, DurableCommit{
				Operation: cloneWriteOperation(operation),
				Persisted: persistedReplica(record),
			})
		}
		records[slot] = record
	}
	return backlog, highWater, nil
}
