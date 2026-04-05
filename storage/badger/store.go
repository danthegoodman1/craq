package badger

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	badgerdb "github.com/dgraph-io/badger/v4"

	"github.com/danthegoodman1/craq/storage"
)

const (
	keyReplicaMeta   byte = 'm'
	keyCommittedData byte = 'c'
	keyStagedOp      byte = 's'
	keyLocalReplica  byte = 'l'
	keyLocalNodeMeta byte = 'n'
)

const (
	opCreateReplica               = "create_replica"
	opDeleteReplica               = "delete_replica"
	opInstallSnapshot             = "install_snapshot"
	opSetHighestCommittedSequence = "set_highest_committed_sequence"
	opCommitSequence              = "commit_sequence"
	opApplyCommitted              = "apply_committed"
	opUpsertLocalReplica          = "upsert_local_replica"
	opDeleteLocalReplica          = "delete_local_replica"
	opSetLocalNodeMeta            = "set_local_node_meta"
	opCleanupStagedOnOpen         = "cleanup_staged_on_open"
)

type Store struct {
	owner *owner
}

type owner struct {
	db        *badgerdb.DB
	closeErr  error
	closeOnce sync.Once
}

type durableWriteHooks struct {
	mu                 sync.Mutex
	beforeDurableWrite func(op string) error
}

var testHooks durableWriteHooks

type Backend struct {
	owner *owner
}

type LocalStore struct {
	owner *owner
}

type stagedValue struct {
	Kind     storage.OperationKind  `json:"kind"`
	Key      string                 `json:"key"`
	Value    string                 `json:"value"`
	Metadata storage.ObjectMetadata `json:"metadata"`
}

func Open(path string) (*Store, error) {
	opts := badgerdb.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = true
	opts.Dir = path
	opts.ValueDir = path

	db, err := badgerdb.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("err in badger.Open: %w", err)
	}
	owner := &owner{db: db}
	if err := owner.cleanupStagedOperationsOnOpen(); err != nil {
		closeErr := owner.Close()
		if closeErr != nil {
			return nil, errors.Join(fmt.Errorf("err in cleanupStagedOperationsOnOpen: %w", err), closeErr)
		}
		return nil, fmt.Errorf("err in cleanupStagedOperationsOnOpen: %w", err)
	}
	return &Store{owner: owner}, nil
}

func (s *Store) Backend() storage.Backend {
	return &Backend{owner: s.owner}
}

func (s *Store) LocalStateStore() storage.LocalStateStore {
	return &LocalStore{owner: s.owner}
}

func (s *Store) Close() error {
	return s.owner.Close()
}

func (o *owner) Close() error {
	o.closeOnce.Do(func() {
		o.closeErr = o.db.Close()
		if o.closeErr != nil {
			o.closeErr = fmt.Errorf("err in o.db.Close: %w", o.closeErr)
		}
	})
	return o.closeErr
}

func (b *Backend) Close() error {
	return b.owner.Close()
}

func (l *LocalStore) Close() error {
	return l.owner.Close()
}

func (b *Backend) CreateReplica(slot int) error {
	if _, err := b.HighestCommittedSequence(slot); err == nil {
		return fmt.Errorf("%w: slot %d", storage.ErrReplicaExists, slot)
	} else if !errors.Is(err, storage.ErrUnknownReplica) {
		return err
	}
	if err := b.owner.writeSync(opCreateReplica, func(txn *badgerdb.Txn) error {
		return txn.Set(replicaMetaKey(slot), encodeUint64(0))
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(create replica): %w", err)
	}
	return nil
}

func (b *Backend) DeleteReplica(slot int) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	if err := b.owner.writeSync(opDeleteReplica, func(txn *badgerdb.Txn) error {
		if err := deletePrefixTxn(txn, committedPrefix(slot)); err != nil {
			return err
		}
		if err := deletePrefixTxn(txn, stagedPrefix(slot)); err != nil {
			return err
		}
		if err := txn.Delete(replicaMetaKey(slot)); err != nil {
			return fmt.Errorf("err in txn.Delete(replica meta): %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(delete replica): %w", err)
	}
	return nil
}

func (b *Backend) Snapshot(slot int) (storage.Snapshot, error) {
	return b.CommittedSnapshot(slot)
}

func (b *Backend) InstallSnapshot(slot int, snap storage.Snapshot) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	if err := b.owner.writeSync(opInstallSnapshot, func(txn *badgerdb.Txn) error {
		if err := deletePrefixTxn(txn, committedPrefix(slot)); err != nil {
			return err
		}
		if err := deletePrefixTxn(txn, stagedPrefix(slot)); err != nil {
			return err
		}
		keys := sortedSnapshotKeys(snap)
		for _, key := range keys {
			encoded, err := json.Marshal(snap[key])
			if err != nil {
				return fmt.Errorf("err in json.Marshal(committed snapshot): %w", err)
			}
			if err := txn.Set(committedKey(slot, key), encoded); err != nil {
				return fmt.Errorf("err in txn.Set(committed snapshot): %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(install snapshot): %w", err)
	}
	return nil
}

func (b *Backend) SetHighestCommittedSequence(slot int, sequence uint64) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	if err := b.owner.writeSync(opSetHighestCommittedSequence, func(txn *badgerdb.Txn) error {
		if err := deletePrefixTxn(txn, stagedPrefix(slot)); err != nil {
			return err
		}
		if err := txn.Set(replicaMetaKey(slot), encodeUint64(sequence)); err != nil {
			return fmt.Errorf("err in txn.Set(replica meta): %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(set highest committed sequence): %w", err)
	}
	return nil
}

func (b *Backend) ApplyCommitted(_ context.Context, nodeID string, operation storage.WriteOperation, persisted *storage.PersistedReplica) error {
	highestCommitted, err := b.HighestCommittedSequence(operation.Slot)
	if err != nil {
		return err
	}
	if operation.Sequence != highestCommitted+1 {
		return fmt.Errorf(
			"%w: slot %d expected commit sequence %d, got %d",
			storage.ErrSequenceMismatch,
			operation.Slot,
			highestCommitted+1,
			operation.Sequence,
		)
	}

	if err := b.owner.writeSync(opApplyCommitted, func(txn *badgerdb.Txn) error {
		switch operation.Kind {
		case storage.OperationKindPut:
			encoded, err := json.Marshal(storage.CommittedObject{
				Value:    operation.Value,
				Metadata: operation.Metadata,
			})
			if err != nil {
				return fmt.Errorf("err in json.Marshal(committed put): %w", err)
			}
			if err := txn.Set(committedKey(operation.Slot, operation.Key), encoded); err != nil {
				return fmt.Errorf("err in txn.Set(committed put): %w", err)
			}
		case storage.OperationKindDelete:
			if err := txn.Delete(committedKey(operation.Slot, operation.Key)); err != nil {
				return fmt.Errorf("err in txn.Delete(committed delete): %w", err)
			}
		default:
			return fmt.Errorf("%w: unsupported operation kind %q", storage.ErrInvalidConfig, operation.Kind)
		}
		if err := txn.Delete(stagedKey(operation.Slot, operation.Sequence)); err != nil {
			return fmt.Errorf("err in txn.Delete(staged op): %w", err)
		}
		if err := txn.Set(replicaMetaKey(operation.Slot), encodeUint64(operation.Sequence)); err != nil {
			return fmt.Errorf("err in txn.Set(replica meta): %w", err)
		}
		if persisted != nil {
			encoded, err := json.Marshal(*persisted)
			if err != nil {
				return fmt.Errorf("err in json.Marshal(local replica): %w", err)
			}
			if err := txn.Set(localReplicaKey(nodeID, persisted.Assignment.Slot), encoded); err != nil {
				return fmt.Errorf("err in txn.Set(local replica): %w", err)
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(apply committed): %w", err)
	}
	return nil
}

func (b *Backend) StagePut(slot int, sequence uint64, key string, value string, metadata storage.ObjectMetadata) error {
	return b.stageOperation(slot, sequence, stagedValue{
		Kind:     storage.OperationKindPut,
		Key:      key,
		Value:    value,
		Metadata: metadata,
	})
}

func (b *Backend) StageDelete(slot int, sequence uint64, key string, metadata storage.ObjectMetadata) error {
	return b.stageOperation(slot, sequence, stagedValue{
		Kind:     storage.OperationKindDelete,
		Key:      key,
		Metadata: metadata,
	})
}

func (b *Backend) CommitSequence(slot int, sequence uint64) error {
	highestCommitted, err := b.HighestCommittedSequence(slot)
	if err != nil {
		return err
	}
	if sequence != highestCommitted+1 {
		return fmt.Errorf(
			"%w: slot %d expected commit sequence %d, got %d",
			storage.ErrSequenceMismatch,
			slot,
			highestCommitted+1,
			sequence,
		)
	}
	operation, err := b.loadStagedOperation(slot, sequence)
	if err != nil {
		return err
	}

	if err := b.owner.writeSync(opCommitSequence, func(txn *badgerdb.Txn) error {
		switch operation.Kind {
		case storage.OperationKindPut:
			encoded, err := json.Marshal(storage.CommittedObject{
				Value:    operation.Value,
				Metadata: operation.Metadata,
			})
			if err != nil {
				return fmt.Errorf("err in json.Marshal(committed put): %w", err)
			}
			if err := txn.Set(committedKey(slot, operation.Key), encoded); err != nil {
				return fmt.Errorf("err in txn.Set(committed put): %w", err)
			}
		case storage.OperationKindDelete:
			if err := txn.Delete(committedKey(slot, operation.Key)); err != nil {
				return fmt.Errorf("err in txn.Delete(committed delete): %w", err)
			}
		default:
			return fmt.Errorf("%w: unsupported operation kind %q", storage.ErrInvalidConfig, operation.Kind)
		}
		if err := txn.Delete(stagedKey(slot, sequence)); err != nil {
			return fmt.Errorf("err in txn.Delete(staged op): %w", err)
		}
		if err := txn.Set(replicaMetaKey(slot), encodeUint64(sequence)); err != nil {
			return fmt.Errorf("err in txn.Set(replica meta): %w", err)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(commit sequence): %w", err)
	}
	return nil
}

func (b *Backend) CommittedSnapshot(slot int) (storage.Snapshot, error) {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return nil, err
	}
	snapshot := storage.Snapshot{}
	err := b.owner.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := committedPrefix(slot)
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			key := string(item.KeyCopy(nil)[len(prefix):])
			value, err := item.ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("err in item.ValueCopy(committed snapshot): %w", err)
			}
			var object storage.CommittedObject
			if err := json.Unmarshal(value, &object); err != nil {
				return fmt.Errorf("err in json.Unmarshal(committed snapshot): %w", err)
			}
			snapshot[key] = object
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (b *Backend) GetCommitted(slot int, key string) (storage.CommittedObject, bool, error) {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return storage.CommittedObject{}, false, err
	}
	value, found, err := b.owner.get(committedKey(slot, key))
	if err != nil {
		return storage.CommittedObject{}, false, fmt.Errorf("err in db.Get(committed): %w", err)
	}
	if !found {
		return storage.CommittedObject{}, false, nil
	}
	var object storage.CommittedObject
	if err := json.Unmarshal(value, &object); err != nil {
		return storage.CommittedObject{}, false, fmt.Errorf("err in json.Unmarshal(committed): %w", err)
	}
	return object, true, nil
}

func (b *Backend) HighestCommittedSequence(slot int) (uint64, error) {
	value, found, err := b.owner.get(replicaMetaKey(slot))
	if err != nil {
		return 0, fmt.Errorf("err in db.Get(replica meta): %w", err)
	}
	if !found {
		return 0, fmt.Errorf("%w: slot %d", storage.ErrUnknownReplica, slot)
	}
	return decodeUint64(value)
}

func (b *Backend) StagedSequences(slot int) ([]uint64, error) {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return nil, err
	}
	sequences := make([]uint64, 0)
	err := b.owner.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := stagedPrefix(slot)
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			sequence, err := decodeSequence(iter.Item().KeyCopy(nil)[len(prefix):])
			if err != nil {
				return err
			}
			sequences = append(sequences, sequence)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	return sequences, nil
}

func (l *LocalStore) LoadNode(_ context.Context, nodeID string) (storage.PersistedNodeState, error) {
	state := storage.PersistedNodeState{
		NodeID:   nodeID,
		Replicas: make([]storage.PersistedReplica, 0),
	}
	if value, found, err := l.owner.get(localNodeMetaKey(nodeID)); err != nil {
		return storage.PersistedNodeState{}, fmt.Errorf("err in db.Get(local node meta): %w", err)
	} else if found && len(value) == 8 {
		state.HighestAcceptedCoordinatorEpoch = binary.BigEndian.Uint64(value)
	}

	err := l.owner.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := localReplicaPrefix(nodeID)
		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			value, err := iter.Item().ValueCopy(nil)
			if err != nil {
				return fmt.Errorf("err in item.ValueCopy(local replica): %w", err)
			}
			var replica storage.PersistedReplica
			if err := json.Unmarshal(value, &replica); err != nil {
				return fmt.Errorf("err in json.Unmarshal(local replica): %w", err)
			}
			state.Replicas = append(state.Replicas, replica)
		}
		return nil
	})
	if err != nil {
		return storage.PersistedNodeState{}, err
	}
	sort.Slice(state.Replicas, func(i, j int) bool {
		return state.Replicas[i].Assignment.Slot < state.Replicas[j].Assignment.Slot
	})
	return state, nil
}

func (l *LocalStore) UpsertReplica(_ context.Context, nodeID string, replica storage.PersistedReplica) error {
	encoded, err := json.Marshal(replica)
	if err != nil {
		return fmt.Errorf("err in json.Marshal(local replica): %w", err)
	}
	if err := l.owner.writeSync(opUpsertLocalReplica, func(txn *badgerdb.Txn) error {
		return txn.Set(localReplicaKey(nodeID, replica.Assignment.Slot), encoded)
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(local replica): %w", err)
	}
	return nil
}

func (l *LocalStore) DeleteReplica(_ context.Context, nodeID string, slot int) error {
	if err := l.owner.writeSync(opDeleteLocalReplica, func(txn *badgerdb.Txn) error {
		return txn.Delete(localReplicaKey(nodeID, slot))
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(local replica delete): %w", err)
	}
	return nil
}

func (l *LocalStore) SetHighestAcceptedCoordinatorEpoch(_ context.Context, nodeID string, epoch uint64) error {
	if err := l.owner.writeSync(opSetLocalNodeMeta, func(txn *badgerdb.Txn) error {
		return txn.Set(localNodeMetaKey(nodeID), encodeUint64(epoch))
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(local node meta): %w", err)
	}
	return nil
}

func (b *Backend) stageOperation(slot int, sequence uint64, operation stagedValue) error {
	if _, err := b.HighestCommittedSequence(slot); err != nil {
		return err
	}
	if _, err := b.loadStagedOperation(slot, sequence); err == nil {
		return fmt.Errorf("%w: slot %d sequence %d already staged", storage.ErrSequenceMismatch, slot, sequence)
	} else if !errors.Is(err, storage.ErrSequenceMismatch) {
		return err
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("err in json.Marshal(staged op): %w", err)
	}
	if err := b.owner.writeSync("stage_operation", func(txn *badgerdb.Txn) error {
		return txn.Set(stagedKey(slot, sequence), encoded)
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(staged op): %w", err)
	}
	return nil
}

func (b *Backend) loadStagedOperation(slot int, sequence uint64) (stagedValue, error) {
	value, found, err := b.owner.get(stagedKey(slot, sequence))
	if err != nil {
		return stagedValue{}, fmt.Errorf("err in db.Get(staged op): %w", err)
	}
	if !found {
		return stagedValue{}, fmt.Errorf("%w: slot %d sequence %d is not staged", storage.ErrSequenceMismatch, slot, sequence)
	}
	var operation stagedValue
	if err := json.Unmarshal(value, &operation); err != nil {
		return stagedValue{}, fmt.Errorf("err in json.Unmarshal(staged op): %w", err)
	}
	return operation, nil
}

func (o *owner) cleanupStagedOperationsOnOpen() error {
	hasStaged := false
	err := o.db.View(func(txn *badgerdb.Txn) error {
		opts := badgerdb.DefaultIteratorOptions
		opts.PrefetchValues = false
		iter := txn.NewIterator(opts)
		defer iter.Close()

		prefix := []byte{keyStagedOp}
		iter.Seek(prefix)
		hasStaged = iter.ValidForPrefix(prefix)
		return nil
	})
	if err != nil {
		return err
	}
	if !hasStaged {
		return nil
	}
	if err := o.writeSync(opCleanupStagedOnOpen, func(txn *badgerdb.Txn) error {
		return deletePrefixTxn(txn, []byte{keyStagedOp})
	}); err != nil {
		return fmt.Errorf("err in owner.writeSync(staged cleanup): %w", err)
	}
	return nil
}

func (o *owner) writeSync(op string, fn func(txn *badgerdb.Txn) error) error {
	if err := runBeforeDurableWrite(op); err != nil {
		return err
	}
	return o.db.Update(fn)
}

func (o *owner) get(key []byte) ([]byte, bool, error) {
	var value []byte
	err := o.db.View(func(txn *badgerdb.Txn) error {
		item, err := txn.Get(key)
		if errors.Is(err, badgerdb.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	if value == nil {
		return nil, false, nil
	}
	return value, true, nil
}

func deletePrefixTxn(txn *badgerdb.Txn, prefix []byte) error {
	opts := badgerdb.DefaultIteratorOptions
	opts.PrefetchValues = false
	iter := txn.NewIterator(opts)
	defer iter.Close()

	keys := make([][]byte, 0)
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		keys = append(keys, iter.Item().KeyCopy(nil))
	}
	for _, key := range keys {
		if err := txn.Delete(key); err != nil {
			return fmt.Errorf("err in txn.Delete(prefix entry): %w", err)
		}
	}
	return nil
}

func runBeforeDurableWrite(op string) error {
	testHooks.mu.Lock()
	hook := testHooks.beforeDurableWrite
	testHooks.mu.Unlock()
	if hook == nil {
		return nil
	}
	return hook(op)
}

func setBeforeDurableWriteForTest(hook func(op string) error) func() {
	testHooks.mu.Lock()
	prev := testHooks.beforeDurableWrite
	testHooks.beforeDurableWrite = hook
	testHooks.mu.Unlock()
	return func() {
		testHooks.mu.Lock()
		testHooks.beforeDurableWrite = prev
		testHooks.mu.Unlock()
	}
}

func replicaMetaKey(slot int) []byte {
	return append([]byte{keyReplicaMeta}, encodeSlot(slot)...)
}

func committedPrefix(slot int) []byte {
	return append([]byte{keyCommittedData}, encodeSlot(slot)...)
}

func committedKey(slot int, key string) []byte {
	return append(committedPrefix(slot), []byte(key)...)
}

func stagedPrefix(slot int) []byte {
	return append([]byte{keyStagedOp}, encodeSlot(slot)...)
}

func stagedKey(slot int, sequence uint64) []byte {
	return append(stagedPrefix(slot), encodeUint64(sequence)...)
}

func localReplicaPrefix(nodeID string) []byte {
	prefix := []byte{keyLocalReplica}
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(nodeID)))
	prefix = append(prefix, length...)
	prefix = append(prefix, []byte(nodeID)...)
	return prefix
}

func localReplicaKey(nodeID string, slot int) []byte {
	return append(localReplicaPrefix(nodeID), encodeSlot(slot)...)
}

func localNodeMetaKey(nodeID string) []byte {
	prefix := []byte{keyLocalNodeMeta}
	length := make([]byte, 4)
	binary.BigEndian.PutUint32(length, uint32(len(nodeID)))
	prefix = append(prefix, length...)
	prefix = append(prefix, []byte(nodeID)...)
	return prefix
}

func encodeSlot(slot int) []byte {
	return encodeUint64(uint64(slot))
}

func encodeUint64(value uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, value)
	return encoded
}

func decodeUint64(value []byte) (uint64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("%w: invalid uint64 length %d", storage.ErrStateMismatch, len(value))
	}
	return binary.BigEndian.Uint64(value), nil
}

func decodeSequence(value []byte) (uint64, error) {
	return decodeUint64(value)
}

func sortedSnapshotKeys(snapshot storage.Snapshot) []string {
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
