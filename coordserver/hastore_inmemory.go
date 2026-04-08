package coordserver

import (
	"context"
	"sync"
	"time"
)

type InMemoryHAStore struct {
	mu          sync.RWMutex
	lease       LeaderLease
	hasLease    bool
	snapshot    HASnapshot
	snapshotSet bool
}

func NewInMemoryHAStore() *InMemoryHAStore {
	return &InMemoryHAStore{}
}

func (s *InMemoryHAStore) CurrentLease(_ context.Context) (LeaderLease, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneLeaderLease(s.lease), s.hasLease, nil
}

func (s *InMemoryHAStore) AcquireOrRenew(_ context.Context, holderID string, holderEndpoint string, now time.Time, ttl time.Duration) (LeaderLease, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasLease {
		s.lease = LeaderLease{
			HolderID:          holderID,
			HolderEndpoint:    holderEndpoint,
			Epoch:             1,
			ExpiresAtUnixNano: now.Add(ttl).UnixNano(),
		}
		s.hasLease = true
		return cloneLeaderLease(s.lease), true, nil
	}

	currentExpired := now.UnixNano() >= s.lease.ExpiresAtUnixNano
	switch {
	case s.lease.HolderID == holderID && !currentExpired:
		s.lease.HolderEndpoint = holderEndpoint
		s.lease.ExpiresAtUnixNano = now.Add(ttl).UnixNano()
		return cloneLeaderLease(s.lease), true, nil
	case currentExpired:
		s.lease = LeaderLease{
			HolderID:          holderID,
			HolderEndpoint:    holderEndpoint,
			Epoch:             s.lease.Epoch + 1,
			ExpiresAtUnixNano: now.Add(ttl).UnixNano(),
		}
		return cloneLeaderLease(s.lease), true, nil
	default:
		return cloneLeaderLease(s.lease), false, nil
	}
}

func (s *InMemoryHAStore) LoadSnapshot(_ context.Context) (HASnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.snapshotSet {
		return zeroHASnapshot(), nil
	}
	return cloneHASnapshot(s.snapshot), nil
}

func (s *InMemoryHAStore) SaveSnapshot(_ context.Context, lease LeaderLease, now time.Time, expectedSnapshotVersion uint64, snapshot HASnapshot) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasLease || s.lease.HolderID != lease.HolderID || s.lease.Epoch != lease.Epoch || now.UnixNano() >= s.lease.ExpiresAtUnixNano {
		return 0, ErrNotLeader
	}
	currentVersion := uint64(0)
	if s.snapshotSet {
		currentVersion = s.snapshot.SnapshotVersion
	}
	if currentVersion != expectedSnapshotVersion {
		return 0, ErrHASnapshotConflict
	}
	cloned := cloneHASnapshot(snapshot)
	cloned.SnapshotVersion = expectedSnapshotVersion + 1
	s.snapshot = cloned
	s.snapshotSet = true
	return cloned.SnapshotVersion, nil
}

func (s *InMemoryHAStore) Close() error {
	return nil
}
