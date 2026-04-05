package coordserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type PostgresHAStore struct {
	db *sql.DB
}

func OpenPostgresHAStore(ctx context.Context, dsn string) (*PostgresHAStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres ha store: %w", err)
	}
	store := &PostgresHAStore{db: db}
	if err := store.initSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func NewPostgresHAStore(db *sql.DB) *PostgresHAStore {
	return &PostgresHAStore{db: db}
}

func (s *PostgresHAStore) initSchema(ctx context.Context) error {
	if s.db == nil {
		return fmt.Errorf("postgres ha store requires db")
	}
	const schema = `
CREATE TABLE IF NOT EXISTS craq_coord_ha_lease (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  holder_id TEXT NOT NULL,
  holder_endpoint TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  expires_at_unix_nano BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS craq_coord_ha_snapshot (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  snapshot_version BIGINT NOT NULL,
  payload JSONB NOT NULL
);`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("init postgres ha schema: %w", err)
	}
	return nil
}

func (s *PostgresHAStore) CurrentLease(ctx context.Context) (LeaderLease, bool, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT holder_id, holder_endpoint, epoch, expires_at_unix_nano
FROM craq_coord_ha_lease
WHERE id = 1`)
	var lease LeaderLease
	switch err := row.Scan(&lease.HolderID, &lease.HolderEndpoint, &lease.Epoch, &lease.ExpiresAtUnixNano); err {
	case nil:
		return lease, true, nil
	case sql.ErrNoRows:
		return LeaderLease{}, false, nil
	default:
		return LeaderLease{}, false, fmt.Errorf("query current lease: %w", err)
	}
}

func (s *PostgresHAStore) AcquireOrRenew(ctx context.Context, holderID string, holderEndpoint string, now time.Time, ttl time.Duration) (LeaderLease, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return LeaderLease{}, false, fmt.Errorf("begin acquire lease tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	current, hasLease, err := selectLeaseForUpdate(ctx, tx)
	if err != nil {
		return LeaderLease{}, false, err
	}
	expiresAt := now.Add(ttl).UnixNano()
	if !hasLease {
		lease := LeaderLease{
			HolderID:          holderID,
			HolderEndpoint:    holderEndpoint,
			Epoch:             1,
			ExpiresAtUnixNano: expiresAt,
		}
		if _, err := tx.ExecContext(ctx, `
INSERT INTO craq_coord_ha_lease (id, holder_id, holder_endpoint, epoch, expires_at_unix_nano)
VALUES (1, $1, $2, $3, $4)`,
			lease.HolderID, lease.HolderEndpoint, lease.Epoch, lease.ExpiresAtUnixNano,
		); err != nil {
			return LeaderLease{}, false, fmt.Errorf("insert initial lease: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return LeaderLease{}, false, fmt.Errorf("commit initial lease: %w", err)
		}
		return lease, true, nil
	}

	currentExpired := now.UnixNano() >= current.ExpiresAtUnixNano
	switch {
	case current.HolderID == holderID && !currentExpired:
		current.HolderEndpoint = holderEndpoint
		current.ExpiresAtUnixNano = expiresAt
	case currentExpired:
		current = LeaderLease{
			HolderID:          holderID,
			HolderEndpoint:    holderEndpoint,
			Epoch:             current.Epoch + 1,
			ExpiresAtUnixNano: expiresAt,
		}
	default:
		if err := tx.Commit(); err != nil {
			return LeaderLease{}, false, fmt.Errorf("commit observing lease: %w", err)
		}
		return current, false, nil
	}

	if _, err := tx.ExecContext(ctx, `
UPDATE craq_coord_ha_lease
SET holder_id = $1, holder_endpoint = $2, epoch = $3, expires_at_unix_nano = $4
WHERE id = 1`,
		current.HolderID, current.HolderEndpoint, current.Epoch, current.ExpiresAtUnixNano,
	); err != nil {
		return LeaderLease{}, false, fmt.Errorf("update lease: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return LeaderLease{}, false, fmt.Errorf("commit lease update: %w", err)
	}
	return current, true, nil
}

func (s *PostgresHAStore) LoadSnapshot(ctx context.Context) (HASnapshot, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT snapshot_version, payload
FROM craq_coord_ha_snapshot
WHERE id = 1`)
	var (
		version uint64
		payload []byte
	)
	switch err := row.Scan(&version, &payload); err {
	case nil:
	case sql.ErrNoRows:
		return zeroHASnapshot(), nil
	default:
		return HASnapshot{}, fmt.Errorf("query ha snapshot: %w", err)
	}
	snapshot := zeroHASnapshot()
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return HASnapshot{}, fmt.Errorf("unmarshal ha snapshot: %w", err)
	}
	snapshot = normalizeHASnapshot(snapshot)
	snapshot.SnapshotVersion = version
	return snapshot, nil
}

func (s *PostgresHAStore) SaveSnapshot(ctx context.Context, lease LeaderLease, now time.Time, expectedSnapshotVersion uint64, snapshot HASnapshot) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin save snapshot tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	currentLease, hasLease, err := selectLeaseForUpdate(ctx, tx)
	if err != nil {
		return 0, err
	}
	if !hasLease ||
		currentLease.HolderID != lease.HolderID ||
		currentLease.Epoch != lease.Epoch ||
		now.UnixNano() >= currentLease.ExpiresAtUnixNano {
		return 0, ErrNotLeader
	}

	currentVersion, hasSnapshot, err := selectSnapshotVersionForUpdate(ctx, tx)
	if err != nil {
		return 0, err
	}
	if !hasSnapshot {
		currentVersion = 0
	}
	if currentVersion != expectedSnapshotVersion {
		return 0, ErrHASnapshotConflict
	}

	nextVersion := expectedSnapshotVersion + 1
	cloned := normalizeHASnapshot(snapshot)
	cloned.SnapshotVersion = 0
	payload, err := json.Marshal(cloned)
	if err != nil {
		return 0, fmt.Errorf("marshal ha snapshot: %w", err)
	}

	if hasSnapshot {
		if _, err := tx.ExecContext(ctx, `
UPDATE craq_coord_ha_snapshot
SET snapshot_version = $1, payload = $2
WHERE id = 1`,
			nextVersion, payload,
		); err != nil {
			return 0, fmt.Errorf("update ha snapshot: %w", err)
		}
	} else {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO craq_coord_ha_snapshot (id, snapshot_version, payload)
VALUES (1, $1, $2)`,
			nextVersion, payload,
		); err != nil {
			return 0, fmt.Errorf("insert ha snapshot: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit ha snapshot: %w", err)
	}
	return nextVersion, nil
}

func (s *PostgresHAStore) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *PostgresHAStore) Reset(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
DELETE FROM craq_coord_ha_snapshot;
DELETE FROM craq_coord_ha_lease;`,
	); err != nil {
		return fmt.Errorf("reset postgres ha store: %w", err)
	}
	return nil
}

func selectLeaseForUpdate(ctx context.Context, tx *sql.Tx) (LeaderLease, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT holder_id, holder_endpoint, epoch, expires_at_unix_nano
FROM craq_coord_ha_lease
WHERE id = 1
FOR UPDATE`)
	var lease LeaderLease
	switch err := row.Scan(&lease.HolderID, &lease.HolderEndpoint, &lease.Epoch, &lease.ExpiresAtUnixNano); err {
	case nil:
		return lease, true, nil
	case sql.ErrNoRows:
		return LeaderLease{}, false, nil
	default:
		return LeaderLease{}, false, fmt.Errorf("select lease for update: %w", err)
	}
}

func selectSnapshotVersionForUpdate(ctx context.Context, tx *sql.Tx) (uint64, bool, error) {
	row := tx.QueryRowContext(ctx, `
SELECT snapshot_version
FROM craq_coord_ha_snapshot
WHERE id = 1
FOR UPDATE`)
	var version uint64
	switch err := row.Scan(&version); err {
	case nil:
		return version, true, nil
	case sql.ErrNoRows:
		return 0, false, nil
	default:
		return 0, false, fmt.Errorf("select snapshot version for update: %w", err)
	}
}
