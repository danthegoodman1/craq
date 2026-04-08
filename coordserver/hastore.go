package coordserver

import (
	"context"
	"errors"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/storage"
)

var (
	ErrLeaseHeld               = errors.New("coordinator ha lease is held by another coordinator")
	ErrHASnapshotConflict      = errors.New("coordinator ha snapshot version conflict")
	ErrUnknownOutboxCommand    = errors.New("unknown coordinator ha outbox command")
	ErrInvalidOutboxTransition = errors.New("invalid coordinator ha outbox transition")
)

type LeaderLease struct {
	HolderID          string
	HolderEndpoint    string
	Epoch             uint64
	ExpiresAtUnixNano int64
}

type OutboxCommandKind string

const (
	OutboxCommandAddReplicaAsTail   OutboxCommandKind = "add_replica_as_tail"
	OutboxCommandUpdateChainPeers   OutboxCommandKind = "update_chain_peers"
	OutboxCommandMarkReplicaLeaving OutboxCommandKind = "mark_replica_leaving"
	OutboxCommandResumeRecovered    OutboxCommandKind = "resume_recovered_replica"
	OutboxCommandRecoverReplica     OutboxCommandKind = "recover_replica"
	OutboxCommandDropRecovered      OutboxCommandKind = "drop_recovered_replica"
)

type OutboxEntry struct {
	ID           string
	Epoch        uint64
	NodeID       string
	Slot         int
	SlotVersion  uint64
	CommandID    string
	Kind         OutboxCommandKind
	Assignment   *storage.ReplicaAssignment
	SourceNodeID string
}

type HASnapshot struct {
	SnapshotVersion     uint64
	State               coordruntime.State
	Pending             map[int]PendingWork
	Heartbeats          map[string]storage.NodeStatus
	Liveness            map[string]coordruntime.NodeLivenessRecord
	LastPolicy          coordinator.ReconfigurationPolicy
	UnavailableReplicas map[string]map[int]bool
	LastRecoveryReports map[string]storage.NodeRecoveryReport
	Outbox              []OutboxEntry
}

type HAStore interface {
	CurrentLease(ctx context.Context) (LeaderLease, bool, error)
	AcquireOrRenew(ctx context.Context, holderID string, holderEndpoint string, now time.Time, ttl time.Duration) (LeaderLease, bool, error)
	LoadSnapshot(ctx context.Context) (HASnapshot, error)
	SaveSnapshot(ctx context.Context, lease LeaderLease, now time.Time, expectedSnapshotVersion uint64, snapshot HASnapshot) (uint64, error)
	Close() error
}

func zeroHASnapshot() HASnapshot {
	return HASnapshot{
		State:               coordruntime.State{SlotVersions: map[int]uint64{}, CompletedProgressBySlot: map[int][]coordruntime.CompletedProgressRecord{}, NodeLivenessByID: map[string]coordruntime.NodeLivenessRecord{}, AppliedCommands: map[string]coordruntime.AppliedCommand{}},
		Pending:             map[int]PendingWork{},
		Heartbeats:          map[string]storage.NodeStatus{},
		Liveness:            map[string]coordruntime.NodeLivenessRecord{},
		UnavailableReplicas: map[string]map[int]bool{},
		LastRecoveryReports: map[string]storage.NodeRecoveryReport{},
		Outbox:              []OutboxEntry{},
	}
}

func normalizeHASnapshot(snapshot HASnapshot) HASnapshot {
	normalized := snapshot
	if normalized.State.SlotVersions == nil {
		normalized.State.SlotVersions = map[int]uint64{}
	}
	if normalized.State.CompletedProgressBySlot == nil {
		normalized.State.CompletedProgressBySlot = map[int][]coordruntime.CompletedProgressRecord{}
	}
	if normalized.State.NodeLivenessByID == nil {
		normalized.State.NodeLivenessByID = map[string]coordruntime.NodeLivenessRecord{}
	}
	if normalized.State.AppliedCommands == nil {
		normalized.State.AppliedCommands = map[string]coordruntime.AppliedCommand{}
	}
	if normalized.Pending == nil {
		normalized.Pending = map[int]PendingWork{}
	}
	if normalized.Heartbeats == nil {
		normalized.Heartbeats = map[string]storage.NodeStatus{}
	}
	if normalized.Liveness == nil {
		normalized.Liveness = map[string]coordruntime.NodeLivenessRecord{}
	}
	if normalized.UnavailableReplicas == nil {
		normalized.UnavailableReplicas = map[string]map[int]bool{}
	}
	if normalized.LastRecoveryReports == nil {
		normalized.LastRecoveryReports = map[string]storage.NodeRecoveryReport{}
	}
	if normalized.Outbox == nil {
		normalized.Outbox = []OutboxEntry{}
	}
	return normalized
}

func cloneLeaderLease(lease LeaderLease) LeaderLease {
	return LeaderLease{
		HolderID:          lease.HolderID,
		HolderEndpoint:    lease.HolderEndpoint,
		Epoch:             lease.Epoch,
		ExpiresAtUnixNano: lease.ExpiresAtUnixNano,
	}
}

func cloneHASnapshot(snapshot HASnapshot) HASnapshot {
	if snapshot.State.SlotVersions == nil &&
		snapshot.State.CompletedProgressBySlot == nil &&
		snapshot.State.NodeLivenessByID == nil &&
		snapshot.State.AppliedCommands == nil &&
		snapshot.Pending == nil &&
		snapshot.Heartbeats == nil &&
		snapshot.Liveness == nil &&
		snapshot.UnavailableReplicas == nil &&
		snapshot.LastRecoveryReports == nil &&
		snapshot.Outbox == nil {
		snapshot = zeroHASnapshot()
	} else {
		snapshot = normalizeHASnapshot(snapshot)
	}
	cloned := HASnapshot{
		SnapshotVersion:     snapshot.SnapshotVersion,
		State:               snapshot.State,
		Pending:             make(map[int]PendingWork, len(snapshot.Pending)),
		Heartbeats:          make(map[string]storage.NodeStatus, len(snapshot.Heartbeats)),
		Liveness:            make(map[string]coordruntime.NodeLivenessRecord, len(snapshot.Liveness)),
		LastPolicy:          snapshot.LastPolicy,
		UnavailableReplicas: make(map[string]map[int]bool, len(snapshot.UnavailableReplicas)),
		LastRecoveryReports: make(map[string]storage.NodeRecoveryReport, len(snapshot.LastRecoveryReports)),
		Outbox:              make([]OutboxEntry, 0, len(snapshot.Outbox)),
	}
	cloned.State = coordruntime.OpenInMemoryFromState(snapshot.State).Current()
	for slot, pending := range snapshot.Pending {
		cloned.Pending[slot] = pending
	}
	for nodeID, status := range snapshot.Heartbeats {
		cloned.Heartbeats[nodeID] = cloneNodeStatus(status)
	}
	for nodeID, record := range snapshot.Liveness {
		cloned.Liveness[nodeID] = cloneLivenessRecord(record)
	}
	for nodeID, slots := range snapshot.UnavailableReplicas {
		clonedSlots := make(map[int]bool, len(slots))
		for slot, unavailable := range slots {
			clonedSlots[slot] = unavailable
		}
		cloned.UnavailableReplicas[nodeID] = clonedSlots
	}
	for nodeID, report := range snapshot.LastRecoveryReports {
		cloned.LastRecoveryReports[nodeID] = cloneRecoveryReport(report)
	}
	for _, entry := range snapshot.Outbox {
		cloned.Outbox = append(cloned.Outbox, cloneOutboxEntry(entry))
	}
	return cloned
}

func cloneOutboxEntry(entry OutboxEntry) OutboxEntry {
	cloned := OutboxEntry{
		ID:           entry.ID,
		Epoch:        entry.Epoch,
		NodeID:       entry.NodeID,
		Slot:         entry.Slot,
		SlotVersion:  entry.SlotVersion,
		CommandID:    entry.CommandID,
		Kind:         entry.Kind,
		SourceNodeID: entry.SourceNodeID,
	}
	if entry.Assignment != nil {
		assignment := cloneReplicaAssignment(*entry.Assignment)
		cloned.Assignment = &assignment
	}
	return cloned
}

func cloneReplicaAssignment(assignment storage.ReplicaAssignment) storage.ReplicaAssignment {
	return storage.ReplicaAssignment{
		Slot:         assignment.Slot,
		ChainVersion: assignment.ChainVersion,
		Role:         assignment.Role,
		Peers: storage.ChainPeers{
			PredecessorNodeID: assignment.Peers.PredecessorNodeID,
			PredecessorTarget: assignment.Peers.PredecessorTarget,
			SuccessorNodeID:   assignment.Peers.SuccessorNodeID,
			SuccessorTarget:   assignment.Peers.SuccessorTarget,
			TailNodeID:        assignment.Peers.TailNodeID,
			TailTarget:        assignment.Peers.TailTarget,
		},
	}
}
