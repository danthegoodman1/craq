package storage

import "sort"

type publishedReplicaSnapshot struct {
	assignment               ReplicaAssignment
	state                    ReplicaState
	lastKnownState           ReplicaState
	highestCommittedSequence uint64
	localDataPresent         bool
	inFlightClientWrites     int
	bufferedForwards         int
	bufferedCommits          int
	dirtyKeyCount            int
}

func publishedReplicaFromRecord(record replicaRecord) publishedReplicaSnapshot {
	record = ensureProtocolReplicaState(record)
	return publishedReplicaSnapshot{
		assignment:               cloneAssignment(record.assignment),
		state:                    record.state,
		lastKnownState:           record.lastKnownState,
		highestCommittedSequence: record.highestCommittedSequence,
		localDataPresent:         record.localDataPresent,
		inFlightClientWrites:     record.inFlightClientWrites,
		bufferedForwards:         len(record.bufferedForwards),
		bufferedCommits:          len(record.bufferedCommits),
		dirtyKeyCount:            dirtyKeyCount(record),
	}
}

func replicaRecordFromPublished(snapshot publishedReplicaSnapshot) replicaRecord {
	return ensureProtocolReplicaState(replicaRecord{
		assignment:               cloneAssignment(snapshot.assignment),
		state:                    snapshot.state,
		nextSequence:             snapshot.highestCommittedSequence + 1,
		highestCommittedSequence: snapshot.highestCommittedSequence,
		localDataPresent:         snapshot.localDataPresent,
		lastKnownState:           snapshot.lastKnownState,
		inFlightClientWrites:     snapshot.inFlightClientWrites,
	})
}

func (s publishedReplicaSnapshot) bufferedReplicaMessages() int {
	return s.bufferedForwards + s.bufferedCommits
}

func (n *Node) publishedReplicaSnapshot(slot int) (publishedReplicaSnapshot, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	snapshot, ok := n.publishedReplicas[slot]
	return snapshot, ok
}

func (n *Node) publishedReplicaMapSnapshot() map[int]publishedReplicaSnapshot {
	n.mu.RLock()
	defer n.mu.RUnlock()
	cloned := make(map[int]publishedReplicaSnapshot, len(n.publishedReplicas))
	for slot, snapshot := range n.publishedReplicas {
		cloned[slot] = snapshot
	}
	return cloned
}

func (n *Node) publishReplicaRecord(slot int, record replicaRecord) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.publishReplicaSnapshotLocked(slot, publishedReplicaFromRecord(record))
}

func (n *Node) deletePublishedReplica(slot int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.deletePublishedReplicaLocked(slot)
}

func (n *Node) publishReplicaSnapshotLocked(slot int, snapshot publishedReplicaSnapshot) {
	var before *publishedReplicaSnapshot
	if existing, ok := n.publishedReplicas[slot]; ok {
		cloned := existing
		before = &cloned
	}
	n.applyPublishedReplicaStatsLocked(slot, before, &snapshot)
	n.publishedReplicas[slot] = snapshot
}

func (n *Node) deletePublishedReplicaLocked(slot int) {
	existing, ok := n.publishedReplicas[slot]
	if !ok {
		return
	}
	n.applyPublishedReplicaStatsLocked(slot, &existing, nil)
	delete(n.publishedReplicas, slot)
}

func (n *Node) applyPublishedReplicaStatsLocked(slot int, before *publishedReplicaSnapshot, after *publishedReplicaSnapshot) {
	if before != nil {
		n.publishedReplicaCount--
		n.publishedBufferedReplicaMessages -= before.bufferedReplicaMessages()
		switch before.state {
		case ReplicaStateActive:
			n.publishedActiveCount--
		case ReplicaStateCatchingUp:
			n.publishedCatchingUpCount--
			delete(n.publishedCatchingUpSlots, slot)
		case ReplicaStateLeaving:
			n.publishedLeavingCount--
			delete(n.publishedLeavingSlots, slot)
		}
	}
	if after != nil {
		n.publishedReplicaCount++
		n.publishedBufferedReplicaMessages += after.bufferedReplicaMessages()
		switch after.state {
		case ReplicaStateActive:
			n.publishedActiveCount++
		case ReplicaStateCatchingUp:
			n.publishedCatchingUpCount++
			n.publishedCatchingUpSlots[slot] = struct{}{}
		case ReplicaStateLeaving:
			n.publishedLeavingCount++
			n.publishedLeavingSlots[slot] = struct{}{}
		}
	}
}

func sortedPublishedReplicaSlots(replicas map[int]publishedReplicaSnapshot) []int {
	slots := make([]int, 0, len(replicas))
	for slot := range replicas {
		slots = append(slots, slot)
	}
	sort.Ints(slots)
	return slots
}
