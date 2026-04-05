package grpcx

import (
	"fmt"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	grpcproto "github.com/danthegoodman1/craq/proto/craq/v1"
	"github.com/danthegoodman1/craq/storage"
)

func protoNode(node coordinator.Node) *grpcproto.Node {
	domains := make([]*grpcproto.FailureDomain, 0, len(node.FailureDomains))
	for key, value := range node.FailureDomains {
		domains = append(domains, &grpcproto.FailureDomain{Key: key, Value: value})
	}
	return &grpcproto.Node{
		Id:             node.ID,
		RpcAddress:     node.RPCAddress,
		FailureDomains: domains,
	}
}

func fromProtoNode(node *grpcproto.Node) coordinator.Node {
	if node == nil {
		return coordinator.Node{}
	}
	domains := make(map[string]string, len(node.FailureDomains))
	for _, domain := range node.FailureDomains {
		domains[domain.Key] = domain.Value
	}
	return coordinator.Node{
		ID:             node.Id,
		RPCAddress:     node.RpcAddress,
		FailureDomains: domains,
	}
}

func protoRoutingSnapshot(snapshot coordserver.RoutingSnapshot) *grpcproto.RoutingSnapshotResponse {
	slots := make([]*grpcproto.SlotRoute, 0, len(snapshot.Slots))
	for _, slot := range snapshot.Slots {
		readReplicas := make([]*grpcproto.ReadReplicaRoute, 0, len(slot.ReadReplicas))
		for _, replica := range slot.ReadReplicas {
			readReplicas = append(readReplicas, &grpcproto.ReadReplicaRoute{
				NodeId:   replica.NodeID,
				Endpoint: replica.Endpoint,
				Role:     string(replica.Role),
			})
		}
		slots = append(slots, &grpcproto.SlotRoute{
			Slot:         int32(slot.Slot),
			ChainVersion: slot.ChainVersion,
			HeadNodeId:   slot.HeadNodeID,
			HeadEndpoint: slot.HeadEndpoint,
			TailNodeId:   slot.TailNodeID,
			TailEndpoint: slot.TailEndpoint,
			Writable:     slot.Writable,
			Readable:     slot.Readable,
			ReadReplicas: readReplicas,
		})
	}
	return &grpcproto.RoutingSnapshotResponse{
		Version:   snapshot.Version,
		SlotCount: int32(snapshot.SlotCount),
		Slots:     slots,
	}
}

func fromProtoRoutingSnapshot(snapshot *grpcproto.RoutingSnapshotResponse) coordserver.RoutingSnapshot {
	if snapshot == nil {
		return coordserver.RoutingSnapshot{}
	}
	slots := make([]coordserver.SlotRoute, 0, len(snapshot.Slots))
	for _, slot := range snapshot.Slots {
		readReplicas := make([]coordserver.ReadReplicaRoute, 0, len(slot.ReadReplicas))
		for _, replica := range slot.ReadReplicas {
			readReplicas = append(readReplicas, coordserver.ReadReplicaRoute{
				NodeID:   replica.NodeId,
				Endpoint: replica.Endpoint,
				Role:     storage.ReplicaRole(replica.Role),
			})
		}
		slots = append(slots, coordserver.SlotRoute{
			Slot:         int(slot.Slot),
			ChainVersion: slot.ChainVersion,
			HeadNodeID:   slot.HeadNodeId,
			HeadEndpoint: slot.HeadEndpoint,
			TailNodeID:   slot.TailNodeId,
			TailEndpoint: slot.TailEndpoint,
			Writable:     slot.Writable,
			Readable:     slot.Readable,
			ReadReplicas: readReplicas,
		})
	}
	return coordserver.RoutingSnapshot{
		Version:   snapshot.Version,
		SlotCount: int(snapshot.SlotCount),
		Slots:     slots,
	}
}

func protoAssignment(assignment storage.ReplicaAssignment) *grpcproto.ReplicaAssignment {
	return &grpcproto.ReplicaAssignment{
		Slot:         int32(assignment.Slot),
		ChainVersion: assignment.ChainVersion,
		Role:         string(assignment.Role),
		Peers:        protoChainPeers(assignment.Peers),
	}
}

func fromProtoAssignment(assignment *grpcproto.ReplicaAssignment) storage.ReplicaAssignment {
	if assignment == nil {
		return storage.ReplicaAssignment{}
	}
	return storage.ReplicaAssignment{
		Slot:         int(assignment.Slot),
		ChainVersion: assignment.ChainVersion,
		Role:         storage.ReplicaRole(assignment.Role),
		Peers:        fromProtoChainPeers(assignment.Peers),
	}
}

func protoChainPeers(peers storage.ChainPeers) *grpcproto.ChainPeers {
	return &grpcproto.ChainPeers{
		PredecessorNodeId: peers.PredecessorNodeID,
		PredecessorTarget: peers.PredecessorTarget,
		SuccessorNodeId:   peers.SuccessorNodeID,
		SuccessorTarget:   peers.SuccessorTarget,
		TailNodeId:        peers.TailNodeID,
		TailTarget:        peers.TailTarget,
	}
}

func fromProtoChainPeers(peers *grpcproto.ChainPeers) storage.ChainPeers {
	if peers == nil {
		return storage.ChainPeers{}
	}
	return storage.ChainPeers{
		PredecessorNodeID: peers.PredecessorNodeId,
		PredecessorTarget: peers.PredecessorTarget,
		SuccessorNodeID:   peers.SuccessorNodeId,
		SuccessorTarget:   peers.SuccessorTarget,
		TailNodeID:        peers.TailNodeId,
		TailTarget:        peers.TailTarget,
	}
}

func protoNodeStatus(status storage.NodeStatus) *grpcproto.NodeStatus {
	return &grpcproto.NodeStatus{
		NodeId:          status.NodeID,
		ReplicaCount:    int32(status.ReplicaCount),
		ActiveCount:     int32(status.ActiveCount),
		CatchingUpCount: int32(status.CatchingUpCount),
		LeavingCount:    int32(status.LeavingCount),
	}
}

func fromProtoNodeStatus(status *grpcproto.NodeStatus) storage.NodeStatus {
	if status == nil {
		return storage.NodeStatus{}
	}
	return storage.NodeStatus{
		NodeID:          status.NodeId,
		ReplicaCount:    int(status.ReplicaCount),
		ActiveCount:     int(status.ActiveCount),
		CatchingUpCount: int(status.CatchingUpCount),
		LeavingCount:    int(status.LeavingCount),
	}
}

func protoNodeRecovery(report storage.NodeRecoveryReport) *grpcproto.NodeRecoveryReport {
	replicas := make([]*grpcproto.RecoveredReplica, 0, len(report.Replicas))
	for _, replica := range report.Replicas {
		replicas = append(replicas, &grpcproto.RecoveredReplica{
			Assignment:               protoAssignment(replica.Assignment),
			LastKnownState:           string(replica.LastKnownState),
			HighestCommittedSequence: replica.HighestCommittedSequence,
			HasCommittedData:         replica.HasCommittedData,
		})
	}
	return &grpcproto.NodeRecoveryReport{
		NodeId:   report.NodeID,
		Replicas: replicas,
	}
}

func fromProtoNodeRecovery(report *grpcproto.NodeRecoveryReport) storage.NodeRecoveryReport {
	if report == nil {
		return storage.NodeRecoveryReport{}
	}
	replicas := make([]storage.RecoveredReplica, 0, len(report.Replicas))
	for _, replica := range report.Replicas {
		replicas = append(replicas, storage.RecoveredReplica{
			Assignment:               fromProtoAssignment(replica.Assignment),
			LastKnownState:           storage.ReplicaState(replica.LastKnownState),
			HighestCommittedSequence: replica.HighestCommittedSequence,
			HasCommittedData:         replica.HasCommittedData,
		})
	}
	return storage.NodeRecoveryReport{
		NodeID:   report.NodeId,
		Replicas: replicas,
	}
}

func protoServerState(state coordruntime.State) *grpcproto.ServerState {
	return &grpcproto.ServerState{Version: state.Version}
}

func fromProtoCommitResult(result *grpcproto.CommitResult) storage.CommitResult {
	if result == nil {
		return storage.CommitResult{}
	}
	return storage.CommitResult{
		Slot:     int(result.Slot),
		Sequence: result.Sequence,
		Applied:  result.Applied,
		Metadata: fromProtoObjectMetadata(result.Metadata),
	}
}

func protoCommitResult(result storage.CommitResult) *grpcproto.CommitResult {
	return &grpcproto.CommitResult{
		Slot:     int32(result.Slot),
		Sequence: result.Sequence,
		Applied:  result.Applied,
		Metadata: protoObjectMetadata(result.Metadata),
	}
}

func protoReadResult(result storage.ReadResult) *grpcproto.ReadResult {
	return &grpcproto.ReadResult{
		Slot:         int32(result.Slot),
		ChainVersion: result.ChainVersion,
		Found:        result.Found,
		Value:        result.Value,
		Metadata:     protoObjectMetadata(result.Metadata),
	}
}

func fromProtoReadResult(result *grpcproto.ReadResult) storage.ReadResult {
	if result == nil {
		return storage.ReadResult{}
	}
	return storage.ReadResult{
		Slot:         int(result.Slot),
		ChainVersion: result.ChainVersion,
		Found:        result.Found,
		Value:        result.Value,
		Metadata:     fromProtoObjectMetadata(result.Metadata),
	}
}

func protoSnapshot(snapshot storage.Snapshot) []*grpcproto.SnapshotEntry {
	entries := make([]*grpcproto.SnapshotEntry, 0, len(snapshot))
	for key, object := range snapshot {
		entries = append(entries, &grpcproto.SnapshotEntry{
			Key:      key,
			Value:    object.Value,
			Metadata: protoObjectMetadata(&object.Metadata),
		})
	}
	return entries
}

func snapshotFromProtoEntries(entries []*grpcproto.SnapshotEntry) (storage.Snapshot, error) {
	snapshot := make(storage.Snapshot, len(entries))
	for _, entry := range entries {
		if entry == nil {
			return nil, fmt.Errorf("nil snapshot entry")
		}
		metadata := fromProtoObjectMetadata(entry.Metadata)
		if metadata == nil {
			return nil, fmt.Errorf("snapshot entry %q missing metadata", entry.Key)
		}
		snapshot[entry.Key] = storage.CommittedObject{
			Value:    entry.Value,
			Metadata: *metadata,
		}
	}
	return snapshot, nil
}

func protoObjectMetadata(metadata *storage.ObjectMetadata) *grpcproto.ObjectMetadata {
	if metadata == nil {
		return nil
	}
	return &grpcproto.ObjectMetadata{
		Version:           metadata.Version,
		CreatedAtUnixNano: metadata.CreatedAt.UnixNano(),
		UpdatedAtUnixNano: metadata.UpdatedAt.UnixNano(),
	}
}

func fromProtoObjectMetadata(metadata *grpcproto.ObjectMetadata) *storage.ObjectMetadata {
	if metadata == nil {
		return nil
	}
	return &storage.ObjectMetadata{
		Version:   metadata.Version,
		CreatedAt: time.Unix(0, metadata.CreatedAtUnixNano).UTC(),
		UpdatedAt: time.Unix(0, metadata.UpdatedAtUnixNano).UTC(),
	}
}

func derefObjectMetadata(metadata *storage.ObjectMetadata) storage.ObjectMetadata {
	if metadata == nil {
		return storage.ObjectMetadata{}
	}
	return *metadata
}

func protoWriteConditions(conditions storage.WriteConditions) *grpcproto.WriteConditions {
	result := &grpcproto.WriteConditions{}
	if conditions.Exists != nil {
		result.Exists = &grpcproto.BoolCondition{Value: *conditions.Exists}
	}
	if conditions.Version != nil {
		result.Version = &grpcproto.VersionComparison{
			Operator: protoComparisonOperator(conditions.Version.Operator),
			Value:    conditions.Version.Value,
		}
	}
	if conditions.UpdatedAt != nil {
		result.UpdatedAt = &grpcproto.TimeComparison{
			Operator: protoComparisonOperator(conditions.UpdatedAt.Operator),
			UnixNano: conditions.UpdatedAt.Value.UnixNano(),
		}
	}
	if result.Exists == nil && result.Version == nil && result.UpdatedAt == nil {
		return nil
	}
	return result
}

func fromProtoWriteConditions(conditions *grpcproto.WriteConditions) storage.WriteConditions {
	if conditions == nil {
		return storage.WriteConditions{}
	}
	result := storage.WriteConditions{}
	if conditions.Exists != nil {
		value := conditions.Exists.Value
		result.Exists = &value
	}
	if conditions.Version != nil {
		result.Version = &storage.VersionComparison{
			Operator: fromProtoComparisonOperator(conditions.Version.Operator),
			Value:    conditions.Version.Value,
		}
	}
	if conditions.UpdatedAt != nil {
		result.UpdatedAt = &storage.TimeComparison{
			Operator: fromProtoComparisonOperator(conditions.UpdatedAt.Operator),
			Value:    time.Unix(0, conditions.UpdatedAt.UnixNano).UTC(),
		}
	}
	return result
}

func protoReadConsistency(consistency storage.ReadConsistency) grpcproto.ReadConsistency {
	switch storage.ReadConsistency(consistency) {
	case storage.ReadConsistencyLocalCommitted:
		return grpcproto.ReadConsistency_READ_CONSISTENCY_LOCAL_COMMITTED
	default:
		return grpcproto.ReadConsistency_READ_CONSISTENCY_LINEARIZABLE
	}
}

func fromProtoReadConsistency(consistency grpcproto.ReadConsistency) storage.ReadConsistency {
	switch consistency {
	case grpcproto.ReadConsistency_READ_CONSISTENCY_LOCAL_COMMITTED:
		return storage.ReadConsistencyLocalCommitted
	default:
		return storage.ReadConsistencyLinearizable
	}
}

func protoComparisonOperator(operator storage.ComparisonOperator) grpcproto.ComparisonOperator {
	switch operator {
	case storage.ComparisonOperatorEqual:
		return grpcproto.ComparisonOperator_COMPARISON_OPERATOR_EQ
	case storage.ComparisonOperatorLessThan:
		return grpcproto.ComparisonOperator_COMPARISON_OPERATOR_LT
	case storage.ComparisonOperatorLessThanOrEqual:
		return grpcproto.ComparisonOperator_COMPARISON_OPERATOR_LTE
	case storage.ComparisonOperatorGreaterThan:
		return grpcproto.ComparisonOperator_COMPARISON_OPERATOR_GT
	case storage.ComparisonOperatorGreaterThanOrEqual:
		return grpcproto.ComparisonOperator_COMPARISON_OPERATOR_GTE
	default:
		return grpcproto.ComparisonOperator_COMPARISON_OPERATOR_UNSPECIFIED
	}
}

func fromProtoComparisonOperator(operator grpcproto.ComparisonOperator) storage.ComparisonOperator {
	switch operator {
	case grpcproto.ComparisonOperator_COMPARISON_OPERATOR_EQ:
		return storage.ComparisonOperatorEqual
	case grpcproto.ComparisonOperator_COMPARISON_OPERATOR_LT:
		return storage.ComparisonOperatorLessThan
	case grpcproto.ComparisonOperator_COMPARISON_OPERATOR_LTE:
		return storage.ComparisonOperatorLessThanOrEqual
	case grpcproto.ComparisonOperator_COMPARISON_OPERATOR_GT:
		return storage.ComparisonOperatorGreaterThan
	case grpcproto.ComparisonOperator_COMPARISON_OPERATOR_GTE:
		return storage.ComparisonOperatorGreaterThanOrEqual
	default:
		return ""
	}
}
