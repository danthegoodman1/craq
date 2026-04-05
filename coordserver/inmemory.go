package coordserver

import (
	"context"
	"fmt"

	"github.com/danthegoodman1/craq/storage"
)

type queuedProgressReportKind string

const (
	queuedProgressReportReady     queuedProgressReportKind = "ready"
	queuedProgressReportRemoved   queuedProgressReportKind = "removed"
	queuedProgressReportHeartbeat queuedProgressReportKind = "heartbeat"
	queuedProgressReportRecovered queuedProgressReportKind = "recovered"
)

type queuedProgressReport struct {
	kind     queuedProgressReportKind
	slot     int
	epoch    uint64
	status   storage.NodeStatus
	recovery storage.NodeRecoveryReport
}

type InMemoryNodeAdapter struct {
	nodeID        string
	node          *storage.Node
	sink          *Server
	local         storage.LocalStateStore
	queued        bool
	progressQueue []queuedProgressReport
}

func NewInMemoryNodeAdapter(
	ctx context.Context,
	nodeID string,
	backend storage.Backend,
	repl storage.ReplicationTransport,
) (*InMemoryNodeAdapter, error) {
	return OpenInMemoryNodeAdapter(ctx, nodeID, backend, storage.NewInMemoryLocalStateStore(), repl)
}

func OpenInMemoryNodeAdapter(
	ctx context.Context,
	nodeID string,
	backend storage.Backend,
	local storage.LocalStateStore,
	repl storage.ReplicationTransport,
) (*InMemoryNodeAdapter, error) {
	adapter := &InMemoryNodeAdapter{nodeID: nodeID, local: local}
	node, err := storage.OpenNode(
		ctx,
		storage.Config{NodeID: nodeID},
		backend,
		local,
		adapter,
		repl,
	)
	if err != nil {
		return nil, fmt.Errorf("err in storage.OpenNode: %w", err)
	}
	adapter.node = node
	return adapter, nil
}

func (a *InMemoryNodeAdapter) BindServer(server *Server) {
	a.sink = server
}

func (a *InMemoryNodeAdapter) RegisterNode(ctx context.Context, reg storage.NodeRegistration) error {
	if a.sink == nil {
		return fmt.Errorf("%w: coordinator server not bound", ErrStateMismatch)
	}
	_, err := a.sink.RegisterNode(ctx, reg)
	return err
}

func (a *InMemoryNodeAdapter) EnableQueuedProgress() {
	a.queued = true
}

func (a *InMemoryNodeAdapter) PendingProgress() int {
	return len(a.progressQueue)
}

func (a *InMemoryNodeAdapter) PendingProgressReports() []queuedProgressReport {
	cloned := make([]queuedProgressReport, 0, len(a.progressQueue))
	for _, report := range a.progressQueue {
		cloned = append(cloned, cloneQueuedProgressReport(report))
	}
	return cloned
}

func (a *InMemoryNodeAdapter) DropProgressAt(index int) error {
	if index < 0 || index >= len(a.progressQueue) {
		return fmt.Errorf("%w: progress queue index %d", ErrStateMismatch, index)
	}
	a.progressQueue = append(a.progressQueue[:index], a.progressQueue[index+1:]...)
	return nil
}

func (a *InMemoryNodeAdapter) DuplicateProgressAt(index int) error {
	if index < 0 || index >= len(a.progressQueue) {
		return fmt.Errorf("%w: progress queue index %d", ErrStateMismatch, index)
	}
	a.progressQueue = append(a.progressQueue, cloneQueuedProgressReport(a.progressQueue[index]))
	return nil
}

func (a *InMemoryNodeAdapter) MoveProgressToFront(index int) error {
	return a.MoveProgressTo(index, 0)
}

func (a *InMemoryNodeAdapter) MoveProgressTo(index int, destination int) error {
	if index < 0 || index >= len(a.progressQueue) {
		return fmt.Errorf("%w: progress queue index %d", ErrStateMismatch, index)
	}
	if destination < 0 || destination >= len(a.progressQueue) {
		return fmt.Errorf("%w: progress queue destination %d", ErrStateMismatch, destination)
	}
	if index == destination {
		return nil
	}
	report := a.progressQueue[index]
	a.progressQueue = append(a.progressQueue[:index], a.progressQueue[index+1:]...)
	if destination >= len(a.progressQueue) {
		a.progressQueue = append(a.progressQueue, report)
		return nil
	}
	a.progressQueue = append(a.progressQueue, queuedProgressReport{})
	copy(a.progressQueue[destination+1:], a.progressQueue[destination:])
	a.progressQueue[destination] = report
	return nil
}

func (a *InMemoryNodeAdapter) DeliverNextProgress(ctx context.Context) error {
	return a.DeliverProgressAt(ctx, 0)
}

func (a *InMemoryNodeAdapter) DeliverProgressAt(ctx context.Context, index int) error {
	if len(a.progressQueue) == 0 || a.sink == nil {
		return nil
	}
	if index < 0 || index >= len(a.progressQueue) {
		return fmt.Errorf("%w: progress queue index %d", ErrStateMismatch, index)
	}
	report := a.progressQueue[index]
	a.progressQueue = append(a.progressQueue[:index], a.progressQueue[index+1:]...)
	switch report.kind {
	case queuedProgressReportReady:
		_, err := a.sink.ReportReplicaReady(ctx, a.nodeID, report.slot, report.epoch, "")
		return err
	case queuedProgressReportRemoved:
		_, err := a.sink.ReportReplicaRemoved(ctx, a.nodeID, report.slot, report.epoch, "")
		return err
	case queuedProgressReportHeartbeat:
		return a.sink.ReportNodeHeartbeat(ctx, report.status)
	case queuedProgressReportRecovered:
		return a.sink.ReportNodeRecovered(ctx, report.recovery)
	default:
		return nil
	}
}

func (a *InMemoryNodeAdapter) DeliverAllProgress(ctx context.Context) error {
	for len(a.progressQueue) > 0 {
		if err := a.DeliverNextProgress(ctx); err != nil {
			return err
		}
	}
	return nil
}

func cloneQueuedProgressReport(report queuedProgressReport) queuedProgressReport {
	cloned := queuedProgressReport{
		kind:   report.kind,
		slot:   report.slot,
		epoch:  report.epoch,
		status: report.status,
	}
	if report.kind == queuedProgressReportRecovered {
		cloned.recovery = storage.NodeRecoveryReport{
			NodeID:   report.recovery.NodeID,
			Replicas: append([]storage.RecoveredReplica(nil), report.recovery.Replicas...),
		}
	}
	return cloned
}

func (a *InMemoryNodeAdapter) Node() *storage.Node {
	return a.node
}

func (a *InMemoryNodeAdapter) AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error {
	return a.node.AddReplicaAsTail(ctx, cmd)
}

func (a *InMemoryNodeAdapter) ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error {
	return a.node.ActivateReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	return a.node.MarkReplicaLeaving(ctx, cmd)
}

func (a *InMemoryNodeAdapter) RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error {
	return a.node.RemoveReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error {
	return a.node.UpdateChainPeers(ctx, cmd)
}

func (a *InMemoryNodeAdapter) ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	return a.node.ResumeRecoveredReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error {
	return a.node.RecoverReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	return a.node.DropRecoveredReplica(ctx, cmd)
}

func (a *InMemoryNodeAdapter) ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error {
	if a.sink == nil {
		return nil
	}
	if a.queued {
		a.progressQueue = append(a.progressQueue, queuedProgressReport{
			kind: queuedProgressReportReady,
			slot: slot,
			epoch: epoch,
		})
		return nil
	}
	_, err := a.sink.ReportReplicaReady(ctx, a.nodeID, slot, epoch, "")
	return err
}

func (a *InMemoryNodeAdapter) ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error {
	if a.sink == nil {
		return nil
	}
	if a.queued {
		a.progressQueue = append(a.progressQueue, queuedProgressReport{
			kind: queuedProgressReportRemoved,
			slot: slot,
			epoch: epoch,
		})
		return nil
	}
	_, err := a.sink.ReportReplicaRemoved(ctx, a.nodeID, slot, epoch, "")
	return err
}

func (a *InMemoryNodeAdapter) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	if a.sink == nil {
		return nil
	}
	if a.queued {
		a.progressQueue = append(a.progressQueue, queuedProgressReport{
			kind:   queuedProgressReportHeartbeat,
			status: status,
		})
		return nil
	}
	return a.sink.ReportNodeHeartbeat(ctx, status)
}

func (a *InMemoryNodeAdapter) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	if a.sink == nil {
		return nil
	}
	if a.queued {
		a.progressQueue = append(a.progressQueue, queuedProgressReport{
			kind:     queuedProgressReportRecovered,
			recovery: report,
		})
		return nil
	}
	return a.sink.ReportNodeRecovered(ctx, report)
}
