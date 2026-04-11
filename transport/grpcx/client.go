package grpcx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	grpcproto "github.com/danthegoodman1/craq/proto/craq/v1"
	"github.com/danthegoodman1/craq/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnPool struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	creds grpc.DialOption
}

func NewConnPool() *ConnPool {
	return &ConnPool{
		conns: map[string]*grpc.ClientConn{},
		creds: grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
}

func NewConnPoolWithTLS(cfg ClientTLSConfig) (*ConnPool, error) {
	creds, err := newClientTransportCredentials(cfg)
	if err != nil {
		return nil, fmt.Errorf("new client transport credentials: %w", err)
	}
	return &ConnPool{
		conns: map[string]*grpc.ClientConn{},
		creds: grpc.WithTransportCredentials(creds),
	}, nil
}

func (p *ConnPool) DialContext(ctx context.Context, target string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	if conn, ok := p.conns[target]; ok {
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	// Apply a default dial timeout if the caller didn't set a deadline.
	dialCtx := ctx
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		dialCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}

	dialer := func(ctx context.Context, address string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", address)
	}
	conn, err := grpc.DialContext(
		dialCtx,
		target,
		p.creds,
		grpc.WithContextDialer(dialer),
		grpc.WithBlock(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithReturnConnectionError(),
	)
	if err != nil {
		return nil, wrapDialError(err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if existing, ok := p.conns[target]; ok {
		_ = conn.Close()
		return existing, nil
	}
	p.conns[target] = conn
	return conn, nil
}

func (p *ConnPool) Remove(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if conn, ok := p.conns[target]; ok {
		_ = conn.Close()
		delete(p.conns, target)
	}
}

func (p *ConnPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var errs []error
	for target, conn := range p.conns {
		errs = append(errs, conn.Close())
		delete(p.conns, target)
	}
	return joinErrors(errs...)
}

type CoordinatorAdminClient struct {
	mu        sync.Mutex
	targets   []string
	preferred int
	pool      *ConnPool
}

func NewCoordinatorAdminClient(target string, pool *ConnPool) *CoordinatorAdminClient {
	return NewCoordinatorAdminFailoverClient([]string{target}, pool)
}

func NewCoordinatorAdminFailoverClient(targets []string, pool *ConnPool) *CoordinatorAdminClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &CoordinatorAdminClient{targets: append([]string(nil), targets...), pool: pool}
}

func (c *CoordinatorAdminClient) RoutingSnapshot(ctx context.Context) (coordserver.RoutingSnapshot, error) {
	var snapshot coordserver.RoutingSnapshot
	err := c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		resp, err := client.RoutingSnapshot(ctx, &grpcproto.RoutingSnapshotRequest{})
		if err != nil {
			return decodeError(err)
		}
		snapshot = fromProtoRoutingSnapshot(resp)
		return nil
	})
	return snapshot, err
}

func (c *CoordinatorAdminClient) Bootstrap(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	var state coordruntime.State
	req := &grpcproto.BootstrapRequest{
		CommandId:         cmd.ID,
		ExpectedVersion:   cmd.ExpectedVersion,
		SlotCount:         int32(cmd.Bootstrap.Config.SlotCount),
		ReplicationFactor: int32(cmd.Bootstrap.Config.ReplicationFactor),
	}
	for _, node := range cmd.Bootstrap.Nodes {
		req.Nodes = append(req.Nodes, protoNode(node))
	}
	err := c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		resp, err := client.Bootstrap(ctx, req)
		if err != nil {
			return decodeError(err)
		}
		state = coordruntime.State{Version: resp.Version}
		return nil
	})
	return state, err
}

func (c *CoordinatorAdminClient) AddNode(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	return c.mutateMembership(ctx, func(client grpcproto.CoordinatorServiceClient, ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
		return client.AddNode(ctx, req)
	}, cmd)
}

func (c *CoordinatorAdminClient) BeginDrainNode(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	return c.mutateMembership(ctx, func(client grpcproto.CoordinatorServiceClient, ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
		return client.BeginDrainNode(ctx, req)
	}, cmd)
}

func (c *CoordinatorAdminClient) MarkNodeDead(
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	return c.mutateMembership(ctx, func(client grpcproto.CoordinatorServiceClient, ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
		return client.MarkNodeDead(ctx, req)
	}, cmd)
}

func (c *CoordinatorAdminClient) EvaluateLiveness(ctx context.Context) error {
	return c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		_, err := client.EvaluateLiveness(ctx, &grpcproto.Empty{})
		return decodeError(err)
	})
}

func (c *CoordinatorAdminClient) mutateMembership(
	ctx context.Context,
	call func(grpcproto.CoordinatorServiceClient, context.Context, *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error),
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	req := &grpcproto.MembershipMutationRequest{
		CommandId:        cmd.ID,
		ExpectedVersion:  cmd.ExpectedVersion,
		MaxChangedChains: int32(cmd.Reconfigure.Policy.MaxChangedChains),
	}
	if len(cmd.Reconfigure.Events) > 0 {
		event := cmd.Reconfigure.Events[0]
		req.Node = protoNode(event.Node)
		req.NodeId = event.NodeID
	}
	var state coordruntime.State
	err := c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		resp, err := call(client, ctx, req)
		if err != nil {
			return decodeError(err)
		}
		state = coordruntime.State{Version: resp.Version}
		return nil
	})
	return state, err
}

type CoordinatorReporterClient struct {
	mu        sync.Mutex
	targets   []string
	preferred int
	pool      *ConnPool
	nodeID    string
}

func NewCoordinatorReporterClient(nodeID string, target string, pool *ConnPool) *CoordinatorReporterClient {
	return NewCoordinatorReporterFailoverClient(nodeID, []string{target}, pool)
}

func NewCoordinatorReporterFailoverClient(nodeID string, targets []string, pool *ConnPool) *CoordinatorReporterClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &CoordinatorReporterClient{nodeID: nodeID, targets: append([]string(nil), targets...), pool: pool}
}

func (c *CoordinatorReporterClient) ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error {
	return c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		_, err := client.ReportReplicaReady(ctx, &grpcproto.ReplicaReadyReport{
			NodeId: c.nodeID,
			Slot:   int32(slot),
			Epoch:  epoch,
		})
		return decodeError(err)
	})
}

func (c *CoordinatorReporterClient) RegisterNode(ctx context.Context, reg storage.NodeRegistration) error {
	return c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		_, err := client.RegisterNode(ctx, &grpcproto.RegisterNodeRequest{
			Node: protoNode(coordinator.Node{
				ID:             reg.NodeID,
				RPCAddress:     reg.RPCAddress,
				FailureDomains: reg.FailureDomains,
			}),
		})
		return decodeError(err)
	})
}

func (c *CoordinatorReporterClient) ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error {
	return c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		_, err := client.ReportReplicaRemoved(ctx, &grpcproto.ReplicaRemovedReport{
			NodeId: c.nodeID,
			Slot:   int32(slot),
			Epoch:  epoch,
		})
		return decodeError(err)
	})
}

func (c *CoordinatorReporterClient) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	return c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		_, err := client.ReportNodeRecovered(ctx, protoNodeRecovery(report))
		return decodeError(err)
	})
}

func (c *CoordinatorReporterClient) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	return c.withFailover(ctx, func(client grpcproto.CoordinatorServiceClient) error {
		_, err := client.ReportNodeHeartbeat(ctx, protoNodeStatus(status))
		return decodeError(err)
	})
}

type StorageNodeClient struct {
	target string
	pool   *ConnPool
}

func NewStorageNodeClient(target string, pool *ConnPool) *StorageNodeClient {
	if pool == nil {
		pool = NewConnPool()
	}
	return &StorageNodeClient{target: target, pool: pool}
}

func (c *StorageNodeClient) storageClient(ctx context.Context) (grpcproto.StorageServiceClient, error) {
	conn, err := c.pool.DialContext(ctx, c.target)
	if err != nil {
		return nil, err
	}
	return grpcproto.NewStorageServiceClient(conn), nil
}

func (c *StorageNodeClient) AddReplicaAsTail(ctx context.Context, cmd storage.AddReplicaAsTailCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.AddReplicaAsTail(ctx, &grpcproto.AddReplicaAsTailCommand{
		Assignment: protoAssignment(cmd.Assignment),
		Epoch:      cmd.Epoch,
	})
	return decodeError(err)
}

func (c *StorageNodeClient) ActivateReplica(ctx context.Context, cmd storage.ActivateReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ActivateReplica(ctx, &grpcproto.ActivateReplicaCommand{Slot: int32(cmd.Slot), Epoch: cmd.Epoch})
	return decodeError(err)
}

func (c *StorageNodeClient) MarkReplicaLeaving(ctx context.Context, cmd storage.MarkReplicaLeavingCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.MarkReplicaLeaving(ctx, &grpcproto.MarkReplicaLeavingCommand{Slot: int32(cmd.Slot), Epoch: cmd.Epoch})
	return decodeError(err)
}

func (c *StorageNodeClient) RemoveReplica(ctx context.Context, cmd storage.RemoveReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.RemoveReplica(ctx, &grpcproto.RemoveReplicaCommand{Slot: int32(cmd.Slot), Epoch: cmd.Epoch})
	return decodeError(err)
}

func (c *StorageNodeClient) UpdateChainPeers(ctx context.Context, cmd storage.UpdateChainPeersCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.UpdateChainPeers(ctx, &grpcproto.UpdateChainPeersCommand{Assignment: protoAssignment(cmd.Assignment), Epoch: cmd.Epoch})
	return decodeError(err)
}

func (c *StorageNodeClient) ResumeRecoveredReplica(ctx context.Context, cmd storage.ResumeRecoveredReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.ResumeRecoveredReplica(ctx, &grpcproto.ResumeRecoveredReplicaCommand{Assignment: protoAssignment(cmd.Assignment), Epoch: cmd.Epoch})
	return decodeError(err)
}

func (c *StorageNodeClient) RecoverReplica(ctx context.Context, cmd storage.RecoverReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.RecoverReplica(ctx, &grpcproto.RecoverReplicaCommand{
		Assignment:   protoAssignment(cmd.Assignment),
		SourceNodeId: cmd.SourceNodeID,
		Epoch:        cmd.Epoch,
	})
	return decodeError(err)
}

func (c *StorageNodeClient) DropRecoveredReplica(ctx context.Context, cmd storage.DropRecoveredReplicaCommand) error {
	client, err := c.storageClient(ctx)
	if err != nil {
		return err
	}
	_, err = client.DropRecoveredReplica(ctx, &grpcproto.DropRecoveredReplicaCommand{Slot: int32(cmd.Slot), Epoch: cmd.Epoch})
	return decodeError(err)
}

func (c *CoordinatorAdminClient) withFailover(ctx context.Context, fn func(grpcproto.CoordinatorServiceClient) error) error {
	targets := c.targetOrder()
	if len(targets) == 0 {
		return fmt.Errorf("%w: no coordinator targets configured", coordserver.ErrUnknownNode)
	}
	var lastErr error
	for _, candidate := range targets {
		conn, err := c.pool.DialContext(ctx, candidate.target)
		if err != nil {
			lastErr = err
			continue
		}
		err = fn(grpcproto.NewCoordinatorServiceClient(conn))
		if err == nil {
			c.setPreferred(candidate.index)
			return nil
		}
		lastErr = err
		if !shouldFailoverCoordinator(err) {
			return err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return coordserver.ErrNotLeader
}

func (c *CoordinatorReporterClient) withFailover(ctx context.Context, fn func(grpcproto.CoordinatorServiceClient) error) error {
	targets := c.targetOrder()
	if len(targets) == 0 {
		return fmt.Errorf("%w: no coordinator targets configured", coordserver.ErrUnknownNode)
	}
	var lastErr error
	for _, candidate := range targets {
		conn, err := c.pool.DialContext(ctx, candidate.target)
		if err != nil {
			lastErr = err
			continue
		}
		err = fn(grpcproto.NewCoordinatorServiceClient(conn))
		if err == nil {
			c.setPreferred(candidate.index)
			return nil
		}
		lastErr = err
		if !shouldFailoverCoordinator(err) {
			return err
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return coordserver.ErrNotLeader
}

type failoverTarget struct {
	index  int
	target string
}

func (c *CoordinatorAdminClient) targetOrder() []failoverTarget {
	c.mu.Lock()
	defer c.mu.Unlock()
	return buildFailoverTargets(c.targets, c.preferred)
}

func (c *CoordinatorAdminClient) setPreferred(index int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preferred = index
}

func (c *CoordinatorReporterClient) targetOrder() []failoverTarget {
	c.mu.Lock()
	defer c.mu.Unlock()
	return buildFailoverTargets(c.targets, c.preferred)
}

func (c *CoordinatorReporterClient) setPreferred(index int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.preferred = index
}

func buildFailoverTargets(targets []string, preferred int) []failoverTarget {
	if len(targets) == 0 {
		return nil
	}
	out := make([]failoverTarget, 0, len(targets))
	seen := make(map[int]bool, len(targets))
	if preferred >= 0 && preferred < len(targets) {
		out = append(out, failoverTarget{index: preferred, target: targets[preferred]})
		seen[preferred] = true
	}
	for i, target := range targets {
		if seen[i] {
			continue
		}
		out = append(out, failoverTarget{index: i, target: target})
	}
	return out
}

func shouldFailoverCoordinator(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, coordserver.ErrNotLeader) {
		return true
	}
	var authErr *TransportAuthError
	if errors.As(err, &authErr) {
		return false
	}
	return false
}

type DynamicNodeClientFactory struct {
	pool *ConnPool
}

func NewDynamicNodeClientFactory(pool *ConnPool) *DynamicNodeClientFactory {
	if pool == nil {
		pool = NewConnPool()
	}
	return &DynamicNodeClientFactory{pool: pool}
}

func (f *DynamicNodeClientFactory) ClientForNode(node coordinator.Node) (coordserver.StorageNodeClient, error) {
	if node.RPCAddress == "" {
		return nil, fmt.Errorf("%w: node %q has empty rpc address", coordserver.ErrUnknownNode, node.ID)
	}
	return NewStorageNodeClient(node.RPCAddress, f.pool), nil
}

type ReplicationTransport struct {
	pool *ConnPool
}

func NewReplicationTransport(pool *ConnPool) *ReplicationTransport {
	if pool == nil {
		pool = NewConnPool()
	}
	return &ReplicationTransport{pool: pool}
}

func (t *ReplicationTransport) FetchSnapshot(ctx context.Context, fromTarget string, slot int) (storage.Snapshot, uint64, error) {
	conn, err := t.pool.DialContext(ctx, fromTarget)
	if err != nil {
		return nil, 0, err
	}
	stream, err := grpcproto.NewStorageServiceClient(conn).FetchSnapshot(ctx, &grpcproto.FetchSnapshotRequest{Slot: int32(slot)})
	if err != nil {
		return nil, 0, decodeError(err)
	}
	var entries []*grpcproto.SnapshotEntry
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, decodeError(err)
		}
		entries = append(entries, entry)
	}
	snapshot, err := snapshotFromProtoEntries(entries)
	if err != nil {
		return nil, 0, err
	}
	trailer := stream.Trailer()
	var committedSequence uint64
	if values := trailer.Get("x-craq-committed-sequence"); len(values) > 0 {
		fmt.Sscanf(values[0], "%d", &committedSequence)
	}
	return snapshot, committedSequence, nil
}

func (t *ReplicationTransport) FetchCommittedSequence(ctx context.Context, fromTarget string, slot int) (uint64, error) {
	conn, err := t.pool.DialContext(ctx, fromTarget)
	if err != nil {
		return 0, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).FetchCommittedSequence(ctx, &grpcproto.FetchCommittedSequenceRequest{Slot: int32(slot)})
	if err != nil {
		return 0, decodeError(err)
	}
	return resp.Sequence, nil
}

func (t *ReplicationTransport) ForwardWrite(ctx context.Context, toTarget string, req storage.ForwardWriteRequest) error {
	conn, err := t.pool.DialContext(ctx, toTarget)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewStorageServiceClient(conn).ForwardWrite(ctx, &grpcproto.ForwardWriteRequest{
		Operation: &grpcproto.WriteOperation{
			Slot:     int32(req.Operation.Slot),
			Sequence: req.Operation.Sequence,
			Kind:     string(req.Operation.Kind),
			Key:      req.Operation.Key,
			Value:    req.Operation.Value,
			Metadata: protoObjectMetadata(&req.Operation.Metadata),
		},
		FromNodeId:   req.FromNodeID,
		ChainVersion: req.ChainVersion,
	})
	return decodeError(err)
}

func (t *ReplicationTransport) CommitWrite(ctx context.Context, toTarget string, req storage.CommitWriteRequest) error {
	conn, err := t.pool.DialContext(ctx, toTarget)
	if err != nil {
		return err
	}
	_, err = grpcproto.NewStorageServiceClient(conn).CommitWrite(ctx, &grpcproto.CommitWriteRequest{
		Slot:         int32(req.Slot),
		Sequence:     req.Sequence,
		FromNodeId:   req.FromNodeID,
		ChainVersion: req.ChainVersion,
	})
	return decodeError(err)
}

type ClientTransport struct {
	pool *ConnPool
}

func NewClientTransport(pool *ConnPool) *ClientTransport {
	if pool == nil {
		pool = NewConnPool()
	}
	return &ClientTransport{pool: pool}
}

func (t *ClientTransport) Get(ctx context.Context, target string, req storage.ClientGetRequest) (storage.ReadResult, error) {
	conn, err := t.pool.DialContext(ctx, target)
	if err != nil {
		return storage.ReadResult{}, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).Get(ctx, &grpcproto.ClientGetRequest{
		Slot:                 int32(req.Slot),
		Key:                  req.Key,
		ExpectedChainVersion: req.ExpectedChainVersion,
		Consistency:          protoReadConsistency(req.Consistency),
	})
	if err != nil {
		return storage.ReadResult{}, decodeError(err)
	}
	return fromProtoReadResult(resp), nil
}

func (t *ClientTransport) Put(ctx context.Context, target string, req storage.ClientPutRequest) (storage.CommitResult, error) {
	conn, err := t.pool.DialContext(ctx, target)
	if err != nil {
		return storage.CommitResult{}, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).Put(ctx, &grpcproto.ClientPutRequest{
		Slot:                 int32(req.Slot),
		Key:                  req.Key,
		Value:                req.Value,
		ExpectedChainVersion: req.ExpectedChainVersion,
		Conditions:           protoWriteConditions(req.Conditions),
	})
	if err != nil {
		return storage.CommitResult{}, decodeError(err)
	}
	return fromProtoCommitResult(resp), nil
}

func (t *ClientTransport) Delete(ctx context.Context, target string, req storage.ClientDeleteRequest) (storage.CommitResult, error) {
	conn, err := t.pool.DialContext(ctx, target)
	if err != nil {
		return storage.CommitResult{}, err
	}
	resp, err := grpcproto.NewStorageServiceClient(conn).Delete(ctx, &grpcproto.ClientDeleteRequest{
		Slot:                 int32(req.Slot),
		Key:                  req.Key,
		ExpectedChainVersion: req.ExpectedChainVersion,
		Conditions:           protoWriteConditions(req.Conditions),
	})
	if err != nil {
		return storage.CommitResult{}, decodeError(err)
	}
	return fromProtoCommitResult(resp), nil
}
