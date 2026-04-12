package grpcx

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	grpcproto "github.com/danthegoodman1/craq/proto/craq/v1"
	"github.com/danthegoodman1/craq/storage"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type CoordinatorGRPCServer struct {
	grpcproto.UnimplementedCoordinatorServiceServer
	server     *coordserver.Server
	grpc       *grpc.Server
	lis        net.Listener
	authorizer *rpcAuthorizer
	logger     zerolog.Logger
	observer   *grpcObserver
}

func NewCoordinatorGRPCServer(server *coordserver.Server) *CoordinatorGRPCServer {
	s, err := NewCoordinatorGRPCServerWithTLS(server, nil)
	if err != nil {
		panic(err)
	}
	return s
}

func NewCoordinatorGRPCServerWithTLS(server *coordserver.Server, cfg *ServerTLSConfig) (*CoordinatorGRPCServer, error) {
	var opts []grpc.ServerOption
	authorizer := (*rpcAuthorizer)(nil)
	var logger zerolog.Logger
	var observer *grpcObserver
	if cfg != nil {
		creds, err := newServerTransportCredentials(*cfg)
		if err != nil {
			return nil, err
		}
		authorizer = newRPCAuthorizer()
		logger = transportLoggerFromConfig(cfg.Logger)
		observer = newGRPCObserver(cfg.Logger, cfg.MetricsRegistry)
		opts = append(opts,
			grpc.Creds(creds),
			grpc.UnaryInterceptor(chainUnaryInterceptors(observer.unaryInterceptor("coordinator"), authorizer.unaryInterceptor(coordinatorRPCPlane))),
			grpc.StreamInterceptor(chainStreamInterceptors(observer.streamInterceptor("coordinator"), authorizer.streamInterceptor(coordinatorRPCPlane))),
		)
	} else {
		logger = transportLoggerFromConfig(nil)
	}
	s := &CoordinatorGRPCServer{
		server:     server,
		grpc:       grpc.NewServer(opts...),
		authorizer: authorizer,
		logger:     logger,
		observer:   observer,
	}
	grpcproto.RegisterCoordinatorServiceServer(s.grpc, s)
	return s, nil
}

func (s *CoordinatorGRPCServer) Serve(lis net.Listener) error {
	s.lis = lis
	s.logger.Info().Str("component", "grpc").Str("grpc_component", "coordinator").Str("address", lis.Addr().String()).Msg("grpc server listening")
	return s.grpc.Serve(lis)
}

func (s *CoordinatorGRPCServer) Close() error {
	s.logger.Info().Str("component", "grpc").Str("grpc_component", "coordinator").Msg("grpc server closing")
	if s.grpc != nil {
		s.grpc.Stop()
	}
	if s.lis != nil {
		return s.lis.Close()
	}
	return nil
}

func (s *CoordinatorGRPCServer) Bootstrap(ctx context.Context, req *grpcproto.BootstrapRequest) (*grpcproto.ServerState, error) {
	nodes := make([]coordinator.Node, 0, len(req.Nodes))
	for _, node := range req.Nodes {
		nodes = append(nodes, fromProtoNode(node))
	}
	state, err := s.server.Bootstrap(ctx, coordruntime.Command{
		ID:              req.CommandId,
		ExpectedVersion: req.ExpectedVersion,
		Kind:            coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{
				SlotCount:         int(req.SlotCount),
				ReplicationFactor: int(req.ReplicationFactor),
			},
			Nodes: nodes,
		},
	})
	if err != nil {
		return nil, encodeError(err)
	}
	return protoServerState(state), nil
}

func (s *CoordinatorGRPCServer) RegisterNode(ctx context.Context, req *grpcproto.RegisterNodeRequest) (*grpcproto.ServerState, error) {
	state, err := s.server.RegisterNode(ctx, storage.NodeRegistration{
		NodeID:         req.Node.Id,
		RPCAddress:     req.Node.RpcAddress,
		FailureDomains: fromProtoNode(req.Node).FailureDomains,
	})
	if err != nil {
		return nil, encodeError(err)
	}
	return protoServerState(state), nil
}

func (s *CoordinatorGRPCServer) AddNode(ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
	return s.applyMembership(ctx, coordinator.EventKindAddNode, req)
}

func (s *CoordinatorGRPCServer) BeginDrainNode(ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
	return s.applyMembership(ctx, coordinator.EventKindBeginDrainNode, req)
}

func (s *CoordinatorGRPCServer) MarkNodeDead(ctx context.Context, req *grpcproto.MembershipMutationRequest) (*grpcproto.ServerState, error) {
	return s.applyMembership(ctx, coordinator.EventKindMarkNodeDead, req)
}

func (s *CoordinatorGRPCServer) applyMembership(
	ctx context.Context,
	kind coordinator.EventKind,
	req *grpcproto.MembershipMutationRequest,
) (*grpcproto.ServerState, error) {
	state, err := mapMembershipMethod(kind, s.server, ctx, coordruntime.Command{
		ID:              req.CommandId,
		ExpectedVersion: req.ExpectedVersion,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Policy: coordinator.ReconfigurationPolicy{
				MaxChangedChains: int(req.MaxChangedChains),
			},
			Events: []coordinator.Event{{
				Kind:   kind,
				Node:   fromProtoNode(req.Node),
				NodeID: req.NodeId,
			}},
		},
	})
	if err != nil {
		return nil, encodeError(err)
	}
	return protoServerState(state), nil
}

func mapMembershipMethod(
	kind coordinator.EventKind,
	server *coordserver.Server,
	ctx context.Context,
	cmd coordruntime.Command,
) (coordruntime.State, error) {
	switch kind {
	case coordinator.EventKindAddNode:
		return server.AddNode(ctx, cmd)
	case coordinator.EventKindBeginDrainNode:
		return server.BeginDrainNode(ctx, cmd)
	case coordinator.EventKindMarkNodeDead:
		return server.MarkNodeDead(ctx, cmd)
	default:
		return coordruntime.State{}, errors.New("unsupported membership method")
	}
}

func (s *CoordinatorGRPCServer) RoutingSnapshot(ctx context.Context, _ *grpcproto.RoutingSnapshotRequest) (*grpcproto.RoutingSnapshotResponse, error) {
	snapshot, err := s.server.RoutingSnapshot(ctx)
	if err != nil {
		return nil, encodeError(err)
	}
	return protoRoutingSnapshot(snapshot), nil
}

func (s *CoordinatorGRPCServer) ReportReplicaReady(ctx context.Context, req *grpcproto.ReplicaReadyReport) (*grpcproto.ServerState, error) {
	if s.authorizer != nil {
		if err := s.authorizer.requireStorageIdentityMatch(ctx, req.NodeId); err != nil {
			return nil, encodeError(err)
		}
	}
	state, err := s.server.ReportReplicaReady(ctx, req.NodeId, int(req.Slot), req.Epoch, req.CommandId)
	if err != nil {
		return nil, encodeError(err)
	}
	return protoServerState(state), nil
}

func (s *CoordinatorGRPCServer) ReportReplicaRemoved(ctx context.Context, req *grpcproto.ReplicaRemovedReport) (*grpcproto.ServerState, error) {
	if s.authorizer != nil {
		if err := s.authorizer.requireStorageIdentityMatch(ctx, req.NodeId); err != nil {
			return nil, encodeError(err)
		}
	}
	state, err := s.server.ReportReplicaRemoved(ctx, req.NodeId, int(req.Slot), req.Epoch, req.CommandId)
	if err != nil {
		return nil, encodeError(err)
	}
	return protoServerState(state), nil
}

func (s *CoordinatorGRPCServer) ReportNodeHeartbeat(ctx context.Context, req *grpcproto.NodeStatus) (*grpcproto.Empty, error) {
	if s.authorizer != nil {
		if err := s.authorizer.requireStorageIdentityMatch(ctx, req.NodeId); err != nil {
			return nil, encodeError(err)
		}
	}
	if err := s.server.ReportNodeHeartbeat(ctx, fromProtoNodeStatus(req)); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *CoordinatorGRPCServer) ReportNodeRecovered(ctx context.Context, req *grpcproto.NodeRecoveryReport) (*grpcproto.Empty, error) {
	if s.authorizer != nil {
		if err := s.authorizer.requireStorageIdentityMatch(ctx, req.NodeId); err != nil {
			return nil, encodeError(err)
		}
	}
	if err := s.server.ReportNodeRecovered(ctx, fromProtoNodeRecovery(req)); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *CoordinatorGRPCServer) EvaluateLiveness(ctx context.Context, _ *grpcproto.Empty) (*grpcproto.ServerState, error) {
	if err := s.server.EvaluateLiveness(ctx); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.ServerState{Version: s.server.CurrentVersion()}, nil
}

type StorageGRPCServer struct {
	grpcproto.UnimplementedStorageServiceServer
	node       *storage.Node
	grpc       *grpc.Server
	lis        net.Listener
	authorizer *rpcAuthorizer
	logger     zerolog.Logger
	observer   *grpcObserver
}

func NewStorageGRPCServer(node *storage.Node) *StorageGRPCServer {
	s, err := NewStorageGRPCServerWithTLS(node, nil)
	if err != nil {
		panic(err)
	}
	return s
}

func NewStorageGRPCServerWithTLS(node *storage.Node, cfg *ServerTLSConfig) (*StorageGRPCServer, error) {
	var opts []grpc.ServerOption
	authorizer := (*rpcAuthorizer)(nil)
	var logger zerolog.Logger
	var observer *grpcObserver
	if cfg != nil {
		creds, err := newServerTransportCredentials(*cfg)
		if err != nil {
			return nil, err
		}
		authorizer = newRPCAuthorizer()
		logger = transportLoggerFromConfig(cfg.Logger)
		observer = newGRPCObserver(cfg.Logger, cfg.MetricsRegistry)
		opts = append(opts,
			grpc.Creds(creds),
			grpc.UnaryInterceptor(chainUnaryInterceptors(observer.unaryInterceptor("storage"), authorizer.unaryInterceptor(storageRPCPlane))),
			grpc.StreamInterceptor(chainStreamInterceptors(observer.streamInterceptor("storage"), authorizer.streamInterceptor(storageRPCPlane))),
		)
	} else {
		logger = transportLoggerFromConfig(nil)
	}
	s := &StorageGRPCServer{
		node:       node,
		grpc:       grpc.NewServer(opts...),
		authorizer: authorizer,
		logger:     logger,
		observer:   observer,
	}
	grpcproto.RegisterStorageServiceServer(s.grpc, s)
	return s, nil
}

func (s *StorageGRPCServer) Serve(lis net.Listener) error {
	s.lis = lis
	s.logger.Info().Str("component", "grpc").Str("grpc_component", "storage").Str("address", lis.Addr().String()).Msg("grpc server listening")
	return s.grpc.Serve(lis)
}

func (s *StorageGRPCServer) Close() error {
	s.logger.Info().Str("component", "grpc").Str("grpc_component", "storage").Msg("grpc server closing")
	if s.grpc != nil {
		s.grpc.Stop()
	}
	if s.lis != nil {
		return s.lis.Close()
	}
	return nil
}

func (s *StorageGRPCServer) Get(ctx context.Context, req *grpcproto.ClientGetRequest) (*grpcproto.ReadResult, error) {
	result, err := s.node.HandleClientGet(ctx, storage.ClientGetRequest{
		Slot:                 int(req.Slot),
		Key:                  req.Key,
		ExpectedChainVersion: req.ExpectedChainVersion,
		Consistency:          fromProtoReadConsistency(req.Consistency),
	})
	if err != nil {
		return nil, encodeError(err)
	}
	return protoReadResult(result), nil
}

func (s *StorageGRPCServer) Put(ctx context.Context, req *grpcproto.ClientPutRequest) (*grpcproto.CommitResult, error) {
	result, err := s.node.HandleClientPut(ctx, storage.ClientPutRequest{
		Slot:                 int(req.Slot),
		Key:                  req.Key,
		Value:                req.Value,
		ExpectedChainVersion: req.ExpectedChainVersion,
		Conditions:           fromProtoWriteConditions(req.Conditions),
	})
	if err != nil {
		return nil, encodeError(err)
	}
	return protoCommitResult(result), nil
}

func (s *StorageGRPCServer) Delete(ctx context.Context, req *grpcproto.ClientDeleteRequest) (*grpcproto.CommitResult, error) {
	result, err := s.node.HandleClientDelete(ctx, storage.ClientDeleteRequest{
		Slot:                 int(req.Slot),
		Key:                  req.Key,
		ExpectedChainVersion: req.ExpectedChainVersion,
		Conditions:           fromProtoWriteConditions(req.Conditions),
	})
	if err != nil {
		return nil, encodeError(err)
	}
	return protoCommitResult(result), nil
}

func (s *StorageGRPCServer) AddReplicaAsTail(ctx context.Context, req *grpcproto.AddReplicaAsTailCommand) (*grpcproto.Empty, error) {
	if err := s.node.AddReplicaAsTail(ctx, storage.AddReplicaAsTailCommand{
		Assignment: fromProtoAssignment(req.Assignment),
		Epoch:      req.Epoch,
	}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) ActivateReplica(ctx context.Context, req *grpcproto.ActivateReplicaCommand) (*grpcproto.Empty, error) {
	if err := s.node.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: int(req.Slot), Epoch: req.Epoch}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) MarkReplicaLeaving(ctx context.Context, req *grpcproto.MarkReplicaLeavingCommand) (*grpcproto.Empty, error) {
	if err := s.node.MarkReplicaLeaving(ctx, storage.MarkReplicaLeavingCommand{Slot: int(req.Slot), Epoch: req.Epoch}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) RemoveReplica(ctx context.Context, req *grpcproto.RemoveReplicaCommand) (*grpcproto.Empty, error) {
	if err := s.node.RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: int(req.Slot), Epoch: req.Epoch}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) UpdateChainPeers(ctx context.Context, req *grpcproto.UpdateChainPeersCommand) (*grpcproto.Empty, error) {
	if err := s.node.UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{
		Assignment: fromProtoAssignment(req.Assignment),
		Epoch:      req.Epoch,
	}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) ResumeRecoveredReplica(ctx context.Context, req *grpcproto.ResumeRecoveredReplicaCommand) (*grpcproto.Empty, error) {
	if err := s.node.ResumeRecoveredReplica(ctx, storage.ResumeRecoveredReplicaCommand{
		Assignment: fromProtoAssignment(req.Assignment),
		Epoch:      req.Epoch,
	}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) RecoverReplica(ctx context.Context, req *grpcproto.RecoverReplicaCommand) (*grpcproto.Empty, error) {
	if err := s.node.RecoverReplica(ctx, storage.RecoverReplicaCommand{
		Assignment:   fromProtoAssignment(req.Assignment),
		SourceNodeID: req.SourceNodeId,
		Epoch:        req.Epoch,
	}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) DropRecoveredReplica(ctx context.Context, req *grpcproto.DropRecoveredReplicaCommand) (*grpcproto.Empty, error) {
	if err := s.node.DropRecoveredReplica(ctx, storage.DropRecoveredReplicaCommand{Slot: int(req.Slot), Epoch: req.Epoch}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) ForwardWrite(ctx context.Context, req *grpcproto.ForwardWriteRequest) (*grpcproto.Empty, error) {
	if s.authorizer != nil {
		if err := s.authorizer.requireStorageIdentityMatch(ctx, req.FromNodeId); err != nil {
			return nil, encodeError(err)
		}
	}
	if err := s.node.HandleForwardWrite(ctx, storage.ForwardWriteRequest{
		Operation: storage.WriteOperation{
			Slot:     int(req.Operation.Slot),
			Sequence: req.Operation.Sequence,
			Kind:     storage.OperationKind(req.Operation.Kind),
			Key:      req.Operation.Key,
			Value:    req.Operation.Value,
			Metadata: derefObjectMetadata(fromProtoObjectMetadata(req.Operation.Metadata)),
		},
		FromNodeID:   req.FromNodeId,
		ChainVersion: req.ChainVersion,
	}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) CommitWrite(ctx context.Context, req *grpcproto.CommitWriteRequest) (*grpcproto.Empty, error) {
	if s.authorizer != nil {
		if err := s.authorizer.requireStorageIdentityMatch(ctx, req.FromNodeId); err != nil {
			return nil, encodeError(err)
		}
	}
	if err := s.node.HandleCommitWrite(ctx, storage.CommitWriteRequest{
		Slot:         int(req.Slot),
		Sequence:     req.Sequence,
		FromNodeID:   req.FromNodeId,
		ChainVersion: req.ChainVersion,
	}); err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.Empty{}, nil
}

func (s *StorageGRPCServer) FetchSnapshot(req *grpcproto.FetchSnapshotRequest, stream grpcproto.StorageService_FetchSnapshotServer) error {
	snapshot, committedSequence, err := s.node.CommittedSnapshotWithSequence(int(req.Slot))
	if err != nil {
		return encodeError(err)
	}
	for _, entry := range protoSnapshot(snapshot) {
		if err := stream.Send(entry); err != nil {
			return err
		}
	}
	stream.SetTrailer(metadata.Pairs("x-craq-committed-sequence", fmt.Sprintf("%d", committedSequence)))
	return nil
}

func (s *StorageGRPCServer) FetchCommittedSequence(ctx context.Context, req *grpcproto.FetchCommittedSequenceRequest) (*grpcproto.FetchCommittedSequenceResponse, error) {
	sequence, err := s.node.HighestCommittedSequence(int(req.Slot))
	if err != nil {
		return nil, encodeError(err)
	}
	return &grpcproto.FetchCommittedSequenceResponse{Sequence: sequence}, nil
}

func joinErrors(errs ...error) error {
	var filtered []error
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	return errors.Join(filtered...)
}
