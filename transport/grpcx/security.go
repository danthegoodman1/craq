package grpcx

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/danthegoodman1/craq/gologger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type ClientAuthMode string

const (
	ClientAuthModeDisabled         ClientAuthMode = "disabled"
	ClientAuthModeVerifyIfGiven    ClientAuthMode = "verify-if-given"
	ClientAuthModeRequireAndVerify ClientAuthMode = "require-and-verify"
)

type ClientTLSConfig struct {
	CAFile          string
	CertFile        string
	KeyFile         string
	ServerName      string
	Logger          *zerolog.Logger
	MetricsRegistry *prometheus.Registry
}

type ServerTLSConfig struct {
	CAFile          string
	CertFile        string
	KeyFile         string
	ClientAuth      ClientAuthMode
	Logger          *zerolog.Logger
	MetricsRegistry *prometheus.Registry
}

type rpcPlane string

const (
	rpcPlaneClientData        rpcPlane = "client_data"
	rpcPlaneCoordinatorAdmin  rpcPlane = "coordinator_admin"
	rpcPlaneCoordinatorReport rpcPlane = "coordinator_report"
	rpcPlaneStorageControl    rpcPlane = "storage_control"
	rpcPlaneStorageReplica    rpcPlane = "storage_replication"
)

type rpcAuthorizer struct{}

const (
	peerRoleCoordinator = "coordinator"
	peerRoleStorage     = "storage"
)

type peerIdentity struct {
	role      string
	logicalID string
}

func transportLoggerFromConfig(logger *zerolog.Logger) zerolog.Logger {
	if logger != nil {
		return logger.With().Logger()
	}
	return gologger.NewLogger()
}

func newClientTransportCredentials(cfg ClientTLSConfig) (credentials.TransportCredentials, error) {
	if err := validateClientTLSConfig(cfg); err != nil {
		return nil, err
	}
	rootCAs, err := loadCertPool(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    rootCAs,
		ServerName: cfg.ServerName,
	}
	if cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client key pair: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsConfig), nil
}

func newServerTransportCredentials(cfg ServerTLSConfig) (credentials.TransportCredentials, error) {
	if err := validateServerTLSConfig(cfg); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server key pair: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   mapClientAuthMode(cfg.ClientAuth),
	}
	if cfg.ClientAuth != ClientAuthModeDisabled {
		clientCAs, err := loadCertPool(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.ClientCAs = clientCAs
	}
	return credentials.NewTLS(tlsConfig), nil
}

func validateClientTLSConfig(cfg ClientTLSConfig) error {
	if cfg.CAFile == "" {
		return errors.New("client tls config requires ca file")
	}
	if (cfg.CertFile == "") != (cfg.KeyFile == "") {
		return errors.New("client tls config requires both cert and key when either is provided")
	}
	return nil
}

func validateServerTLSConfig(cfg ServerTLSConfig) error {
	if cfg.CertFile == "" || cfg.KeyFile == "" {
		return errors.New("server tls config requires cert and key")
	}
	switch cfg.ClientAuth {
	case "", ClientAuthModeDisabled, ClientAuthModeVerifyIfGiven, ClientAuthModeRequireAndVerify:
	default:
		return fmt.Errorf("invalid client auth mode %q", cfg.ClientAuth)
	}
	if cfg.ClientAuth == "" {
		cfg.ClientAuth = ClientAuthModeVerifyIfGiven
	}
	if cfg.ClientAuth != ClientAuthModeDisabled && cfg.CAFile == "" {
		return errors.New("server tls config requires ca file when client auth is enabled")
	}
	return nil
}

func mapClientAuthMode(mode ClientAuthMode) tls.ClientAuthType {
	switch mode {
	case ClientAuthModeDisabled:
		return tls.NoClientCert
	case ClientAuthModeRequireAndVerify:
		return tls.RequireAndVerifyClientCert
	case "", ClientAuthModeVerifyIfGiven:
		return tls.VerifyClientCertIfGiven
	default:
		return tls.VerifyClientCertIfGiven
	}
}

func loadCertPool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca file %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("append certs from %s: no certificates found", path)
	}
	return pool, nil
}

func newRPCAuthorizer() *rpcAuthorizer {
	return &rpcAuthorizer{}
}

func (a *rpcAuthorizer) unaryInterceptor(policy func(string) rpcPlane) grpc.UnaryServerInterceptor {
	if a == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := a.authorizePlane(ctx, policy(info.FullMethod)); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func (a *rpcAuthorizer) streamInterceptor(policy func(string) rpcPlane) grpc.StreamServerInterceptor {
	if a == nil {
		return nil
	}
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := a.authorizePlane(stream.Context(), policy(info.FullMethod)); err != nil {
			return err
		}
		return handler(srv, stream)
	}
}

func (a *rpcAuthorizer) authorizePlane(ctx context.Context, plane rpcPlane) error {
	switch plane {
	case rpcPlaneStorageControl:
		return a.requireRole(ctx, peerRoleCoordinator)
	case rpcPlaneCoordinatorReport, rpcPlaneStorageReplica:
		return a.requireRole(ctx, peerRoleStorage)
	default:
		return nil
	}
}

func (a *rpcAuthorizer) requireStorageIdentityMatch(ctx context.Context, claimed string) error {
	identity, err := a.requireIdentity(ctx)
	if err != nil {
		return err
	}
	if identity.role != peerRoleStorage {
		return status.Error(codes.PermissionDenied, "storage identity required")
	}
	if claimed != "" && claimed != identity.logicalID {
		return status.Error(codes.PermissionDenied, "peer identity does not match claimed node id")
	}
	return nil
}

func (a *rpcAuthorizer) requireRole(ctx context.Context, role string) error {
	identity, err := a.requireIdentity(ctx)
	if err != nil {
		return err
	}
	if identity.role != role {
		return status.Error(codes.PermissionDenied, fmt.Sprintf("%s identity required", role))
	}
	return nil
}

func (a *rpcAuthorizer) requireIdentity(ctx context.Context) (peerIdentity, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return peerIdentity{}, status.Error(codes.Unauthenticated, "missing peer context")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return peerIdentity{}, status.Error(codes.Unauthenticated, "missing tls peer identity")
	}
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.PeerCertificates) == 0 {
		return peerIdentity{}, status.Error(codes.Unauthenticated, "client certificate required")
	}
	identity, err := certificateIdentity(tlsInfo.State.PeerCertificates[0])
	if err != nil {
		return peerIdentity{}, status.Error(codes.PermissionDenied, err.Error())
	}
	return identity, nil
}

func certificateIdentity(cert *x509.Certificate) (peerIdentity, error) {
	if cert == nil {
		return peerIdentity{}, errors.New("missing peer certificate")
	}
	if cert.Subject.CommonName == "" {
		return peerIdentity{}, errors.New("peer certificate must use common name as role")
	}
	if cert.Subject.CommonName != peerRoleCoordinator && cert.Subject.CommonName != peerRoleStorage {
		return peerIdentity{}, fmt.Errorf("peer certificate common name %q is not a supported role", cert.Subject.CommonName)
	}
	if len(cert.DNSNames) == 0 || cert.DNSNames[0] == "" {
		return peerIdentity{}, errors.New("peer certificate must use first dns san as logical identity")
	}
	return peerIdentity{
		role:      cert.Subject.CommonName,
		logicalID: cert.DNSNames[0],
	}, nil
}

func (p peerIdentity) String() string {
	if p.logicalID == "" {
		return p.role
	}
	return p.role + ":" + p.logicalID
}

func coordinatorRPCPlane(fullMethod string) rpcPlane {
	switch fullMethod {
	case "/craq.v1.CoordinatorService/ReportReplicaReady",
		"/craq.v1.CoordinatorService/ReportReplicaRemoved",
		"/craq.v1.CoordinatorService/ReportNodeHeartbeat",
		"/craq.v1.CoordinatorService/ReportNodeRecovered":
		return rpcPlaneCoordinatorReport
	default:
		return rpcPlaneCoordinatorAdmin
	}
}

func storageRPCPlane(fullMethod string) rpcPlane {
	switch fullMethod {
	case "/craq.v1.StorageService/AddReplicaAsTail",
		"/craq.v1.StorageService/ActivateReplica",
		"/craq.v1.StorageService/MarkReplicaLeaving",
		"/craq.v1.StorageService/RemoveReplica",
		"/craq.v1.StorageService/UpdateChainPeers",
		"/craq.v1.StorageService/ResumeRecoveredReplica",
		"/craq.v1.StorageService/RecoverReplica",
		"/craq.v1.StorageService/DropRecoveredReplica":
		return rpcPlaneStorageControl
	case "/craq.v1.StorageService/ForwardWrite",
		"/craq.v1.StorageService/CommitWrite",
		"/craq.v1.StorageService/Replicate",
		"/craq.v1.StorageService/FetchSnapshot",
		"/craq.v1.StorageService/FetchCommittedSequence":
		return rpcPlaneStorageReplica
	default:
		return rpcPlaneClientData
	}
}
