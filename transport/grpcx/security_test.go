package grpcx_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/client"
	"github.com/danthegoodman1/craq/coordinator"
	coordruntime "github.com/danthegoodman1/craq/coordinator/runtime"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

func TestClientTransportTLSModesOverGRPC(t *testing.T) {
	t.Run("tls only", func(t *testing.T) {
		fixture := newSecurityFixture(t)
		serverFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
		node := mustSingleReplicaNode(t, context.Background(), "storage-a", storage.Config{NodeID: "storage-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 7, 1)
		server, address := mustStartSecureStorageServer(t, node, serverFiles, grpcx.ServerTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   serverFiles.certPath,
			KeyFile:    serverFiles.keyPath,
			ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
		})
		defer func() { _ = server.Close() }()

		pool := fixture.mustClientPool(grpcx.ClientTLSConfig{CAFile: fixture.clusterCA.caPath, ServerName: "storage"})
		transport := grpcx.NewClientTransport(pool)
		assertPutGetDelete(t, transport, address, 7)
	})

	t.Run("tls with optional client cert", func(t *testing.T) {
		fixture := newSecurityFixture(t)
		serverFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
		clientFiles := fixture.mustWriteLeaf("client", "client-a", fixture.clusterCA)
		node := mustSingleReplicaNode(t, context.Background(), "storage-a", storage.Config{NodeID: "storage-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 8, 1)
		server, address := mustStartSecureStorageServer(t, node, serverFiles, grpcx.ServerTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   serverFiles.certPath,
			KeyFile:    serverFiles.keyPath,
			ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
		})
		defer func() { _ = server.Close() }()

		pool := fixture.mustClientPool(grpcx.ClientTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   clientFiles.certPath,
			KeyFile:    clientFiles.keyPath,
			ServerName: "storage",
		})
		transport := grpcx.NewClientTransport(pool)
		assertPutGetDelete(t, transport, address, 8)
	})

	t.Run("client cert required", func(t *testing.T) {
		fixture := newSecurityFixture(t)
		serverFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
		node := mustSingleReplicaNode(t, context.Background(), "storage-a", storage.Config{NodeID: "storage-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 9, 1)
		server, address := mustStartSecureStorageServer(t, node, serverFiles, grpcx.ServerTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   serverFiles.certPath,
			KeyFile:    serverFiles.keyPath,
			ClientAuth: grpcx.ClientAuthModeRequireAndVerify,
		})
		defer func() { _ = server.Close() }()

		pool := fixture.mustClientPool(grpcx.ClientTLSConfig{CAFile: fixture.clusterCA.caPath, ServerName: "storage"})
		transport := grpcx.NewClientTransport(pool)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := transport.Get(ctx, address, storage.ClientGetRequest{
			Slot:                 9,
			Key:                  "alpha",
			ExpectedChainVersion: 1,
		})
		var authErr *grpcx.TransportAuthError
		if !errors.As(err, &authErr) || !errors.Is(err, grpcx.ErrTransportUnauthenticated) {
			t.Fatalf("Get error = %v, want unauthenticated transport error", err)
		}
	})
}

func TestInternalGRPCSecurityAuthorization(t *testing.T) {
	t.Run("storage control rejects storage-node certificate", func(t *testing.T) {
		fixture := newSecurityFixture(t)
		serverFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
		storageClientFiles := fixture.mustWriteLeaf("storage", "storage-b", fixture.clusterCA)
		node := mustOpenNode(t, context.Background(), storage.Config{NodeID: "storage-a"}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport())
		server, address := mustStartSecureStorageServer(t, node, serverFiles, grpcx.ServerTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   serverFiles.certPath,
			KeyFile:    serverFiles.keyPath,
			ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
		})
		defer func() { _ = server.Close() }()

		pool := fixture.mustClientPool(grpcx.ClientTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   storageClientFiles.certPath,
			KeyFile:    storageClientFiles.keyPath,
			ServerName: "storage",
		})
		err := grpcx.NewStorageNodeClient(address, pool).AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{
			Assignment: storage.ReplicaAssignment{Slot: 1, ChainVersion: 1, Role: storage.ReplicaRoleSingle},
		})
		if !errors.Is(err, grpcx.ErrTransportPermissionDenied) {
			t.Fatalf("AddReplicaAsTail error = %v, want permission denied", err)
		}
	})

	t.Run("replication rejects coordinator certificate", func(t *testing.T) {
		fixture := newSecurityFixture(t)
		serverFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
		coordinatorFiles := fixture.mustWriteLeaf("coordinator", "coordinator", fixture.clusterCA)
		node := mustSingleReplicaNode(t, context.Background(), "storage-a", storage.Config{NodeID: "storage-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 2, 1)
		server, address := mustStartSecureStorageServer(t, node, serverFiles, grpcx.ServerTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   serverFiles.certPath,
			KeyFile:    serverFiles.keyPath,
			ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
		})
		defer func() { _ = server.Close() }()

		pool := fixture.mustClientPool(grpcx.ClientTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   coordinatorFiles.certPath,
			KeyFile:    coordinatorFiles.keyPath,
			ServerName: "storage",
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := grpcx.NewReplicationTransport(pool).FetchCommittedSequence(ctx, address, 2)
		if !errors.Is(err, grpcx.ErrTransportPermissionDenied) {
			t.Fatalf("FetchCommittedSequence error = %v, want permission denied", err)
		}
	})

	t.Run("report rpc rejects mismatched node identity", func(t *testing.T) {
		fixture := newSecurityFixture(t)
		coordFiles := fixture.mustWriteLeaf("coordinator", "coordinator", fixture.clusterCA)
		storageFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
		server, address := mustStartSecureCoordinator(t, fixture.clusterCA.caPath, coordFiles, nil)
		defer func() { _ = server.Close() }()

		pool := fixture.mustClientPool(grpcx.ClientTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   storageFiles.certPath,
			KeyFile:    storageFiles.keyPath,
			ServerName: "coordinator",
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := grpcx.NewCoordinatorReporterClient("storage-b", address, pool).ReportNodeHeartbeat(ctx, storage.NodeStatus{
			NodeID: "storage-b",
		})
		if !errors.Is(err, grpcx.ErrTransportPermissionDenied) {
			t.Fatalf("ReportNodeHeartbeat error = %v, want permission denied", err)
		}
	})

	t.Run("ca signed identity with mismatched from_node_id is denied", func(t *testing.T) {
		fixture := newSecurityFixture(t)
		serverFiles := fixture.mustWriteLeaf("storage", "storage-a", fixture.clusterCA)
		intruderFiles := fixture.mustWriteLeaf("storage", "intruder", fixture.clusterCA)
		node := mustSingleReplicaNode(t, context.Background(), "storage-a", storage.Config{NodeID: "storage-a"}, storage.NewInMemoryCoordinatorClient(), storage.NewInMemoryReplicationTransport(), 3, 1)
		server, address := mustStartSecureStorageServer(t, node, serverFiles, grpcx.ServerTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   serverFiles.certPath,
			KeyFile:    serverFiles.keyPath,
			ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
		})
		defer func() { _ = server.Close() }()

		pool := fixture.mustClientPool(grpcx.ClientTLSConfig{
			CAFile:     fixture.clusterCA.caPath,
			CertFile:   intruderFiles.certPath,
			KeyFile:    intruderFiles.keyPath,
			ServerName: "storage",
		})
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err := grpcx.NewStorageNodeClient(address, pool).UpdateChainPeers(ctx, storage.UpdateChainPeersCommand{
			Assignment: storage.ReplicaAssignment{
				Slot:         3,
				ChainVersion: 1,
				Role:         storage.ReplicaRoleSingle,
			},
		})
		if !errors.Is(err, grpcx.ErrTransportPermissionDenied) {
			t.Fatalf("UpdateChainPeers error = %v, want permission denied", err)
		}
	})
}

func TestSecureEndToEndClusterOverGRPC(t *testing.T) {
	ctx := context.Background()
	fixture := newSecurityFixture(t)
	coordFiles := fixture.mustWriteLeaf("coordinator", "coordinator", fixture.clusterCA)
	aFiles := fixture.mustWriteLeaf("storage", "a", fixture.clusterCA)
	cFiles := fixture.mustWriteLeaf("storage", "c", fixture.clusterCA)

	coordPool := fixture.mustClientPool(grpcx.ClientTLSConfig{
		CAFile:     fixture.clusterCA.caPath,
		CertFile:   coordFiles.certPath,
		KeyFile:    coordFiles.keyPath,
		ServerName: "storage",
	})
	coordStore := coordruntime.NewInMemoryStore()
	server, err := coordserver.OpenWithConfig(ctx, coordStore, nil, coordserver.ServerConfig{
		NodeClientFactory: grpcx.NewDynamicNodeClientFactory(coordPool),
	})
	if err != nil {
		t.Fatalf("OpenWithConfig returned error: %v", err)
	}
	coordAddress := mustReserveAddress(t)
	coordListener, err := net.Listen("tcp", coordAddress)
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	coordService, err := grpcx.NewCoordinatorGRPCServerWithTLS(server, &grpcx.ServerTLSConfig{
		CAFile:     fixture.clusterCA.caPath,
		CertFile:   coordFiles.certPath,
		KeyFile:    coordFiles.keyPath,
		ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
	})
	if err != nil {
		t.Fatalf("NewCoordinatorGRPCServerWithTLS returned error: %v", err)
	}
	go func() {
		if serveErr := coordService.Serve(coordListener); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			panic(serveErr)
		}
	}()
	defer func() { _ = coordService.Close() }()

	adminPool := fixture.mustClientPool(grpcx.ClientTLSConfig{CAFile: fixture.clusterCA.caPath, ServerName: "coordinator"})
	admin := grpcx.NewCoordinatorAdminClient(coordAddress, adminPool)

	nodeA := coordinator.Node{ID: "a", RPCAddress: mustReserveAddress(t), FailureDomains: map[string]string{"rack": "r1", "az": "az1"}}
	nodeC := coordinator.Node{ID: "c", RPCAddress: mustReserveAddress(t), FailureDomains: map[string]string{"rack": "r2", "az": "az2"}}

	aCoordinatorPool := fixture.mustClientPool(grpcx.ClientTLSConfig{
		CAFile:     fixture.clusterCA.caPath,
		CertFile:   aFiles.certPath,
		KeyFile:    aFiles.keyPath,
		ServerName: "coordinator",
	})
	aNode := mustSingleReplicaNode(t, ctx, "a", storage.Config{NodeID: "a"}, &suppressingReadyCoordinatorClient{
		delegate: grpcx.NewCoordinatorReporterClient("a", coordAddress, aCoordinatorPool),
	}, storage.NewInMemoryReplicationTransport(), 0, 1)
	aServer := mustStartSecureStorageServerAt(t, aNode, nodeA.RPCAddress, aFiles, grpcx.ServerTLSConfig{
		CAFile:     fixture.clusterCA.caPath,
		CertFile:   aFiles.certPath,
		KeyFile:    aFiles.keyPath,
		ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
	})
	defer func() { _ = aServer.Close() }()

	cCoordinatorPool := fixture.mustClientPool(grpcx.ClientTLSConfig{
		CAFile:     fixture.clusterCA.caPath,
		CertFile:   cFiles.certPath,
		KeyFile:    cFiles.keyPath,
		ServerName: "coordinator",
	})
	cReplicationPool := fixture.mustClientPool(grpcx.ClientTLSConfig{
		CAFile:     fixture.clusterCA.caPath,
		CertFile:   cFiles.certPath,
		KeyFile:    cFiles.keyPath,
		ServerName: "storage",
	})
	cNode := mustOpenNode(t, ctx, storage.Config{NodeID: "c"}, storage.NewInMemoryBackend(), grpcx.NewCoordinatorReporterClient("c", coordAddress, cCoordinatorPool), grpcx.NewReplicationTransport(cReplicationPool))
	cServer := mustStartSecureStorageServerAt(t, cNode, nodeC.RPCAddress, cFiles, grpcx.ServerTLSConfig{
		CAFile:     fixture.clusterCA.caPath,
		CertFile:   cFiles.certPath,
		KeyFile:    cFiles.keyPath,
		ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
	})
	defer func() { _ = cServer.Close() }()

	if _, err := admin.Bootstrap(ctx, coordruntime.Command{
		ID:   "bootstrap-secure",
		Kind: coordruntime.CommandKindBootstrap,
		Bootstrap: &coordruntime.BootstrapCommand{
			Config: coordinator.Config{SlotCount: 1, ReplicationFactor: 1},
			Nodes:  []coordinator.Node{nodeA, nodeC},
		},
	}); err != nil {
		t.Fatalf("Bootstrap returned error: %v", err)
	}

	if _, err := admin.BeginDrainNode(ctx, coordruntime.Command{
		ID:              "drain-a",
		ExpectedVersion: 1,
		Kind:            coordruntime.CommandKindReconfigure,
		Reconfigure: &coordruntime.ReconfigureCommand{
			Events: []coordinator.Event{{Kind: coordinator.EventKindBeginDrainNode, NodeID: "a"}},
		},
	}); err != nil {
		t.Fatalf("BeginDrainNode returned error: %v", err)
	}

	state := cNode.State()
	replica, ok := state.Replicas[0]
	if !ok || replica.State != storage.ReplicaStateCatchingUp {
		t.Fatalf("node c replica = %#v, want catching_up", replica)
	}

	coordStorageClient := grpcx.NewStorageNodeClient(nodeC.RPCAddress, coordPool)
	if err := coordStorageClient.ActivateReplica(ctx, storage.ActivateReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
	if err := grpcx.NewStorageNodeClient(nodeA.RPCAddress, coordPool).RemoveReplica(ctx, storage.RemoveReplicaCommand{Slot: 0}); err != nil {
		t.Fatalf("RemoveReplica returned error: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	for {
		snapCtx, snapCancel := context.WithTimeout(waitCtx, 100*time.Millisecond)
		snapshot, err := admin.RoutingSnapshot(snapCtx)
		snapCancel()
		if err == nil &&
			snapshot.SlotCount == 1 &&
			len(snapshot.Slots) == 1 &&
			snapshot.Slots[0].Readable &&
			snapshot.Slots[0].Writable {
			break
		}
		select {
		case <-waitCtx.Done():
			t.Fatalf("timed out waiting for secure drained cluster to become writable: %v", waitCtx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}

	routerPool := fixture.mustClientPool(grpcx.ClientTLSConfig{CAFile: fixture.clusterCA.caPath, ServerName: "storage"})
	router, err := client.NewRouter(admin, grpcx.NewClientTransport(routerPool))
	if err != nil {
		t.Fatalf("NewRouter returned error: %v", err)
	}
	if err := router.Refresh(ctx); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}
	if _, err := router.Put(ctx, "alpha", "one"); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	result, err := router.Get(ctx, "alpha")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !result.Found || result.Value != "one" {
		t.Fatalf("Get result = %#v, want found value", result)
	}
}

func assertPutGetDelete(t *testing.T, transport *grpcx.ClientTransport, address string, slot int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := transport.Put(ctx, address, storage.ClientPutRequest{
		Slot:                 slot,
		Key:                  "alpha",
		Value:                "one",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("Put returned error: %v", err)
	}
	result, err := transport.Get(ctx, address, storage.ClientGetRequest{
		Slot:                 slot,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	})
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if !result.Found || result.Value != "one" {
		t.Fatalf("Get result = %#v, want found value", result)
	}
	if _, err := transport.Delete(ctx, address, storage.ClientDeleteRequest{
		Slot:                 slot,
		Key:                  "alpha",
		ExpectedChainVersion: 1,
	}); err != nil {
		t.Fatalf("Delete returned error: %v", err)
	}
}

type securityFixture struct {
	t         *testing.T
	rootDir   string
	clusterCA *testCA
}

func newSecurityFixture(t *testing.T) *securityFixture {
	t.Helper()
	rootDir := t.TempDir()
	return &securityFixture{
		t:         t,
		rootDir:   rootDir,
		clusterCA: mustNewTestCA(t, filepath.Join(rootDir, "cluster-ca"), "cluster-ca"),
	}
}

func (f *securityFixture) mustNewCA(name string) *testCA {
	f.t.Helper()
	return mustNewTestCA(f.t, filepath.Join(f.rootDir, name), name)
}

func (f *securityFixture) mustWriteLeaf(role string, identity string, ca *testCA) certFiles {
	f.t.Helper()
	dir := filepath.Join(f.rootDir, role+"-"+identity)
	certPEM, keyPEM := mustIssueLeafCertificate(f.t, ca, role, identity)
	certPath := filepath.Join(dir, "tls.crt")
	keyPath := filepath.Join(dir, "tls.key")
	writeBytes(f.t, certPath, certPEM)
	writeBytes(f.t, keyPath, keyPEM)
	return certFiles{certPath: certPath, keyPath: keyPath}
}

func (f *securityFixture) mustClientPool(cfg grpcx.ClientTLSConfig) *grpcx.ConnPool {
	f.t.Helper()
	pool, err := grpcx.NewConnPoolWithTLS(cfg)
	if err != nil {
		f.t.Fatalf("NewConnPoolWithTLS returned error: %v", err)
	}
	f.t.Cleanup(func() { _ = pool.Close() })
	return pool
}

type suppressingReadyCoordinatorClient struct {
	delegate storage.CoordinatorClient
}

func (c *suppressingReadyCoordinatorClient) RegisterNode(ctx context.Context, reg storage.NodeRegistration) error {
	return c.delegate.RegisterNode(ctx, reg)
}

func (c *suppressingReadyCoordinatorClient) ReportReplicaReady(ctx context.Context, slot int, epoch uint64) error {
	return nil
}

func (c *suppressingReadyCoordinatorClient) ReportReplicaRemoved(ctx context.Context, slot int, epoch uint64) error {
	return c.delegate.ReportReplicaRemoved(ctx, slot, epoch)
}

func (c *suppressingReadyCoordinatorClient) ReportNodeRecovered(ctx context.Context, report storage.NodeRecoveryReport) error {
	return c.delegate.ReportNodeRecovered(ctx, report)
}

func (c *suppressingReadyCoordinatorClient) ReportNodeHeartbeat(ctx context.Context, status storage.NodeStatus) error {
	return c.delegate.ReportNodeHeartbeat(ctx, status)
}

type certFiles struct {
	certPath string
	keyPath  string
}

type testCA struct {
	caPath  string
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

func mustNewTestCA(t *testing.T, dir string, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey returned error: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(time.Now().UnixNano()),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, key.Public(), key)
	if err != nil {
		t.Fatalf("CreateCertificate returned error: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate returned error: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	writeBytes(t, filepath.Join(dir, "ca.pem"), certPEM)
	return &testCA{
		caPath:  filepath.Join(dir, "ca.pem"),
		cert:    cert,
		key:     key,
		certPEM: certPEM,
	}
}

func mustIssueLeafCertificate(t *testing.T, ca *testCA, role string, identity string) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey returned error: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: role},
		DNSNames:     certificateDNSNames(role, identity),
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "craq", Path: "/" + identity}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca.cert, key.Public(), ca.key)
	if err != nil {
		t.Fatalf("CreateCertificate returned error: %v", err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("MarshalECPrivateKey returned error: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
}

func certificateDNSNames(role string, identity string) []string {
	if identity == role {
		return []string{identity}
	}
	return []string{identity, role}
}

func mustStartSecureStorageServer(t *testing.T, node *storage.Node, files certFiles, cfg grpcx.ServerTLSConfig) (*grpcx.StorageGRPCServer, string) {
	t.Helper()
	address := mustReserveAddress(t)
	return mustStartSecureStorageServerAt(t, node, address, files, cfg), address
}

func mustStartSecureStorageServerAt(t *testing.T, node *storage.Node, address string, files certFiles, cfg grpcx.ServerTLSConfig) *grpcx.StorageGRPCServer {
	t.Helper()
	cfg.CertFile = files.certPath
	cfg.KeyFile = files.keyPath
	server, err := grpcx.NewStorageGRPCServerWithTLS(node, &cfg)
	if err != nil {
		t.Fatalf("NewStorageGRPCServerWithTLS returned error: %v", err)
	}
	lis, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	go func() {
		if serveErr := server.Serve(lis); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			panic(serveErr)
		}
	}()
	return server
}

func mustStartSecureCoordinator(t *testing.T, caPath string, files certFiles, coordinatorPool *grpcx.ConnPool) (*grpcx.CoordinatorGRPCServer, string) {
	t.Helper()
	server, err := coordserver.OpenWithConfig(context.Background(), coordruntime.NewInMemoryStore(), nil, coordserver.ServerConfig{
		NodeClientFactory: grpcx.NewDynamicNodeClientFactory(coordinatorPool),
	})
	if err != nil {
		t.Fatalf("coordserver.OpenWithConfig returned error: %v", err)
	}
	service, err := grpcx.NewCoordinatorGRPCServerWithTLS(server, &grpcx.ServerTLSConfig{
		CAFile:     caPath,
		CertFile:   files.certPath,
		KeyFile:    files.keyPath,
		ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
	})
	if err != nil {
		t.Fatalf("NewCoordinatorGRPCServerWithTLS returned error: %v", err)
	}
	address := mustReserveAddress(t)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	go func() {
		if serveErr := service.Serve(lis); serveErr != nil && !errors.Is(serveErr, net.ErrClosed) {
			panic(serveErr)
		}
	}()
	return service, address
}

func writeBytes(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	return contents
}
