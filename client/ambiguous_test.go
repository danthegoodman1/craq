package client

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
)

func TestEndToEndAmbiguousWriteMayCommitLater(t *testing.T) {
	ctx := context.Background()
	h := newAmbiguousRouterHarness(t, true)

	if _, err := h.router.Put(ctx, "k", "v1"); err == nil {
		t.Fatal("Put unexpectedly succeeded")
	} else {
		var ambiguous *storage.AmbiguousWriteError
		if !errors.As(err, &ambiguous) {
			t.Fatalf("Put error = %v, want ambiguous write", err)
		}
		if !errors.Is(err, storage.ErrAmbiguousWrite) {
			t.Fatalf("Put error = %v, want ErrAmbiguousWrite", err)
		}
	}

	readResult, err := h.router.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get before delayed delivery returned error: %v", err)
	}
	if readResult.Found {
		t.Fatalf("read before delayed delivery = %#v, want not found", readResult)
	}

	if err := h.repl.DeliverNextForward(ctx); err != nil {
		t.Fatalf("DeliverNextForward returned error: %v", err)
	}

	readResult, err = h.router.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get after delayed delivery returned error: %v", err)
	}
	assertFoundReadResult(t, readResult, 0, 1, "v1")
}

func TestEndToEndAmbiguousWriteMayNeverCommit(t *testing.T) {
	ctx := context.Background()
	h := newAmbiguousRouterHarness(t, true)

	if _, err := h.router.Put(ctx, "k", "v1"); err == nil {
		t.Fatal("Put unexpectedly succeeded")
	} else if !errors.Is(err, storage.ErrAmbiguousWrite) {
		t.Fatalf("Put error = %v, want ambiguous write", err)
	}

	readResult, err := h.router.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if readResult.Found {
		t.Fatalf("read result = %#v, want not found", readResult)
	}
}

func TestRetryAfterAmbiguousWriteIsANewWrite(t *testing.T) {
	ctx := context.Background()
	h := newAmbiguousRouterHarness(t, true)

	if _, err := h.router.Put(ctx, "k", "v1"); err == nil {
		t.Fatal("first Put unexpectedly succeeded")
	} else if !errors.Is(err, storage.ErrAmbiguousWrite) {
		t.Fatalf("first Put error = %v, want ambiguous write", err)
	}
	if err := h.repl.DeliverNextForward(ctx); err != nil {
		t.Fatalf("DeliverNextForward returned error: %v", err)
	}

	h.repl.SetBlockAwait(false)
	result, err := routerPutWithDelivery(t, ctx, h.router, h.repl, "k", "v1")
	if err != nil {
		t.Fatalf("second Put returned error: %v", err)
	}
	assertAppliedCommitResult(t, result, 0, 2)

	readResult, err := h.router.Get(ctx, "k")
	if err != nil {
		t.Fatalf("Get after retry returned error: %v", err)
	}
	assertFoundReadResult(t, readResult, 0, 1, "v1")
	if got, want := mustHighestCommittedSequence(t, h.head, 0), uint64(2); got != want {
		t.Fatalf("head highest committed = %d, want %d", got, want)
	}
	if got, want := mustHighestCommittedSequence(t, h.tail, 0), uint64(2); got != want {
		t.Fatalf("tail highest committed = %d, want %d", got, want)
	}
}

func TestAmbiguousWriteHistoryIsDeterministic(t *testing.T) {
	run := func(t *testing.T) (storage.ReadResult, uint64, uint64) {
		t.Helper()
		ctx := context.Background()
		h := newAmbiguousRouterHarness(t, true)
		if _, err := h.router.Put(ctx, "k", "v1"); err == nil {
			t.Fatal("Put unexpectedly succeeded")
		} else if !errors.Is(err, storage.ErrAmbiguousWrite) {
			t.Fatalf("Put error = %v, want ambiguous write", err)
		}
		if err := h.repl.DeliverNextForward(ctx); err != nil {
			t.Fatalf("DeliverNextForward returned error: %v", err)
		}
		readResult, err := h.router.Get(ctx, "k")
		if err != nil {
			t.Fatalf("Get returned error: %v", err)
		}
		return readResult, mustHighestCommittedSequence(t, h.head, 0), mustHighestCommittedSequence(t, h.tail, 0)
	}

	firstRead, firstHeadSequence, firstTailSequence := run(t)
	secondRead, secondHeadSequence, secondTailSequence := run(t)

	if !reflect.DeepEqual(firstRead, secondRead) {
		t.Fatalf("read results differ across repeated histories: first=%#v second=%#v", firstRead, secondRead)
	}
	if firstHeadSequence != secondHeadSequence {
		t.Fatalf("head committed sequence differs across repeated histories: first=%d second=%d", firstHeadSequence, secondHeadSequence)
	}
	if firstTailSequence != secondTailSequence {
		t.Fatalf("tail committed sequence differs across repeated histories: first=%d second=%d", firstTailSequence, secondTailSequence)
	}
}

type ambiguousRouterHarness struct {
	router *Router
	repl   *manualAmbiguousReplicationTransport
	head   *storage.Node
	tail   *storage.Node
}

func newAmbiguousRouterHarness(t *testing.T, blockAwait bool) *ambiguousRouterHarness {
	t.Helper()

	repl := newManualAmbiguousReplicationTransport()
	repl.blockAwait = blockAwait

	headBackend := storage.NewInMemoryBackend()
	tailBackend := storage.NewInMemoryBackend()
	head := mustNewStorageNode(t, context.Background(), storage.Config{
		NodeID:             "head",
		WriteCommitTimeout: time.Nanosecond,
	}, headBackend, storage.NewInMemoryCoordinatorClient(), repl)
	tail := mustNewStorageNode(t, context.Background(), storage.Config{
		NodeID: "tail",
	}, tailBackend, storage.NewInMemoryCoordinatorClient(), repl)

	repl.Register("head", headBackend, head)
	repl.Register("tail", tailBackend, tail)

	mustAddAndActivateReplica(t, head, storage.ReplicaAssignment{
		Slot:         0,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleHead,
		Peers: storage.ChainPeers{
			SuccessorNodeID: "tail",
		},
	})
	mustAddAndActivateReplica(t, tail, storage.ReplicaAssignment{
		Slot:         0,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleTail,
		Peers: storage.ChainPeers{
			PredecessorNodeID: "head",
		},
	})

	transport := NewInMemoryTransport()
	transport.RegisterNode("head", head)
	transport.RegisterNode("tail", tail)
	router := mustNewRouter(t, &scriptedSnapshotSource{
		snapshots: []coordserver.RoutingSnapshot{{
			Version:   1,
			SlotCount: 1,
			Slots: []coordserver.SlotRoute{{
				Slot:         0,
				ChainVersion: 1,
				HeadNodeID:   "head",
				TailNodeID:   "tail",
				Writable:     true,
				Readable:     true,
			}},
		}},
	}, transport)
	if err := router.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh returned error: %v", err)
	}

	return &ambiguousRouterHarness{
		router: router,
		repl:   repl,
		head:   head,
		tail:   tail,
	}
}

type replicationTestNode interface {
	HandleForwardWrite(ctx context.Context, req storage.ForwardWriteRequest) error
	HandleCommitWrite(ctx context.Context, req storage.CommitWriteRequest) error
}

type capturedForward struct {
	toNodeID string
	req      storage.ForwardWriteRequest
}

type manualAmbiguousReplicationTransport struct {
	mu         sync.Mutex
	backends   map[string]storage.Backend
	nodes      map[string]replicationTestNode
	forwards   []capturedForward
	blockAwait bool
}

func newManualAmbiguousReplicationTransport() *manualAmbiguousReplicationTransport {
	return &manualAmbiguousReplicationTransport{
		backends: map[string]storage.Backend{},
		nodes:    map[string]replicationTestNode{},
	}
}

func (t *manualAmbiguousReplicationTransport) Register(nodeID string, backend storage.Backend, node replicationTestNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.backends[nodeID] = backend
	t.nodes[nodeID] = node
}

func (t *manualAmbiguousReplicationTransport) SetBlockAwait(block bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.blockAwait = block
}

func (t *manualAmbiguousReplicationTransport) FetchSnapshot(_ context.Context, fromNodeID string, slot int) (storage.Snapshot, uint64, error) {
	t.mu.Lock()
	backend, ok := t.backends[fromNodeID]
	t.mu.Unlock()
	if !ok {
		return nil, 0, fmt.Errorf("%w: node %q", storage.ErrSnapshotSourceUnavailable, fromNodeID)
	}
	snap, err := backend.CommittedSnapshot(slot)
	return snap, 0, err
}

func (t *manualAmbiguousReplicationTransport) FetchCommittedSequence(_ context.Context, fromNodeID string, slot int) (uint64, error) {
	t.mu.Lock()
	backend, ok := t.backends[fromNodeID]
	t.mu.Unlock()
	if !ok {
		return 0, fmt.Errorf("%w: node %q", storage.ErrSnapshotSourceUnavailable, fromNodeID)
	}
	return backend.HighestCommittedSequence(slot)
}

func (t *manualAmbiguousReplicationTransport) ForwardWrite(ctx context.Context, toNodeID string, req storage.ForwardWriteRequest) error {
	t.mu.Lock()
	node, ok := t.nodes[toNodeID]
	blockAwait := t.blockAwait
	if !ok {
		t.mu.Unlock()
		return fmt.Errorf("%w: node %q", storage.ErrSnapshotSourceUnavailable, toNodeID)
	}
	cloned := req
	cloned.Operation = storage.WriteOperation{
		Slot:     req.Operation.Slot,
		Sequence: req.Operation.Sequence,
		Kind:     req.Operation.Kind,
		Key:      req.Operation.Key,
		Value:    req.Operation.Value,
	}
	if !blockAwait {
		t.mu.Unlock()
		return node.HandleForwardWrite(ctx, cloned)
	}
	t.forwards = append(t.forwards, capturedForward{toNodeID: toNodeID, req: cloned})
	t.mu.Unlock()
	return nil
}

func (t *manualAmbiguousReplicationTransport) CommitWrite(ctx context.Context, toNodeID string, req storage.CommitWriteRequest) error {
	t.mu.Lock()
	node, ok := t.nodes[toNodeID]
	t.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: node %q", storage.ErrSnapshotSourceUnavailable, toNodeID)
	}
	return node.HandleCommitWrite(ctx, req)
}

func (t *manualAmbiguousReplicationTransport) AwaitWriteCommit(ctx context.Context, check func() bool) error {
	if check() {
		return nil
	}
	t.mu.Lock()
	blockAwait := t.blockAwait
	t.mu.Unlock()
	if blockAwait {
		<-ctx.Done()
		return ctx.Err()
	}
	for !check() {
		if t.Pending() == 0 {
			return fmt.Errorf("%w: no captured forwards available", storage.ErrStateMismatch)
		}
		if err := t.DeliverNextForward(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (t *manualAmbiguousReplicationTransport) Pending() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.forwards)
}

func (t *manualAmbiguousReplicationTransport) DeliverNext(ctx context.Context) error {
	return t.DeliverNextForward(ctx)
}

func (t *manualAmbiguousReplicationTransport) DeliverNextForward(ctx context.Context) error {
	t.mu.Lock()
	if len(t.forwards) == 0 {
		t.mu.Unlock()
		return fmt.Errorf("%w: no captured forwards available", storage.ErrStateMismatch)
	}
	msg := t.forwards[0]
	t.forwards = t.forwards[1:]
	node, ok := t.nodes[msg.toNodeID]
	t.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: node %q", storage.ErrSnapshotSourceUnavailable, msg.toNodeID)
	}
	return node.HandleForwardWrite(ctx, msg.req)
}

func mustNewStorageNode(
	t *testing.T,
	ctx context.Context,
	cfg storage.Config,
	backend storage.Backend,
	coord storage.CoordinatorClient,
	repl storage.ReplicationTransport,
) *storage.Node {
	t.Helper()
	if cfg.Clock == nil {
		cfg.Clock = &fakeStorageClock{now: time.Unix(0, 0).UTC()}
	}
	node, err := storage.NewNode(ctx, cfg, backend, coord, repl)
	if err != nil {
		t.Fatalf("storage.NewNode returned error: %v", err)
	}
	return node
}

func assertAppliedCommitResult(t *testing.T, result storage.CommitResult, slot int, sequence uint64) {
	t.Helper()
	if got, want := result.Slot, slot; got != want {
		t.Fatalf("slot = %d, want %d", got, want)
	}
	if got, want := result.Sequence, sequence; got != want {
		t.Fatalf("sequence = %d, want %d", got, want)
	}
	if !result.Applied || result.Metadata == nil {
		t.Fatalf("result = %#v, want applied metadata-bearing commit", result)
	}
}

func assertFoundReadResult(t *testing.T, result storage.ReadResult, slot int, chainVersion uint64, value string) {
	t.Helper()
	if got, want := result.Slot, slot; got != want {
		t.Fatalf("slot = %d, want %d", got, want)
	}
	if got, want := result.ChainVersion, chainVersion; got != want {
		t.Fatalf("chain version = %d, want %d", got, want)
	}
	if !result.Found || result.Value != value || result.Metadata == nil {
		t.Fatalf("result = %#v, want found value with metadata", result)
	}
}

type fakeStorageClock struct {
	now time.Time
}

func (c *fakeStorageClock) Now() time.Time {
	return c.now
}

func mustAddAndActivateReplica(t *testing.T, node *storage.Node, assignment storage.ReplicaAssignment) {
	t.Helper()
	if err := node.AddReplicaAsTail(context.Background(), storage.AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		t.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(context.Background(), storage.ActivateReplicaCommand{Slot: assignment.Slot}); err != nil {
		t.Fatalf("ActivateReplica returned error: %v", err)
	}
}

func mustHighestCommittedSequence(t *testing.T, node *storage.Node, slot int) uint64 {
	t.Helper()
	sequence, err := node.HighestCommittedSequence(slot)
	if err != nil {
		t.Fatalf("HighestCommittedSequence returned error: %v", err)
	}
	return sequence
}
