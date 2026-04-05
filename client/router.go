package client

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"sync"

	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidConfig     = errors.New("invalid client router config")
	ErrSnapshotNotLoaded = errors.New("client routing snapshot not loaded")
	ErrNoRoute           = errors.New("client route unavailable")
)

type SnapshotSource interface {
	RoutingSnapshot(ctx context.Context) (coordserver.RoutingSnapshot, error)
}

type Transport interface {
	Get(ctx context.Context, nodeID string, req storage.ClientGetRequest) (storage.ReadResult, error)
	Put(ctx context.Context, nodeID string, req storage.ClientPutRequest) (storage.CommitResult, error)
	Delete(ctx context.Context, nodeID string, req storage.ClientDeleteRequest) (storage.CommitResult, error)
}

type Router struct {
	mu              sync.RWMutex
	source          SnapshotSource
	transport       Transport
	snapshot        *coordserver.RoutingSnapshot
	nextReadReplica map[int]int
}

func NewRouter(source SnapshotSource, transport Transport) (*Router, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: snapshot source must not be nil", ErrInvalidConfig)
	}
	if transport == nil {
		return nil, fmt.Errorf("%w: transport must not be nil", ErrInvalidConfig)
	}
	return &Router{
		source:          source,
		transport:       transport,
		nextReadReplica: map[int]int{},
	}, nil
}

func (r *Router) Refresh(ctx context.Context) error {
	snapshot, err := r.source.RoutingSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("err in r.source.RoutingSnapshot: %w", err)
	}
	cloned := cloneSnapshot(snapshot)
	r.mu.Lock()
	r.snapshot = &cloned
	r.mu.Unlock()
	return nil
}

func (r *Router) Snapshot() (coordserver.RoutingSnapshot, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.snapshot == nil {
		return coordserver.RoutingSnapshot{}, false
	}
	return cloneSnapshot(*r.snapshot), true
}

func RouteForKey(snapshot coordserver.RoutingSnapshot, key string) (coordserver.SlotRoute, error) {
	return routeForKey(&snapshot, key)
}

func (r *Router) Get(ctx context.Context, key string) (storage.ReadResult, error) {
	return r.GetWithConsistency(ctx, key, storage.ReadConsistencyLinearizable)
}

func (r *Router) GetWithConsistency(ctx context.Context, key string, consistency storage.ReadConsistency) (storage.ReadResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.ReadResult{}, err
	}
	return r.getWithSnapshot(ctx, key, consistency, snapshot, true)
}

func (r *Router) Put(ctx context.Context, key string, value string) (storage.CommitResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.CommitResult{}, err
	}
	return r.putWithSnapshot(ctx, key, value, storage.WriteConditions{}, snapshot, true)
}

func (r *Router) PutIf(ctx context.Context, key string, value string, conditions storage.WriteConditions) (storage.CommitResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.CommitResult{}, err
	}
	return r.putWithSnapshot(ctx, key, value, conditions, snapshot, true)
}

func (r *Router) Delete(ctx context.Context, key string) (storage.CommitResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.CommitResult{}, err
	}
	return r.deleteWithSnapshot(ctx, key, storage.WriteConditions{}, snapshot, true)
}

func (r *Router) DeleteIf(ctx context.Context, key string, conditions storage.WriteConditions) (storage.CommitResult, error) {
	snapshot, err := r.loadedSnapshot()
	if err != nil {
		return storage.CommitResult{}, err
	}
	return r.deleteWithSnapshot(ctx, key, conditions, snapshot, true)
}

func (r *Router) loadedSnapshot() (*coordserver.RoutingSnapshot, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.snapshot == nil {
		return nil, ErrSnapshotNotLoaded
	}
	cloned := cloneSnapshot(*r.snapshot)
	return &cloned, nil
}

func (r *Router) getWithSnapshot(
	ctx context.Context,
	key string,
	consistency storage.ReadConsistency,
	snapshot *coordserver.RoutingSnapshot,
	allowRefresh bool,
) (storage.ReadResult, error) {
	route, err := routeForKey(snapshot, key)
	if err != nil {
		return storage.ReadResult{}, err
	}
	readReplicas := routeReadReplicas(route)
	if !route.Readable || len(readReplicas) == 0 {
		return storage.ReadResult{}, fmt.Errorf("%w: slot %d is not readable", ErrNoRoute, route.Slot)
	}
	start := r.nextReadStart(route.Slot, len(readReplicas))
	ordered := orderedReadReplicas(readReplicas, start)
	var lastErr error
	for _, replica := range ordered {
		req := storage.ClientGetRequest{
			Slot:                 route.Slot,
			Key:                  key,
			ExpectedChainVersion: route.ChainVersion,
			Consistency:          consistency,
		}
		result, err := r.transport.Get(ctx, routeTarget(replica.Endpoint, replica.NodeID), req)
		if err == nil {
			return result, nil
		}
		if isRoutingMismatch(err) {
			if allowRefresh {
				if refreshErr := r.Refresh(ctx); refreshErr != nil {
					return storage.ReadResult{}, refreshErr
				}
				refreshed, loadErr := r.loadedSnapshot()
				if loadErr != nil {
					return storage.ReadResult{}, loadErr
				}
				return r.getWithSnapshot(ctx, key, consistency, refreshed, false)
			}
			return storage.ReadResult{}, err
		}
		if isReadDependencyUnavailable(err) {
			tail := tailReadReplica(route)
			if tail != nil && tail.NodeID != replica.NodeID {
				tailResult, tailErr := r.transport.Get(ctx, routeTarget(tail.Endpoint, tail.NodeID), req)
				if tailErr == nil {
					return tailResult, nil
				}
				if isRoutingMismatch(tailErr) {
					if allowRefresh {
						if refreshErr := r.Refresh(ctx); refreshErr != nil {
							return storage.ReadResult{}, refreshErr
						}
						refreshed, loadErr := r.loadedSnapshot()
						if loadErr != nil {
							return storage.ReadResult{}, loadErr
						}
						return r.getWithSnapshot(ctx, key, consistency, refreshed, false)
					}
					return storage.ReadResult{}, tailErr
				}
				lastErr = tailErr
				if !isTransportUnavailable(tailErr) {
					return storage.ReadResult{}, tailErr
				}
				continue
			}
			lastErr = err
			continue
		}
		if isTransportUnavailable(err) {
			lastErr = err
			continue
		}
		return storage.ReadResult{}, err
	}
	if allowRefresh && isTransportUnavailable(lastErr) {
		if refreshErr := r.Refresh(ctx); refreshErr != nil {
			return storage.ReadResult{}, refreshErr
		}
		refreshed, loadErr := r.loadedSnapshot()
		if loadErr != nil {
			return storage.ReadResult{}, loadErr
		}
		return r.getWithSnapshot(ctx, key, consistency, refreshed, false)
	}
	if lastErr != nil {
		return storage.ReadResult{}, lastErr
	}
	return storage.ReadResult{}, fmt.Errorf("%w: slot %d is not readable", ErrNoRoute, route.Slot)
}

func (r *Router) putWithSnapshot(
	ctx context.Context,
	key string,
	value string,
	conditions storage.WriteConditions,
	snapshot *coordserver.RoutingSnapshot,
	allowRefresh bool,
) (storage.CommitResult, error) {
	route, err := routeForKey(snapshot, key)
	if err != nil {
		return storage.CommitResult{}, err
	}
	if !route.Writable || route.HeadNodeID == "" {
		return storage.CommitResult{}, fmt.Errorf("%w: slot %d is not writable", ErrNoRoute, route.Slot)
	}
	result, err := r.transport.Put(ctx, routeTarget(route.HeadEndpoint, route.HeadNodeID), storage.ClientPutRequest{
		Slot:                 route.Slot,
		Key:                  key,
		Value:                value,
		ExpectedChainVersion: route.ChainVersion,
		Conditions:           conditions,
	})
	if err != nil {
		if allowRefresh && isRoutingMismatch(err) {
			if refreshErr := r.Refresh(ctx); refreshErr != nil {
				return storage.CommitResult{}, refreshErr
			}
			refreshed, loadErr := r.loadedSnapshot()
			if loadErr != nil {
				return storage.CommitResult{}, loadErr
			}
			return r.putWithSnapshot(ctx, key, value, conditions, refreshed, false)
		}
		return storage.CommitResult{}, err
	}
	return result, nil
}

func (r *Router) deleteWithSnapshot(
	ctx context.Context,
	key string,
	conditions storage.WriteConditions,
	snapshot *coordserver.RoutingSnapshot,
	allowRefresh bool,
) (storage.CommitResult, error) {
	route, err := routeForKey(snapshot, key)
	if err != nil {
		return storage.CommitResult{}, err
	}
	if !route.Writable || route.HeadNodeID == "" {
		return storage.CommitResult{}, fmt.Errorf("%w: slot %d is not writable", ErrNoRoute, route.Slot)
	}
	result, err := r.transport.Delete(ctx, routeTarget(route.HeadEndpoint, route.HeadNodeID), storage.ClientDeleteRequest{
		Slot:                 route.Slot,
		Key:                  key,
		ExpectedChainVersion: route.ChainVersion,
		Conditions:           conditions,
	})
	if err != nil {
		if allowRefresh && isRoutingMismatch(err) {
			if refreshErr := r.Refresh(ctx); refreshErr != nil {
				return storage.CommitResult{}, refreshErr
			}
			refreshed, loadErr := r.loadedSnapshot()
			if loadErr != nil {
				return storage.CommitResult{}, loadErr
			}
			return r.deleteWithSnapshot(ctx, key, conditions, refreshed, false)
		}
		return storage.CommitResult{}, err
	}
	return result, nil
}

func routeForKey(snapshot *coordserver.RoutingSnapshot, key string) (coordserver.SlotRoute, error) {
	if snapshot == nil {
		return coordserver.SlotRoute{}, ErrSnapshotNotLoaded
	}
	if snapshot.SlotCount <= 0 || len(snapshot.Slots) != snapshot.SlotCount {
		return coordserver.SlotRoute{}, fmt.Errorf("%w: invalid snapshot slot count %d", ErrNoRoute, snapshot.SlotCount)
	}
	slot := int(crc32.ChecksumIEEE([]byte(key)) % uint32(snapshot.SlotCount))
	return snapshot.Slots[slot], nil
}

func isRoutingMismatch(err error) bool {
	var mismatch *storage.RoutingMismatchError
	return errors.As(err, &mismatch)
}

func isReadDependencyUnavailable(err error) bool {
	var dependency *storage.ReadDependencyError
	return errors.As(err, &dependency)
}

func isTransportUnavailable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrNoRoute) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Unavailable
}

func routeTarget(endpoint string, fallbackNodeID string) string {
	if endpoint != "" {
		return endpoint
	}
	return fallbackNodeID
}

func routeReadReplicas(route coordserver.SlotRoute) []coordserver.ReadReplicaRoute {
	if len(route.ReadReplicas) > 0 {
		return append([]coordserver.ReadReplicaRoute(nil), route.ReadReplicas...)
	}
	if route.TailNodeID == "" {
		return nil
	}
	role := storage.ReplicaRoleTail
	if route.HeadNodeID == route.TailNodeID {
		role = storage.ReplicaRoleSingle
	}
	return []coordserver.ReadReplicaRoute{{
		NodeID:   route.TailNodeID,
		Endpoint: route.TailEndpoint,
		Role:     role,
	}}
}

func tailReadReplica(route coordserver.SlotRoute) *coordserver.ReadReplicaRoute {
	readReplicas := routeReadReplicas(route)
	if len(readReplicas) == 0 {
		return nil
	}
	tail := readReplicas[len(readReplicas)-1]
	return &tail
}

func orderedReadReplicas(readReplicas []coordserver.ReadReplicaRoute, start int) []coordserver.ReadReplicaRoute {
	if len(readReplicas) == 0 {
		return nil
	}
	ordered := make([]coordserver.ReadReplicaRoute, 0, len(readReplicas))
	for i := 0; i < len(readReplicas); i++ {
		ordered = append(ordered, readReplicas[(start+i)%len(readReplicas)])
	}
	return ordered
}

func (r *Router) nextReadStart(slot int, count int) int {
	if count <= 0 {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	start := r.nextReadReplica[slot] % count
	r.nextReadReplica[slot] = (start + 1) % count
	return start
}

func cloneSnapshot(snapshot coordserver.RoutingSnapshot) coordserver.RoutingSnapshot {
	clonedSlots := make([]coordserver.SlotRoute, 0, len(snapshot.Slots))
	for _, slot := range snapshot.Slots {
		cloned := slot
		cloned.ReadReplicas = append([]coordserver.ReadReplicaRoute(nil), slot.ReadReplicas...)
		clonedSlots = append(clonedSlots, cloned)
	}
	cloned := coordserver.RoutingSnapshot{
		Version:   snapshot.Version,
		SlotCount: snapshot.SlotCount,
		Slots:     clonedSlots,
	}
	return cloned
}
