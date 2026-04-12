package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkQueuedTransportSubmitPut(b *testing.B) {
	ctx := context.Background()
	nodes, _, repl := setupActiveChainWithQueuedTransportForBenchmark(b, 21, []string{"head", "mid", "tail"})

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("k-%d", i)
		if _, err := runQueuedCommitResultWithDelivery(ctx, repl, func() (CommitResult, error) {
			return nodes["head"].SubmitPut(ctx, 21, key, "value")
		}); err != nil {
			b.Fatalf("SubmitPut returned error: %v", err)
		}
	}
}

func BenchmarkInMemoryTransportSubmitPutThroughput(b *testing.B) {
	for _, concurrency := range []int{32, 64, 128, 256} {
		b.Run(fmt.Sprintf("c%d", concurrency), func(b *testing.B) {
			slotCount := concurrency
			if slotCount < 64 {
				slotCount = 64
			}
			slots := make([]int, 0, slotCount)
			for slot := 0; slot < slotCount; slot++ {
				slots = append(slots, 1000+slot)
			}
			nodes := setupActiveSlotsWithInMemoryTransportForBenchmark(b, slots, []string{"head", "mid", "tail"})
			seedProtocolBenchmarkKeys(b, nodes["head"], slots, 8)
			benchmarkDirectSubmitPutThroughput(b, nodes["head"], slots, concurrency, 8)
		})
	}
}

func setupActiveChainWithQueuedTransportForBenchmark(
	b *testing.B,
	slot int,
	nodeIDs []string,
) (map[string]*Node, map[string]*InMemoryBackend, *QueuedInMemoryReplicationTransport) {
	b.Helper()
	ctx := context.Background()
	transport := NewQueuedInMemoryReplicationTransport()
	nodes := make(map[string]*Node, len(nodeIDs))
	backends := make(map[string]*InMemoryBackend, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := NewInMemoryBackend()
		backends[nodeID] = backend
		transport.Register(nodeID, backend)
		node, err := NewNode(ctx, Config{NodeID: nodeID}, backend, NewInMemoryCoordinatorClient(), transport)
		if err != nil {
			b.Fatalf("NewNode(%q) returned error: %v", nodeID, err)
		}
		nodes[nodeID] = node
		transport.RegisterNode(nodeID, node)
	}

	mustActivateReplicaForBenchmark(b, nodes[nodeIDs[0]], slot, ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         ReplicaRoleSingle,
	})
	for i := 1; i < len(nodeIDs); i++ {
		if err := nodes[nodeIDs[i]].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
			Assignment: ReplicaAssignment{
				Slot:         slot,
				ChainVersion: 1,
				Role:         ReplicaRoleTail,
				Peers:        ChainPeers{PredecessorNodeID: nodeIDs[i-1]},
			},
		}); err != nil {
			b.Fatalf("AddReplicaAsTail(%q) returned error: %v", nodeIDs[i], err)
		}
		if err := nodes[nodeIDs[i]].ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
			b.Fatalf("ActivateReplica(%q) returned error: %v", nodeIDs[i], err)
		}
	}
	for i, nodeID := range nodeIDs {
		role := ReplicaRoleMiddle
		switch {
		case len(nodeIDs) == 1:
			role = ReplicaRoleSingle
		case i == 0:
			role = ReplicaRoleHead
		case i == len(nodeIDs)-1:
			role = ReplicaRoleTail
		}
		assignment := ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 1,
			Role:         role,
		}
		if i > 0 {
			assignment.Peers.PredecessorNodeID = nodeIDs[i-1]
		}
		if i+1 < len(nodeIDs) {
			assignment.Peers.SuccessorNodeID = nodeIDs[i+1]
		}
		if err := nodes[nodeID].UpdateChainPeers(ctx, UpdateChainPeersCommand{Assignment: assignment}); err != nil {
			b.Fatalf("UpdateChainPeers(%q) returned error: %v", nodeID, err)
		}
	}
	return nodes, backends, transport
}

func mustActivateReplicaForBenchmark(b *testing.B, node *Node, slot int, assignment ReplicaAssignment) {
	b.Helper()
	ctx := context.Background()
	if err := node.AddReplicaAsTail(ctx, AddReplicaAsTailCommand{Assignment: assignment}); err != nil {
		b.Fatalf("AddReplicaAsTail returned error: %v", err)
	}
	if err := node.ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
		b.Fatalf("ActivateReplica returned error: %v", err)
	}
}

func setupActiveSlotsWithInMemoryTransportForBenchmark(
	b *testing.B,
	slots []int,
	nodeIDs []string,
) map[string]*Node {
	b.Helper()
	ctx := context.Background()
	transport := NewInMemoryReplicationTransport()
	nodes := make(map[string]*Node, len(nodeIDs))
	for _, nodeID := range nodeIDs {
		backend := NewInMemoryBackend()
		transport.Register(nodeID, backend)
		node, err := NewNode(ctx, Config{NodeID: nodeID}, backend, NewInMemoryCoordinatorClient(), transport)
		if err != nil {
			b.Fatalf("NewNode(%q) returned error: %v", nodeID, err)
		}
		nodes[nodeID] = node
		transport.RegisterNode(nodeID, node)
	}
	for _, slot := range slots {
		mustActivateReplicaForBenchmark(b, nodes[nodeIDs[0]], slot, ReplicaAssignment{
			Slot:         slot,
			ChainVersion: 1,
			Role:         ReplicaRoleSingle,
		})
		for i := 1; i < len(nodeIDs); i++ {
			if err := nodes[nodeIDs[i]].AddReplicaAsTail(ctx, AddReplicaAsTailCommand{
				Assignment: ReplicaAssignment{
					Slot:         slot,
					ChainVersion: 1,
					Role:         ReplicaRoleTail,
					Peers:        ChainPeers{PredecessorNodeID: nodeIDs[i-1]},
				},
			}); err != nil {
				b.Fatalf("AddReplicaAsTail(%q, slot=%d) returned error: %v", nodeIDs[i], slot, err)
			}
			if err := nodes[nodeIDs[i]].ActivateReplica(ctx, ActivateReplicaCommand{Slot: slot}); err != nil {
				b.Fatalf("ActivateReplica(%q, slot=%d) returned error: %v", nodeIDs[i], slot, err)
			}
		}
		for i, nodeID := range nodeIDs {
			role := ReplicaRoleMiddle
			switch {
			case len(nodeIDs) == 1:
				role = ReplicaRoleSingle
			case i == 0:
				role = ReplicaRoleHead
			case i == len(nodeIDs)-1:
				role = ReplicaRoleTail
			}
			assignment := ReplicaAssignment{
				Slot:         slot,
				ChainVersion: 1,
				Role:         role,
			}
			if i > 0 {
				assignment.Peers.PredecessorNodeID = nodeIDs[i-1]
			}
			if i+1 < len(nodeIDs) {
				assignment.Peers.SuccessorNodeID = nodeIDs[i+1]
			}
			if err := nodes[nodeID].UpdateChainPeers(ctx, UpdateChainPeersCommand{Assignment: assignment}); err != nil {
				b.Fatalf("UpdateChainPeers(%q, slot=%d) returned error: %v", nodeID, slot, err)
			}
		}
	}
	return nodes
}

func seedProtocolBenchmarkKeys(tb testing.TB, head *Node, slots []int, keysPerSlot int) {
	tb.Helper()
	ctx := context.Background()
	for _, slot := range slots {
		for keyIndex := 0; keyIndex < keysPerSlot; keyIndex++ {
			key := fmt.Sprintf("bench-put-slot-%d-%02d", slot, keyIndex)
			if _, err := head.SubmitPut(ctx, slot, key, "seed"); err != nil {
				tb.Fatalf("SubmitPut(seed, slot=%d, key=%q) returned error: %v", slot, key, err)
			}
		}
	}
}

func benchmarkDirectSubmitPutThroughput(b *testing.B, head *Node, slots []int, concurrency int, keysPerSlot int) {
	ctx := context.Background()
	var next atomic.Uint64
	start := make(chan struct{})
	var wg sync.WaitGroup
	var (
		errMu    sync.Mutex
		firstErr error
	)

	b.ReportAllocs()
	b.ResetTimer()

	for worker := 0; worker < concurrency; worker++ {
		workerID := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				index := int(next.Add(1) - 1)
				if index >= b.N {
					return
				}
				slot := slots[index%len(slots)]
				keyIndex := (index/len(slots) + workerID) % keysPerSlot
				key := fmt.Sprintf("bench-put-slot-%d-%02d", slot, keyIndex)
				value := fmt.Sprintf("value-%d-%d", workerID, index)
				if _, err := head.SubmitPut(ctx, slot, key, value); err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	b.StopTimer()
	errMu.Lock()
	defer errMu.Unlock()
	if firstErr != nil {
		b.Fatalf("SubmitPut returned error: %v", firstErr)
	}
}
