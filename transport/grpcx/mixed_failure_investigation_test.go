package grpcx_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/storage"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

type mixedFailureNodeFixture struct {
	node         *storage.Node
	address      string
	tracePath    string
	artifactPath string
}

func TestHotSlotMixedLoadGRPCChainCapturesTimeoutArtifactsIfStalled(t *testing.T) {
	ctx := context.Background()
	pool := grpcx.NewConnPool()
	t.Cleanup(func() { _ = pool.Close() })

	repl := grpcx.NewReplicationTransport(pool)
	clientTransport := grpcx.NewClientTransport(pool)
	slot := 361

	fixtures := map[string]mixedFailureNodeFixture{}
	for _, nodeID := range []string{"head", "mid", "tail"} {
		root := filepath.Join(t.TempDir(), nodeID)
		tracePath := filepath.Join(root, "write-trace.jsonl")
		artifactPath := filepath.Join(root, "write-timeout-artifacts.jsonl")
		node := mustOpenNode(t, ctx, storage.Config{
			NodeID:                         nodeID,
			WriteCommitTimeout:             250 * time.Millisecond,
			WriteTraceOutputPath:           tracePath,
			WriteTraceSampleRate:           1,
			WriteTimeoutArtifactOutputPath: artifactPath,
		}, storage.NewInMemoryBackend(), storage.NewInMemoryCoordinatorClient(), repl)
		address := mustReserveAddress(t)
		server := mustStartStorageServerAt(t, node, address)
		t.Cleanup(func() { _ = server.Close() })
		fixtures[nodeID] = mixedFailureNodeFixture{
			node:         node,
			address:      address,
			tracePath:    tracePath,
			artifactPath: artifactPath,
		}
	}

	mustActivateReplica(t, fixtures["head"].node, storage.ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleHead,
		Peers: storage.ChainPeers{
			SuccessorNodeID: "mid",
			SuccessorTarget: fixtures["mid"].address,
			TailNodeID:      "tail",
			TailTarget:      fixtures["tail"].address,
		},
	})
	mustActivateReplica(t, fixtures["mid"].node, storage.ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleMiddle,
		Peers: storage.ChainPeers{
			PredecessorNodeID: "head",
			PredecessorTarget: fixtures["head"].address,
			SuccessorNodeID:   "tail",
			SuccessorTarget:   fixtures["tail"].address,
			TailNodeID:        "tail",
			TailTarget:        fixtures["tail"].address,
		},
	})
	mustActivateReplica(t, fixtures["tail"].node, storage.ReplicaAssignment{
		Slot:         slot,
		ChainVersion: 1,
		Role:         storage.ReplicaRoleTail,
		Peers: storage.ChainPeers{
			PredecessorNodeID: "mid",
			PredecessorTarget: fixtures["mid"].address,
			TailNodeID:        "tail",
			TailTarget:        fixtures["tail"].address,
		},
	})

	for attempt := 0; attempt < 10; attempt++ {
		var writeErrs atomic.Int64
		var wg sync.WaitGroup
		for workerID := 0; workerID < 8; workerID++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				rng := rand.New(rand.NewSource(int64(attempt*100 + workerID + 1)))
				for op := 0; op < 128; op++ {
					key := fmt.Sprintf("hot-%03d", rng.Intn(16))
					reqCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
					if rng.Intn(100) < 80 {
						_, err := clientTransport.Put(reqCtx, fixtures["head"].address, storage.ClientPutRequest{
							Slot:                 slot,
							Key:                  key,
							Value:                fmt.Sprintf("v-%d-%d-%d", attempt, workerID, op),
							ExpectedChainVersion: 1,
						})
						if err != nil {
							writeErrs.Add(1)
						}
					} else {
						_, _ = clientTransport.Get(reqCtx, fixtures["head"].address, storage.ClientGetRequest{
							Slot:                 slot,
							Key:                  key,
							ExpectedChainVersion: 1,
						})
					}
					cancel()
				}
			}(workerID)
		}
		wg.Wait()
		if writeErrs.Load() == 0 {
			continue
		}
		artifact := loadFirstTimeoutArtifact(t, fixtures)
		if artifact.SlotState.NextSequence == 0 {
			t.Fatalf("timeout artifact missing slot state: %#v", artifact)
		}
		if len(artifact.SessionState) == 0 {
			t.Fatalf("timeout artifact missing session state: %#v", artifact)
		}
		return
	}
}

type timeoutArtifactForTest struct {
	SlotState struct {
		NextSequence uint64 `json:"next_sequence"`
	} `json:"slot_state"`
	SessionState []struct {
		Kind            string `json:"kind"`
		LocalSpoolDepth int    `json:"local_spool_depth"`
	} `json:"session_state"`
}

func loadFirstTimeoutArtifact(t *testing.T, fixtures map[string]mixedFailureNodeFixture) timeoutArtifactForTest {
	t.Helper()
	for _, fixture := range fixtures {
		data, err := os.ReadFile(fixture.artifactPath)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			t.Fatalf("ReadFile(%s) returned error: %v", fixture.artifactPath, err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var artifact timeoutArtifactForTest
			if err := json.Unmarshal([]byte(line), &artifact); err != nil {
				t.Fatalf("Unmarshal timeout artifact returned error: %v", err)
			}
			return artifact
		}
	}
	t.Fatal("expected at least one timeout artifact")
	return timeoutArtifactForTest{}
}
