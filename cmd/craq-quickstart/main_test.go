package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/quickstart"
	"github.com/danthegoodman1/craq/storage"
)

var (
	buildQuickstartOnce sync.Once
	buildQuickstartPath string
	buildQuickstartErr  error
)

func TestQuickstartSteadyStateCRUD(t *testing.T) {
	bin := buildQuickstartBinary(t)
	cfg := testQuickstartConfig(t)
	manifestPath := writeManifest(t, cfg)

	coord := startQuickstartProcess(t, bin, "coordinator", "--manifest", manifestPath)
	startQuickstartProcess(t, bin, "storage", "--manifest", manifestPath, "--node", "a")
	startQuickstartProcess(t, bin, "storage", "--manifest", manifestPath, "--node", "b")
	startQuickstartProcess(t, bin, "storage", "--manifest", manifestPath, "--node", "c")

	waitForRoute(t, cfg.Coordinator.AdminAddress, 5*time.Second, func(route coordserver.SlotRoute) bool {
		return route.Readable && route.Writable && route.HeadNodeID == "a" && route.TailNodeID == "c"
	}, coord)
	waitForStorageState(t, cfg.Nodes[1].AdminAddress, 2*time.Second, func(state storage.AdminState) bool {
		replica, ok := state.Node.Replicas[0]
		return ok && replica.State == storage.ReplicaStateActive && replica.Assignment.Peers.SuccessorNodeID == "c"
	})
	waitForStorageState(t, cfg.Nodes[2].AdminAddress, 2*time.Second, func(state storage.AdminState) bool {
		replica, ok := state.Node.Replicas[0]
		return ok && replica.State == storage.ReplicaStateActive && replica.Assignment.Role == storage.ReplicaRoleTail && replica.Assignment.Peers.PredecessorNodeID == "b"
	})

	out := runQuickstartCLI(t, bin, "set", "--manifest", manifestPath, "alpha", "one")
	assertContainsAll(t, out, []string{
		"operation=set",
		"slot=0",
		"contacted_node=a",
		"contacted_endpoint=" + cfg.Nodes[0].RPCAddress,
		"applied=true",
		"metadata.version=1",
	})

	out = waitForCLIContains(t, 2*time.Second, func() string {
		return runQuickstartCLI(t, bin, "get", "--manifest", manifestPath, "alpha")
	}, []string{
		"operation=get",
		"slot=0",
		"found=true",
		"value=one",
		"metadata.version=1",
	})

	out = runQuickstartCLI(t, bin, "del", "--manifest", manifestPath, "alpha")
	assertContainsAll(t, out, []string{
		"operation=del",
		"slot=0",
		"contacted_node=a",
		"contacted_endpoint=" + cfg.Nodes[0].RPCAddress,
		"applied=true",
	})
}

func TestQuickstartFailureAndReplacementFlow(t *testing.T) {
	bin := buildQuickstartBinary(t)
	cfg := testQuickstartConfig(t)
	manifestPath := writeManifest(t, cfg)

	coord := startQuickstartProcess(t, bin, "coordinator", "--manifest", manifestPath)
	startQuickstartProcess(t, bin, "storage", "--manifest", manifestPath, "--node", "a")
	startQuickstartProcess(t, bin, "storage", "--manifest", manifestPath, "--node", "b")
	storageC := startQuickstartProcess(t, bin, "storage", "--manifest", manifestPath, "--node", "c")

	waitForRoute(t, cfg.Coordinator.AdminAddress, 5*time.Second, func(route coordserver.SlotRoute) bool {
		return route.Readable && route.Writable && route.HeadNodeID == "a" && route.TailNodeID == "c"
	}, coord, storageC)
	waitForStorageState(t, cfg.Nodes[1].AdminAddress, 2*time.Second, func(state storage.AdminState) bool {
		replica, ok := state.Node.Replicas[0]
		return ok && replica.State == storage.ReplicaStateActive && replica.Assignment.Peers.SuccessorNodeID == "c"
	})
	waitForStorageState(t, cfg.Nodes[2].AdminAddress, 2*time.Second, func(state storage.AdminState) bool {
		replica, ok := state.Node.Replicas[0]
		return ok && replica.State == storage.ReplicaStateActive && replica.Assignment.Role == storage.ReplicaRoleTail && replica.Assignment.Peers.PredecessorNodeID == "b"
	})

	out := runQuickstartCLI(t, bin, "set", "--manifest", manifestPath, "alpha", "one")
	assertContainsAll(t, out, []string{"contacted_node=a", "metadata.version=1"})

	start := time.Now()
	storageC.stop(t)
	waitForAdminState(t, cfg.Coordinator.AdminAddress, 3*time.Second, func(state coordserver.AdminState) bool {
		route, ok := routeForSlotZero(state)
		if !ok {
			return false
		}
		return state.Current.Cluster.NodeHealthByID["c"] == coordinator.NodeHealthDead &&
			route.Readable &&
			route.TailNodeID == "b"
	}, coord)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("dead-node detection took %s, want <= 3s", elapsed)
	}

	out = waitForCLIContains(t, 2*time.Second, func() string {
		return runQuickstartCLI(t, bin, "get", "--manifest", manifestPath, "alpha")
	}, []string{
		"operation=get",
		"slot=0",
		"value=one",
	})

	startQuickstartProcess(t, bin, "storage", "--manifest", manifestPath, "--node", "d")
	waitForAdminState(t, cfg.Coordinator.AdminAddress, 8*time.Second, func(state coordserver.AdminState) bool {
		route, ok := routeForSlotZero(state)
		if !ok {
			return false
		}
		return state.Current.Cluster.ReadyNodeIDs["d"] &&
			route.Readable &&
			route.Writable &&
			route.TailNodeID == "d"
	}, coord)

	out = waitForCLIContains(t, 3*time.Second, func() string {
		return runQuickstartCLI(t, bin, "get", "--manifest", manifestPath, "alpha")
	}, []string{
		"operation=get",
		"slot=0",
		"value=one",
	})
}

type quickstartProcess struct {
	name    string
	cmd     *exec.Cmd
	logs    bytes.Buffer
	doneCh  chan struct{}
	waitMu  sync.Mutex
	waitErr error
	once    sync.Once
}

func startQuickstartProcess(t *testing.T, bin string, args ...string) *quickstartProcess {
	t.Helper()
	ctx := context.Background()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Dir = repoRoot(t)
	proc := &quickstartProcess{
		name:   strings.Join(args, " "),
		cmd:    cmd,
		doneCh: make(chan struct{}),
	}
	cmd.Stdout = &proc.logs
	cmd.Stderr = &proc.logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", proc.name, err)
	}
	go func() {
		proc.waitMu.Lock()
		proc.waitErr = cmd.Wait()
		proc.waitMu.Unlock()
		close(proc.doneCh)
	}()
	t.Cleanup(func() { proc.stop(t) })
	return proc
}

func (p *quickstartProcess) stop(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		if p.cmd.Process == nil {
			return
		}
		_ = p.cmd.Process.Kill()
		select {
		case <-p.doneCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for process %s to exit", p.name)
		}
	})
}

func (p *quickstartProcess) errIfExited() error {
	select {
	case <-p.doneCh:
		p.waitMu.Lock()
		defer p.waitMu.Unlock()
		if p.waitErr == nil {
			return fmt.Errorf("process %s exited unexpectedly", p.name)
		}
		return fmt.Errorf("process %s exited: %w\n%s", p.name, p.waitErr, p.logs.String())
	default:
		return nil
	}
}

func runQuickstartCLI(t *testing.T, bin string, args ...string) string {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = repoRoot(t)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("run %s: %v\n%s", strings.Join(args, " "), err, string(output))
	}
	return string(output)
}

func waitForRoute(t *testing.T, adminAddress string, timeout time.Duration, pred func(coordserver.SlotRoute) bool, procs ...*quickstartProcess) coordserver.SlotRoute {
	t.Helper()
	var matched coordserver.SlotRoute
	waitForAdminState(t, adminAddress, timeout, func(state coordserver.AdminState) bool {
		route, ok := routeForSlotZero(state)
		if !ok {
			return false
		}
		if pred(route) {
			matched = route
			return true
		}
		return false
	}, procs...)
	return matched
}

func waitForAdminState(t *testing.T, adminAddress string, timeout time.Duration, pred func(coordserver.AdminState) bool, procs ...*quickstartProcess) coordserver.AdminState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last coordserver.AdminState
	var lastErr error
	for time.Now().Before(deadline) {
		for _, proc := range procs {
			if proc == nil {
				continue
			}
			if err := proc.errIfExited(); err != nil {
				t.Fatal(err)
			}
		}
		state, err := fetchAdminState(adminAddress)
		if err == nil {
			last = state
			if pred(state) {
				return state
			}
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastErr != nil && last.Current.Version == 0 && len(last.RoutingSnapshot.Slots) == 0 {
		t.Fatalf("admin state for %s did not become ready: %v", adminAddress, lastErr)
	}
	encoded, _ := json.MarshalIndent(last, "", "  ")
	t.Fatalf("admin state for %s did not satisfy predicate within %s\n%s", adminAddress, timeout, string(encoded))
	return coordserver.AdminState{}
}

func fetchAdminState(address string) (coordserver.AdminState, error) {
	var state coordserver.AdminState
	err := fetchJSON("http://"+address+"/admin/v1/state", &state)
	return state, err
}

func fetchStorageAdminState(address string) (storage.AdminState, error) {
	var state storage.AdminState
	err := fetchJSON("http://"+address+"/admin/v1/state", &state)
	return state, err
}

func fetchJSON(url string, out any) error {
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("admin http status %d: %s", resp.StatusCode, string(body))
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return err
	}
	return nil
}

func routeForSlotZero(state coordserver.AdminState) (coordserver.SlotRoute, bool) {
	for _, route := range state.RoutingSnapshot.Slots {
		if route.Slot == 0 {
			return route, true
		}
	}
	return coordserver.SlotRoute{}, false
}

func assertContainsAll(t *testing.T, output string, want []string) {
	t.Helper()
	for _, needle := range want {
		if !strings.Contains(output, needle) {
			t.Fatalf("output missing %q\n%s", needle, output)
		}
	}
}

func waitForCLIContains(t *testing.T, timeout time.Duration, run func() string, want []string) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		last = run()
		all := true
		for _, needle := range want {
			if !strings.Contains(last, needle) {
				all = false
				break
			}
		}
		if all {
			return last
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("cli output did not satisfy expected substrings within %s\n%s", timeout, last)
	return ""
}

func waitForStorageState(t *testing.T, adminAddress string, timeout time.Duration, pred func(storage.AdminState) bool) storage.AdminState {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last storage.AdminState
	var lastErr error
	for time.Now().Before(deadline) {
		state, err := fetchStorageAdminState(adminAddress)
		if err == nil {
			last = state
			if pred(state) {
				return state
			}
		} else {
			lastErr = err
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("storage admin state for %s did not become ready: %v", adminAddress, lastErr)
	}
	encoded, _ := json.MarshalIndent(last, "", "  ")
	t.Fatalf("storage admin state for %s did not satisfy predicate within %s\n%s", adminAddress, timeout, string(encoded))
	return storage.AdminState{}
}

func buildQuickstartBinary(t *testing.T) string {
	t.Helper()
	buildQuickstartOnce.Do(func() {
		dir, err := os.MkdirTemp("", "craq-quickstart-bin")
		if err != nil {
			buildQuickstartErr = err
			return
		}
		buildQuickstartPath = filepath.Join(dir, "craq-quickstart")
		cmd := exec.Command("go", "build", "-o", buildQuickstartPath, "./cmd/craq-quickstart")
		cmd.Dir = repoRoot(t)
		output, err := cmd.CombinedOutput()
		if err != nil {
			buildQuickstartErr = fmt.Errorf("go build quickstart binary: %w\n%s", err, string(output))
		}
	})
	if buildQuickstartErr != nil {
		t.Fatal(buildQuickstartErr)
	}
	return buildQuickstartPath
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	root := filepath.Clean(filepath.Join(wd, "..", ".."))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root %q invalid: %v", root, err)
	}
	return root
}

func writeManifest(t *testing.T, cfg quickstart.Config) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cluster.json")
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}
	return path
}

func testQuickstartConfig(t *testing.T) quickstart.Config {
	t.Helper()
	return quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        reserveAddress(t),
			AdminAddress:      reserveAddress(t),
			SlotCount:         1,
			ReplicationFactor: 3,
		},
		Nodes: []quickstart.Node{
			testNode("a", reserveAddress(t), reserveAddress(t)),
			testNode("b", reserveAddress(t), reserveAddress(t)),
			testNode("c", reserveAddress(t), reserveAddress(t)),
			testNode("d", reserveAddress(t), reserveAddress(t)),
		},
	}
}

func testNode(id string, rpcAddress string, adminAddress string) quickstart.Node {
	return quickstart.Node{
		ID:           id,
		RPCAddress:   rpcAddress,
		AdminAddress: adminAddress,
		FailureDomains: map[string]string{
			"host": "host-" + id,
			"rack": "rack-" + id,
			"az":   "az-" + id,
		},
	}
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	defer func() { _ = lis.Close() }()
	return lis.Addr().String()
}

func TestMain(m *testing.M) {
	code := m.Run()
	if buildQuickstartPath != "" {
		_ = os.Remove(buildQuickstartPath)
	}
	os.Exit(code)
}
