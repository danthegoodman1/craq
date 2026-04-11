package benchmark

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/danthegoodman1/craq/client"
	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/coordserver"
	"github.com/danthegoodman1/craq/quickstart"
	"github.com/danthegoodman1/craq/transport/grpcx"
)

type SmokeLocalOptions struct {
	ProfilePath string
	RunName     string
	RepoRoot    string
}

type LocalSmokeReport struct {
	RunID          string                 `json:"run_id"`
	StartedAt      time.Time              `json:"started_at"`
	RoutingReadyAt time.Time              `json:"routing_ready_at"`
	FinishedAt     time.Time              `json:"finished_at"`
	Sanity         LocalSmokeSanity       `json:"sanity"`
	LastProgress   LocalSmokeProgress     `json:"last_progress"`
	RoutingSummary RoutingProgressSummary `json:"routing_summary"`
}

type LocalSmokeSanity struct {
	Key           string        `json:"key"`
	PutApplied    bool          `json:"put_applied"`
	DeleteApplied bool          `json:"delete_applied"`
	GetFound      bool          `json:"get_found"`
	GetValue      string        `json:"get_value"`
	FinalMissing  bool          `json:"final_missing"`
	RefreshTime   time.Duration `json:"refresh_time"`
	PutTime       time.Duration `json:"put_time"`
	GetTime       time.Duration `json:"get_time"`
	DeleteTime    time.Duration `json:"delete_time"`
	FinalGetTime  time.Duration `json:"final_get_time"`
}

type localDaemonSpec struct {
	Name       string
	Role       string
	ConfigPath string
	LogPath    string
}

type localDaemonHandle struct {
	Name    string
	LogPath string
	done    chan error
}

type LocalSmokeProgress struct {
	Version             uint64 `json:"version"`
	SlotCount           int    `json:"slot_count"`
	WritableSlots       int    `json:"writable_slots"`
	ReadableSlots       int    `json:"readable_slots"`
	SettledSlots        int    `json:"settled_slots"`
	PendingSlots        int    `json:"pending_slots"`
	OutboxEntries       int    `json:"outbox_entries"`
	ActivePeerRefreshes int    `json:"active_peer_refreshes"`
	HealthyNodes        int    `json:"healthy_nodes"`
	SuspectNodes        int    `json:"suspect_nodes"`
	DeadNodes           int    `json:"dead_nodes"`
}

type localSmokeLauncher interface {
	Start(ctx context.Context, spec localDaemonSpec) (*localDaemonHandle, error)
}

type execLocalSmokeLauncher struct{}

type inProcessLocalSmokeLauncher struct{}

type routingProgressRecord struct {
	Time time.Time `json:"time"`
	LocalSmokeProgress
}

func SmokeLocal(ctx context.Context, opts SmokeLocalOptions) (string, error) {
	return smokeLocalWithLauncher(ctx, opts, execLocalSmokeLauncher{})
}

func smokeLocalWithLauncher(ctx context.Context, opts SmokeLocalOptions, launcher localSmokeLauncher) (string, error) {
	profilePath := strings.TrimSpace(opts.ProfilePath)
	if profilePath == "" {
		profilePath = filepath.Join("profiles", "bench", "local_smoke.yaml")
	}
	logProgress("loading local smoke profile from %s", profilePath)

	repoRoot := opts.RepoRoot
	if repoRoot == "" {
		var err error
		repoRoot, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}
	profile, err := LoadProfile(profilePath)
	if err != nil {
		return "", err
	}

	git := gitSHA(ctx, repoRoot)
	runID := buildRunID(opts.RunName, git)
	runDir := filepath.Join(repoRoot, profile.Artifacts.RootDir, runID)
	logProgress("preparing local smoke run %s in %s", runID, runDir)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir local smoke run dir: %w", err)
	}

	manifest, err := renderLocalSmokeManifest(profile)
	if err != nil {
		return runDir, err
	}
	manifestPath := filepath.Join(runDir, "rendered", "manifest.json")
	if err := SaveManifest(manifestPath, manifest); err != nil {
		return runDir, err
	}

	state := RunState{
		RunID:           runID,
		RunName:         opts.RunName,
		GitSHA:          git,
		ProfilePath:     profilePath,
		Profile:         profile,
		CreatedAt:       time.Now().UTC(),
		Region:          "local",
		Topology:        "local",
		ClientPlacement: "local",
		ArtifactsDir:    filepath.Join(runDir, "artifacts"),
		Status:          "local_smoke_created",
		ManifestPath:    manifestPath,
		Notes:           []string{"local smoke preflight"},
	}
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return runDir, err
	}

	metadata := RunMetadata{
		RunID:           runID,
		RunName:         opts.RunName,
		GitSHA:          git,
		StartedAt:       time.Now().UTC(),
		Profile:         profile,
		Topology:        "local",
		ClientPlacement: "local",
		Manifest:        manifest,
	}
	if err := SaveJSON(filepath.Join(runDir, RunMetadataFileName), metadata); err != nil {
		return runDir, err
	}

	report, err := runLocalSmoke(ctx, runDir, state, manifest, launcher)
	if err != nil {
		state.Status = "local_smoke_failed"
		state.Notes = append(state.Notes, err.Error())
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return runDir, err
	}

	metadata.CompletedAt = report.FinishedAt
	if err := SaveJSON(filepath.Join(runDir, RunMetadataFileName), metadata); err != nil {
		state.Status = "local_smoke_failed"
		state.Notes = append(state.Notes, err.Error())
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return runDir, err
	}

	state.Status = "local_smoke_ok"
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return runDir, err
	}
	logProgress("local smoke run %s completed successfully", runID)
	return runDir, nil
}

func runLocalSmoke(ctx context.Context, runDir string, state RunState, manifest quickstart.Config, launcher localSmokeLauncher) (LocalSmokeReport, error) {
	startedAt := time.Now().UTC()
	artifactsDir := filepath.Join(runDir, "artifacts")
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		return LocalSmokeReport{}, fmt.Errorf("mkdir local artifacts: %w", err)
	}

	if err := os.MkdirAll(filepath.Join(runDir, "rendered", "local"), 0o755); err != nil {
		return LocalSmokeReport{}, fmt.Errorf("mkdir local rendered dir: %w", err)
	}

	coordCfg := CoordinatorProcessConfig{
		ManifestPath: state.ManifestPath,
		DataDir:      filepath.Join(runDir, "local", "coordinator"),
		Liveness:     benchmarkCoordinatorLivenessPolicy(state.Profile.Cluster),
		Reconfiguration: coordinator.ReconfigurationPolicy{
			MaxChangedChains: state.Profile.Cluster.Reconfiguration.MaxChangedChains,
		},
		TickInterval: state.Profile.Cluster.LivenessInterval,
		RPCDeadline:  state.Profile.Cluster.RPCDeadline,
	}
	if err := SaveJSON(filepath.Join(runDir, "rendered", "local", "coordinator.json"), coordCfg); err != nil {
		return LocalSmokeReport{}, err
	}

	for _, nodeID := range []string{"a", "b", "c"} {
		cfg := StorageProcessConfig{
			ManifestPath:       state.ManifestPath,
			NodeID:             nodeID,
			DataDir:            filepath.Join(runDir, "local", "storage-"+nodeID),
			HeartbeatInterval:  state.Profile.Cluster.HeartbeatInterval,
			ActivationInterval: state.Profile.Cluster.ActivationInterval,
			RPCDeadline:        state.Profile.Cluster.RPCDeadline,
		}
		if err := SaveJSON(filepath.Join(runDir, "rendered", "local", "storage-"+nodeID+".json"), cfg); err != nil {
			return LocalSmokeReport{}, err
		}
	}

	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	handles := make([]*localDaemonHandle, 0, 4)
	startHandle := func(name string, role string, configPath string) error {
		logProgress("starting local %s daemon", name)
		logPath := filepath.Join(artifactsDir, name, "daemon.log")
		if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
			return err
		}
		handle, err := launcher.Start(processCtx, localDaemonSpec{
			Name:       name,
			Role:       role,
			ConfigPath: configPath,
			LogPath:    logPath,
		})
		if err != nil {
			return err
		}
		handles = append(handles, handle)
		return nil
	}

	if err := startHandle("coordinator", "coordinator", filepath.Join(runDir, "rendered", "local", "coordinator.json")); err != nil {
		return LocalSmokeReport{}, err
	}
	logProgress("waiting for local coordinator admin server on %s", manifest.Coordinator.AdminAddress)
	if err := waitForHTTP200(processCtx, "http://"+manifest.Coordinator.AdminAddress+"/livez", handles); err != nil {
		_ = collectLocalSmokeArtifacts(manifest, artifactsDir, LocalSmokeReport{RunID: state.RunID, StartedAt: startedAt})
		return LocalSmokeReport{}, fmt.Errorf("wait for local coordinator admin server: %w", err)
	}
	for _, nodeID := range []string{"a", "b", "c"} {
		if err := startHandle("storage-"+nodeID, "storage", filepath.Join(runDir, "rendered", "local", "storage-"+nodeID+".json")); err != nil {
			return LocalSmokeReport{}, err
		}
	}

	defer func() {
		cancel()
		waitForLocalDaemons(handles)
	}()

	if err := waitForLocalAdminServers(processCtx, manifest, handles); err != nil {
		_ = collectLocalSmokeArtifacts(manifest, artifactsDir, LocalSmokeReport{RunID: state.RunID, StartedAt: startedAt})
		return LocalSmokeReport{}, err
	}

	progressLogPath := filepath.Join(artifactsDir, "coordinator", "routing-progress.jsonl")
	routingReadyAt, lastProgress, err := waitForLocalRoutingReady(processCtx, state.Profile, manifest.Coordinator.AdminAddress, handles, progressLogPath)
	if err != nil {
		_ = collectLocalSmokeArtifacts(manifest, artifactsDir, LocalSmokeReport{RunID: state.RunID, StartedAt: startedAt, LastProgress: localSmokeProgress(lastProgress)})
		return LocalSmokeReport{}, err
	}
	progressRecords, err := readRoutingProgressRecords(progressLogPath)
	if err != nil {
		_ = collectLocalSmokeArtifacts(manifest, artifactsDir, LocalSmokeReport{RunID: state.RunID, StartedAt: startedAt, LastProgress: localSmokeProgress(lastProgress)})
		return LocalSmokeReport{}, fmt.Errorf("read routing progress log: %w", err)
	}

	sanity, err := runLocalSmokeSanity(processCtx, manifest)
	report := LocalSmokeReport{
		RunID:          state.RunID,
		StartedAt:      startedAt,
		RoutingReadyAt: routingReadyAt,
		FinishedAt:     time.Now().UTC(),
		Sanity:         sanity,
		LastProgress:   localSmokeProgress(lastProgress),
		RoutingSummary: summarizeRoutingProgressRecords(progressRecords),
	}
	if collectErr := collectLocalSmokeArtifacts(manifest, artifactsDir, report); collectErr != nil {
		if err != nil {
			return report, fmt.Errorf("%w; artifact collection also failed: %v", err, collectErr)
		}
		return report, collectErr
	}
	if err != nil {
		return report, err
	}
	return report, nil
}

func renderLocalSmokeManifest(profile Profile) (quickstart.Config, error) {
	coordinatorRPC, err := reserveLoopbackAddress()
	if err != nil {
		return quickstart.Config{}, err
	}
	coordinatorAdmin, err := reserveLoopbackAddress()
	if err != nil {
		return quickstart.Config{}, err
	}
	nodes := make([]quickstart.Node, 0, 3)
	for _, nodeID := range []string{"a", "b", "c"} {
		rpcAddr, err := reserveLoopbackAddress()
		if err != nil {
			return quickstart.Config{}, err
		}
		adminAddr, err := reserveLoopbackAddress()
		if err != nil {
			return quickstart.Config{}, err
		}
		nodes = append(nodes, quickstart.Node{
			ID:           nodeID,
			RPCAddress:   rpcAddr,
			AdminAddress: adminAddr,
			FailureDomains: map[string]string{
				"host": "storage-" + nodeID,
				"rack": "storage-" + nodeID,
			},
		})
	}
	manifest := quickstart.Config{
		Coordinator: quickstart.Coordinator{
			RPCAddress:        coordinatorRPC,
			AdminAddress:      coordinatorAdmin,
			SlotCount:         profile.Cluster.SlotCount,
			ReplicationFactor: profile.Cluster.ReplicationFactor,
		},
		Nodes: nodes,
	}
	return manifest, manifest.Validate()
}

func reserveLoopbackAddress() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", fmt.Errorf("reserve loopback address: %w", err)
	}
	defer lis.Close()
	return lis.Addr().String(), nil
}

func (execLocalSmokeLauncher) Start(ctx context.Context, spec localDaemonSpec) (*localDaemonHandle, error) {
	if err := os.MkdirAll(filepath.Dir(spec.LogPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(spec.LogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", spec.LogPath, err)
	}
	exePath, err := os.Executable()
	if err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("locate current executable: %w", err)
	}
	args := []string{"daemon", spec.Role, "--config", spec.ConfigPath}
	cmd := exec.CommandContext(ctx, exePath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("start %s daemon: %w", spec.Name, err)
	}
	go func() {
		err := cmd.Wait()
		_ = logFile.Close()
		done <- err
		close(done)
	}()
	return &localDaemonHandle{Name: spec.Name, LogPath: spec.LogPath, done: done}, nil
}

func (inProcessLocalSmokeLauncher) Start(ctx context.Context, spec localDaemonSpec) (*localDaemonHandle, error) {
	if err := os.MkdirAll(filepath.Dir(spec.LogPath), 0o755); err != nil {
		return nil, err
	}
	logFile, err := os.OpenFile(spec.LogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open log file %s: %w", spec.LogPath, err)
	}
	done := make(chan error, 1)
	go func() {
		defer close(done)
		defer logFile.Close()
		_, _ = fmt.Fprintf(logFile, "starting %s with %s\n", spec.Role, spec.ConfigPath)
		var runErr error
		switch spec.Role {
		case "coordinator":
			var cfg CoordinatorProcessConfig
			runErr = LoadJSON(spec.ConfigPath, &cfg)
			if runErr == nil {
				runErr = RunCoordinatorProcess(ctx, cfg)
			}
		case "storage":
			var cfg StorageProcessConfig
			runErr = LoadJSON(spec.ConfigPath, &cfg)
			if runErr == nil {
				runErr = RunStorageProcess(ctx, cfg)
			}
		default:
			runErr = fmt.Errorf("unknown local smoke role %q", spec.Role)
		}
		if runErr != nil && !errors.Is(runErr, context.Canceled) {
			_, _ = fmt.Fprintf(logFile, "exit error: %v\n", runErr)
		}
		done <- runErr
	}()
	return &localDaemonHandle{Name: spec.Name, LogPath: spec.LogPath, done: done}, nil
}

func waitForLocalDaemons(handles []*localDaemonHandle) {
	for _, handle := range handles {
		select {
		case err := <-handle.done:
			if err != nil && !errors.Is(err, context.Canceled) {
				logProgress("local daemon %s exited during cleanup: %v", handle.Name, err)
			}
		case <-time.After(5 * time.Second):
			logProgress("timed out waiting for local daemon %s to stop", handle.Name)
		}
	}
}

func waitForLocalAdminServers(ctx context.Context, manifest quickstart.Config, handles []*localDaemonHandle) error {
	for _, node := range manifest.Nodes {
		logProgress("waiting for local storage admin server on %s (%s)", node.ID, node.AdminAddress)
		if err := waitForHTTP200(ctx, "http://"+node.AdminAddress+"/livez", handles); err != nil {
			return fmt.Errorf("wait for local storage admin server %s: %w", node.ID, err)
		}
	}
	return nil
}

func waitForHTTP200(ctx context.Context, url string, handles []*localDaemonHandle) error {
	deadline := time.Now().Add(30 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	for time.Now().Before(deadline) {
		if err := unexpectedLocalDaemonError(handles); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out waiting for %s", url)
}

func waitForLocalRoutingReady(
	ctx context.Context,
	profile Profile,
	coordinatorAdminAddr string,
	handles []*localDaemonHandle,
	progressLogPath string,
) (time.Time, routingProgress, error) {
	logProgress("waiting for local coordinator routing state to become fully settled")
	if err := os.MkdirAll(filepath.Dir(progressLogPath), 0o755); err != nil {
		return time.Time{}, routingProgress{}, err
	}
	file, err := os.OpenFile(progressLogPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return time.Time{}, routingProgress{}, err
	}
	defer file.Close()

	overallTimeout := routingReadyOverallTimeout(profile)
	stallTimeout := routingReadyStallTimeout(profile)
	deadline := time.Now().Add(overallTimeout)
	stallDeadline := time.Now().Add(stallTimeout)
	routingURL := "http://" + coordinatorAdminAddr + "/admin/v1/routing"
	stateURL := "http://" + coordinatorAdminAddr + "/admin/v1/state"
	var lastState []byte
	var lastErr error
	lastProgress := routingProgress{writableSlots: -1, readableSlots: -1, pendingSlots: -1}
	var lastLogged time.Time
	httpClient := &http.Client{Timeout: time.Second}

	for time.Now().Before(deadline) {
		if err := unexpectedLocalDaemonError(handles); err != nil {
			return time.Time{}, lastProgress, err
		}
		data, err := fetchURLBytes(ctx, httpClient, routingURL)
		lastErr = err
		if err == nil {
			progress, progressErr := decodeRoutingProgress(data)
			if progressErr == nil {
				record := routingProgressRecord{Time: time.Now().UTC(), LocalSmokeProgress: localSmokeProgress(progress)}
				if encoded, marshalErr := json.Marshal(record); marshalErr == nil {
					_, _ = file.Write(append(encoded, '\n'))
				}
				if routingProgressReady(progress) {
					logProgress("local coordinator routing state is fully settled")
					return time.Now().UTC(), progress, nil
				}
				if routingProgressChanged(lastProgress, progress) {
					stallDeadline = time.Now().Add(stallTimeout)
					logProgress("routing progress: %s", routingProgressSummary(progress))
					lastLogged = time.Now()
					lastProgress = progress
				} else if lastLogged.IsZero() || time.Since(lastLogged) >= 15*time.Second {
					logProgress("still waiting for settled routing: %s", routingProgressSummary(progress))
					lastLogged = time.Now()
				}
			}
		}
		if time.Now().After(stallDeadline) {
			break
		}
		select {
		case <-ctx.Done():
			return time.Time{}, lastProgress, ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	data, err := fetchURLBytes(ctx, httpClient, stateURL)
	if len(data) > 0 {
		lastState = append(lastState[:0], data...)
	}
	if err != nil {
		lastErr = err
	}

	diag := strings.TrimSpace(string(lastState))
	if summary := summarizeCoordinatorAdminState(lastState); summary != "" {
		diag = summary
	} else if len(diag) > 1200 {
		diag = diag[:1200] + "...(truncated)"
	}
	progressSummary := ""
	if lastProgress.slotCount > 0 {
		progressSummary = fmt.Sprintf("last routing progress: %s; ", routingProgressSummary(lastProgress))
	}
	switch {
	case diag != "":
		return time.Time{}, lastProgress, fmt.Errorf("timed out waiting for settled routing state after %s; %slast coordinator state: %s", overallTimeout, progressSummary, diag)
	case lastErr != nil:
		return time.Time{}, lastProgress, fmt.Errorf("timed out waiting for settled routing state after %s; %slast poll error: %w", overallTimeout, progressSummary, lastErr)
	default:
		return time.Time{}, lastProgress, fmt.Errorf("timed out waiting for settled routing state after %s; %s", overallTimeout, progressSummary)
	}
}

func summarizeCoordinatorAdminState(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	var state coordserver.AdminState
	if err := json.Unmarshal(data, &state); err != nil {
		return ""
	}
	readyNodes := make([]string, 0, len(state.Current.Cluster.ReadyNodeIDs))
	for nodeID, ready := range state.Current.Cluster.ReadyNodeIDs {
		if ready {
			readyNodes = append(readyNodes, nodeID)
		}
	}
	sort.Strings(readyNodes)

	pendingByNode := map[string]int{}
	pendingByKind := map[string]int{}
	pendingSlots := make([]int, 0, len(state.Pending))
	for slot, pending := range state.Pending {
		pendingByNode[pending.NodeID]++
		pendingByKind[string(pending.Kind)]++
		pendingSlots = append(pendingSlots, slot)
	}
	sort.Ints(pendingSlots)
	if len(pendingSlots) > 12 {
		pendingSlots = pendingSlots[:12]
	}

	outboxByNode := map[string]int{}
	outboxByKind := map[string]int{}
	for _, entry := range state.Current.Outbox {
		outboxByNode[entry.NodeID]++
		outboxByKind[string(entry.Kind)]++
	}

	oneActive := 0
	twoActive := 0
	threeActive := 0
	activeByNode := map[string]int{}
	joiningByNode := map[string]int{}
	for _, chain := range state.Current.Cluster.Chains {
		active := 0
		for _, replica := range chain.Replicas {
			switch replica.State {
			case coordinator.ReplicaStateActive:
				active++
				activeByNode[replica.NodeID]++
			case coordinator.ReplicaStateJoining:
				joiningByNode[replica.NodeID]++
			}
		}
		switch active {
		case 1:
			oneActive++
		case 2:
			twoActive++
		case 3:
			threeActive++
		}
	}

	liveness := make([]string, 0, len(state.Liveness))
	for _, nodeID := range sortedStringKeys(state.Liveness) {
		liveness = append(liveness, fmt.Sprintf("%s=%s", nodeID, state.Liveness[nodeID].State))
	}

	recent := make([]string, 0, minInt(len(state.Recent), 6))
	for i := maxInt(0, len(state.Recent)-6); i < len(state.Recent); i++ {
		event := state.Recent[i]
		if event.Error != "" {
			recent = append(recent, fmt.Sprintf("%s:%s", event.Kind, event.Error))
			continue
		}
		recent = append(recent, event.Kind)
	}

	return fmt.Sprintf(
		"version=%d ready=%v health=%v liveness=%v pending=%d pendingByNode=%v pendingByKind=%v firstPendingSlots=%v outbox=%d activePeerRefreshes=%d outboxByNode=%v outboxByKind=%v chains(one=%d two=%d three=%d) activeByNode=%v joiningByNode=%v recent=%v",
		state.Current.Version,
		readyNodes,
		state.Current.Cluster.NodeHealthByID,
		liveness,
		len(state.Pending),
		pendingByNode,
		pendingByKind,
		pendingSlots,
		len(state.Current.Outbox),
		state.ActivePeerRefreshes,
		outboxByNode,
		outboxByKind,
		oneActive,
		twoActive,
		threeActive,
		activeByNode,
		joiningByNode,
		recent,
	)
}

func sortedStringKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func minInt(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func fetchURLBytes(ctx context.Context, client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("%s returned status %s", url, resp.Status)
	}
	return io.ReadAll(resp.Body)
}

func runLocalSmokeSanity(ctx context.Context, manifest quickstart.Config) (LocalSmokeSanity, error) {
	pool := grpcx.NewConnPool()
	defer func() { _ = pool.Close() }()

	admin := grpcx.NewCoordinatorAdminClient(manifest.Coordinator.RPCAddress, pool)
	verifyCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	snapshot, err := waitForWritableRoutingIncludingNodes(verifyCtx, admin, "a", "b", "c")
	if err != nil {
		return LocalSmokeSanity{}, err
	}
	for _, route := range snapshot.Slots {
		if !route.Readable || !route.Writable {
			return LocalSmokeSanity{}, fmt.Errorf("route %#v is not fully readable+writable", route)
		}
	}
	return runLocalSmokeTraffic(verifyCtx, admin, pool)
}

func runLocalSmokeTraffic(ctx context.Context, admin *grpcx.CoordinatorAdminClient, pool *grpcx.ConnPool) (LocalSmokeSanity, error) {
	router, err := client.NewRouter(admin, grpcx.NewClientTransport(pool))
	if err != nil {
		return LocalSmokeSanity{}, fmt.Errorf("create local smoke router: %w", err)
	}
	key := fmt.Sprintf("smoke-local-%d", time.Now().UTC().UnixNano())
	result := LocalSmokeSanity{Key: key}

	start := time.Now()
	if err := router.Refresh(ctx); err != nil {
		return result, fmt.Errorf("router.Refresh: %w", err)
	}
	result.RefreshTime = time.Since(start)

	start = time.Now()
	put, err := router.Put(ctx, key, "ok")
	if err != nil {
		return result, fmt.Errorf("router.Put: %w", err)
	}
	result.PutTime = time.Since(start)
	result.PutApplied = put.Applied

	start = time.Now()
	read, err := router.Get(ctx, key)
	if err != nil {
		return result, fmt.Errorf("router.Get: %w", err)
	}
	result.GetTime = time.Since(start)
	result.GetFound = read.Found
	result.GetValue = read.Value
	if !read.Found || read.Value != "ok" {
		return result, fmt.Errorf("router.Get returned %#v, want found value", read)
	}

	start = time.Now()
	del, err := router.Delete(ctx, key)
	if err != nil {
		return result, fmt.Errorf("router.Delete: %w", err)
	}
	result.DeleteTime = time.Since(start)
	result.DeleteApplied = del.Applied

	start = time.Now()
	missing, err := router.Get(ctx, key)
	if err != nil {
		return result, fmt.Errorf("router.Get after delete: %w", err)
	}
	result.FinalGetTime = time.Since(start)
	result.FinalMissing = !missing.Found
	if missing.Found {
		return result, fmt.Errorf("router.Get after delete returned %#v, want not found", missing)
	}
	return result, nil
}

func collectLocalSmokeArtifacts(manifest quickstart.Config, artifactsDir string, report LocalSmokeReport) error {
	clientDir := filepath.Join(artifactsDir, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(clientDir, "smoke-result.json"), report); err != nil {
		return err
	}

	if err := collectHTTP("http://"+manifest.Coordinator.AdminAddress+"/metrics", filepath.Join(artifactsDir, "coordinator", "metrics.prom")); err != nil {
		return err
	}
	if err := collectHTTP("http://"+manifest.Coordinator.AdminAddress+"/admin/v1/state", filepath.Join(artifactsDir, "coordinator", "state.json")); err != nil {
		return err
	}
	for _, node := range manifest.Nodes {
		nodeDir := filepath.Join(artifactsDir, "storage-"+node.ID)
		if err := collectHTTP("http://"+node.AdminAddress+"/metrics", filepath.Join(nodeDir, "metrics.prom")); err != nil {
			return err
		}
		if err := collectHTTP("http://"+node.AdminAddress+"/admin/v1/state", filepath.Join(nodeDir, "state.json")); err != nil {
			return err
		}
	}
	manifestFile, err := BuildArtifactManifest(artifactsDir, report.RunID)
	if err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(artifactsDir, ArtifactManifestName), manifestFile); err != nil {
		return err
	}
	return writeTarGz(filepath.Join(artifactsDir, ArtifactBundleFileName), artifactsDir)
}

func unexpectedLocalDaemonError(handles []*localDaemonHandle) error {
	for _, handle := range handles {
		select {
		case err, ok := <-handle.done:
			if !ok || err == nil || errors.Is(err, context.Canceled) {
				continue
			}
			tail := tailFile(handle.LogPath, 40)
			if tail != "" {
				return fmt.Errorf("%s exited unexpectedly: %w\n%s", handle.Name, err, tail)
			}
			return fmt.Errorf("%s exited unexpectedly: %w", handle.Name, err)
		default:
		}
	}
	return nil
}

func tailFile(path string, maxLines int) string {
	file, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer file.Close()
	lines := make([]string, 0, maxLines)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
		if len(lines) > maxLines {
			lines = lines[1:]
		}
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func waitForWritableRoutingIncludingNodes(
	ctx context.Context,
	admin *grpcx.CoordinatorAdminClient,
	includedNodeIDs ...string,
) (coordserver.RoutingSnapshot, error) {
	included := make(map[string]struct{}, len(includedNodeIDs))
	for _, nodeID := range includedNodeIDs {
		included[nodeID] = struct{}{}
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return coordserver.RoutingSnapshot{}, ctx.Err()
		case <-ticker.C:
			snapCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
			snapshot, err := admin.RoutingSnapshot(snapCtx)
			cancel()
			if err != nil {
				continue
			}
			if snapshot.SlotCount == 0 || len(snapshot.Slots) != snapshot.SlotCount {
				continue
			}
			healthy := true
			seen := make(map[string]bool, len(included))
			for _, route := range snapshot.Slots {
				if !route.Readable || !route.Writable {
					healthy = false
					break
				}
				if _, tracked := included[route.HeadNodeID]; tracked {
					seen[route.HeadNodeID] = true
				}
				if _, tracked := included[route.TailNodeID]; tracked {
					seen[route.TailNodeID] = true
				}
				for _, replica := range route.ReadReplicas {
					if _, tracked := included[replica.NodeID]; tracked {
						seen[replica.NodeID] = true
					}
				}
			}
			if healthy {
				for nodeID := range included {
					if !seen[nodeID] {
						healthy = false
						break
					}
				}
			}
			if healthy {
				return snapshot, nil
			}
		}
	}
}

func localSmokeProgress(progress routingProgress) LocalSmokeProgress {
	return LocalSmokeProgress{
		Version:             progress.version,
		SlotCount:           progress.slotCount,
		WritableSlots:       progress.writableSlots,
		ReadableSlots:       progress.readableSlots,
		SettledSlots:        progress.settledSlots,
		PendingSlots:        progress.pendingSlots,
		OutboxEntries:       progress.outboxEntries,
		ActivePeerRefreshes: progress.activePeerRefreshes,
		HealthyNodes:        progress.healthyNodes,
		SuspectNodes:        progress.suspectNodes,
		DeadNodes:           progress.deadNodes,
	}
}
