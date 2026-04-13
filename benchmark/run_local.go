package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/quickstart"
)

type RunLocalOptions struct {
	ProfilePath string
	RunName     string
	RepoRoot    string
}

type LocalBenchmarkReport struct {
	RunID          string                 `json:"run_id"`
	StartedAt      time.Time              `json:"started_at"`
	RoutingReadyAt time.Time              `json:"routing_ready_at"`
	FinishedAt     time.Time              `json:"finished_at"`
	LastProgress   LocalSmokeProgress     `json:"last_progress"`
	RoutingSummary RoutingProgressSummary `json:"routing_summary"`
	LoadGen        LoadGenReport          `json:"loadgen"`
}

func RunLocal(ctx context.Context, opts RunLocalOptions) (string, error) {
	return runLocalWithLauncher(ctx, opts, execLocalSmokeLauncher{})
}

func runLocalWithLauncher(ctx context.Context, opts RunLocalOptions, launcher localSmokeLauncher) (string, error) {
	profilePath := strings.TrimSpace(opts.ProfilePath)
	if profilePath == "" {
		profilePath = filepath.Join("profiles", "bench", "local_put_scaling.yaml")
	}
	logProgress("loading local benchmark profile from %s", profilePath)

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
	logProgress("preparing local benchmark run %s in %s", runID, runDir)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir local benchmark run dir: %w", err)
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
		Status:          "local_run_created",
		ManifestPath:    manifestPath,
		Notes:           []string{"local steady/scaling benchmark"},
	}
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return "", err
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
		return "", err
	}

	report, err := runLocalBenchmark(ctx, runDir, state, manifest, launcher)
	if err != nil {
		state.Status = "local_run_failed"
		state.Notes = append(state.Notes, err.Error())
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return runDir, err
	}

	metadata.CompletedAt = report.FinishedAt
	if err := SaveJSON(filepath.Join(runDir, RunMetadataFileName), metadata); err != nil {
		state.Status = "local_run_failed"
		state.Notes = append(state.Notes, err.Error())
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return runDir, err
	}
	if _, err := AnalyzeRun(runDir); err != nil {
		state.Status = "local_run_failed"
		state.Notes = append(state.Notes, err.Error())
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return runDir, err
	}

	state.Status = "local_run_ok"
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return runDir, err
	}
	logProgress("local benchmark run %s completed successfully", runID)
	return runDir, nil
}

func runLocalBenchmark(ctx context.Context, runDir string, state RunState, manifest quickstart.Config, launcher localSmokeLauncher) (LocalBenchmarkReport, error) {
	startedAt := time.Now().UTC()
	artifactsDir := filepath.Join(runDir, "artifacts")
	if err := os.MkdirAll(artifactsDir, 0o755); err != nil {
		return LocalBenchmarkReport{}, fmt.Errorf("mkdir local artifacts: %w", err)
	}
	if err := os.MkdirAll(filepath.Join(runDir, "rendered", "local"), 0o755); err != nil {
		return LocalBenchmarkReport{}, fmt.Errorf("mkdir local rendered dir: %w", err)
	}
	storageRoot, cleanupStorageRoot, err := prepareLocalStorageRoot(ctx, runDir, state.RunID, state.Profile.Local)
	if err != nil {
		return LocalBenchmarkReport{}, err
	}
	defer cleanupStorageRoot()

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
		return LocalBenchmarkReport{}, err
	}

	for _, nodeID := range []string{"a", "b", "c"} {
		cfg := StorageProcessConfig{
			ManifestPath:               state.ManifestPath,
			NodeID:                     nodeID,
			DataDir:                    filepath.Join(storageRoot, "storage-"+nodeID),
			HeartbeatInterval:          state.Profile.Cluster.HeartbeatInterval,
			ActivationInterval:         state.Profile.Cluster.ActivationInterval,
			RPCDeadline:                state.Profile.Cluster.RPCDeadline,
			WriteTraceOutput:           filepath.Join(artifactsDir, "storage-"+nodeID, "write-pipeline-trace.jsonl"),
			WriteTraceSampleRate:       1024,
			WriteTimeoutArtifacts:      filepath.Join(artifactsDir, "storage-"+nodeID, "write-timeout-artifacts.jsonl"),
			JournalShards:              state.Profile.Storage.JournalShards,
			JournalBatchDelayLow:       state.Profile.Storage.JournalBatchDelayLow,
			JournalBatchDelayHigh:      state.Profile.Storage.JournalBatchDelayHigh,
			JournalBatchDepthThreshold: state.Profile.Storage.JournalBatchDepthThreshold,
			JournalBatchMaxOps:         state.Profile.Storage.JournalBatchMaxOps,
			JournalExperiment:          state.Profile.Storage.JournalExperiment,
		}
		if err := SaveJSON(filepath.Join(runDir, "rendered", "local", "storage-"+nodeID+".json"), cfg); err != nil {
			return LocalBenchmarkReport{}, err
		}
	}

	loadCfg := LoadGenProcessConfig{
		RunID:        state.RunID,
		ManifestPath: state.ManifestPath,
		OutputDir:    filepath.Join(artifactsDir, "client"),
		Workload:     state.Profile.Workload,
		Telemetry:    state.Profile.Telemetry,
	}
	if err := SaveJSON(filepath.Join(runDir, "rendered", "local", "loadgen.json"), loadCfg); err != nil {
		return LocalBenchmarkReport{}, err
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
		return LocalBenchmarkReport{}, err
	}
	logProgress("waiting for local coordinator admin server on %s", manifest.Coordinator.AdminAddress)
	if err := waitForHTTP200(processCtx, "http://"+manifest.Coordinator.AdminAddress+"/livez", handles); err != nil {
		_ = collectLocalBenchmarkArtifacts(manifest, artifactsDir, LocalBenchmarkReport{RunID: state.RunID, StartedAt: startedAt})
		return LocalBenchmarkReport{}, fmt.Errorf("wait for local coordinator admin server: %w", err)
	}
	for _, nodeID := range []string{"a", "b", "c"} {
		if err := startHandle("storage-"+nodeID, "storage", filepath.Join(runDir, "rendered", "local", "storage-"+nodeID+".json")); err != nil {
			return LocalBenchmarkReport{}, err
		}
	}

	defer func() {
		cancel()
		waitForLocalDaemons(handles)
	}()

	if err := waitForLocalAdminServers(processCtx, manifest, handles); err != nil {
		_ = collectLocalBenchmarkArtifacts(manifest, artifactsDir, LocalBenchmarkReport{RunID: state.RunID, StartedAt: startedAt})
		return LocalBenchmarkReport{}, err
	}

	progressLogPath := filepath.Join(artifactsDir, "coordinator", "routing-progress.jsonl")
	routingReadyAt, lastProgress, err := waitForLocalRoutingReady(processCtx, state.Profile, manifest.Coordinator.AdminAddress, handles, progressLogPath)
	if err != nil {
		_ = collectLocalBenchmarkArtifacts(manifest, artifactsDir, LocalBenchmarkReport{RunID: state.RunID, StartedAt: startedAt, LastProgress: localSmokeProgress(lastProgress)})
		return LocalBenchmarkReport{}, err
	}
	progressRecords, err := readRoutingProgressRecords(progressLogPath)
	if err != nil {
		_ = collectLocalBenchmarkArtifacts(manifest, artifactsDir, LocalBenchmarkReport{RunID: state.RunID, StartedAt: startedAt, LastProgress: localSmokeProgress(lastProgress)})
		return LocalBenchmarkReport{}, fmt.Errorf("read routing progress log: %w", err)
	}

	loadReport, err := RunLoadGen(processCtx, loadCfg)
	report := LocalBenchmarkReport{
		RunID:          state.RunID,
		StartedAt:      startedAt,
		RoutingReadyAt: routingReadyAt,
		FinishedAt:     time.Now().UTC(),
		LastProgress:   localSmokeProgress(lastProgress),
		RoutingSummary: summarizeRoutingProgressRecords(progressRecords),
		LoadGen:        loadReport,
	}
	if collectErr := collectLocalBenchmarkArtifacts(manifest, artifactsDir, report); collectErr != nil {
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

func collectLocalBenchmarkArtifacts(manifest quickstart.Config, artifactsDir string, report LocalBenchmarkReport) error {
	clientDir := filepath.Join(artifactsDir, "client")
	if err := os.MkdirAll(clientDir, 0o755); err != nil {
		return err
	}
	if err := SaveJSON(filepath.Join(clientDir, "local-run-report.json"), report); err != nil {
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
	return nil
}
