package benchmark

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/danthegoodman1/craq/coordinator"
	"github.com/danthegoodman1/craq/coordserver"
)

type remoteTarget struct {
	Name string
	Host string
}

type RunOptions struct {
	ProfilePath     string
	RunName         string
	Region          string
	Topology        string
	ClientPlacement string
	RepoRoot        string
}

type DestroyOptions struct {
	RunDir   string
	RepoRoot string
}

func RunBenchmark(ctx context.Context, opts RunOptions) (string, error) {
	logProgress("loading benchmark profile from %s", opts.ProfilePath)
	repoRoot := opts.RepoRoot
	if repoRoot == "" {
		var err error
		repoRoot, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}
	profile, err := LoadProfile(opts.ProfilePath)
	if err != nil {
		return "", err
	}
	if opts.Region != "" {
		profile.GCP.Region = opts.Region
	}
	if err := profile.ValidateRunnable(); err != nil {
		return "", err
	}
	topology := NormalizeTopology(opts.Topology)
	clientPlacement := NormalizeClientPlacement(opts.ClientPlacement)
	gitSHA := gitSHA(ctx, repoRoot)
	runID := buildRunID(opts.RunName, gitSHA)
	runDir := filepath.Join(repoRoot, profile.Artifacts.RootDir, runID)
	logProgress("preparing run %s in %s", runID, runDir)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir run dir: %w", err)
	}
	terraformDir := filepath.Join(runDir, "terraform")
	logProgress("copying terraform config into %s", terraformDir)
	if err := copyTree(filepath.Join(repoRoot, "infra", "benchmark", "gcp"), terraformDir); err != nil {
		return "", fmt.Errorf("copy terraform root: %w", err)
	}

	sshKeyBase := filepath.Join(runDir, "ssh", "bench")
	if err := os.MkdirAll(filepath.Dir(sshKeyBase), 0o755); err != nil {
		return "", err
	}
	logProgress("generating temporary ssh keypair for benchmark hosts")
	if err := runCommand(ctx, nil, "", "ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", sshKeyBase); err != nil {
		return "", fmt.Errorf("generate ssh key: %w", err)
	}

	state := RunState{
		RunID:           runID,
		RunName:         opts.RunName,
		GitSHA:          gitSHA,
		ProfilePath:     opts.ProfilePath,
		Profile:         profile,
		CreatedAt:       time.Now().UTC(),
		Region:          profile.GCP.Region,
		Topology:        topology,
		ClientPlacement: clientPlacement,
		ArtifactsDir:    filepath.Join(runDir, "artifacts"),
		TerraformDir:    terraformDir,
		TerraformState:  filepath.Join(terraformDir, "terraform.tfstate"),
		SSHPrivateKey:   sshKeyBase,
		SSHPublicKey:    sshKeyBase + ".pub",
		Status:          "created",
	}
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return "", err
	}

	binPath := filepath.Join(runDir, "bin", "craq-bench")
	if err := os.MkdirAll(filepath.Dir(binPath), 0o755); err != nil {
		return "", err
	}
	buildEnv := map[string]string{
		"GOOS":        "linux",
		"GOARCH":      "arm64",
		"CGO_ENABLED": "0",
	}
	logProgress("building linux/arm64 craq-bench binary")
	if err := runCommand(ctx, buildEnv, repoRoot, "go", "build", "-o", binPath, "./cmd/craq-bench"); err != nil {
		return "", err
	}

	tfVars, err := renderTerraformVars(state, runDir)
	if err != nil {
		return "", err
	}
	if err := SaveJSON(filepath.Join(terraformDir, "terraform.tfvars.json"), tfVars); err != nil {
		return "", err
	}

	logProgress("initializing terraform")
	if err := runCommand(ctx, nil, terraformDir, "terraform", "init", "-input=false"); err != nil {
		return "", err
	}
	logProgress("applying terraform to create benchmark infrastructure")
	if err := runCommand(ctx, nil, terraformDir, "terraform", "apply", "-auto-approve", "-input=false"); err != nil {
		state.Status = "needs_cleanup"
		state.Notes = append(state.Notes, "terraform apply failed before benchmark execution")
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		if destroyErr := bestEffortTerraformDestroy(context.WithoutCancel(ctx), terraformDir); destroyErr != nil {
			state.Notes = append(state.Notes, "best-effort terraform destroy after apply failure also failed: "+destroyErr.Error())
			_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
			return "", fmt.Errorf("terraform apply failed: %w; cleanup also failed: %v", err, destroyErr)
		}
		state.Status = "apply_failed_cleaned"
		state.Notes = append(state.Notes, "best-effort terraform destroy completed after apply failure")
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return "", fmt.Errorf("terraform apply failed and partial resources were cleaned up: %w", err)
	}
	outputData, err := captureCommand(ctx, nil, terraformDir, "terraform", "output", "-json")
	if err != nil {
		return "", err
	}
	outputs, err := decodeTerraformOutputs(outputData)
	if err != nil {
		return "", err
	}
	state.TerraformOutputs = outputs
	state.ClientPublicIP = outputs.PublicClientIP
	state.Status = "infra_ready"
	logProgress("infrastructure ready: client=%s primary-zone=%s", outputs.PublicClientIP, outputs.PrimaryZone)
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return "", err
	}

	clientWaitCtx, clientWaitCancel := context.WithTimeout(ctx, 5*time.Minute)
	defer clientWaitCancel()
	logProgress("waiting for ssh on public client %s", outputs.PublicClientIP)
	if err := waitForSSH(clientWaitCtx, SSHConfig{
		User:         profile.GCP.SSHUser,
		PrivateKey:   state.SSHPrivateKey,
		JumpPublicIP: outputs.PublicClientIP,
		DisableJump:  true,
	}, outputs.PublicClientIP); err != nil {
		return "", err
	}

	manifest, err := RenderManifest(RenderedManifestParams{
		SlotCount:         profile.Cluster.SlotCount,
		ReplicationFactor: profile.Cluster.ReplicationFactor,
		PrivateIPs:        outputs.PrivateIPs,
		NodeZones:         outputs.NodeZones,
	})
	if err != nil {
		return "", err
	}
	manifestPath := filepath.Join(runDir, "rendered", "manifest.json")
	logProgress("rendering benchmark manifest to %s", manifestPath)
	if err := SaveManifest(manifestPath, manifest); err != nil {
		return "", err
	}
	state.ManifestPath = manifestPath
	metadata := RunMetadata{
		RunID:           state.RunID,
		RunName:         state.RunName,
		GitSHA:          state.GitSHA,
		StartedAt:       time.Now().UTC(),
		Profile:         profile,
		Topology:        topology,
		ClientPlacement: clientPlacement,
		Manifest:        manifest,
		Terraform:       outputs,
	}
	if err := SaveJSON(filepath.Join(runDir, RunMetadataFileName), metadata); err != nil {
		return "", err
	}
	logProgress("deploying benchmark binary and configs to remote nodes")
	if err := deployAndRun(ctx, state, manifestPath, binPath); err != nil {
		state.Status = "needs_cleanup"
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return runDir, err
	}
	state.Status = "artifacts_pulled"
	logProgress("benchmark artifacts collected to %s", state.ArtifactsDir)
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return runDir, err
	}
	logProgress("destroying benchmark infrastructure")
	if err := DestroyBenchmark(ctx, DestroyOptions{RunDir: runDir, RepoRoot: repoRoot}); err != nil {
		return runDir, err
	}
	logProgress("benchmark run %s completed successfully", runID)
	return runDir, nil
}

func DestroyBenchmark(ctx context.Context, opts DestroyOptions) error {
	logProgress("loading run state from %s", opts.RunDir)
	state, err := ReadRunState(filepath.Join(opts.RunDir, RunStateFileName))
	if err != nil {
		return err
	}
	logProgress("running terraform destroy for %s", state.RunID)
	if err := bestEffortTerraformDestroy(ctx, state.TerraformDir); err != nil {
		return err
	}
	state.Status = "destroyed"
	logProgress("benchmark infrastructure destroyed for %s", state.RunID)
	return WriteRunState(filepath.Join(opts.RunDir, RunStateFileName), state)
}

func bestEffortTerraformDestroy(ctx context.Context, terraformDir string) error {
	return runCommand(ctx, nil, terraformDir, "terraform", "destroy", "-auto-approve", "-input=false")
}

func renderTerraformVars(state RunState, runDir string) (map[string]any, error) {
	pubKey, err := os.ReadFile(state.SSHPublicKey)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"gcp_project":               state.Profile.GCP.Project,
		"gcp_region":                state.Region,
		"run_id":                    state.RunID,
		"topology":                  state.Topology,
		"client_placement":          state.ClientPlacement,
		"operator_cidrs":            state.Profile.GCP.OperatorCIDRs,
		"ssh_public_key":            strings.TrimSpace(string(pubKey)),
		"ssh_user":                  state.Profile.GCP.SSHUser,
		"coordinator_machine_type":  state.Profile.GCP.CoordinatorMachineType,
		"client_machine_type":       state.Profile.GCP.ClientMachineType,
		"storage_machine_type":      state.Profile.GCP.StorageMachineType,
		"coordinator_boot_disk_gib": state.Profile.GCP.CoordinatorBootDiskGiB,
	}, nil
}

func decodeTerraformOutputs(data []byte) (TerraformOutputs, error) {
	var raw map[string]struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return TerraformOutputs{}, fmt.Errorf("decode terraform output envelope: %w", err)
	}
	var out TerraformOutputs
	read := func(key string, target any) error {
		entry, ok := raw[key]
		if !ok {
			return fmt.Errorf("missing terraform output %q", key)
		}
		if err := json.Unmarshal(entry.Value, target); err != nil {
			return fmt.Errorf("decode terraform output %q: %w", key, err)
		}
		return nil
	}
	if err := read("region", &out.Region); err != nil {
		return TerraformOutputs{}, err
	}
	if err := read("primary_zone", &out.PrimaryZone); err != nil {
		return TerraformOutputs{}, err
	}
	if err := read("public_client_ip", &out.PublicClientIP); err != nil {
		return TerraformOutputs{}, err
	}
	if err := read("private_ips", &out.PrivateIPs); err != nil {
		return TerraformOutputs{}, err
	}
	if err := read("instance_names", &out.InstanceNames); err != nil {
		return TerraformOutputs{}, err
	}
	if err := read("node_zones", &out.NodeZones); err != nil {
		return TerraformOutputs{}, err
	}
	return out, nil
}

func gitSHA(ctx context.Context, repoRoot string) string {
	data, err := captureCommand(ctx, nil, repoRoot, "git", "rev-parse", "--short", "HEAD")
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(data))
}

func buildRunID(runName string, gitSHA string) string {
	base := strings.TrimSpace(runName)
	if base == "" {
		base = "run"
	}
	base = strings.ReplaceAll(strings.ToLower(base), " ", "-")
	return fmt.Sprintf("%s-%s-%d", base, gitSHA, time.Now().UTC().Unix())
}

func deployAndRun(ctx context.Context, state RunState, manifestPath string, binPath string) error {
	ssh := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP}
	clientSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP, DisableJump: true}
	for _, target := range orderedRemoteTargets(state) {
		name := target.Name
		host := target.Host
		currentSSH := ssh
		if name == "client" {
			currentSSH = clientSSH
		}
		logProgress("waiting for ssh on %s (%s)", name, host)
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		err := waitForSSH(waitCtx, currentSSH, host)
		cancel()
		if err != nil {
			return fmt.Errorf("wait for ssh %s: %w", name, err)
		}
		logProgress("waiting for bootstrap completion on %s", name)
		bootstrapTimeout := 5 * time.Minute
		if strings.HasPrefix(name, "storage-") {
			bootstrapTimeout = 12 * time.Minute
		}
		bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, bootstrapTimeout)
		err = waitForRemoteBootstrap(bootstrapCtx, currentSSH, host)
		bootstrapCancel()
		if err != nil {
			if detail, detailErr := captureBootstrapDiagnostics(ctx, currentSSH, host); detailErr == nil {
				return fmt.Errorf("wait for bootstrap %s: %w\n%s", name, err, detail)
			}
			return fmt.Errorf("wait for bootstrap %s: %w", name, err)
		}
		logProgress("uploading craq-bench binary to %s", name)
		if err := retryRemoteStep(ctx, "prepare "+name, func() error {
			return SSH(ctx, currentSSH, host, "mkdir -p /tmp/craq-bench")
		}); err != nil {
			return err
		}
		if err := retryRemoteStep(ctx, "mkdirs "+name, func() error {
			return SSH(ctx, currentSSH, host, "sudo mkdir -p /opt/craq-bench/bin /etc/craq-bench /var/lib/craq-bench")
		}); err != nil {
			return err
		}
		if err := retryRemoteStep(ctx, "upload binary "+name, func() error {
			return SCP(ctx, currentSSH, binPath, host, "/tmp/craq-bench/craq-bench")
		}); err != nil {
			return err
		}
		if err := retryRemoteStep(ctx, "install binary "+name, func() error {
			return SSH(ctx, currentSSH, host, "sudo install -m 0755 /tmp/craq-bench/craq-bench /opt/craq-bench/bin/craq-bench")
		}); err != nil {
			return err
		}
	}
	logProgress("copying benchmark ssh key to client jump host")
	if err := SCP(ctx, clientSSH, state.SSHPrivateKey, state.TerraformOutputs.PublicClientIP, "/tmp/craq-bench/id_ed25519"); err != nil {
		return err
	}
	if err := SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "chmod 600 /tmp/craq-bench/id_ed25519"); err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Join(filepath.Dir(manifestPath), "remote"), 0o755); err != nil {
		return err
	}
	if err := SCP(ctx, clientSSH, manifestPath, state.TerraformOutputs.PublicClientIP, "/tmp/craq-bench/manifest.json"); err != nil {
		return err
	}
	logProgress("copying manifest to private cluster nodes")
	for _, host := range []string{state.TerraformOutputs.PrivateIPs["coordinator"], state.TerraformOutputs.PrivateIPs["storage-a"], state.TerraformOutputs.PrivateIPs["storage-b"], state.TerraformOutputs.PrivateIPs["storage-c"]} {
		if err := SCP(ctx, ssh, manifestPath, host, "/tmp/craq-bench/manifest.json"); err != nil {
			return err
		}
	}

	if err := installRemoteConfigs(ctx, state, manifestPath); err != nil {
		return err
	}
	if err := installSystemdUnits(ctx, state); err != nil {
		return err
	}
	if err := startRemoteTelemetry(ctx, state); err != nil {
		return err
	}
	if err := startServices(ctx, state); err != nil {
		return err
	}
	if err := waitForRoutingReady(ctx, state); err != nil {
		return err
	}
	if err := runRemoteLoadgen(ctx, state); err != nil {
		return err
	}
	if err := pullArtifacts(ctx, state); err != nil {
		return err
	}
	return nil
}

func installRemoteConfigs(ctx context.Context, state RunState, manifestPath string) error {
	logProgress("installing benchmark config files on remote nodes")
	manifestJSON, err := os.ReadFile(manifestPath)
	if err != nil {
		return err
	}
	runRoot := "/var/lib/craq-bench/runs/" + state.RunID
	coordCfg := CoordinatorProcessConfig{
		ManifestPath: "/etc/craq-bench/manifest.json",
		DataDir:      "/var/lib/craq-bench/coordinator",
		Liveness: coordserver.LivenessPolicy{
			SuspectAfter: state.Profile.Cluster.SuspectAfter,
			DeadAfter:    state.Profile.Cluster.DeadAfter,
		},
		Reconfiguration: coordinator.ReconfigurationPolicy{
			MaxChangedChains: state.Profile.Cluster.Reconfiguration.MaxChangedChains,
		},
		TickInterval: state.Profile.Cluster.LivenessInterval,
		RPCDeadline:  state.Profile.Cluster.RPCDeadline,
	}
	loadCfg := LoadGenProcessConfig{
		RunID:        state.RunID,
		ManifestPath: "/etc/craq-bench/manifest.json",
		OutputDir:    filepath.Join(runRoot, "client"),
		Workload:     state.Profile.Workload,
	}
	collectCfg := CollectConfig{
		RunID:         state.RunID,
		ManifestPath:  "/etc/craq-bench/manifest.json",
		RemoteRunRoot: runRoot,
		OutputDir:     filepath.Join(runRoot, "bundle"),
		SSHUser:       state.Profile.GCP.SSHUser,
		SSHPrivateKey: "/tmp/craq-bench/id_ed25519",
	}
	configs := map[string][]byte{
		"manifest.json":    manifestJSON,
		"coordinator.json": mustJSON(coordCfg),
		"loadgen.json":     mustJSON(loadCfg),
		"collect.json":     mustJSON(collectCfg),
	}
	for _, nodeID := range []string{"a", "b", "c"} {
		cfg := StorageProcessConfig{
			ManifestPath:       "/etc/craq-bench/manifest.json",
			NodeID:             nodeID,
			DataDir:            filepath.Join("/var/lib/craq-bench/storage-data", nodeID),
			HeartbeatInterval:  state.Profile.Cluster.HeartbeatInterval,
			ActivationInterval: state.Profile.Cluster.ActivationInterval,
			RPCDeadline:        state.Profile.Cluster.RPCDeadline,
		}
		configs["storage-"+nodeID+".json"] = mustJSON(cfg)
	}
	for name, host := range map[string]string{
		"client":      state.TerraformOutputs.PublicClientIP,
		"coordinator": state.TerraformOutputs.PrivateIPs["coordinator"],
		"storage-a":   state.TerraformOutputs.PrivateIPs["storage-a"],
		"storage-b":   state.TerraformOutputs.PrivateIPs["storage-b"],
		"storage-c":   state.TerraformOutputs.PrivateIPs["storage-c"],
	} {
		ssh := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP}
		if name == "client" {
			ssh.DisableJump = true
		}
		for fileName, content := range configs {
			if strings.HasPrefix(fileName, "storage-") {
				want := strings.TrimSuffix(strings.TrimPrefix(fileName, "storage-"), ".json")
				if !strings.HasSuffix(name, want) {
					continue
				}
			}
			local := filepath.Join(filepath.Dir(state.ManifestPath), "remote", fileName)
			if err := os.MkdirAll(filepath.Dir(local), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(local, content, 0o644); err != nil {
				return err
			}
			if err := SCP(ctx, ssh, local, host, "/tmp/craq-bench/"+fileName); err != nil {
				return err
			}
			targetName := fileName
			if strings.HasPrefix(fileName, "storage-") {
				targetName = "storage.json"
			}
			if err := SSH(ctx, ssh, host, "sudo install -m 0644 /tmp/craq-bench/"+fileName+" /etc/craq-bench/"+targetName); err != nil {
				return err
			}
		}
	}
	return nil
}

func installSystemdUnits(ctx context.Context, state RunState) error {
	logProgress("installing systemd units on remote nodes")
	units := map[string]string{
		"craq-bench-coordinator.service": craqCoordinatorUnit(),
		"craq-bench-storage.service":     craqStorageUnit(),
	}
	clientUnits := map[string]string{
		"craq-bench-client-loadgen.service": craqLoadgenUnit(),
	}
	for name, host := range map[string]string{
		"client":      state.TerraformOutputs.PublicClientIP,
		"coordinator": state.TerraformOutputs.PrivateIPs["coordinator"],
		"storage-a":   state.TerraformOutputs.PrivateIPs["storage-a"],
		"storage-b":   state.TerraformOutputs.PrivateIPs["storage-b"],
		"storage-c":   state.TerraformOutputs.PrivateIPs["storage-c"],
	} {
		ssh := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP}
		if name == "client" {
			ssh.DisableJump = true
		}
		currentUnits := units
		if name == "client" {
			currentUnits = clientUnits
		}
		for fileName, content := range currentUnits {
			local := filepath.Join(filepath.Dir(state.ManifestPath), "remote", fileName)
			if err := os.WriteFile(local, []byte(content), 0o644); err != nil {
				return err
			}
			if err := SCP(ctx, ssh, local, host, "/tmp/craq-bench/"+fileName); err != nil {
				return err
			}
			if err := SSH(ctx, ssh, host, "sudo install -m 0644 /tmp/craq-bench/"+fileName+" /etc/systemd/system/"+fileName+" && sudo systemctl daemon-reload"); err != nil {
				return err
			}
		}
	}
	return nil
}

func startRemoteTelemetry(ctx context.Context, state RunState) error {
	logProgress("starting background telemetry collection on all nodes")
	duration := state.Profile.TotalRunDuration().Round(time.Second) + time.Minute
	for name, host := range map[string]string{
		"client":      state.TerraformOutputs.PublicClientIP,
		"coordinator": state.TerraformOutputs.PrivateIPs["coordinator"],
		"storage-a":   state.TerraformOutputs.PrivateIPs["storage-a"],
		"storage-b":   state.TerraformOutputs.PrivateIPs["storage-b"],
		"storage-c":   state.TerraformOutputs.PrivateIPs["storage-c"],
	} {
		ssh := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP}
		if name == "client" {
			ssh.DisableJump = true
		}
		logProgress("starting telemetry on %s", name)
		command := buildRemoteTelemetryCommand(filepath.Join("/var/lib/craq-bench/runs", state.RunID, name), state.Profile.Telemetry.ProbeInterval, duration)
		if err := SSH(ctx, ssh, host, "bash -lc "+shellQuote(command)); err != nil {
			return err
		}
	}
	return nil
}

func buildRemoteTelemetryCommand(outputDir string, probeInterval time.Duration, duration time.Duration) string {
	timeoutSeconds := int(duration.Seconds())
	return strings.Join([]string{
		fmt.Sprintf("mkdir -p %s", shellQuote(outputDir)),
		fmt.Sprintf("nohup timeout %ds /opt/craq-bench/bin/craq-bench probe --output %s --interval %s --duration %s >/dev/null 2>&1 &", timeoutSeconds, shellQuote(filepath.Join(outputDir, "probe.jsonl")), probeInterval, duration),
		fmt.Sprintf("nohup timeout %ds vmstat 1 > %s 2>&1 &", timeoutSeconds, shellQuote(filepath.Join(outputDir, "vmstat.txt"))),
		fmt.Sprintf("nohup timeout %ds iostat -x 1 > %s 2>&1 &", timeoutSeconds, shellQuote(filepath.Join(outputDir, "iostat.txt"))),
		fmt.Sprintf("nohup timeout %ds pidstat -dur 1 > %s 2>&1 &", timeoutSeconds, shellQuote(filepath.Join(outputDir, "pidstat.txt"))),
	}, "\n")
}

func startServices(ctx context.Context, state RunState) error {
	logProgress("starting coordinator service")
	coordSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP}
	if err := SSH(ctx, coordSSH, state.TerraformOutputs.PrivateIPs["coordinator"], "sudo systemctl restart craq-bench-coordinator.service"); err != nil {
		return err
	}
	logProgress("starting storage services")
	for _, name := range []string{"storage-a", "storage-b", "storage-c"} {
		if err := SSH(ctx, coordSSH, state.TerraformOutputs.PrivateIPs[name], "sudo systemctl restart craq-bench-storage.service"); err != nil {
			return err
		}
	}
	return nil
}

func waitForRoutingReady(ctx context.Context, state RunState) error {
	logProgress("waiting for coordinator routing state to become writable")
	clientSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP, DisableJump: true}
	routingCommand := "curl -fsS http://" + state.TerraformOutputs.PrivateIPs["coordinator"] + ":7401/admin/v1/routing"
	stateCommand := "curl -fsS http://" + state.TerraformOutputs.PrivateIPs["coordinator"] + ":7401/admin/v1/state"
	overallTimeout := routingReadyOverallTimeout(state.Profile)
	stallTimeout := routingReadyStallTimeout(state.Profile)
	deadline := time.Now().Add(overallTimeout)
	stallDeadline := time.Now().Add(stallTimeout)
	var lastState []byte
	var lastErr error
	lastProgress := routingProgress{writableSlots: -1, readableSlots: -1, pendingSlots: -1}
	var lastLogged time.Time
	for time.Now().Before(deadline) {
		data, err := SSHCapture(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, routingCommand)
		lastErr = err
		if err == nil {
			progress, progressErr := decodeRoutingProgress(data)
			if progressErr == nil {
				if progress.slotCount > 0 && progress.writableSlots == progress.slotCount {
					logProgress("coordinator routing state is writable")
					return nil
				}
				if routingProgressChanged(lastProgress, progress) {
					stallDeadline = time.Now().Add(stallTimeout)
					logProgress(
						"routing progress: writable=%d/%d readable=%d pending=%d version=%d",
						progress.writableSlots,
						progress.slotCount,
						progress.readableSlots,
						progress.pendingSlots,
						progress.version,
					)
					lastLogged = time.Now()
					lastProgress = progress
				} else if lastLogged.IsZero() || time.Since(lastLogged) >= 15*time.Second {
					logProgress(
						"still waiting for writable routing: writable=%d/%d readable=%d pending=%d version=%d",
						progress.writableSlots,
						progress.slotCount,
						progress.readableSlots,
						progress.pendingSlots,
						progress.version,
					)
					lastLogged = time.Now()
				}
			}
		}
		if time.Now().After(stallDeadline) {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	data, err := SSHCapture(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, stateCommand)
	if len(data) > 0 {
		lastState = append(lastState[:0], data...)
	}
	if err != nil {
		lastErr = err
	}
	diag := strings.TrimSpace(string(lastState))
	if len(diag) > 1200 {
		diag = diag[:1200] + "...(truncated)"
	}
	progressSummary := ""
	if lastProgress.slotCount > 0 {
		progressSummary = fmt.Sprintf(
			"last routing progress: writable=%d/%d readable=%d pending=%d version=%d; ",
			lastProgress.writableSlots,
			lastProgress.slotCount,
			lastProgress.readableSlots,
			lastProgress.pendingSlots,
			lastProgress.version,
		)
	}
	switch {
	case diag != "":
		return fmt.Errorf("timed out waiting for writable routing state after %s; %slast coordinator state: %s", overallTimeout, progressSummary, diag)
	case lastErr != nil:
		return fmt.Errorf("timed out waiting for writable routing state after %s; %slast poll error: %w", overallTimeout, progressSummary, lastErr)
	default:
		return fmt.Errorf("timed out waiting for writable routing state after %s; %s", overallTimeout, progressSummary)
	}
}

type routingProgress struct {
	version       uint64
	slotCount     int
	writableSlots int
	readableSlots int
	pendingSlots  int
}

func decodeRoutingProgress(data []byte) (routingProgress, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return routingProgress{}, err
	}
	if _, ok := envelope["current"]; ok {
		var state coordserver.AdminState
		if err := json.Unmarshal(data, &state); err != nil {
			return routingProgress{}, err
		}
		progress := routingProgress{
			version:      state.Current.Version,
			slotCount:    state.RoutingSnapshot.SlotCount,
			pendingSlots: len(state.Pending),
		}
		for _, route := range state.RoutingSnapshot.Slots {
			if route.Readable {
				progress.readableSlots++
			}
			if route.Readable && route.Writable {
				progress.writableSlots++
			}
		}
		return progress, nil
	}
	var status coordserver.RoutingStatus
	if err := json.Unmarshal(data, &status); err != nil {
		return routingProgress{}, err
	}
	progress := routingProgress{
		version:      status.RoutingSnapshot.Version,
		slotCount:    status.RoutingSnapshot.SlotCount,
		pendingSlots: status.PendingCount,
	}
	for _, route := range status.RoutingSnapshot.Slots {
		if route.Readable {
			progress.readableSlots++
		}
		if route.Readable && route.Writable {
			progress.writableSlots++
		}
	}
	return progress, nil
}

func routingProgressChanged(prev routingProgress, next routingProgress) bool {
	return prev.version != next.version ||
		prev.slotCount != next.slotCount ||
		prev.writableSlots != next.writableSlots ||
		prev.readableSlots != next.readableSlots ||
		prev.pendingSlots != next.pendingSlots
}

func routingReadyOverallTimeout(profile Profile) time.Duration {
	timeout := 10 * time.Minute
	if profile.Cluster.SlotCount <= 0 {
		return timeout
	}
	perReplica := 500 * time.Millisecond
	estimated := time.Duration(profile.Cluster.SlotCount*profile.Cluster.ReplicationFactor) * perReplica
	if estimated > timeout {
		timeout = estimated
	}
	return timeout
}

func routingReadyStallTimeout(profile Profile) time.Duration {
	timeout := 45 * time.Second
	if profile.Cluster.RPCDeadline > 0 {
		candidate := profile.Cluster.RPCDeadline * 6
		if candidate > timeout {
			timeout = candidate
		}
	}
	return timeout
}

func runRemoteLoadgen(ctx context.Context, state RunState) error {
	clientSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP, DisableJump: true}
	logProgress("starting remote load generator on client")
	if err := SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "sudo systemctl restart craq-bench-client-loadgen.service"); err != nil {
		return err
	}
	if err := SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "sudo systemctl is-active --quiet craq-bench-client-loadgen.service"); err != nil {
		return err
	}
	runRoot := "/var/lib/craq-bench/runs/" + state.RunID
	command := "bash -lc " + shellQuote(fmt.Sprintf("until test -f %s; do sleep 5; done", filepath.Join(runRoot, "client", "loadgen-report.json")))
	waitCtx, cancel := context.WithTimeout(ctx, state.Profile.TotalRunDuration()+2*time.Minute)
	defer cancel()
	logProgress("waiting for loadgen report, expected workload duration about %s", state.Profile.TotalRunDuration())
	if err := SSH(waitCtx, clientSSH, state.TerraformOutputs.PublicClientIP, command); err != nil {
		status, statusErr := SSHCapture(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "sudo systemctl status craq-bench-client-loadgen.service --no-pager || sudo journalctl -u craq-bench-client-loadgen.service --no-pager -n 100")
		if statusErr == nil {
			return fmt.Errorf("wait for loadgen report: %w\n%s", err, strings.TrimSpace(string(status)))
		}
		return fmt.Errorf("wait for loadgen report: %w", err)
	}
	logProgress("load generator completed")
	return nil
}

func pullArtifacts(ctx context.Context, state RunState) error {
	clientSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP, DisableJump: true}
	logProgress("collecting benchmark artifacts on client")
	if err := SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "/opt/craq-bench/bin/craq-bench collect --config /etc/craq-bench/collect.json"); err != nil {
		return err
	}
	remoteBundleRoot := filepath.Join("/var/lib/craq-bench/runs", state.RunID, "bundle")
	localArtifacts := filepath.Join(filepath.Dir(state.TerraformDir), "artifacts")
	logProgress("copying artifact bundle back to local machine")
	if err := SCPFrom(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, filepath.Join(remoteBundleRoot, ArtifactManifestName), filepath.Join(localArtifacts, ArtifactManifestName)); err != nil {
		return err
	}
	if err := SCPFrom(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, filepath.Join(remoteBundleRoot, ArtifactBundleFileName), filepath.Join(localArtifacts, ArtifactBundleFileName)); err != nil {
		return err
	}
	if err := extractTarGz(filepath.Join(localArtifacts, ArtifactBundleFileName), localArtifacts); err != nil {
		return err
	}
	var manifest ArtifactManifest
	if err := LoadJSON(filepath.Join(localArtifacts, ArtifactManifestName), &manifest); err != nil {
		return err
	}
	if err := VerifyArtifactManifest(localArtifacts, manifest); err != nil {
		return err
	}
	return nil
}

func mustJSON(value any) []byte {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		panic(err)
	}
	return append(data, '\n')
}

func craqCoordinatorUnit() string {
	return `[Unit]
Description=craq benchmark coordinator
After=network-online.target

[Service]
Type=simple
ExecStart=/opt/craq-bench/bin/craq-bench daemon coordinator --config /etc/craq-bench/coordinator.json
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
`
}

func craqStorageUnit() string {
	return `[Unit]
Description=craq benchmark storage
After=network-online.target

[Service]
Type=simple
ExecStart=/opt/craq-bench/bin/craq-bench daemon storage --config /etc/craq-bench/storage.json
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
`
}

func craqLoadgenUnit() string {
	return `[Unit]
Description=craq benchmark load generator
After=network-online.target

[Service]
Type=simple
ExecStart=/opt/craq-bench/bin/craq-bench loadgen --config /etc/craq-bench/loadgen.json

[Install]
WantedBy=multi-user.target
`
}

func extractTarGz(path string, dest string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	gz, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		target := filepath.Join(dest, header.Name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			out, err := os.Create(target)
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return err
			}
			if err := out.Close(); err != nil {
				return err
			}
		}
	}
}

func waitForRemoteBootstrap(ctx context.Context, ssh SSHConfig, host string) error {
	command := "test -f /var/lib/craq-bench/bootstrap.ready"
	started := time.Now()
	lastLog := time.Time{}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := SSH(ctx, ssh, host, command); err == nil {
			return nil
		} else if time.Since(lastLog) >= 15*time.Second {
			logProgress("still waiting for bootstrap on %s after %s", host, time.Since(started).Round(time.Second))
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
}

func orderedRemoteTargets(state RunState) []remoteTarget {
	return []remoteTarget{
		{Name: "client", Host: state.TerraformOutputs.PublicClientIP},
		{Name: "coordinator", Host: state.TerraformOutputs.PrivateIPs["coordinator"]},
		{Name: "storage-a", Host: state.TerraformOutputs.PrivateIPs["storage-a"]},
		{Name: "storage-b", Host: state.TerraformOutputs.PrivateIPs["storage-b"]},
		{Name: "storage-c", Host: state.TerraformOutputs.PrivateIPs["storage-c"]},
	}
}

func retryRemoteStep(ctx context.Context, label string, fn func() error) error {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := fn(); err == nil {
			return nil
		} else {
			lastErr = err
			if attempt < 3 {
				logProgress("%s failed on attempt %d/3, retrying: %v", label, attempt, err)
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(5 * time.Second):
				}
			}
		}
	}
	return lastErr
}

func captureBootstrapDiagnostics(ctx context.Context, ssh SSHConfig, host string) (string, error) {
	diagCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	command := strings.Join([]string{
		"echo '=== bootstrap marker ==='",
		"ls -l /var/lib/craq-bench/bootstrap.ready || true",
		"echo '=== startup service ==='",
		"systemctl is-active google-startup-scripts.service || true",
		"echo '=== startup journal ==='",
		"sudo journalctl -u google-startup-scripts.service --no-pager -n 200 || true",
		"echo '=== cloud-init status ==='",
		"sudo cloud-init status || true",
		"echo '=== local ssd by-id ==='",
		"find /dev/disk/by-id -maxdepth 1 -type l \\( -name 'google-local-nvme-ssd-*' -o -name 'google-local-ssd-*' \\) | sort || true",
		"echo '=== mdstat ==='",
		"cat /proc/mdstat || true",
		"echo '=== mount ==='",
		"mount | grep craq-bench || true",
	}, "; ")
	data, err := SSHCapture(diagCtx, ssh, host, "bash -lc "+shellQuote(command))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
