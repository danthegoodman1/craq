package benchmark

import (
	"archive/tar"
	"bytes"
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

	"github.com/danthegoodman1/craq/coordserver"
)

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
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		return "", fmt.Errorf("mkdir run dir: %w", err)
	}
	terraformDir := filepath.Join(runDir, "terraform")
	if err := copyTree(filepath.Join(repoRoot, "infra", "benchmark", "gcp"), terraformDir); err != nil {
		return "", fmt.Errorf("copy terraform root: %w", err)
	}

	sshKeyBase := filepath.Join(runDir, "ssh", "bench")
	if err := os.MkdirAll(filepath.Dir(sshKeyBase), 0o755); err != nil {
		return "", err
	}
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

	if err := runCommand(ctx, nil, terraformDir, "terraform", "init", "-input=false"); err != nil {
		return "", err
	}
	if err := runCommand(ctx, nil, terraformDir, "terraform", "apply", "-auto-approve", "-input=false"); err != nil {
		state.Status = "needs_cleanup"
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return "", err
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
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return "", err
	}

	if err := waitForSSH(context.WithoutCancel(ctx), SSHConfig{
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
	})
	if err != nil {
		return "", err
	}
	manifestPath := filepath.Join(runDir, "rendered", "manifest.json")
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
	if err := deployAndRun(ctx, state, manifestPath, binPath); err != nil {
		state.Status = "needs_cleanup"
		_ = WriteRunState(filepath.Join(runDir, RunStateFileName), state)
		return runDir, err
	}
	state.Status = "artifacts_pulled"
	if err := WriteRunState(filepath.Join(runDir, RunStateFileName), state); err != nil {
		return runDir, err
	}
	if err := DestroyBenchmark(ctx, DestroyOptions{RunDir: runDir, RepoRoot: repoRoot}); err != nil {
		return runDir, err
	}
	return runDir, nil
}

func DestroyBenchmark(ctx context.Context, opts DestroyOptions) error {
	state, err := ReadRunState(filepath.Join(opts.RunDir, RunStateFileName))
	if err != nil {
		return err
	}
	if err := runCommand(ctx, nil, state.TerraformDir, "terraform", "destroy", "-auto-approve", "-input=false"); err != nil {
		return err
	}
	state.Status = "destroyed"
	return WriteRunState(filepath.Join(opts.RunDir, RunStateFileName), state)
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
	targets := map[string]string{
		"client":      state.TerraformOutputs.PublicClientIP,
		"coordinator": state.TerraformOutputs.PrivateIPs["coordinator"],
		"storage-a":   state.TerraformOutputs.PrivateIPs["storage-a"],
		"storage-b":   state.TerraformOutputs.PrivateIPs["storage-b"],
		"storage-c":   state.TerraformOutputs.PrivateIPs["storage-c"],
	}
	for name, host := range targets {
		currentSSH := ssh
		if name == "client" {
			currentSSH = clientSSH
		}
		if err := waitForSSH(ctx, currentSSH, host); err != nil {
			return fmt.Errorf("wait for ssh %s: %w", name, err)
		}
		if err := SSH(ctx, currentSSH, host, "mkdir -p /tmp/craq-bench"); err != nil {
			return err
		}
		if err := SCP(ctx, currentSSH, binPath, host, "/tmp/craq-bench/craq-bench"); err != nil {
			return err
		}
		if err := SSH(ctx, currentSSH, host, "sudo install -m 0755 /tmp/craq-bench/craq-bench /opt/craq-bench/bin/craq-bench"); err != nil {
			return err
		}
	}
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
	duration := state.Profile.TotalRunDuration().Round(time.Second) + time.Minute
	runRoot := "/var/lib/craq-bench/runs/" + state.RunID
	commands := []string{
		fmt.Sprintf("mkdir -p %s && timeout %ds /opt/craq-bench/bin/craq-bench probe --output %s --interval %s --duration %s > /dev/null 2>&1 &", shellQuote(runRoot), int(duration.Seconds()), shellQuote(filepath.Join(runRoot, "probe.jsonl")), state.Profile.Telemetry.ProbeInterval, duration),
		fmt.Sprintf("timeout %ds vmstat 1 > %s 2>&1 &", int(duration.Seconds()), shellQuote(filepath.Join(runRoot, "vmstat.txt"))),
		fmt.Sprintf("timeout %ds iostat -x 1 > %s 2>&1 &", int(duration.Seconds()), shellQuote(filepath.Join(runRoot, "iostat.txt"))),
		fmt.Sprintf("timeout %ds pidstat -dur 1 > %s 2>&1 &", int(duration.Seconds()), shellQuote(filepath.Join(runRoot, "pidstat.txt"))),
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
		if err := SSH(ctx, ssh, host, "bash -lc "+shellQuote(strings.Join(commands, " "))); err != nil {
			return err
		}
	}
	return nil
}

func startServices(ctx context.Context, state RunState) error {
	coordSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP}
	if err := SSH(ctx, coordSSH, state.TerraformOutputs.PrivateIPs["coordinator"], "sudo systemctl restart craq-bench-coordinator.service"); err != nil {
		return err
	}
	for _, name := range []string{"storage-a", "storage-b", "storage-c"} {
		if err := SSH(ctx, coordSSH, state.TerraformOutputs.PrivateIPs[name], "sudo systemctl restart craq-bench-storage.service"); err != nil {
			return err
		}
	}
	return nil
}

func waitForRoutingReady(ctx context.Context, state RunState) error {
	clientSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP, DisableJump: true}
	command := "curl -fsS http://" + state.TerraformOutputs.PrivateIPs["coordinator"] + ":7401/admin/v1/state"
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		data, err := SSHCapture(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, command)
		if err == nil && bytes.Contains(data, []byte(`"writable":true`)) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
	}
	return fmt.Errorf("timed out waiting for writable routing state")
}

func runRemoteLoadgen(ctx context.Context, state RunState) error {
	clientSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP, DisableJump: true}
	if err := SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "sudo systemctl restart craq-bench-client-loadgen.service"); err != nil {
		return err
	}
	if err := SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "sudo systemctl is-active --quiet craq-bench-client-loadgen.service || true"); err != nil {
		return err
	}
	runRoot := "/var/lib/craq-bench/runs/" + state.RunID
	command := "bash -lc " + shellQuote(fmt.Sprintf("until test -f %s; do sleep 5; done", filepath.Join(runRoot, "client", "loadgen-report.json")))
	return SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, command)
}

func pullArtifacts(ctx context.Context, state RunState) error {
	clientSSH := SSHConfig{User: state.Profile.GCP.SSHUser, PrivateKey: state.SSHPrivateKey, JumpPublicIP: state.TerraformOutputs.PublicClientIP, DisableJump: true}
	if err := SSH(ctx, clientSSH, state.TerraformOutputs.PublicClientIP, "/opt/craq-bench/bin/craq-bench collect --config /etc/craq-bench/collect.json"); err != nil {
		return err
	}
	remoteBundleRoot := filepath.Join("/var/lib/craq-bench/runs", state.RunID, "bundle")
	localArtifacts := filepath.Join(filepath.Dir(state.TerraformDir), "artifacts")
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
Type=oneshot
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
