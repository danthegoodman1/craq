package benchmark

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultTopology        = "single-zone"
	DefaultClientPlacement = "same-zone"
)

type Profile struct {
	Name      string           `yaml:"name" json:"name"`
	GCP       GCPProfile       `yaml:"gcp" json:"gcp"`
	Cluster   ClusterProfile   `yaml:"cluster" json:"cluster"`
	Workload  WorkloadProfile  `yaml:"workload" json:"workload"`
	Telemetry TelemetryProfile `yaml:"telemetry" json:"telemetry"`
	Artifacts ArtifactProfile  `yaml:"artifacts" json:"artifacts"`
}

type GCPProfile struct {
	Project                string   `yaml:"project" json:"project"`
	Region                 string   `yaml:"region" json:"region"`
	OperatorCIDRs          []string `yaml:"operator_cidrs" json:"operator_cidrs"`
	SSHUser                string   `yaml:"ssh_user" json:"ssh_user"`
	CoordinatorMachineType string   `yaml:"coordinator_machine_type" json:"coordinator_machine_type"`
	ClientMachineType      string   `yaml:"client_machine_type" json:"client_machine_type"`
	StorageMachineType     string   `yaml:"storage_machine_type" json:"storage_machine_type"`
	CoordinatorBootDiskGiB int      `yaml:"coordinator_boot_disk_gib" json:"coordinator_boot_disk_gib"`
}

type ClusterProfile struct {
	SlotCount          int             `yaml:"slot_count" json:"slot_count"`
	ReplicationFactor  int             `yaml:"replication_factor" json:"replication_factor"`
	StorageNodeCount   int             `yaml:"storage_node_count" json:"storage_node_count"`
	HeartbeatInterval  time.Duration   `yaml:"heartbeat_interval" json:"heartbeat_interval"`
	LivenessInterval   time.Duration   `yaml:"liveness_interval" json:"liveness_interval"`
	SuspectAfter       time.Duration   `yaml:"suspect_after" json:"suspect_after"`
	DeadAfter          time.Duration   `yaml:"dead_after" json:"dead_after"`
	ActivationInterval time.Duration   `yaml:"activation_interval" json:"activation_interval"`
	RPCDeadline        time.Duration   `yaml:"rpc_deadline" json:"rpc_deadline"`
	Reconfiguration    ReconfigProfile `yaml:"reconfiguration" json:"reconfiguration"`
}

type ReconfigProfile struct {
	MaxChangedChains int `yaml:"max_changed_chains" json:"max_changed_chains"`
}

type WorkloadProfile struct {
	Seed             int64             `yaml:"seed" json:"seed"`
	PreloadKeys      int               `yaml:"preload_keys" json:"preload_keys"`
	ValueBytes       int               `yaml:"value_bytes" json:"value_bytes"`
	RequestTimeout   time.Duration     `yaml:"request_timeout" json:"request_timeout"`
	PerScenarioPause time.Duration     `yaml:"per_scenario_pause" json:"per_scenario_pause"`
	Interval         time.Duration     `yaml:"interval" json:"interval"`
	Scenarios        []ScenarioProfile `yaml:"scenarios" json:"scenarios"`
}

type ScenarioProfile struct {
	Name        string        `yaml:"name" json:"name"`
	Kind        string        `yaml:"kind" json:"kind"`
	Concurrency int           `yaml:"concurrency" json:"concurrency"`
	Warmup      time.Duration `yaml:"warmup" json:"warmup"`
	Duration    time.Duration `yaml:"duration" json:"duration"`
	ReadPercent int           `yaml:"read_percent,omitempty" json:"read_percent,omitempty"`
	ValueBytes  int           `yaml:"value_bytes,omitempty" json:"value_bytes,omitempty"`
}

type TelemetryProfile struct {
	ProbeInterval    time.Duration `yaml:"probe_interval" json:"probe_interval"`
	CommandInterval  time.Duration `yaml:"command_interval" json:"command_interval"`
	ExtraRunDuration time.Duration `yaml:"extra_run_duration" json:"extra_run_duration"`
}

type ArtifactProfile struct {
	RootDir string `yaml:"root_dir" json:"root_dir"`
}

func LoadProfile(path string) (Profile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Profile{}, fmt.Errorf("read profile %q: %w", path, err)
	}
	var profile Profile
	if err := yaml.Unmarshal(data, &profile); err != nil {
		return Profile{}, fmt.Errorf("decode profile %q: %w", path, err)
	}
	profile.ApplyDefaults()
	if err := profile.Validate(); err != nil {
		return Profile{}, fmt.Errorf("validate profile %q: %w", path, err)
	}
	return profile, nil
}

func (p *Profile) ApplyDefaults() {
	if p.Name == "" {
		p.Name = "gcp-c4a-steady"
	}
	if p.GCP.Region == "" {
		p.GCP.Region = "us-central1"
	}
	if len(p.GCP.OperatorCIDRs) == 0 {
		p.GCP.OperatorCIDRs = []string{"0.0.0.0/0"}
	}
	if p.GCP.SSHUser == "" {
		p.GCP.SSHUser = "ubuntu"
	}
	if p.GCP.CoordinatorMachineType == "" {
		p.GCP.CoordinatorMachineType = "c4a-standard-8"
	}
	if p.GCP.ClientMachineType == "" {
		p.GCP.ClientMachineType = "c4a-standard-16"
	}
	if p.GCP.StorageMachineType == "" {
		p.GCP.StorageMachineType = "c4a-standard-48-lssd"
	}
	if p.GCP.CoordinatorBootDiskGiB == 0 {
		p.GCP.CoordinatorBootDiskGiB = 100
	}

	if p.Cluster.SlotCount == 0 {
		p.Cluster.SlotCount = 1024
	}
	if p.Cluster.ReplicationFactor == 0 {
		p.Cluster.ReplicationFactor = 3
	}
	if p.Cluster.StorageNodeCount == 0 {
		p.Cluster.StorageNodeCount = 3
	}
	if p.Cluster.HeartbeatInterval == 0 {
		p.Cluster.HeartbeatInterval = time.Second
	}
	if p.Cluster.LivenessInterval == 0 {
		p.Cluster.LivenessInterval = time.Second
	}
	if p.Cluster.SuspectAfter == 0 {
		p.Cluster.SuspectAfter = 3 * time.Second
	}
	if p.Cluster.DeadAfter == 0 {
		p.Cluster.DeadAfter = 6 * time.Second
	}
	if p.Cluster.ActivationInterval == 0 {
		p.Cluster.ActivationInterval = 250 * time.Millisecond
	}
	if p.Cluster.RPCDeadline == 0 {
		p.Cluster.RPCDeadline = 5 * time.Second
	}
	if p.Cluster.Reconfiguration.MaxChangedChains == 0 {
		p.Cluster.Reconfiguration.MaxChangedChains = 32
	}

	if p.Workload.Seed == 0 {
		p.Workload.Seed = 42
	}
	if p.Workload.PreloadKeys == 0 {
		p.Workload.PreloadKeys = 10000
	}
	if p.Workload.ValueBytes == 0 {
		p.Workload.ValueBytes = 256
	}
	if p.Workload.RequestTimeout == 0 {
		p.Workload.RequestTimeout = 3 * time.Second
	}
	if p.Workload.PerScenarioPause == 0 {
		p.Workload.PerScenarioPause = 2 * time.Second
	}
	if p.Workload.Interval == 0 {
		p.Workload.Interval = time.Second
	}
	if len(p.Workload.Scenarios) == 0 {
		p.Workload.Scenarios = []ScenarioProfile{
			{Name: "get-only-c64", Kind: "get", Concurrency: 64, Warmup: 10 * time.Second, Duration: 30 * time.Second},
			{Name: "put-only-c32", Kind: "put", Concurrency: 32, Warmup: 10 * time.Second, Duration: 30 * time.Second},
			{Name: "mixed-80-20-c64", Kind: "mixed", Concurrency: 64, Warmup: 10 * time.Second, Duration: 30 * time.Second, ReadPercent: 80},
		}
	}
	for i := range p.Workload.Scenarios {
		if p.Workload.Scenarios[i].ValueBytes == 0 {
			p.Workload.Scenarios[i].ValueBytes = p.Workload.ValueBytes
		}
	}

	if p.Telemetry.ProbeInterval == 0 {
		p.Telemetry.ProbeInterval = time.Second
	}
	if p.Telemetry.CommandInterval == 0 {
		p.Telemetry.CommandInterval = time.Second
	}
	if p.Telemetry.ExtraRunDuration == 0 {
		p.Telemetry.ExtraRunDuration = 30 * time.Second
	}
	if p.Artifacts.RootDir == "" {
		p.Artifacts.RootDir = filepath.Join("artifacts", "benchmarks")
	}
}

func (p Profile) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	if p.GCP.Region == "" {
		return fmt.Errorf("gcp.region must not be empty")
	}
	if p.GCP.SSHUser == "" {
		return fmt.Errorf("gcp.ssh_user must not be empty")
	}
	if p.GCP.CoordinatorMachineType == "" {
		return fmt.Errorf("gcp.coordinator_machine_type must not be empty")
	}
	if p.GCP.ClientMachineType == "" {
		return fmt.Errorf("gcp.client_machine_type must not be empty")
	}
	if p.GCP.StorageMachineType == "" {
		return fmt.Errorf("gcp.storage_machine_type must not be empty")
	}
	if p.GCP.CoordinatorBootDiskGiB <= 0 {
		return fmt.Errorf("gcp.coordinator_boot_disk_gib must be > 0")
	}
	if p.Cluster.SlotCount <= 0 {
		return fmt.Errorf("cluster.slot_count must be > 0")
	}
	if p.Cluster.ReplicationFactor <= 0 {
		return fmt.Errorf("cluster.replication_factor must be > 0")
	}
	if p.Cluster.StorageNodeCount != 3 {
		return fmt.Errorf("cluster.storage_node_count must be 3 in v1, got %d", p.Cluster.StorageNodeCount)
	}
	if p.Cluster.HeartbeatInterval <= 0 || p.Cluster.LivenessInterval <= 0 || p.Cluster.SuspectAfter <= 0 || p.Cluster.DeadAfter <= 0 {
		return fmt.Errorf("cluster timing values must be > 0")
	}
	if p.Workload.PreloadKeys <= 0 {
		return fmt.Errorf("workload.preload_keys must be > 0")
	}
	if p.Workload.ValueBytes <= 0 {
		return fmt.Errorf("workload.value_bytes must be > 0")
	}
	if len(p.Workload.Scenarios) == 0 {
		return fmt.Errorf("workload.scenarios must not be empty")
	}
	seen := map[string]bool{}
	for _, scenario := range p.Workload.Scenarios {
		if scenario.Name == "" {
			return fmt.Errorf("scenario name must not be empty")
		}
		if seen[scenario.Name] {
			return fmt.Errorf("duplicate scenario %q", scenario.Name)
		}
		seen[scenario.Name] = true
		if !slices.Contains([]string{"get", "put", "mixed"}, scenario.Kind) {
			return fmt.Errorf("scenario %q has unsupported kind %q", scenario.Name, scenario.Kind)
		}
		if scenario.Concurrency <= 0 {
			return fmt.Errorf("scenario %q concurrency must be > 0", scenario.Name)
		}
		if scenario.Warmup < 0 || scenario.Duration <= 0 {
			return fmt.Errorf("scenario %q warmup must be >= 0 and duration must be > 0", scenario.Name)
		}
		if scenario.Kind == "mixed" && (scenario.ReadPercent < 0 || scenario.ReadPercent > 100) {
			return fmt.Errorf("scenario %q read_percent must be between 0 and 100", scenario.Name)
		}
		if scenario.ValueBytes <= 0 {
			return fmt.Errorf("scenario %q value_bytes must be > 0", scenario.Name)
		}
	}
	return nil
}

func (p Profile) TotalRunDuration() time.Duration {
	total := time.Duration(0)
	for _, scenario := range p.Workload.Scenarios {
		total += scenario.Warmup + scenario.Duration + p.Workload.PerScenarioPause
	}
	total += p.Telemetry.ExtraRunDuration
	return total
}

func NormalizeTopology(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "", DefaultTopology:
		return DefaultTopology
	case "single-az":
		return "single-zone"
	case "multi-az":
		return "multi-zone"
	}
	return value
}

func NormalizeClientPlacement(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	switch value {
	case "", DefaultClientPlacement:
		return DefaultClientPlacement
	case "same-az":
		return "same-zone"
	case "remote-az":
		return "remote-zone"
	}
	return value
}

func (p Profile) ValidateRunnable() error {
	project := strings.TrimSpace(p.GCP.Project)
	if project == "" {
		return fmt.Errorf("gcp.project must be set before running a benchmark")
	}
	if strings.Contains(strings.ToUpper(project), "CHANGEME") {
		return fmt.Errorf("gcp.project must be replaced with a real GCP project before running a benchmark")
	}
	return nil
}
