package benchmark

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/danthegoodman1/craq/quickstart"
)

const (
	RunStateFileName       = "run-state.json"
	RunMetadataFileName    = "run-metadata.json"
	ArtifactManifestName   = "artifact-manifest.json"
	ArtifactBundleFileName = "artifacts.tar.gz"
)

type RunState struct {
	RunID            string           `json:"run_id"`
	RunName          string           `json:"run_name"`
	GitSHA           string           `json:"git_sha"`
	ProfilePath      string           `json:"profile_path"`
	Profile          Profile          `json:"profile"`
	CreatedAt        time.Time        `json:"created_at"`
	UpdatedAt        time.Time        `json:"updated_at"`
	Region           string           `json:"region"`
	Topology         string           `json:"topology"`
	ClientPlacement  string           `json:"client_placement"`
	ArtifactsDir     string           `json:"artifacts_dir"`
	TerraformDir     string           `json:"terraform_dir"`
	TerraformState   string           `json:"terraform_state"`
	SSHPrivateKey    string           `json:"ssh_private_key"`
	SSHPublicKey     string           `json:"ssh_public_key"`
	ClientPublicIP   string           `json:"client_public_ip"`
	Status           string           `json:"status"`
	TerraformOutputs TerraformOutputs `json:"terraform_outputs"`
	ManifestPath     string           `json:"manifest_path"`
	Notes            []string         `json:"notes,omitempty"`
}

type TerraformOutputs struct {
	Region         string            `json:"region"`
	PrimaryZone    string            `json:"primary_zone"`
	PublicClientIP string            `json:"public_client_ip"`
	PrivateIPs     map[string]string `json:"private_ips"`
	InstanceNames  map[string]string `json:"instance_names"`
	NodeZones      map[string]string `json:"node_zones"`
}

type ArtifactManifest struct {
	RunID     string          `json:"run_id"`
	CreatedAt time.Time       `json:"created_at"`
	Files     []ArtifactEntry `json:"files"`
}

type ArtifactEntry struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type RunMetadata struct {
	RunID           string            `json:"run_id"`
	RunName         string            `json:"run_name"`
	GitSHA          string            `json:"git_sha"`
	StartedAt       time.Time         `json:"started_at"`
	CompletedAt     time.Time         `json:"completed_at,omitempty"`
	Profile         Profile           `json:"profile"`
	Topology        string            `json:"topology"`
	ClientPlacement string            `json:"client_placement"`
	Manifest        quickstart.Config `json:"manifest"`
	Terraform       TerraformOutputs  `json:"terraform"`
}

type LoadGenReport struct {
	RunID      string           `json:"run_id"`
	StartedAt  time.Time        `json:"started_at"`
	FinishedAt time.Time        `json:"finished_at"`
	Scenarios  []ScenarioReport `json:"scenarios"`
	Preload    PreloadReport    `json:"preload"`
	Errors     []string         `json:"errors,omitempty"`
}

type PreloadReport struct {
	Keys     int           `json:"keys"`
	Duration time.Duration `json:"duration"`
}

type ScenarioReport struct {
	Name        string             `json:"name"`
	Kind        string             `json:"kind"`
	Concurrency int                `json:"concurrency"`
	Warmup      time.Duration      `json:"warmup"`
	Duration    time.Duration      `json:"duration"`
	StartedAt   time.Time          `json:"started_at"`
	FinishedAt  time.Time          `json:"finished_at"`
	TotalOps    int64              `json:"total_ops"`
	SuccessOps  int64              `json:"success_ops"`
	ErrorOps    int64              `json:"error_ops"`
	IgnoredErrorOps int64          `json:"ignored_error_ops,omitempty"`
	ReadOps     int64              `json:"read_ops"`
	WriteOps    int64              `json:"write_ops"`
	P50Millis   float64            `json:"p50_ms"`
	P95Millis   float64            `json:"p95_ms"`
	P99Millis   float64            `json:"p99_ms"`
	MaxMillis   float64            `json:"max_ms"`
	Throughput  float64            `json:"throughput_ops_per_sec"`
	Operations  []OperationSummary `json:"operations,omitempty"`
	Histogram   []HistogramBucket  `json:"histogram"`
	Intervals   []IntervalSample   `json:"intervals"`
}

type OperationSummary struct {
	Operation  string  `json:"operation"`
	TotalOps   int64   `json:"total_ops"`
	SuccessOps int64   `json:"success_ops"`
	ErrorOps   int64   `json:"error_ops"`
	IgnoredErrorOps int64 `json:"ignored_error_ops,omitempty"`
	P50Millis  float64 `json:"p50_ms"`
	P95Millis  float64 `json:"p95_ms"`
	P99Millis  float64 `json:"p99_ms"`
	MaxMillis  float64 `json:"max_ms"`
	Throughput float64 `json:"throughput_ops_per_sec"`
}

type HistogramBucket struct {
	UpperMillis float64 `json:"upper_ms"`
	Count       int64   `json:"count"`
}

type IntervalSample struct {
	Timestamp time.Time `json:"timestamp"`
	Ops       int64     `json:"ops"`
	Errors    int64     `json:"errors"`
	Reads     int64     `json:"reads"`
	Writes    int64     `json:"writes"`
	P50Millis float64   `json:"p50_ms"`
	P95Millis float64   `json:"p95_ms"`
	P99Millis float64   `json:"p99_ms"`
}

func SaveJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", filepath.Dir(path), err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %q: %w", path, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("write %q: %w", path, err)
	}
	return nil
}

func LoadJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %q: %w", path, err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode %q: %w", path, err)
	}
	return nil
}

func WriteRunState(path string, state RunState) error {
	state.UpdatedAt = time.Now().UTC()
	return SaveJSON(path, state)
}

func ReadRunState(path string) (RunState, error) {
	var state RunState
	if err := LoadJSON(path, &state); err != nil {
		return RunState{}, err
	}
	return state, nil
}

func BuildArtifactManifest(root string, runID string) (ArtifactManifest, error) {
	manifest := ArtifactManifest{
		RunID:     runID,
		CreatedAt: time.Now().UTC(),
	}
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == ArtifactManifestName {
			return nil
		}
		sum, err := fileSHA256(path)
		if err != nil {
			return err
		}
		manifest.Files = append(manifest.Files, ArtifactEntry{
			Path:   filepath.ToSlash(rel),
			SHA256: sum,
			Size:   info.Size(),
		})
		return nil
	})
	if err != nil {
		return ArtifactManifest{}, fmt.Errorf("walk artifact root %q: %w", root, err)
	}
	sort.Slice(manifest.Files, func(i, j int) bool {
		return manifest.Files[i].Path < manifest.Files[j].Path
	})
	return manifest, nil
}

func VerifyArtifactManifest(root string, manifest ArtifactManifest) error {
	for _, entry := range manifest.Files {
		path := filepath.Join(root, filepath.FromSlash(entry.Path))
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("stat %q: %w", path, err)
		}
		if info.Size() != entry.Size {
			return fmt.Errorf("artifact %q size mismatch: got %d want %d", entry.Path, info.Size(), entry.Size)
		}
		sum, err := fileSHA256(path)
		if err != nil {
			return fmt.Errorf("checksum %q: %w", path, err)
		}
		if sum != entry.SHA256 {
			return fmt.Errorf("artifact %q checksum mismatch", entry.Path)
		}
	}
	return nil
}

func fileSHA256(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
