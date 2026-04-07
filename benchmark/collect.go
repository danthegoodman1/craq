package benchmark

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/danthegoodman1/craq/quickstart"
)

type CollectConfig struct {
	RunID         string `json:"run_id"`
	ManifestPath  string `json:"manifest_path"`
	RemoteRunRoot string `json:"remote_run_root"`
	OutputDir     string `json:"output_dir"`
	SSHUser       string `json:"ssh_user"`
	SSHPrivateKey string `json:"ssh_private_key"`
}

func CollectArtifacts(ctx context.Context, cfg CollectConfig) (ArtifactManifest, error) {
	if cfg.ManifestPath == "" || cfg.OutputDir == "" || cfg.RemoteRunRoot == "" {
		return ArtifactManifest{}, fmt.Errorf("collect config must include manifest_path, remote_run_root, and output_dir")
	}
	manifest, err := quickstart.Load(cfg.ManifestPath)
	if err != nil {
		return ArtifactManifest{}, err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return ArtifactManifest{}, fmt.Errorf("mkdir collect output: %w", err)
	}
	if err := copyTree(filepath.Join(cfg.RemoteRunRoot, "client"), filepath.Join(cfg.OutputDir, "client")); err != nil {
		return ArtifactManifest{}, fmt.Errorf("copy client artifacts: %w", err)
	}
	coordHost := hostPart(manifest.Coordinator.AdminAddress)
	if err := collectHTTP("http://"+manifest.Coordinator.AdminAddress+"/metrics", filepath.Join(cfg.OutputDir, "coordinator", "metrics.prom")); err != nil {
		return ArtifactManifest{}, err
	}
	if err := collectHTTP("http://"+manifest.Coordinator.AdminAddress+"/admin/v1/state", filepath.Join(cfg.OutputDir, "coordinator", "state.json")); err != nil {
		return ArtifactManifest{}, err
	}
	if err := collectRemoteFile(ctx, cfg, coordHost, "sudo journalctl -u craq-bench-coordinator.service --no-pager", filepath.Join(cfg.OutputDir, "coordinator", "journal.log")); err != nil {
		return ArtifactManifest{}, err
	}
	for _, name := range []string{"probe.jsonl", "vmstat.txt", "iostat.txt", "pidstat.txt"} {
		if err := collectRemoteFile(ctx, cfg, coordHost, "sudo cat "+shellQuote(filepath.Join(cfg.RemoteRunRoot, "coordinator", name)), filepath.Join(cfg.OutputDir, "coordinator", name)); err != nil {
			return ArtifactManifest{}, err
		}
	}
	for _, node := range manifest.Nodes {
		nodeDir := filepath.Join(cfg.OutputDir, "storage-"+node.ID)
		if err := collectHTTP("http://"+node.AdminAddress+"/metrics", filepath.Join(nodeDir, "metrics.prom")); err != nil {
			return ArtifactManifest{}, err
		}
		if err := collectHTTP("http://"+node.AdminAddress+"/admin/v1/state", filepath.Join(nodeDir, "state.json")); err != nil {
			return ArtifactManifest{}, err
		}
		host := hostPart(node.AdminAddress)
		if err := collectRemoteFile(ctx, cfg, host, "sudo journalctl -u craq-bench-storage.service --no-pager", filepath.Join(nodeDir, "journal.log")); err != nil {
			return ArtifactManifest{}, err
		}
		for _, name := range []string{"probe.jsonl", "vmstat.txt", "iostat.txt", "pidstat.txt"} {
			if err := collectRemoteFile(ctx, cfg, host, "sudo cat "+shellQuote(filepath.Join(cfg.RemoteRunRoot, "storage-"+node.ID, name)), filepath.Join(nodeDir, name)); err != nil {
				return ArtifactManifest{}, err
			}
		}
	}
	manifestFile, err := BuildArtifactManifest(cfg.OutputDir, cfg.RunID)
	if err != nil {
		return ArtifactManifest{}, err
	}
	if err := SaveJSON(filepath.Join(cfg.OutputDir, ArtifactManifestName), manifestFile); err != nil {
		return ArtifactManifest{}, err
	}
	if err := writeTarGz(filepath.Join(cfg.OutputDir, ArtifactBundleFileName), cfg.OutputDir); err != nil {
		return ArtifactManifest{}, err
	}
	return manifestFile, nil
}

func collectHTTP(url string, outputPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("get %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("get %s: status %s", url, resp.Status)
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, resp.Body)
	return err
}

func collectRemoteFile(ctx context.Context, cfg CollectConfig, host string, command string, outputPath string) error {
	cmd := exec.CommandContext(ctx, "ssh",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-i", cfg.SSHPrivateKey,
		fmt.Sprintf("%s@%s", cfg.SSHUser, host),
		command,
	)
	data, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("ssh %s %q: %w", host, command, err)
	}
	if err := os.MkdirAll(filepath.Dir(outputPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(outputPath, data, 0o644)
}

func hostPart(address string) string {
	host, _, _ := strings.Cut(address, ":")
	return host
}

func copyTree(src string, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	})
}

func writeTarGz(target string, root string) error {
	file, err := os.Create(target)
	if err != nil {
		return fmt.Errorf("create tarball: %w", err)
	}
	defer file.Close()
	gz := gzip.NewWriter(file)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == target {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil || rel == "." {
			return err
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = filepath.ToSlash(rel)
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		_, err = io.Copy(tw, in)
		return err
	})
}
