package benchmark

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

func prepareLocalStorageRoot(ctx context.Context, runDir string, runID string, cfg LocalProfile) (string, func(), error) {
	layout := strings.TrimSpace(cfg.StorageLayout)
	if layout == "" || layout == "repo_disk" {
		root := filepath.Join(runDir, "local")
		if err := os.MkdirAll(root, 0o755); err != nil {
			return "", nil, fmt.Errorf("mkdir local storage root: %w", err)
		}
		return root, func() {}, nil
	}
	if layout != "ramdisk" {
		return "", nil, fmt.Errorf("unsupported local storage layout %q", layout)
	}
	if cfg.RamdiskPath != "" {
		root := filepath.Join(cfg.RamdiskPath, runID)
		if err := os.MkdirAll(root, 0o755); err != nil {
			return "", nil, fmt.Errorf("mkdir configured ramdisk root: %w", err)
		}
		return root, func() {
			_ = os.RemoveAll(root)
		}, nil
	}
	switch runtime.GOOS {
	case "darwin":
		return prepareDarwinRamdisk(ctx, runID, cfg.RamdiskSizeMiB)
	case "linux":
		return prepareLinuxRamdisk(ctx, runDir, runID, cfg.RamdiskSizeMiB)
	default:
		return "", nil, fmt.Errorf("ramdisk local storage layout is unsupported on %s", runtime.GOOS)
	}
}

func prepareDarwinRamdisk(ctx context.Context, runID string, sizeMiB int) (string, func(), error) {
	if sizeMiB <= 0 {
		return "", nil, fmt.Errorf("ramdisk size must be > 0")
	}
	sectors := sizeMiB * 2048
	attach := exec.CommandContext(ctx, "hdiutil", "attach", "-nomount", fmt.Sprintf("ram://%d", sectors))
	attachOut, err := attach.CombinedOutput()
	if err != nil {
		return "", nil, fmt.Errorf("create darwin ramdisk: %w: %s", err, strings.TrimSpace(string(attachOut)))
	}
	device := strings.TrimSpace(firstOutputLine(string(attachOut)))
	if device == "" {
		return "", nil, fmt.Errorf("create darwin ramdisk: empty device path")
	}
	volumeName := sanitizeLocalVolumeName("craq-" + runID)
	erase := exec.CommandContext(ctx, "diskutil", "erasevolume", "APFS", volumeName, device)
	eraseOut, err := erase.CombinedOutput()
	if err != nil {
		_, _ = exec.Command("diskutil", "eject", device).CombinedOutput()
		return "", nil, fmt.Errorf("format darwin ramdisk: %w: %s", err, strings.TrimSpace(string(eraseOut)))
	}
	root := filepath.Join("/Volumes", volumeName)
	if err := os.MkdirAll(root, 0o755); err != nil {
		_, _ = exec.Command("diskutil", "eject", device).CombinedOutput()
		return "", nil, fmt.Errorf("mkdir darwin ramdisk root: %w", err)
	}
	cleanup := func() {
		_, _ = exec.Command("diskutil", "eject", device).CombinedOutput()
	}
	return root, cleanup, nil
}

func prepareLinuxRamdisk(ctx context.Context, runDir string, runID string, sizeMiB int) (string, func(), error) {
	if sizeMiB <= 0 {
		return "", nil, fmt.Errorf("ramdisk size must be > 0")
	}
	root := filepath.Join(runDir, "ramdisk-"+runID)
	if err := os.MkdirAll(root, 0o755); err != nil {
		return "", nil, fmt.Errorf("mkdir linux ramdisk root: %w", err)
	}
	mount := exec.CommandContext(ctx, "mount", "-t", "tmpfs", "-o", fmt.Sprintf("size=%dm", sizeMiB), "tmpfs", root)
	mountOut, err := mount.CombinedOutput()
	if err != nil {
		_ = os.RemoveAll(root)
		return "", nil, fmt.Errorf("mount linux ramdisk: %w: %s", err, strings.TrimSpace(string(mountOut)))
	}
	cleanup := func() {
		_, _ = exec.Command("umount", root).CombinedOutput()
		_ = os.RemoveAll(root)
	}
	return root, cleanup, nil
}

func sanitizeLocalVolumeName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return "craq-ramdisk"
	}
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "craq-ramdisk"
	}
	if len(out) > 48 {
		out = out[:48]
		out = strings.TrimRight(out, "-")
	}
	if out == "" {
		return "craq-ramdisk"
	}
	return out
}

func firstOutputLine(text string) string {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			return line
		}
	}
	return ""
}
