package benchmark

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type SSHConfig struct {
	User         string
	PrivateKey   string
	JumpPublicIP string
	DisableJump  bool
}

func commonSSHOptions() []string {
	return []string{
		"-o", "BatchMode=yes",
		"-o", "ConnectTimeout=10",
		"-o", "LogLevel=ERROR",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "IdentitiesOnly=yes",
	}
}

func (c SSHConfig) proxyCommand() string {
	if c.DisableJump || c.JumpPublicIP == "" {
		return ""
	}
	return fmt.Sprintf(
		"ssh -i %s -o BatchMode=yes -o ConnectTimeout=10 -o LogLevel=ERROR -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -W %%h:%%p %s@%s",
		c.PrivateKey,
		c.User,
		c.JumpPublicIP,
	)
}

func (c SSHConfig) baseArgs(target string) []string {
	args := append([]string{}, commonSSHOptions()...)
	args = append(args, "-i", c.PrivateKey)
	if target != c.JumpPublicIP {
		if proxy := c.proxyCommand(); proxy != "" {
			args = append(args, "-o", "ProxyCommand="+proxy)
		}
	}
	args = append(args, fmt.Sprintf("%s@%s", c.User, target))
	return args
}

func SSH(ctx context.Context, ssh SSHConfig, target string, command string) error {
	args := ssh.baseArgs(target)
	args = append(args, command)
	return runCommand(ctx, nil, "", "ssh", args...)
}

func SSHCapture(ctx context.Context, ssh SSHConfig, target string, command string) ([]byte, error) {
	args := ssh.baseArgs(target)
	args = append(args, command)
	return captureCommand(ctx, nil, "", "ssh", args...)
}

func SCP(ctx context.Context, ssh SSHConfig, source string, targetHost string, targetPath string) error {
	args := append([]string{}, commonSSHOptions()...)
	args = append(args, "-q")
	args = append(args, "-i", ssh.PrivateKey)
	if targetHost != ssh.JumpPublicIP {
		if proxy := ssh.proxyCommand(); proxy != "" {
			args = append(args, "-o", "ProxyCommand="+proxy)
		}
	}
	args = append(args, source, fmt.Sprintf("%s@%s:%s", ssh.User, targetHost, targetPath))
	return runCommand(ctx, nil, "", "scp", args...)
}

func SCPFrom(ctx context.Context, ssh SSHConfig, sourceHost string, sourcePath string, localPath string) error {
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return fmt.Errorf("mkdir scp destination: %w", err)
	}
	args := []string{}
	args = append(args, commonSSHOptions()...)
	args = append(args, "-q")
	args = append(args, "-i", ssh.PrivateKey)
	if sourceHost != ssh.JumpPublicIP {
		if proxy := ssh.proxyCommand(); proxy != "" {
			args = append(args, "-o", "ProxyCommand="+proxy)
		}
	}
	args = append(args, fmt.Sprintf("%s@%s:%s", ssh.User, sourceHost, sourcePath), localPath)
	return runCommand(ctx, nil, "", "scp", args...)
}

func waitForSSH(ctx context.Context, ssh SSHConfig, host string) error {
	var lastErr error
	for {
		if ctx.Err() != nil {
			if lastErr != nil && !errors.Is(ctx.Err(), context.Canceled) {
				return fmt.Errorf("%w (last ssh error: %v)", ctx.Err(), lastErr)
			}
			return ctx.Err()
		}
		err := SSH(ctx, SSHConfig{
			User:         ssh.User,
			PrivateKey:   ssh.PrivateKey,
			JumpPublicIP: ssh.JumpPublicIP,
			DisableJump:  ssh.DisableJump,
		}, host, "true")
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			if lastErr != nil && !errors.Is(ctx.Err(), context.Canceled) {
				return fmt.Errorf("%w (last ssh error: %v)", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'"'"'`) + "'"
}
