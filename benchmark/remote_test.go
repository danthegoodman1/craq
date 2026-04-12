package benchmark

import (
	"strings"
	"testing"
)

func TestSSHBaseArgsUsesExplicitProxyCommandForJumpHost(t *testing.T) {
	cfg := SSHConfig{
		User:         "ubuntu",
		PrivateKey:   "/tmp/bench-key",
		JumpPublicIP: "34.29.104.78",
	}

	args := cfg.baseArgs("10.42.0.11")
	joined := strings.Join(args, " ")

	if !strings.Contains(joined, "ProxyCommand=ssh -i /tmp/bench-key") {
		t.Fatalf("baseArgs missing proxy command with explicit identity: %q", joined)
	}
	if strings.Contains(joined, " -J ") {
		t.Fatalf("baseArgs still contains ProxyJump: %q", joined)
	}
	if !strings.Contains(joined, "IdentitiesOnly=yes") {
		t.Fatalf("baseArgs missing IdentitiesOnly: %q", joined)
	}
	if !strings.Contains(joined, "BatchMode=yes") {
		t.Fatalf("baseArgs missing BatchMode: %q", joined)
	}
	if !strings.Contains(joined, "LogLevel=ERROR") {
		t.Fatalf("baseArgs missing LogLevel=ERROR: %q", joined)
	}
}

func TestSSHBaseArgsSkipsProxyForJumpHostTarget(t *testing.T) {
	cfg := SSHConfig{
		User:         "ubuntu",
		PrivateKey:   "/tmp/bench-key",
		JumpPublicIP: "34.29.104.78",
	}

	args := cfg.baseArgs("34.29.104.78")
	joined := strings.Join(args, " ")

	if strings.Contains(joined, "ProxyCommand=") {
		t.Fatalf("baseArgs unexpectedly used proxy command for jump host target: %q", joined)
	}
}
