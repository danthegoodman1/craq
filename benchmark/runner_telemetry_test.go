package benchmark

import (
	"strings"
	"testing"
	"time"
)

func TestBuildRemoteTelemetryCommandUsesNodeScopedDirectory(t *testing.T) {
	command := buildRemoteTelemetryCommand("/var/lib/craq-bench/runs/run-123/storage-a", time.Second, 2*time.Minute)

	for _, want := range []string{
		"mkdir -p '/var/lib/craq-bench/runs/run-123/storage-a'",
		"/var/lib/craq-bench/runs/run-123/storage-a/probe.jsonl",
		"/var/lib/craq-bench/runs/run-123/storage-a/vmstat.txt",
		"/var/lib/craq-bench/runs/run-123/storage-a/iostat.txt",
		"/var/lib/craq-bench/runs/run-123/storage-a/pidstat.txt",
		"nohup timeout 120s",
	} {
		if !strings.Contains(command, want) {
			t.Fatalf("telemetry command missing %q: %s", want, command)
		}
	}
}
