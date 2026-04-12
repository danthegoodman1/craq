package benchmark

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	gopprof "runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/danthegoodman1/craq/quickstart"
)

type loadgenProfileTarget struct {
	name      string
	adminAddr string
}

type scenarioProfiler struct {
	wg   sync.WaitGroup
	mu   sync.Mutex
	errs []error
}

func loadgenProfileTargets(manifest quickstart.Config) []loadgenProfileTarget {
	targets := []loadgenProfileTarget{
		{name: "coordinator", adminAddr: manifest.Coordinator.AdminAddress},
	}
	for _, node := range manifest.Nodes {
		targets = append(targets, loadgenProfileTarget{
			name:      "storage-" + node.ID,
			adminAddr: node.AdminAddress,
		})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].name < targets[j].name })
	return targets
}

func startScenarioProfiler(
	ctx context.Context,
	outputDir string,
	scenario ScenarioProfile,
	telemetry TelemetryProfile,
	targets []loadgenProfileTarget,
) *scenarioProfiler {
	profiler := &scenarioProfiler{}
	captureDuration := scenarioProfileDuration(telemetry, scenario)
	if captureDuration == 0 {
		return profiler
	}
	delay := scenario.Warmup
	scenarioDir := filepath.Join(outputDir, "pprof", sanitizeScenarioName(scenario.Name))

	profiler.wg.Add(1)
	go func() {
		defer profiler.wg.Done()
		if err := captureSelfScenarioProfiles(ctx, filepath.Join(scenarioDir, "client"), delay, captureDuration); err != nil {
			profiler.addErr(fmt.Errorf("capture client pprof for scenario %q: %w", scenario.Name, err))
		}
	}()

	for _, target := range targets {
		target := target
		if strings.TrimSpace(target.adminAddr) == "" {
			continue
		}
		profiler.wg.Add(1)
		go func() {
			defer profiler.wg.Done()
			if err := captureRemoteScenarioProfiles(ctx, filepath.Join(scenarioDir, target.name), target.adminAddr, delay, captureDuration); err != nil {
				profiler.addErr(fmt.Errorf("capture %s pprof for scenario %q: %w", target.name, scenario.Name, err))
			}
		}()
	}
	return profiler
}

func (p *scenarioProfiler) addErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errs = append(p.errs, err)
}

func (p *scenarioProfiler) wait() error {
	p.wg.Wait()
	p.mu.Lock()
	defer p.mu.Unlock()
	return errors.Join(p.errs...)
}

func scenarioProfileDuration(telemetry TelemetryProfile, scenario ScenarioProfile) time.Duration {
	if telemetry.PPROFCPUDuration < time.Second || scenario.Duration < time.Second {
		return 0
	}
	if telemetry.PPROFCPUDuration > scenario.Duration {
		return scenario.Duration
	}
	return telemetry.PPROFCPUDuration
}

func sanitizeScenarioName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return "scenario"
	}
	replacer := strings.NewReplacer("/", "-", "\\", "-", " ", "-", ":", "-", "\t", "-")
	return replacer.Replace(name)
}

func waitForScenarioProfileStart(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func captureSelfScenarioProfiles(ctx context.Context, outputDir string, delay time.Duration, duration time.Duration) error {
	if err := waitForScenarioProfileStart(ctx, delay); err != nil {
		return err
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("mkdir client pprof dir: %w", err)
	}
	cpuFile, err := os.Create(filepath.Join(outputDir, "cpu.pprof"))
	if err != nil {
		return fmt.Errorf("create client cpu profile: %w", err)
	}
	if err := gopprof.StartCPUProfile(cpuFile); err != nil {
		_ = cpuFile.Close()
		return fmt.Errorf("start client cpu profile: %w", err)
	}
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
	timer.Stop()
	gopprof.StopCPUProfile()
	if err := cpuFile.Close(); err != nil {
		return fmt.Errorf("close client cpu profile: %w", err)
	}
	runtime.GC()
	if err := writeHeapProfile(filepath.Join(outputDir, "heap.pprof")); err != nil {
		return err
	}
	if err := writeNamedProfile("goroutine", filepath.Join(outputDir, "goroutine.pprof")); err != nil {
		return err
	}
	return nil
}

func captureRemoteScenarioProfiles(ctx context.Context, outputDir string, adminAddr string, delay time.Duration, duration time.Duration) error {
	if err := waitForScenarioProfileStart(ctx, delay); err != nil {
		return err
	}
	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("mkdir remote pprof dir: %w", err)
	}
	baseURL := "http://" + adminAddr + "/debug/pprof"
	cpuSeconds := int(duration.Round(time.Second) / time.Second)
	if cpuSeconds <= 0 {
		cpuSeconds = 1
	}
	if err := fetchProfile(baseURL+"/profile?seconds="+url.QueryEscape(fmt.Sprintf("%d", cpuSeconds)), filepath.Join(outputDir, "cpu.pprof"), duration+20*time.Second); err != nil {
		return err
	}
	if err := fetchProfile(baseURL+"/heap?gc=1", filepath.Join(outputDir, "heap.pprof"), 15*time.Second); err != nil {
		return err
	}
	if err := fetchProfile(baseURL+"/goroutine", filepath.Join(outputDir, "goroutine.pprof"), 15*time.Second); err != nil {
		return err
	}
	return nil
}

func fetchProfile(rawURL string, outputPath string, timeout time.Duration) error {
	client := &http.Client{Timeout: timeout}
	resp, err := client.Get(rawURL)
	if err != nil {
		return fmt.Errorf("get %s: %w", rawURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("get %s: status %s", rawURL, resp.Status)
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", outputPath, err)
	}
	defer file.Close()
	if _, err := io.Copy(file, resp.Body); err != nil {
		return fmt.Errorf("copy %s: %w", outputPath, err)
	}
	return nil
}

func writeHeapProfile(outputPath string) error {
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", outputPath, err)
	}
	defer file.Close()
	if err := gopprof.WriteHeapProfile(file); err != nil {
		return fmt.Errorf("write heap profile %s: %w", outputPath, err)
	}
	return nil
}

func writeNamedProfile(name string, outputPath string) error {
	profile := gopprof.Lookup(name)
	if profile == nil {
		return fmt.Errorf("profile %q not available", name)
	}
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("create %s: %w", outputPath, err)
	}
	defer file.Close()
	if err := profile.WriteTo(file, 0); err != nil {
		return fmt.Errorf("write %s profile %s: %w", name, outputPath, err)
	}
	return nil
}
