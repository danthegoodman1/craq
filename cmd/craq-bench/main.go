package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/danthegoodman1/craq/benchmark"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: craq-bench <run|run-local|smoke-local|destroy|analyze|daemon|loadgen|probe|collect|durability-bench> [flags]")
	}
	switch args[0] {
	case "run":
		return runCommand(args[1:])
	case "run-local":
		return runLocalCommand(args[1:])
	case "smoke-local":
		return smokeLocalCommand(args[1:])
	case "destroy":
		return destroyCommand(args[1:])
	case "analyze":
		return analyzeCommand(args[1:])
	case "daemon":
		return daemonCommand(args[1:])
	case "loadgen":
		return loadgenCommand(args[1:])
	case "probe":
		return probeCommand(args[1:])
	case "collect":
		return collectCommand(args[1:])
	case "durability-bench":
		return durabilityBenchCommand(args[1:])
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runCommand(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	profile := fs.String("profile", "profiles/bench/gcp_c4a_steady.yaml", "benchmark profile yaml")
	region := fs.String("region", "", "gcp region override")
	topology := fs.String("topology", "", "topology: single-zone or multi-zone")
	clientPlacement := fs.String("client-placement", "", "client placement: same-zone or remote-zone")
	runName := fs.String("run-name", "bench", "human readable run name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		runDir, err := benchmark.RunBenchmark(ctx, benchmark.RunOptions{
			ProfilePath:     *profile,
			Region:          *region,
			Topology:        *topology,
			ClientPlacement: *clientPlacement,
			RunName:         *runName,
		})
		if err != nil {
			return err
		}
		fmt.Println(runDir)
		return nil
	})
}

func smokeLocalCommand(args []string) error {
	fs := flag.NewFlagSet("smoke-local", flag.ContinueOnError)
	profile := fs.String("profile", "profiles/bench/local_smoke.yaml", "benchmark profile yaml")
	runName := fs.String("run-name", "smoke-local", "human readable run name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		runDir, err := benchmark.SmokeLocal(ctx, benchmark.SmokeLocalOptions{
			ProfilePath: *profile,
			RunName:     *runName,
		})
		if err != nil {
			if runDir != "" {
				return fmt.Errorf("%w\nrun dir: %s", err, runDir)
			}
			return err
		}
		fmt.Println(runDir)
		return nil
	})
}

func runLocalCommand(args []string) error {
	fs := flag.NewFlagSet("run-local", flag.ContinueOnError)
	profile := fs.String("profile", "profiles/bench/local_put_scaling.yaml", "benchmark profile yaml")
	runName := fs.String("run-name", "run-local", "human readable run name")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		runDir, err := benchmark.RunLocal(ctx, benchmark.RunLocalOptions{
			ProfilePath: *profile,
			RunName:     *runName,
		})
		if err != nil {
			if runDir != "" {
				return fmt.Errorf("%w\nrun dir: %s", err, runDir)
			}
			return err
		}
		fmt.Println(runDir)
		return nil
	})
}

func destroyCommand(args []string) error {
	fs := flag.NewFlagSet("destroy", flag.ContinueOnError)
	runDir := fs.String("run-dir", "", "run directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return fmt.Errorf("destroy requires --run-dir")
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		return benchmark.DestroyBenchmark(ctx, benchmark.DestroyOptions{RunDir: *runDir})
	})
}

func analyzeCommand(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ContinueOnError)
	runDir := fs.String("run-dir", "", "run directory")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *runDir == "" {
		return fmt.Errorf("analyze requires --run-dir")
	}
	_, err := benchmark.AnalyzeRun(*runDir)
	return err
}

func daemonCommand(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: craq-bench daemon <coordinator|storage> --config <path>")
	}
	switch args[0] {
	case "coordinator":
		fs := flag.NewFlagSet("daemon coordinator", flag.ContinueOnError)
		configPath := fs.String("config", "", "coordinator json config")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		var cfg benchmark.CoordinatorProcessConfig
		if err := benchmark.LoadJSON(*configPath, &cfg); err != nil {
			return err
		}
		return benchmark.RunSignalContext(func(ctx context.Context) error { return benchmark.RunCoordinatorProcess(ctx, cfg) })
	case "storage":
		fs := flag.NewFlagSet("daemon storage", flag.ContinueOnError)
		configPath := fs.String("config", "", "storage json config")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		var cfg benchmark.StorageProcessConfig
		if err := benchmark.LoadJSON(*configPath, &cfg); err != nil {
			return err
		}
		return benchmark.RunSignalContext(func(ctx context.Context) error { return benchmark.RunStorageProcess(ctx, cfg) })
	default:
		return fmt.Errorf("unknown daemon role %q", args[0])
	}
}

func loadgenCommand(args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	configPath := fs.String("config", "", "loadgen json config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var cfg benchmark.LoadGenProcessConfig
	if err := benchmark.LoadJSON(*configPath, &cfg); err != nil {
		return err
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		_, err := benchmark.RunLoadGen(ctx, cfg)
		return err
	})
}

func probeCommand(args []string) error {
	fs := flag.NewFlagSet("probe", flag.ContinueOnError)
	output := fs.String("output", "", "probe output path")
	interval := fs.Duration("interval", 0, "sample interval")
	duration := fs.Duration("duration", 0, "optional duration")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		return benchmark.RunProbe(ctx, benchmark.ProbeConfig{
			OutputPath: *output,
			Interval:   *interval,
			Duration:   *duration,
		})
	})
}

func collectCommand(args []string) error {
	fs := flag.NewFlagSet("collect", flag.ContinueOnError)
	configPath := fs.String("config", "", "collect json config")
	if err := fs.Parse(args); err != nil {
		return err
	}
	var cfg benchmark.CollectConfig
	if err := benchmark.LoadJSON(*configPath, &cfg); err != nil {
		return err
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		_, err := benchmark.CollectArtifacts(ctx, cfg)
		return err
	})
}

func durabilityBenchCommand(args []string) error {
	fs := flag.NewFlagSet("durability-bench", flag.ContinueOnError)
	path := fs.String("path", "", "benchmark data path")
	label := fs.String("label", "", "benchmark label")
	output := fs.String("output", "", "output json path")
	count := fs.Int("count", 1000, "operations per sub-benchmark")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return benchmark.RunSignalContext(func(ctx context.Context) error {
		return benchmark.RunDurabilityBench(ctx, benchmark.DurabilityBenchConfig{
			Path:       *path,
			Label:      *label,
			OutputPath: *output,
			Count:      *count,
		})
	})
}
