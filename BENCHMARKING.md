# Benchmarking

`chainrep` includes a separate benchmark operator CLI:

- binary: `craq-bench`
- entrypoint: [`cmd/craq-bench/main.go`](./cmd/craq-bench/main.go)
- default profile: [`profiles/bench/gcp_c4a_steady.yaml`](./profiles/bench/gcp_c4a_steady.yaml)

This is the tool that prepares Terraform-managed GCP infrastructure, runs the
benchmark, pulls artifacts locally, analyzes the run, and tears the stack down.

The current repo state is intentionally not runnable against real cloud infra
out of the box: the default profile contains a placeholder `gcp.project`
setting that you must replace before attempting a live run.

## Build

```bash
go build -o ./bin/craq-bench ./cmd/craq-bench
```

## Prerequisites

Install locally:

- `go`
- `terraform`
- `ssh`
- `scp`

For future live runs against GCP, authenticate locally using your normal GCP
tooling and set a real `gcp.project` value in your profile before running
`craq-bench run`.

## Main Commands

Create infra from scratch, run the benchmark, pull artifacts, and destroy on
success:

```bash
./bin/craq-bench run \
  --profile profiles/bench/gcp_c4a_steady.yaml \
  --run-name my-bench
```

Useful run overrides:

```bash
./bin/craq-bench run \
  --profile profiles/bench/gcp_c4a_steady.yaml \
  --region us-central1 \
  --topology multi-zone \
  --client-placement remote-zone \
  --run-name multi-zone-worst-case
```

## Run Overrides

Current `run` command overrides:

- `--profile`
  - default: `profiles/bench/gcp_c4a_steady.yaml`
  - selects the benchmark profile YAML
- `--region`
  - default: value from the profile
  - overrides the GCP region for this run
- `--topology`
  - values: `single-zone`, `multi-zone`
  - default: `single-zone`
  - controls whether the stack is placed in one zone or spread across three zones
- `--client-placement`
  - values: `same-zone`, `remote-zone`
  - default: `same-zone`
  - relevant for `--topology multi-zone`; `remote-zone` places the client outside the primary zone for a worse client path
- `--run-name`
  - default: `bench`
  - used as the human-readable prefix for the generated run ID

You can inspect the exact flag surface from:

- [`cmd/craq-bench/main.go`](./cmd/craq-bench/main.go)

The current `destroy` and `analyze` commands each take:

- `--run-dir`
  - points at an existing local run directory under `artifacts/benchmarks/...`

Force teardown for a prior run directory:

```bash
./bin/craq-bench destroy \
  --run-dir artifacts/benchmarks/<run-id>
```

Analyze an already-pulled run directory:

```bash
./bin/craq-bench analyze \
  --run-dir artifacts/benchmarks/<run-id>
```

## Run Layout

Each run gets its own local directory under:

```text
artifacts/benchmarks/<run-id>/
```

That directory contains:

- local Terraform working directory and state
- rendered manifest and configs
- run metadata and run state
- pulled artifacts
- generated analysis output

The most important files are:

- `run-state.json`
- `run-metadata.json`
- `artifacts/artifact-manifest.json`
- `artifacts/artifacts.tar.gz`
- `analysis/summary.json`
- `analysis/index.html`

## Safety Notes

- `terraform destroy` is scoped to the benchmark run's local Terraform state.
- The benchmark code does not perform separate cloud delete or terminate calls
  outside Terraform.
- The default sample profile intentionally will not run until `gcp.project` is
  replaced with a real project ID.
- If artifact verification fails, the run is marked `needs_cleanup` and infra is
  intentionally preserved so you can recover data before running `destroy`.

## Current Scope

The current benchmark implementation is:

- GCP only
- steady-state only
- designed around `c4a-standard-48-lssd` storage nodes
- one coordinator, three storage nodes, one client
- storage-node data directories live on RAID0-mounted Local SSD under `/var/lib/craq-bench/storage-data`

It is not currently a failure-injection benchmark.
