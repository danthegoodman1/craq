# Benchmarking

`craq` includes a separate benchmark operator CLI:

- binary: `craq-bench`
- entrypoint: [`cmd/craq-bench/main.go`](./cmd/craq-bench/main.go)
- default profile: [`profiles/bench/gcp_c4a_steady.yaml`](./profiles/bench/gcp_c4a_steady.yaml)
- local smoke profile: [`profiles/bench/local_smoke.yaml`](./profiles/bench/local_smoke.yaml)
- local cloud-shape smoke profile: [`profiles/bench/local_smoke_cloud_shape.yaml`](./profiles/bench/local_smoke_cloud_shape.yaml)

This is the tool that prepares Terraform-managed GCP infrastructure, runs the
benchmark, pulls artifacts locally, analyzes the run, and tears the stack down.

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
tooling and make sure `gcp.project` in your cloud profile points at the project
you actually intend to benchmark before running
`craq-bench run`.

## Recommended Workflow

Do the local-only preflight before spending money on a cloud run:

```bash
scripts/test-race-core.sh
scripts/test-benchmark-preflight.sh
./bin/craq-bench smoke-local
```

For the heavier local-only startup soak that mirrors the real cloud benchmark
shape more closely, run:

```bash
scripts/test-benchmark-soak-local.sh
./bin/craq-bench smoke-local \
  --profile profiles/bench/local_smoke_cloud_shape.yaml \
  --run-name cloud-shape
```

Then do the real GCP run:

```bash
./bin/craq-bench run \
  --profile profiles/bench/gcp_c4a_steady.yaml \
  --run-name my-bench
```

## Local Smoke

`smoke-local` exercises the real benchmark runtime entirely on localhost:

- one coordinator process
- three storage processes
- real gRPC transport
- real client router sanity traffic
- progress-aware routing convergence checks

It does not use Terraform, SSH, or any cloud APIs.

```bash
./bin/craq-bench smoke-local \
  --profile profiles/bench/local_smoke.yaml \
  --run-name local-preflight
```

`smoke-local` ignores the cloud/Terraform parts of the shared profile schema and
uses the local runtime only. Its artifacts go under the normal benchmark run
directory root at `artifacts/benchmarks/<run-id>/`.

For the heavier local cloud-shape startup soak, use:

```bash
./bin/craq-bench smoke-local \
  --profile profiles/bench/local_smoke_cloud_shape.yaml \
  --run-name cloud-shape
```

That profile uses the same `1024`-slot, RF `3`, `max_changed_chains: 32`
startup shape as the real GCP benchmark path, but still runs entirely on
localhost.

## Cloud Run

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

The benchmark implementation is:

- GCP only
- steady-state only
- designed around `c4a-standard-48-lssd` storage nodes
- one coordinator, three storage nodes, one client
- storage-node data directories live on RAID0-mounted Local SSD under `/var/lib/craq-bench/storage-data`
- local-only smoke preflight through `craq-bench smoke-local`
- local-only cloud-shape soak through `scripts/test-benchmark-soak-local.sh` and `craq-bench smoke-local --profile profiles/bench/local_smoke_cloud_shape.yaml`

It is not currently a failure-injection benchmark.
