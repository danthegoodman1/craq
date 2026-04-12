# craq - CRAQ in Go

`craq` is a Go implementation of CRAQ, with a coordinator control plane,
multi-replica storage nodes, gRPC transport, and both non-HA and HA
coordinator modes.

Highlights:

- gRPC transport between clients, coordinator, and storage nodes
- epoch-gated coordinator HA failover
- durable local Badger-backed storage backend
- dynamic storage-node auto-registration and ready gating
- durable non-HA coordinator dispatch retry and restart recovery
- flapping-node eviction through coordinator liveness policy
- conditional writes with per-object metadata
- CRAQ-style reads from any active replica with linearizable and local-committed modes
- optional TLS/mTLS for gRPC transports
- read-only HTTP admin/health endpoints plus Prometheus metrics

Documentation:

- [Architecture](./ARCHITECTURE.md)
- [Benchmarking](./BENCHMARKING.md)
- [Coordinator HA Store](./HA_STORE.md)
- [Coordinator](./coordinator/README.md)
- [Observability](./OBSERVABILITY.md)
- [Quickstart](./QUICKSTART.md)
- [Security](./SECURITY.md)

## Testing

Normal functional coverage stays on the standard Go test path:

```bash
go test ./...
```

For concurrency-sensitive control-plane and storage changes, run the targeted
core race suite before pushing or before a cloud benchmark:

```bash
scripts/test-race-core.sh
```

That race suite intentionally covers the packages with the most live
concurrency:

- `./storage`
- `./benchmark`
- `./coordserver`
- `./coordinator/runtime`
- `./transport/grpcx`
- `./client`
- `./adminhttp`

For the benchmark-critical coordination stack, the more focused pre-benchmark
race command is:

```bash
go test -race -count=1 ./coordserver ./coordinator/runtime ./benchmark ./client ./transport/grpcx
```

External or env-gated suites stay opt-in. For example, Postgres-backed
coordinator HA tests are still gated by `CRAQ_TEST_POSTGRES_DSN` and are not
part of the default race script. When those tests are needed, run them
explicitly with the same env var set instead of folding them into the default
core workflow. A typical manual invocation is:

```bash
CRAQ_TEST_POSTGRES_DSN=postgres://... go test -race -count=1 ./coordserver -run TestPostgres
```

For benchmark startup and convergence changes, run the local preflight suite
before doing a real cloud run:

```bash
scripts/test-benchmark-preflight.sh
```

That script keeps the coverage local-only and focuses on the real benchmark
bring-up path:

- routing convergence under benchmark startup pressure
- control-plane timeout behavior under large pending work
- storage/control-plane overlap
- real-process local benchmark startup

For a heavier local-only startup soak that mirrors the real cloud benchmark
shape more closely, run:

```bash
scripts/test-benchmark-soak-local.sh
```

That soak intentionally stays out of the everyday fast path. It is the local
gate for catching the older GCP-style failure shape where routing progress
advances a little and then collapses or churns instead of converging.

The recommended pre-cloud benchmark workflow is:

```bash
scripts/test-race-core.sh
scripts/test-benchmark-preflight.sh
scripts/test-benchmark-soak-local.sh
go build -o ./bin/craq-bench ./cmd/craq-bench
./bin/craq-bench smoke-local
./bin/craq-bench smoke-local --profile profiles/bench/local_smoke_cloud_shape.yaml --run-name cloud-shape
./bin/craq-bench run --profile profiles/bench/gcp_c4a_steady.yaml --run-name my-bench
```

## Ops Surfaces

Observability is documented in [OBSERVABILITY.md](./OBSERVABILITY.md).

At a high level, each coordinator or storage process can optionally expose a
separate read-only HTTP admin listener with:

- `/livez`
- `/readyz`
- `/metrics`
- `/admin/v1/state`

The admin listener is unauthenticated in v1 and is intended for loopback or a
trusted network only.

## Performance Notes

For cloud benchmark orchestration, see [BENCHMARKING.md](./BENCHMARKING.md).

The repo includes an end-to-end localhost gRPC benchmark in
[`transport/grpcx/grpc_benchmark_test.go`](./transport/grpcx/grpc_benchmark_test.go).
It uses:

- a coordinator gRPC server
- a client router
- storage-node gRPC servers
- gRPC replication between nodes
- Badger-backed storage on local temp directories

These numbers are end-to-end client latencies on localhost through a
setup: router -> coordinator snapshot -> storage-node gRPC server(s) -> storage,
with replication over gRPC where applicable.

Benchmark command:

```bash
go test ./transport/grpcx -run '^$' -bench BenchmarkClientLatencyGRPC_Localhost -benchmem -benchtime=3s -count=5 -cpu=1
```

Average results from 5 localhost benchmark runs on an Apple M3 Max, using the command above:

- `single_replica_get`: `0.049 ms/op`, `11,020 B/op`, `191 allocs/op`
- `single_replica_put`: `0.101 ms/op`, `14,607 B/op`, `299 allocs/op`
- `three_replica_get`: `0.052 ms/op`, `11,430 B/op`, `192 allocs/op`
- `three_replica_put`: `0.331 ms/op`, `60,612 B/op`, `1,137 allocs/op`
- `five_replica_get`: `0.054 ms/op`, `11,879 B/op`, `193 allocs/op`
- `five_replica_put`: `0.595 ms/op`, `108,568 B/op`, `1,993 allocs/op`

These are localhost benchmark numbers, not SLOs or cross-machine production guarantees.
They include gRPC and durable local storage costs, but not network latency, TLS, or
multi-host deployment effects. The read path likely reflects cache-warm access through
Badger and the OS page cache rather than cold disk-read latency.

Those localhost transport benchmarks are steady-state latency checks. They are
not the benchmark startup/convergence gate; use `scripts/test-benchmark-preflight.sh`
and `craq-bench smoke-local` for that. Use `scripts/test-benchmark-soak-local.sh`
and the cloud-shape smoke profile when you need the heavier local startup soak
before spending money on a real cloud run.
