# Performance Plan

This document captures the current cloud benchmark findings, the most likely
high-leverage performance bottlenecks, and the order we should attack them.

It is intentionally evidence-first: every recommendation below is based on a
real benchmark run and the pprof artifacts captured during that run.

## Todo

- [ ] Remove per-request routing snapshot cloning in
  [client/router.go](/Users/dangoodman/code/craq/client/router.go) and rerun the
  profiled cloud benchmark.
- [ ] Remove O(slot-count) buffered-message and metric recomputation from the
  storage write hot path in
  [storage/storage.go](/Users/dangoodman/code/craq/storage/storage.go) and
  [storage/observability.go](/Users/dangoodman/code/craq/storage/observability.go).
- [ ] Replace coordinator liveness/background read-side full-state cloning with
  a lighter immutable read model in
  [coordserver/server.go](/Users/dangoodman/code/craq/coordserver/server.go)
  and
  [coordinator/runtime/runtime.go](/Users/dangoodman/code/craq/coordinator/runtime/runtime.go).
- [ ] Rerun `go test ./...`, `scripts/test-race-core.sh`,
  `scripts/test-benchmark-preflight.sh`, `scripts/test-benchmark-soak-local.sh`,
  and the real profiled cloud benchmark after each major optimization.
- [ ] Reprofile after `P0` through `P2` before deciding whether backend
  encoding, Badger layout, or transport work is the next bottleneck.

## Baseline

Benchmark date:

- April 11, 2026

Primary cloud run:

- profile: `profiles/bench/gcp_c4a_steady_pprof.yaml`
- run directory:
  [`artifacts/benchmarks/bench-2d3241a-1775955610`](/Users/dangoodman/code/craq/artifacts/benchmarks/bench-2d3241a-1775955610)
- loadgen report:
  [`loadgen-report.json`](/Users/dangoodman/code/craq/artifacts/benchmarks/bench-2d3241a-1775955610/artifacts/client/loadgen-report.json)
- summary:
  [`analysis/summary.json`](/Users/dangoodman/code/craq/artifacts/benchmarks/bench-2d3241a-1775955610/analysis/summary.json)

Observed cloud results:

- `get-only-c64`: `11,457 ops/s`, `p50 5.86ms`, `p95 8.01ms`, `p99 9.20ms`
- `put-only-c32`: `734 ops/s`, `p50 45.07ms`, `p95 61.78ms`, `p99 74.74ms`
- `mixed-80-20-c64`: `4,396 ops/s`, `p50 0.40ms`, `p95 1.68ms`, `p99 2.14ms`
- preload of `10,000` keys: `13.73s`

Important caveats:

- The `mixed` run still had a meaningful number of `context deadline exceeded`
  failures.
- For single-AZ expectations, both the read and write latencies are too high.
- The profiles do not point to one giant “disk is saturated” problem. The
  biggest issues are allocation-heavy routing and O(slot-count) bookkeeping.

## Profiling Artifacts

Representative profiles from the same run:

- client read CPU:
  [`get-only-c64/client/cpu.pprof`](/Users/dangoodman/code/craq/artifacts/benchmarks/bench-2d3241a-1775955610/artifacts/client/pprof/get-only-c64/client/cpu.pprof)
- storage read CPU:
  [`get-only-c64/storage-a/cpu.pprof`](/Users/dangoodman/code/craq/artifacts/benchmarks/bench-2d3241a-1775955610/artifacts/client/pprof/get-only-c64/storage-a/cpu.pprof)
- storage write CPU:
  [`put-only-c32/storage-b/cpu.pprof`](/Users/dangoodman/code/craq/artifacts/benchmarks/bench-2d3241a-1775955610/artifacts/client/pprof/put-only-c32/storage-b/cpu.pprof)
- coordinator CPU:
  [`put-only-c32/coordinator/cpu.pprof`](/Users/dangoodman/code/craq/artifacts/benchmarks/bench-2d3241a-1775955610/artifacts/client/pprof/put-only-c32/coordinator/cpu.pprof)

## Findings

### 1. Client routing snapshot cloning is the dominant read-path tax

The most important read-side hotspot is the router cloning the full routing
snapshot on every request.

Relevant code:

- [client/router.go](/Users/dangoodman/code/craq/client/router.go:124)
- [client/router.go](/Users/dangoodman/code/craq/client/router.go:393)

Observed profile behavior:

- `cloneSnapshot` dominates client CPU in `get-only-c64`
- a large amount of the remaining profile is allocator and GC churn
- this cost is paid even when the routing data is already stable

Why this matters:

- `1024` slots with route slices are being copied per request
- this is pure overhead, not useful work
- for same-zone reads, this is a strong candidate for why end-to-end read
  latency is `~6ms` instead of something much closer to `~1ms`

Guidance:

- stop cloning the entire `RoutingSnapshot` for ordinary request routing
- store one immutable published snapshot pointer in the router
- route directly from the published snapshot on the hot path
- keep cloning or rebuilding only for `Refresh` or coordinator-driven updates

Expected outcome:

- materially lower read latency
- much lower client allocation rate and GC work
- better mixed-workload tail behavior because the client spends less CPU per
  request

### 2. Storage PUTs still pay O(slot-count) bookkeeping in the hot path

The largest repeatable write-side hotspot on storage is node-wide scanning of
published slot state for observability/accounting.

Relevant code:

- [storage/storage.go](/Users/dangoodman/code/craq/storage/storage.go:1557)
- [storage/observability.go](/Users/dangoodman/code/craq/storage/observability.go:284)

Observed profile behavior:

- `bufferedReplicaMessagesForNodeLocked` is one of the top storage CPU consumers
- `refreshMetricGaugesLocked` is also a major hotspot during `put-only-c32`
- this work shows up across multiple storage nodes

Why this matters:

- this is effectively an O(slot-count) scan paid during active write traffic
- it competes directly with the actual replication path
- it is not protocol-essential work, so it is a good target for removal from
  the hot path

Guidance:

- move buffered-message and in-flight counters to incremental slot-owner
  published deltas
- let slot owners update node totals as state changes instead of recomputing
  totals by scanning all slots
- move metric gauge refresh to periodic or event-driven publication that reads
  already-maintained aggregates
- keep exact per-slot debug state available, but do not derive node totals by
  recomputing from the full slot map on write traffic

Expected outcome:

- lower write CPU on storage nodes
- lower PUT latency, especially under steady write load
- more predictable performance as slot count grows

### 3. Coordinator background liveness still clones too much state

The coordinator spends a large amount of CPU cloning full runtime state during
background liveness evaluation and related maintenance work.

Relevant code:

- [coordserver/server.go](/Users/dangoodman/code/craq/coordserver/server.go:735)
- [coordinator/runtime/runtime.go](/Users/dangoodman/code/craq/coordinator/runtime/runtime.go:296)

Observed profile behavior:

- `EvaluateLiveness` drives heavy time into `Runtime.Current()`
- `Current()` spends a large amount of time cloning runtime structures
- this is consistent across benchmark scenarios

Why this matters:

- this is not the first-order cause of the bad read numbers
- it is still expensive background tax that steals CPU from useful work
- it also makes the coordinator more expensive as cluster state grows

Guidance:

- stop using full deep-cloned runtime state for routine liveness/background
  decisions
- publish a lighter immutable read model for the parts of liveness that do not
  need a full mutable runtime clone
- reserve deep-copy or full reconstruction for explicit admin/debug paths and
  true state transitions

Expected outcome:

- lower coordinator background CPU
- cleaner separation between mutation ownership and read models
- less benchmark noise from non-request work

### 4. Storage backend and encoding costs exist, but they are not the first move

Badger and encoding work show up in profiles, but they are not the top repeat
offenders from this cloud run.

Relevant code:

- [storage/slot_owner.go](/Users/dangoodman/code/craq/storage/slot_owner.go:724)
- [storage/storage.go](/Users/dangoodman/code/craq/storage/storage.go:1208)
- [storage/badger/store.go](/Users/dangoodman/code/craq/storage/badger/store.go:366)

Interpretation:

- backend reads and writes are real cost
- durable CRAQ writes will always be more expensive than local memory
- however, the current cloud latency problem appears to be dominated by
  unnecessary copying and bookkeeping first

Guidance:

- do not start with a storage-format rewrite
- first remove snapshot cloning and O(slot-count) hot-path work
- then rerun the same pprof-enabled cloud benchmark
- only after that should we decide whether the next step is:
  - backend read caching
  - metadata-only committed lookups
  - more compact on-disk encoding
  - further transport tuning

## Prioritized Improvement Order

### P0. Eliminate per-request routing snapshot cloning

Target area:

- [client/router.go](/Users/dangoodman/code/craq/client/router.go)

Success signal:

- `get-only-c64` p50/p95/p99 drop materially
- client CPU profile no longer dominated by `cloneSnapshot`
- client allocation and GC pressure decrease sharply

### P1. Remove node-wide slot scans from the storage write path

Target areas:

- [storage/storage.go](/Users/dangoodman/code/craq/storage/storage.go)
- [storage/observability.go](/Users/dangoodman/code/craq/storage/observability.go)
- [storage/slot_owner.go](/Users/dangoodman/code/craq/storage/slot_owner.go)

Success signal:

- storage CPU profiles no longer show
  `bufferedReplicaMessagesForNodeLocked` / `refreshMetricGaugesLocked` as major
  hotspots
- `put-only-c32` p50/p95/p99 improve materially
- write throughput rises without increasing error rate

### P2. Replace coordinator full-state cloning for liveness/read-side work

Target areas:

- [coordserver/server.go](/Users/dangoodman/code/craq/coordserver/server.go)
- [coordinator/runtime/runtime.go](/Users/dangoodman/code/craq/coordinator/runtime/runtime.go)

Success signal:

- coordinator profiles stop spending most background CPU in `Current()` and
  clone helpers
- no regression in liveness correctness or routing publication

### P3. Reprofile and only then decide on storage-format/backend optimizations

Candidate areas:

- [storage/badger/store.go](/Users/dangoodman/code/craq/storage/badger/store.go)
- [transport/grpcx](/Users/dangoodman/code/craq/transport/grpcx)

Success signal:

- post-P0/P1/P2 profiles clearly show backend or transport as the next limiting
  factor
- any further optimization work is then based on measured evidence instead of
  guesswork

## Measurement Loop

Every performance change in this area should be validated with the same
benchmark path so comparisons stay honest.

Recommended loop:

1. Keep local safety gates green.
   - `go test ./...`
   - `scripts/test-race-core.sh`
   - `scripts/test-benchmark-preflight.sh`
   - `scripts/test-benchmark-soak-local.sh`
2. Run the real cloud benchmark with profiling enabled.
   - `go run ./cmd/craq-bench run --profile profiles/bench/gcp_c4a_steady_pprof.yaml`
3. Compare against the current baseline:
   - read latency
   - write latency
   - throughput
   - timeout/error counts
   - pprof hotspot movement
4. Only move to the next optimization once the previous hotspot has actually
   shifted in the profiles.

## Practical Targets

These are directional goals, not hard guarantees:

- same-AZ `GET` should move much closer to `~1ms` than the current `~6ms`
- `PUT` latency should materially improve from the current `45ms` p50 headline
  and `~62-75ms` tail
- mixed-workload deadline failures should fall significantly as client and
  storage hot-path overhead comes down

## Non-Goals For The First Optimization Pass

- changing public RPCs or benchmark workflow
- speculative storage-format rewrites before rerunning profiles
- replacing real tests with mocks
- tuning around the symptoms without removing the top measured hotspots
