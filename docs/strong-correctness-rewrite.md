# Strong Correctness Rewrite

## Goal

This document records the target correctness model for the internal CRAQ
rewrite. The priority order is:

1. Correctness
2. Operational clarity
3. Performance, so long as it does not reintroduce unclear ownership

The external validation surface should stay stable while internals move toward
single-owner protocol state and explicit timeout semantics.

## Non-Negotiable Invariants

- Exactly one goroutine owns mutable coordinator control-plane state.
- Exactly one goroutine owns mutable protocol state for each storage slot.
- Reducers are pure: message plus state yields next state plus explicit effects.
- Persistence happens before externally visible progress is published.
- Caller timeout means the caller stopped waiting. It does not silently decide
  whether protocol work should continue.
- Long-lived protocol work must continue or cancel via an explicit state-machine
  event, not detached helper goroutines with hidden ownership.
- Reads and admin endpoints serve immutable published snapshots, never partially
  mutated live maps.
- Hot-path correctness must not depend on lock ordering.

## Ownership Model

### Coordinator

The target model is a serialized `CoordinatorEngine` that owns:

- authoritative coordinator state
- liveness state
- pending and completed work views
- routing snapshot publication
- outbox scheduling and acknowledgement bookkeeping

RPC and admin handlers should remain thin facades that submit work to the
 engine and wait for replies.

### Storage

The target model is:

- one `NodeSupervisor` goroutine for node-global state and lifecycle
- one `SlotActor` goroutine per resident slot for protocol state

The synchronous `Node` API remains the migration facade. Handlers should call
into the facade, which forwards requests to the supervisor or slot actor and
waits for a reply.

## Persistence Ordering

Storage write ordering must remain:

1. stage locally before forwarding
2. commit locally before upstream commit notification
3. publish client completion only after the commit is visible in reducer state

Coordinator ordering must remain:

1. persist runtime or snapshot mutation
2. publish updated views and routing snapshots
3. acknowledge outbox work only after the side effect succeeds

## Timeout And Cancellation Semantics

- Request context bounds how long the caller waits.
- Background coordinator work must run under an explicit server-owned lifecycle
  context so shutdown cancels retries cleanly.
- Replication wait semantics must be consistent across transports. A transport
  may provide an optimized commit waiter, but the node must still honor
  `WriteCommitTimeout` even when the transport only exposes forward and commit
  RPCs.
- Recovery commands use the recovery timeout budget. Normal dispatch uses the
  dispatch timeout budget.
- `context.Background()` is only acceptable at process roots such as signal
  handling and shutdown boundaries.

## Published State

- Routing snapshots are immutable published values.
- Admin endpoints must read published snapshots and metrics only.
- Standby HA reads must not mutate in-process coordinator state.

## Migration Sequence

### Phase 0

- Record this design in the repo.
- Preserve the current benchmark and validation workflows.
- Capture and compare baseline throughput, p95 latency, and startup behavior.

### Phase 1

- Extract pure state-machine logic from the storage protocol and coordinator
  runtime before replacing their execution models.
- Cover duplicate delivery, out-of-order delivery, replay, stale epoch
  rejection, and idempotence with deterministic reducer tests.

### Phase 2

- Rebuild storage around `NodeSupervisor` and per-slot `SlotActor`s.
- Move buffering, idempotence, and completion tracking into slot-owned state.
- Remove hot-path correctness dependencies on `slotMu`, `n.mu`, and comments
  about unlocking before RPC.

### Phase 3

- Rebuild coordinator execution around a serialized `CoordinatorEngine`.
- Eliminate correctness dependencies on background loops racing with handlers.
- Keep routing publication and outbox acknowledgement in the same serialized
  ownership flow.

### Phase 4

- Unify timeout, retry, and ambiguous-write semantics across storage,
  coordinator, transport, and benchmark code.
- Make retry ownership explicit.

### Phase 5

- Keep transport, client, admin, and benchmark entry points stable while the
  internal model changes.

### Phase 6

- Add reducer-level property tests, randomized reorder tests with real in-memory
  transports, and fuzz targets where practical.

### Phase 7

- Tune mailbox sizing, batching, snapshot caching, connection reuse, deadline
  defaults, and dispatch policy only after correctness parity is established.

### Phase 8

- Delete dead lock-based paths and migration shims.
- Update architecture, benchmarking, and HA documentation to describe the
  final owner model.

## Validation Gates

The rewrite is not complete until all of the following pass:

- `go test ./...`
- `scripts/test-race-core.sh`
- `scripts/test-benchmark-preflight.sh`
- `scripts/test-benchmark-soak-local.sh`
- local smoke benchmark profiles
- cloud-shape benchmark profile
- GCP benchmark against the accepted baseline bar

## Current Direction

The migration should continue in small, validated slices:

- reduce ownership ambiguity first
- add tests at the lowest stable layer available
- keep transport and benchmark behavior stable while internal ownership changes
- prefer deleting obsolete code over supporting dual correctness models longer
  than necessary
