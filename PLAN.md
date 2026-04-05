# CRAQ Conversion Plan

## Summary

Implement CRAQ as a first-class read path on top of the existing chain-replication write protocol, not as a separate side system. The final system will provide linearizable reads from any active replica, an explicit relaxed read mode that never exposes uncommitted data, coordinator snapshots that advertise all active read candidates, and a client router that load-balances reads across the chain while retaining the current robustness around stale routing, failures, reconfiguration, recovery, and HA.

The write path, coordinator planner, and replica lifecycle model stay intact. Writes remain head-only, commit still originates at the tail, and writes remain blocked while a `joining` replica exists. The CRAQ conversion is concentrated in storage read resolution, richer assignment/routing metadata, client read selection/fallback, transport/proto plumbing, and a broad test expansion using real in-memory, queue-based, Badger, and gRPC implementations.

## Key Changes

### Storage and Read Semantics

- Add `storage.ReadConsistency` with exactly two modes: `ReadConsistencyLinearizable` and `ReadConsistencyLocalCommitted`.
- Extend `storage.ClientGetRequest` with `Consistency ReadConsistency`; zero/default maps to `ReadConsistencyLinearizable`.
- Keep `client.Router.Get(ctx, key)` as the default linearizable read API and add `GetWithConsistency(ctx, key, consistency)` for explicit relaxed reads.
- Expand `storage.HandleClientGet` so any `active` replica role (`single`, `head`, `middle`, `tail`) is read-eligible; reads on `pending`, `catching_up`, `leaving`, `recovered`, and `removed` replicas remain rejected.
- Add per-replica CRAQ dirty-state tracking in `storage.Node`, separate from the committed backend view: maintain an ordered per-key index over contiguous dirty operations sourced from `pendingWrites` and `stagedForwards`.
- Do not treat `bufferedForwards` as readable CRAQ state; future/out-of-order buffered messages must never affect reads until they become contiguous/staged.
- Use slot sequence numbers, not `ObjectMetadata.Version`, to resolve dirty reads. This preserves current delete semantics and avoids introducing hidden lineage/tombstone contracts into the public object model.
- Linearizable read algorithm:
  - On `tail` or `single`, return the committed backend value directly.
  - On `head` or `middle`, if the requested key has no dirty contiguous operations, return the committed backend value directly.
  - If the key has dirty contiguous operations, query the actual tail for the slot’s highest committed sequence, then return the newest local dirty operation for that key whose sequence is `<= tailCommittedSequence`; if none qualify, return the committed backend value.
  - If the qualifying operation is a delete, return `Found=false` with no metadata, matching current public read semantics.
- Relaxed read algorithm:
  - Never query the tail.
  - Return the locally committed clean state only.
  - Never expose dirty/uncommitted data.
  - On `tail` and `single`, relaxed and linearizable reads are identical.
- Add a typed storage error for “linearizable read dependency unavailable” when a non-tail replica cannot resolve a dirty read against the tail; this is required so the router can fall back intelligently instead of surfacing a generic internal error.
- Extend `storage.ReplicaAssignment` / `ChainPeers` so every assignment includes the current tail target (`TailNodeID`, `TailTarget`); populate these for all active assignments and persist them anywhere assignments already round-trip.

### Coordinator, Routing, and Client

- Keep the coordinator planner and lifecycle states unchanged; no new planner states or reconfiguration phases are needed for CRAQ.
- Extend `coordserver.SlotRoute` with an ordered `ReadReplicas` list covering all active, non-unavailable replicas from head to tail; keep the existing explicit head/tail fields as canonical first/last active replicas.
- Add a small public route type for read candidates, containing exactly `NodeID`, `Endpoint`, and `Role`.
- Update `coordserver.rebuildRoutingSnapshot` so:
  - `ReadReplicas` contains only active, non-unavailable replicas.
  - `Readable` is true when `ReadReplicas` is non-empty.
  - `Writable` keeps today’s rule: true only when the slot has an active head and no `joining` replica.
- Update assignment generation in `coordserver` so every dispatched assignment carries predecessor, successor, and tail metadata for the active serving chain.
- Update recovery and HA dispatch paths to preserve the richer assignment shape everywhere `ReplicaAssignment` is persisted, replayed, retried, or compared.
- Change router read selection to use `ReadReplicas` instead of tail-only routing.
- Make the router’s default read selection policy deterministic round-robin per slot across `ReadReplicas`.
- Router read retry rules:
  - On transport-unavailable errors, try the remaining read replicas in the current snapshot before refreshing.
  - On the new typed read-dependency error, retry the tail directly from the same snapshot before refreshing.
  - On typed routing mismatch, refresh the snapshot once and retry once, preserving today’s stale-routing behavior.
  - On ordinary domain errors from a reachable replica, return immediately; do not spray requests across replicas for application-level failures.
- Leave write routing unchanged: writes and deletes still target the head only.

### Transport, Observability, and Docs

- Extend `proto/craq/v1/transport.proto` with:
  - a `ReadConsistency` enum,
  - the new `consistency` field on `ClientGetRequest`,
  - a read-replica route message for `RoutingSnapshotResponse`,
  - a new error detail for the typed read-dependency failure.
- Do not add new RPC methods; reuse the existing replication-side `FetchCommittedSequence` RPC for dirty-read tail resolution.
- Update all gRPC converters, clients, servers, error encoding/decoding, and integration tests to round-trip the new consistency and read-routing fields.
- Update in-memory client and replication transports so CRAQ behavior is exercised through the same interfaces as gRPC, not through test-only shortcuts.
- Extend storage metrics/admin state with CRAQ-specific visibility:
  - read totals partitioned by consistency mode,
  - tail-resolution query totals and latency,
  - read-dependency failures,
  - per-slot dirty-version or dirty-key counts in admin state/resource snapshots.
- Update `ARCHITECTURE.md`, `README.md`, and `OBSERVABILITY.md` to document:
  - CRAQ read semantics,
  - the two read-consistency modes,
  - the new routing snapshot shape,
  - the unchanged write-blocking behavior during `joining`,
  - the operational meaning of the new metrics.

## Test Plan

- Update storage client-handler tests so linearizable reads succeed on `head`, `middle`, `tail`, and `single` active replicas, and still fail on inactive lifecycle states.
- Add storage tests for dirty-read resolution on both `head` and `middle` replicas using real queued transports and real node instances:
  - single dirty put,
  - dirty delete,
  - put-then-put on the same key,
  - put-then-delete on the same key,
  - delete-then-recreate on the same key,
  - interleaved writes to other keys proving unrelated dirty keys do not trigger tail resolution.
- Add protocol-level tests proving buffered future forwards are never read-visible, duplicate/replayed messages do not corrupt the dirty index, and out-of-order forward/commit delivery still yields correct CRAQ reads after sequence resolution.
- Add router tests with real in-memory server/router harnesses to verify:
  - round-robin read selection across active replicas,
  - direct-tail fallback on typed read-dependency failures,
  - alternate-replica fallback on transport unavailability,
  - snapshot refresh on routing mismatch,
  - default `Get` remains linearizable,
  - `GetWithConsistency(LocalCommitted)` can return stale clean data but never dirty data.
- Update coordinator routing tests so `RoutingSnapshot` exposes ordered read replicas and continues to exclude `joining`, `leaving`, and unavailable replicas.
- Extend reconfiguration tests for join, activation, leaving, dead-tail repair, and recovered-node revalidation so CRAQ reads stay correct through each phase and the active replicas always carry the right tail metadata.
- Extend HA tests so failover preserves CRAQ-ready assignments, routing snapshots, and read correctness after outbox replay and standby takeover.
- Add Badger crash/reopen tests verifying dirty CRAQ state is not authoritative after reopen, recovered replicas remain non-serving until revalidated, and post-recovery reads are correct once the coordinator resumes or rebuilds the replica.
- Add end-to-end gRPC integration tests for:
  - linearizable reads from non-tail replicas with dirty local state,
  - relaxed reads from non-tail replicas,
  - read-dependency error encoding/decoding,
  - routing snapshot read-replica round-trip,
  - coordinator + storage + router operation across reconfiguration.
- Use real implementations in new CRAQ tests: real `storage.Node`, real in-memory/queued transports, real Badger stores, and real gRPC servers/clients. Do not add new mock-only test coverage for behavior that can be validated through the actual interfaces.

## Assumptions and Defaults

- `Router.Get` and raw storage `Get` default to `ReadConsistencyLinearizable`.
- `ReadConsistencyLocalCommitted` is the only relaxed mode in this work; it is explicitly allowed to be stale on non-tail replicas and is not required to provide read-your-writes.
- CRAQ resolution is based on slot sequence order already enforced by the protocol, not on public object metadata versions.
- Writes remain blocked while a slot contains a `joining` replica; no live-write streaming into catch-up replicas is introduced in this change.
- The coordinator planner, replica lifecycle states, and recovery orchestration remain structurally the same; the work adds richer assignment/routing data and CRAQ read behavior on top of them.
- Existing head/tail route fields remain present for clarity and write routing, but `ReadReplicas` becomes the source of truth for client read candidate selection.
