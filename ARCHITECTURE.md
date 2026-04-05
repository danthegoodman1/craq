# Architecture

This repository currently has two main subsystems:

- `coordinator`
- `storage`

They are intentionally separated so placement and reconfiguration logic stay independent from replica execution and networking.

## Roles

### Coordinator

The coordinator is the control-plane planner.

It is responsible for:

- initial chain placement
- deterministic reconfiguration planning
- tracking desired chain membership and replica ordering
- registering storage nodes into cluster membership
- tracking which registered nodes have actually heartbeated and are eligible for fresh placement
- deciding when replicas should join, become active, leave, or be removed
- durably retrying non-HA storage-node control work until it is acknowledged

It is not responsible for:

- copying data between replicas
- serving client requests
- executing chain replication I/O

The key packages are:

- `coordinator`: pure planning and state transitions
- `coordinator/runtime`: optional durable wrapper with WAL, checkpoints, replay, and idempotent command handling
- `coordserver`: synchronous coordinator service that dispatches storage-node commands, durably tracks node liveness from heartbeats, automatically marks dead nodes, and exposes routing snapshots to clients

In non-HA mode, coordinator control work is still durable:

- reconfiguration state changes and storage-node control commands are persisted through the local coordinator runtime store
- failed control RPC dispatch does not lose work
- coordinator restart reopens the local outbox and resumes undispatched work

Node membership is now dynamic by default:

- coordinator bootstrap is config-only; it does not need an initial static node list
- storage nodes register themselves with the coordinator
- a registered node only becomes eligible for fresh placement after its first successful heartbeat
- node IDs marked dead are tombstoned and rejected from future auto-join; replacements must use a new node ID

Coordinator liveness also supports flapping-node eviction:

- liveness policy is configured through `coordserver.LivenessPolicy`
- `SuspectAfter` and `DeadAfter` still control the normal `healthy -> suspect -> dead` path
- `FlapWindow` and `FlapThreshold` optionally evict a node that repeatedly transitions into `suspect`
- defaults are `30s` and `3` when liveness is enabled and flap values are left unset
- flap detection can be disabled by setting `FlapThreshold <= 0` or `FlapWindow <= 0`
- a flap-evicted node follows the same permanent tombstone path as an ordinary dead node

### Coordinator HA

The repository now also supports an active/standby coordinator mode.

- leadership is represented by a monotonic `epoch`
- one coordinator instance at a time holds the active lease for the current epoch
- authoritative coordinator mutations are persisted through a shared HA store and are conditional on that epoch still being current
- storage-node control commands also carry the coordinator epoch, and storage nodes persist the highest accepted epoch so stale control work is rejected even after restart
- clients and storage nodes fail over across a configured list of coordinator RPC endpoints; non-leaders return typed `not leader` errors rather than proxying

The shared HA store owns:

- the current coordinator materialized state
- the current lease holder and epoch
- pending coordinator progress expectations
- a durable outbox of storage-node control commands

This lets a newly elected coordinator resume undispatched or unacknowledged control work deterministically after failover.

The HA store contract and implementation guidance are documented in
[HA_STORE.md](/Users/dangoodman/code/chainrep/HA_STORE.md).

### Storage

The storage subsystem is the execution side.

It is responsible for:

- hosting many slot replicas on one physical node
- creating and deleting local replicas
- fetching and installing snapshots for joining replicas
- executing steady-state chain replication writes between active replicas
- serving direct client requests when the local replica is the correct active head or tail
- tracking local replica lifecycle state
- reporting readiness, removal, and heartbeat information back to the coordinator
- reopening persisted replica metadata and committed state after restart
- reporting recovered local inventory to the coordinator for explicit revalidation
- assigning per-object system metadata at the head and preserving it through replication and catch-up

It is not responsible for:

- deciding global placement
- deciding which chain should change next

## Boundary Between Them

The coordinator computes desired changes.

The storage node executes those changes.

The intended connection is coordinator-driven:

1. coordinator/runtime accepts a durable command
2. storage nodes register themselves with the coordinator and become eligible for fresh placement after first heartbeat
3. coordinator computes the next safe chain step
4. some coordinator server layer turns that step into durable storage-node control outbox work
5. storage-node commands are retried until they are acknowledged
6. storage nodes execute the commands locally
7. storage nodes report progress back to the coordinator
8. coordinator applies that progress and plans the next safe step
9. clients periodically fetch routing snapshots from the coordinator server and talk directly to storage nodes

## Important Interfaces

### In `storage`

`Backend`

- local per-node storage backend
- owns per-slot replica data
- current implementation is in-memory KV
- later this can be replaced with a durable backend without changing node lifecycle logic
- backend atomicity is only required within a slot; cross-slot atomic mutations are intentionally out of scope
- valid implementations may use one DB per node or one DB per slot as long as the per-slot contract is satisfied

Methods:

- `CreateReplica`
- `DeleteReplica`
- `Snapshot`
- `InstallSnapshot`
- `SetHighestCommittedSequence`
- `StagePut`
- `StageDelete`
- `CommitSequence`
- `GetCommitted`

`CoordinatorClient`

- control-plane callback interface from a storage node back to the coordinator side

Methods:

- `RegisterNode`
- `ReportReplicaReady`
- `ReportReplicaRemoved`
- `ReportNodeRecovered`
- `ReportNodeHeartbeat`

`LocalStateStore`

- local metadata persistence for which replicas a physical node believes it hosts
- distinct from `Backend`, which owns the committed replica data itself

`storage.Node` is also where runtime resource controls live. Backpressure for client writes, buffered replica protocol messages, and catch-up concurrency is enforced at the node/process layer rather than by the backend, so custom backends do not need to provide node-wide admission or cross-slot coordination semantics.
- current implementation is an in-memory restart-capable reference store

For the durable Badger-backed reference implementation, the crash-consistency contract is:

- after crash/reopen, visible durable state is equivalent to a prefix of successful completed public backend/local-store operations
- completed `CreateReplica`, `DeleteReplica`, `InstallSnapshot`, `SetHighestCommittedSequence`, `CommitSequence`, `UpsertReplica`, and `DeleteReplica(node,slot)` operations are durable and not torn at their public API boundary
- committed data, highest committed sequence, and persisted local replica metadata are authoritative after reopen
- staged but uncommitted replica operations are not authoritative crash-recovery state; the Badger reference backend removes staged remnants on reopen before the store is exposed

Methods:

- `LoadNode`
- `UpsertReplica`
- `DeleteReplica`

`ReplicationTransport`

- node-to-node data-plane interface used for catch-up and steady-state replication

Methods:

- `FetchSnapshot`
- `FetchCommittedSequence`
- `ForwardWrite`
- `CommitWrite`

### In `coordserver`

`RoutingSnapshot`

- read-only snapshot of current slot routes for clients
- exposes per-slot chain version plus the current active head and tail
- includes concrete head/tail RPC endpoints plus the ordered active CRAQ read candidates for that slot
- marks a slot non-writable while a `joining` replica exists, because the current implementation does not stream live writes into catching-up replicas

### In `client`

`Router`

- caches a coordinator routing snapshot
- hashes keys into dense logical slots
- routes writes to the active head and load-balances reads across the active CRAQ read replicas
- defaults `Get` to a linearizable CRAQ read and also exposes a local-committed relaxed read mode
- refreshes once on a typed stale-routing error from storage and retries once

## Current Storage Node Model

The public storage abstraction is a physical node service, not a single replica process.

One `storage.Node` owns:

- one physical node identity
- one backend
- one coordinator client
- one replication transport
- many hosted slot replicas

Each hosted replica has:

- a slot
- chain metadata
- predecessor/successor information, including the transport target distributed by the coordinator
- a local lifecycle state

Committed objects carry system-managed metadata:

- `version`: per-object monotonic version within the current object lifetime
- `created_at`
- `updated_at`

Client writes may include conditional predicates evaluated against the current committed object at the head:

- `exists == true/false`
- `version` with `eq/lt/lte/gt/gte`
- `updated_at` with `eq/lt/lte/gt/gte`

All supplied conditions are combined with `AND`. Failed conditions are rejected before sequence allocation or replication. Delete removes the object from normal reads and does not retain hidden lineage, so conditional fencing is not preserved across delete and recreate in v1.

Current replica lifecycle states:

- `pending`
- `catching_up`
- `active`
- `leaving`
- `recovered`
- `removed`

## Current Integration Flow

The main coordinator/storage/client interactions are:

- tail add / join
- graceful drain / removal
- heartbeats and liveness
- restart / recovery
- client routing

### Tail add / join

1. coordinator decides a node should join a chain as a new tail
2. coordinator-facing server sends `AddReplicaAsTail` to that storage node
3. storage node creates the local replica
4. storage node fetches a snapshot from its predecessor through `ReplicationTransport`
5. storage node installs the snapshot into `Backend`
6. storage node enters `catching_up`
7. coordinator later sends `ActivateReplica`
8. storage node marks it active and calls `ReportReplicaReady`

### Graceful drain / removal

1. coordinator gets a replacement tail active
2. coordinator sends `MarkReplicaLeaving` to the old replica owner
3. coordinator later sends `RemoveReplica`
4. storage node deletes local data and calls `ReportReplicaRemoved`

### Heartbeats

Storage nodes can summarize local lifecycle state through `ReportNodeHeartbeat`.

The coordinator server persists these observations, tracks `healthy -> suspect -> dead` liveness state, and automatically emits `MarkNodeDead` when a node crosses the dead timeout.

`suspect` is non-disruptive in the current implementation: routing does not change until the node is actually marked dead and the normal coordinator repair planner updates membership.

To prevent a boot-looping or repeatedly unstable node from keeping a chain degraded forever, the coordinator can also evict flapping nodes. A flap is counted when a node transitions into `suspect` from a non-suspect state. If enough suspect transitions happen within `FlapWindow`, the coordinator immediately treats that node as dead and reuses the normal dead-node repair path. Flap history is persisted in coordinator state, so restart or HA failover does not forgive a nearly-evicted node.

### Restart / recovery

1. a restarted storage node reopens its `LocalStateStore` plus committed `Backend` data
2. every reopened replica starts as local `recovered` and is non-serving
3. the node sends `ReportNodeRecovered` with its recovered inventory
4. the coordinator server compares that inventory against current coordinator state
5. if a recovered replica exactly matches the current assignment and still has committed data, the coordinator sends `ResumeRecoveredReplica`
6. otherwise, if the node should still host that slot, the coordinator sends `RecoverReplica` so the node rebuilds in place from a peer
7. stale recovered local replicas that are no longer assigned are removed through `DropRecoveredReplica`
8. routing snapshots exclude slots with unrevalidated recovered replicas, so clients do not route through restarted nodes until revalidation completes

### Client routing

1. a client fetches a `RoutingSnapshot` from `coordserver`
2. the client hashes the key to a logical slot
3. writes go to the active head for that slot and reads can go to any active CRAQ read replica
4. storage nodes reject stale slot ownership, stale chain versions, wrong roles, and inactive replicas with a typed routing-mismatch error
5. the client refreshes the routing snapshot once and retries once

CRAQ read modes are:

- `linearizable`: any active replica may serve the read; non-tail replicas consult the tail's committed sequence only when the key is locally dirty
- `local_committed`: any active replica returns only its locally committed clean state and never consults the tail

Writes are intentionally blocked while a slot contains a `joining` replica. Reads continue from the active read replicas during that phase.

If a client write times out or is canceled after entering the storage write path, the storage node returns a typed ambiguous-write error instead of claiming success or rollback. The client does not retry automatically in that case; it must reconcile by reading the slot again with a linearizable CRAQ read. Retrying the same logical write is treated as a new write, not a replay-safe retry.

## Current Network Transport

The transport layer has both gRPC and in-memory implementations.

- one coordinator gRPC server hosts routing snapshot, admin membership RPCs, liveness evaluation, and storage-node progress/report callbacks
- one storage-node gRPC server hosts client data RPCs, coordinator-issued lifecycle commands, and replica replication RPCs
- storage-node RPC endpoints are part of coordinator node metadata and are treated as the transport target for that node
- routing snapshots expose concrete head/tail endpoints directly
- catch-up snapshot transfer uses streaming gRPC rather than one large unary response
- typed storage-domain errors such as routing mismatch, ambiguous write, and backpressure are preserved across gRPC boundaries with structured status details
- coordinator admin and reporter clients support endpoint-list failover and retry on typed `not leader` responses or transport unavailability
- transport security is optional in v1:
  - default zero-config transport remains insecure for local development
  - internal coordinator/storage RPCs can use mTLS with a shared cluster CA
  - client-facing RPCs can use TLS only or TLS with optional client cert verification
  - internal authorization is identity-bound by RPC plane rather than trusting any CA-signed peer for every internal action

## Observability and Ops

The system exposes an operability surface alongside the storage and transport runtime.

- logging uses `zerolog` through the local `gologger` package
- logging is event-focused rather than per-request by default
- coordinator, storage, and gRPC transport layers expose Prometheus metrics through
  per-process registries
- coordinator and storage processes can each expose a separate optional read-only HTTP
  admin listener

The admin HTTP plane exposes:

- `/livez`
- `/readyz`
- `/metrics`
- `/admin/v1/state`

`/admin/v1/state` is intentionally read-only JSON. Coordinator state includes current
runtime version, routing snapshot, pending work, heartbeats, liveness records,
unavailable replicas, recovery reports, and recent repair events. Storage state
includes `Node.State()`, current node resource usage, and recent replica/recovery or
backpressure events.

The admin plane is intentionally unauthenticated in v1 and should be bound to loopback
or another trusted network boundary.

## Current Shape

So the current shape is:

- coordinator decides
- storage executes
- clients route using coordinator snapshots
- the same interfaces can run either entirely in memory or over gRPC depending on the harness or deployment
