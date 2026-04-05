# Observability

`craq` now includes an operability surface around the coordinator,
storage nodes, and gRPC transport:

- structured event logs with `zerolog`
- Prometheus metrics
- an optional read-only HTTP admin plane
- recent in-memory repair and lifecycle event history

This is intentionally a v1 operational surface:

- admin HTTP is read-only
- admin HTTP is unauthenticated
- admin HTTP is intended for loopback or a trusted network only
- logging is event-focused rather than request-per-request

## Logging

Components accept optional logger injection through their config:

- `storage.Config.Logger`
- `coordserver.ServerConfig.Logger`
- `transport/grpcx` TLS configs for transport-level logging

If no logger is provided, the component falls back to `gologger.NewLogger()`.

The default policy is to log state transitions and failures rather than every
successful read/write/RPC. High-signal events include:

- coordinator bootstrap, membership mutation, liveness evaluation, repair start,
  repair completion, flap detection, dispatch timeout, and dispatch failure
- storage replica add/activate/leave/remove, catch-up and recovery activity,
  ambiguous writes, conditional write failures, replication conflicts, and
  backpressure rejection
- gRPC transport auth failures and request failures
- admin HTTP listener start and shutdown

Stable structured fields used across these logs include:

- `component`
- `node_id`
- `slot`
- `chain_version`
- `sequence`
- `peer_node_id`
- `command_id`
- `error`

## Metrics

Metrics are registered per process through a Prometheus registry. Coordinator,
storage, and transport surfaces each own their own collectors and can be exposed
through the admin HTTP listener.

### Storage Metrics

Storage metrics are registered from
[storage/observability.go](./storage/observability.go):

- `craq_storage_client_reads_total`
- `craq_storage_client_writes_total`
- `craq_storage_ambiguous_writes_total`
- `craq_storage_condition_failures_total`
- `craq_storage_write_wait_seconds`
- `craq_storage_tail_resolutions_total`
- `craq_storage_tail_resolution_seconds`
- `craq_storage_read_dependency_failures_total`
- `craq_storage_replication_forwards_total`
- `craq_storage_replication_commits_total`
- `craq_storage_catchup_operations_total`
- `craq_storage_catchup_duration_seconds`
- `craq_storage_backpressure_rejections_total`
- `craq_storage_in_flight_client_writes`
- `craq_storage_buffered_replica_messages`
- `craq_storage_active_catchups`

These cover CRAQ read outcomes by consistency mode, ambiguous writes,
conditional failures, tail-resolution activity for linearizable reads,
replication protocol activity, catch-up and recovery duration, backpressure,
and current runtime resource usage.

### Coordinator Metrics

Coordinator metrics are registered from
[coordserver/observability.go](./coordserver/observability.go):

- `craq_coordserver_commands_total`
- `craq_coordserver_dispatches_total`
- `craq_coordserver_dispatch_duration_seconds`
- `craq_coordserver_liveness_evaluations_total`
- `craq_coordserver_dead_detections_total`
- `craq_coordserver_flap_detections_total`
- `craq_coordserver_repairs_total`
- `craq_coordserver_pending_work`
- `craq_coordserver_unavailable_replicas`

These cover command execution, coordinator-to-storage dispatch behavior, liveness
evaluation, flap-triggered eviction, repair activity, and current pending or
unavailable work.

### gRPC Transport Metrics

gRPC transport metrics are registered from
[transport/grpcx/observability.go](./transport/grpcx/observability.go):

- `craq_grpc_requests_total`
- `craq_grpc_request_duration_seconds`
- `craq_grpc_auth_failures_total`

These are labeled by component and gRPC method, and auth failures are split out
for `PermissionDenied` and `Unauthenticated` cases.

## Admin HTTP

The read-only admin HTTP plane lives in [adminhttp/server.go](./adminhttp/server.go).
It is separate from the gRPC transport plane.

One optional admin listener may be exposed per coordinator or storage process.

Endpoints:

- `/livez`
- `/readyz`
- `/metrics`
- `/admin/v1/state`

### `/livez`

Returns HTTP `200` with a simple JSON body when the process is alive.

### `/readyz`

Returns:

- HTTP `200` when the coordinator or storage process is opened and serving its
  control/data role
- HTTP `503` once the admin server is shutting down

This endpoint is about process/service readiness, not whether every chain is fully
healthy. Detailed serving state remains in `/admin/v1/state`.

### `/metrics`

Returns Prometheus exposition for the registry attached to the process.

### `/admin/v1/state`

Returns a stable read-only JSON snapshot.

Coordinator state currently includes:

- current runtime state and version
- routing snapshot
- pending work
- node heartbeats
- liveness state
  - including persisted suspect-transition history used for flap detection
- unavailable replicas
- last recovery reports
- recent coordinator events

Storage state currently includes:

- `storage.Node.State()`
- current resource usage:
  - in-flight client writes per node and per slot
  - buffered replica messages per node and per slot
  - dirty CRAQ keys per slot
  - active catch-ups
- recent storage events

## Repair Visibility

Both coordinator and storage nodes maintain a small bounded in-memory recent-event
ring.

Those events are surfaced in three ways:

- structured logs
- `/admin/v1/state`
- metrics for repair, timeout, ambiguous-write, auth-failure, and backpressure paths

Coordinator recent events include:

- liveness transitions
- flap suspect counting and flap-triggered eviction
- repair planning and completion
- dispatch failures and timeouts

## Liveness and Flapping

Coordinator liveness is configured through `coordserver.LivenessPolicy`.

Fields:

- `SuspectAfter`
- `DeadAfter`
- `FlapWindow`
- `FlapThreshold`

Behavior:

- ordinary liveness still follows `healthy -> suspect -> dead`
- `suspect` alone does not change routing
- a flap is counted only when a node transitions into `suspect`
- repeated evaluation ticks while already suspect do not increase the flap count
- if flap detection is enabled and the node reaches `FlapThreshold` suspect
  transitions inside `FlapWindow`, it is evicted through the normal dead-node
  repair path
- flap-evicted node IDs are tombstoned exactly like other dead nodes and are
  rejected from future auto-join

Defaults:

- `FlapWindow = 30s`
- `FlapThreshold = 3`

These defaults apply when liveness is enabled and both flap fields are left
unset. Flap detection can be disabled by setting `FlapThreshold <= 0` or
`FlapWindow <= 0`.

Storage recent events include:

- replica lifecycle transitions
- catch-up and recovery activity
- replication failures
- ambiguous writes
- backpressure rejection

## Wiring It Up

### Storage

`storage.Node` observability is configured through `storage.Config`:

```go
registry := prometheus.NewRegistry()
logger := gologger.NewLogger()

node, err := storage.OpenNode(ctx, storage.Config{
    NodeID:          "node-a",
    Logger:          &logger,
    MetricsRegistry: registry,
}, backend, localState, coordClient, repl)
```

### Coordinator

`coordserver.Server` observability is configured through
`coordserver.ServerConfig`:

```go
registry := prometheus.NewRegistry()
logger := gologger.NewLogger()

server, err := coordserver.OpenWithConfig(ctx, runtime, nodeClientFactory, coordserver.ServerConfig{
    Logger:          &logger,
    MetricsRegistry: registry,
})
```

### Admin HTTP

You can then attach an admin listener to either component:

```go
admin := adminhttp.NewStorage(node, adminhttp.Config{
    Address:  "127.0.0.1:8081",
    Gatherer: node.MetricsRegistry(),
    Logger:   &logger,
})
go admin.ListenAndServe()
```

or:

```go
admin := adminhttp.NewCoordinator(server, adminhttp.Config{
    Address:  "127.0.0.1:8080",
    Gatherer: server.MetricsRegistry(),
    Logger:   &logger,
})
go admin.ListenAndServe()
```

## Testing

Observability coverage includes:

- [storage/observability_test.go](./storage/observability_test.go)
- [transport/grpcx/observability_test.go](./transport/grpcx/observability_test.go)
- [adminhttp/server_test.go](./adminhttp/server_test.go)

Those tests exercise coordinator, storage, gRPC, TLS, metrics registries, and
HTTP admin listeners.
