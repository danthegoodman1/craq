# Quickstart

This quickstart demonstrates craq on localhost with:

- one non-HA coordinator
- three initial storage nodes
- a fifth terminal running `get`, `set`, and `del`
- a late replacement node joining after one node is marked dead

This quickstart uses **in-memory coordinator and storage stores**. It is meant
to demonstrate behavior, routing, and repair flow, not persistence.

## Build

```bash
go build -o ./bin/craq-quickstart ./cmd/craq-quickstart
```

The examples below assume the checked-in manifest:

```bash
examples/quickstart/cluster.json
```

## Terminal 1: Coordinator

```bash
./bin/craq-quickstart coordinator --manifest examples/quickstart/cluster.json
```

The coordinator listens on:

- gRPC: `127.0.0.1:7400`
- admin HTTP: `127.0.0.1:7401`

## Terminals 2-4: Storage Nodes

Terminal 2:

```bash
./bin/craq-quickstart storage --manifest examples/quickstart/cluster.json --node a
```

Terminal 3:

```bash
./bin/craq-quickstart storage --manifest examples/quickstart/cluster.json --node b
```

Terminal 4:

```bash
./bin/craq-quickstart storage --manifest examples/quickstart/cluster.json --node c
```

The nodes auto-register with the coordinator and start heartbeating. The
quickstart uses one slot and replication factor three, so every key goes
through the same `a -> b -> c` chain once the cluster is ready.

You can inspect the coordinator state while it converges:

```bash
curl -s http://127.0.0.1:7401/admin/v1/state | jq '.current.Cluster.ReadyNodeIDs, .routing_snapshot.slots'
```

Once slot `0` is readable and writable, the cluster is ready.

## Terminal 5: CLI

Set a key:

```bash
./bin/craq-quickstart set --manifest examples/quickstart/cluster.json alpha one
```

Read it back:

```bash
./bin/craq-quickstart get --manifest examples/quickstart/cluster.json alpha
```

Delete it:

```bash
./bin/craq-quickstart del --manifest examples/quickstart/cluster.json alpha
```

The CLI prints:

- the operation
- the key
- the slot
- the chain version used
- the read value or commit metadata

Writes and deletes still target the active head. Reads go through the normal
CRAQ client router, so a linearizable read can use any active replica and the
CLI no longer prints a specific contacted read node.

## Failure and Replacement Walkthrough

1. In terminal 5, write a value:

```bash
./bin/craq-quickstart set --manifest examples/quickstart/cluster.json alpha one
```

2. Stop node `c` in terminal 4 with `Ctrl-C`.

3. Within a couple seconds, the coordinator should mark `c` dead. Check:

```bash
curl -s http://127.0.0.1:7401/admin/v1/state | jq '.current.Cluster.NodeHealthByID, .routing_snapshot.slots'
```

You should see:

- node `c` marked `"dead"`
- slot `0` still readable
- the temporary tail move to `b`

4. Read the key again from terminal 5:

```bash
./bin/craq-quickstart get --manifest examples/quickstart/cluster.json alpha
```

At this point the CLI should still return `value=one`.

5. Reuse terminal 4 to start a replacement node with a **new node ID**:

```bash
./bin/craq-quickstart storage --manifest examples/quickstart/cluster.json --node d
```

The replacement must be a new node ID. The current coordinator model tombstones
dead node IDs, so the dead node `c` itself does not rejoin.

The quickstart also uses aggressive coordinator liveness settings so localhost
dead-node detection happens quickly. In the main product, the same liveness
policy can also evict flapping nodes before repeated boot loops keep a chain
degraded indefinitely.

6. Watch the coordinator state:

```bash
curl -s http://127.0.0.1:7401/admin/v1/state | jq '.current.Cluster.ReadyNodeIDs, .routing_snapshot.slots'
```

You should see:

- `d` appear in `readyNodeIDs`
- the slot become writable again once the replacement is active
- the tail move from `b` to `d`

7. Read the key again:

```bash
./bin/craq-quickstart get --manifest examples/quickstart/cluster.json alpha
```

The CLI should still return `value=one`.

8. Write again:

```bash
./bin/craq-quickstart set --manifest examples/quickstart/cluster.json alpha two
```

The write should succeed once `d` is active.

## Notes

- The quickstart is intentionally insecure and non-HA.
- It uses real gRPC and the current dynamic auto-join and non-HA repair path.
- The coordinator runs aggressive liveness timing for demo purposes so dead-node
  detection happens quickly on localhost.
