# Security

`craq` now supports optional gRPC transport security.

The default transport remains insecure so local development and existing tests keep
working without extra setup. For any non-trivial deployment, enable TLS.

## Model

Uses a simple cluster-CA model:

- one CA signs coordinator and storage-node certificates
- internal coordinator/storage RPCs use mTLS
- client-facing RPCs use TLS, with optional client certificate verification
- internal authorization is role- and identity-bound, not CA-only trust

That means:

- a valid cluster-CA certificate is required to connect on internal planes
- the authenticated peer identity must also be authorized for the RPC being called
- client-facing RPCs may allow anonymous TLS clients or require client certs, depending
  on server configuration

## Identity Rules

Internal transport authorization uses one simple certificate convention:

- `Common Name` is the peer role
- first `DNS SAN` is the logical identity

For v1, the supported internal roles are:

- `CN=coordinator`
- `CN=storage`

Examples:

- coordinator cert:
  - `CN=coordinator`
  - first `DNS SAN=coordinator`
- storage node `a` cert:
  - `CN=storage`
  - first `DNS SAN=a`
- storage node `b` cert:
  - `CN=storage`
  - first `DNS SAN=b`

The first DNS SAN should remain the logical identity used by the application.

For stable certificates that do not need to change when hostnames or IPs change:

- keep the first DNS SAN as the logical identity
- set `ClientTLSConfig.ServerName` to a stable SAN value

For example:

- coordinator cert can use `DNS.1=coordinator`
- storage-node cert can use `DNS.1=a` and `DNS.2=storage`
- clients that talk to many storage nodes can use `ServerName: "storage"`

That keeps application identity stable while avoiding IP-based certificate churn.

## RPC Authorization

Internal authorization is enforced by RPC plane:

- coordinator report RPCs require `CN=storage`
- report payload `node_id` must match the authenticated storage-node logical identity
- storage control RPCs require `CN=coordinator`
- storage replication RPCs require `CN=storage`
- replication `from_node_id` must match the authenticated storage-node logical identity
- client/admin RPCs are client-facing and do not require an internal coordinator/storage identity

There is no static transport-level allowlist of storage nodes. A valid storage certificate
proves the caller is a storage node, and the RPC payload must still match that specific
certificate identity. Existing coordinator membership checks and storage peer-state checks
continue to reject unknown or unexpected node IDs.

## Configuration

The gRPC transport layer exposes optional TLS config in `transport/grpcx`.

Client-side config:

- `CAFile`
- optional `CertFile` and `KeyFile`
- optional `ServerName`

Server-side config:

- `CAFile`
- `CertFile`
- `KeyFile`
- `ClientAuth`
  - `disabled`
  - `verify-if-given`
  - `require-and-verify`

Recommended defaults:

- coordinator server: `verify-if-given`
- storage-node server: `verify-if-given`
- internal clients: mTLS with a node/coordinator certificate
- external clients: TLS with CA verification, optionally mTLS

`verify-if-given` is the practical default because one port serves both client-facing and
internal RPCs. It allows anonymous TLS clients for public RPCs while still requiring a
valid authenticated identity on internal RPCs.

## Certificate Rotation

Certificate material is loaded when the process starts.

Current behavior:

- server cert, key, and trusted CA files are read at startup
- client cert, key, and trusted CA files are read when the client connection pool is created
- changing cert or CA files on disk does not affect a running process
- rotating certs therefore requires restarting the affected coordinator, storage node, or client process

This is an intentional v1 tradeoff to keep the security implementation small and predictable.
`craq` is already designed to tolerate node restarts, so rolling restarts are the expected
rotation mechanism.

## Minimal Setup

Create a CA:

```bash
openssl req -x509 -newkey rsa:4096 -sha256 -nodes \
  -keyout ca.key \
  -out ca.crt \
  -days 365 \
  -subj "/CN=craq-cluster-ca"
```

Create a coordinator certificate config, for example `coordinator.cnf`:

```ini
[req]
distinguished_name = dn
req_extensions = req_ext
prompt = no

[dn]
CN = coordinator

[req_ext]
subjectAltName = @alt_names
extendedKeyUsage = serverAuth,clientAuth

[alt_names]
DNS.1 = coordinator
```

Issue the coordinator cert:

```bash
openssl req -new -newkey rsa:4096 -nodes \
  -keyout coordinator.key \
  -out coordinator.csr \
  -config coordinator.cnf

openssl x509 -req \
  -in coordinator.csr \
  -CA ca.crt \
  -CAkey ca.key \
  -CAcreateserial \
  -out coordinator.crt \
  -days 365 \
  -sha256 \
  -extensions req_ext \
  -extfile coordinator.cnf
```

Issue storage-node certs the same way, but with `CN=storage` and the first DNS SAN set to
the logical node ID, for example `a`, `b`, or `c`.

If you want one shared `ServerName` for all storage-node RPCs, add a second stable DNS SAN
such as `storage`.

For example, storage node `a` would use:

```ini
[req]
distinguished_name = dn
req_extensions = req_ext
prompt = no

[dn]
CN = storage

[req_ext]
subjectAltName = @alt_names
extendedKeyUsage = serverAuth,clientAuth

[alt_names]
DNS.1 = a
DNS.2 = storage
```

## Example Usage

Server:

```go
service, err := grpcx.NewStorageGRPCServerWithTLS(node, &grpcx.ServerTLSConfig{
    CAFile:     "ca.crt",
    CertFile:   "storage-a.crt",
    KeyFile:    "storage-a.key",
    ClientAuth: grpcx.ClientAuthModeVerifyIfGiven,
})
```

Internal client:

```go
pool, err := grpcx.NewConnPoolWithTLS(grpcx.ClientTLSConfig{
    CAFile:     "ca.crt",
    CertFile:   "coordinator.crt",
    KeyFile:    "coordinator.key",
    ServerName: "storage",
})
```

Storage-node internal client:

```go
pool, err := grpcx.NewConnPoolWithTLS(grpcx.ClientTLSConfig{
    CAFile:     "ca.crt",
    CertFile:   "storage-a.crt",
    KeyFile:    "storage-a.key",
    ServerName: "coordinator",
})
```

External client with TLS only:

```go
pool, err := grpcx.NewConnPoolWithTLS(grpcx.ClientTLSConfig{
    CAFile:     "ca.crt",
    ServerName: "storage",
})
```

## Testing

The transport package includes TLS integration coverage for:

- TLS-only client access
- optional client mTLS
- required-client-cert rejection
- internal authorization denial for wrong identities
- end-to-end secure coordinator/storage/router flows
- restart-based certificate handling

Run:

```bash
go test ./transport/grpcx
```
