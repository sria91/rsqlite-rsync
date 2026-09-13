# rsqlite-rsync

[![CI](https://github.com/sria91/rsqlite-rsync/actions/workflows/ci.yml/badge.svg)](https://github.com/sria91/rsqlite-rsync/actions/workflows/ci.yml)

A bandwidth-efficient SQLite database sync tool written in Rust, inspired by
the C utility [`sqlite3_rsync`](https://www.sqlite.org/rsync.html). It also
bundles an HA control loop and a gRPC SQL Gateway for running SQLite as a
single-writer cluster.

## Overview

`rsqlite-rsync` makes **REPLICA** a consistent snapshot of **ORIGIN** by
exchanging cryptographic page hashes and transferring only the pages that
differ — much like `rsync` does for ordinary files, but with full awareness
of SQLite transaction boundaries. ORIGIN may remain live while the tool
runs; REPLICA is treated as exclusive to the sync process (concurrent
writers to it are not coordinated) and ends up a consistent snapshot of
ORIGIN as it existed when the command started.

```
rsqlite-rsync [OPTIONS] ORIGIN REPLICA            # one-shot local/remote sync
rsqlite-rsync --batch-manifest PATH [OPTIONS]     # sync many databases at once
rsqlite-rsync --ha [HA OPTIONS]                   # run as a single-writer cluster node
rsqlite-rsync client <SUBCOMMAND> [OPTIONS]       # query the cluster via the SQL Gateway
rsqlite-rsync sql [OPTIONS] -d <DATABASE> "<SQL>" # shorthand for one query/statement
```

The last three forms are covered in
[HA control loop mode](#ha-control-loop-mode) and
[SQL Gateway and client](#sql-gateway-and-client).

## Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Local sync](#local-sync) · [Push to remote](#push-to-remote) · [Pull from remote](#pull-from-remote) · [Options](#options)
  - [Batch mode](#batch-mode-multiple-databases)
  - [HA control loop mode](#ha-control-loop-mode)
  - [SQL Gateway and client](#sql-gateway-and-client)
    - [Security](#security)
- [Protocol](#protocol)
- [Crate structure](#crate-structure)
- [Performance tuning](#performance-tuning-optional)
- [Running tests](#running-tests) · [Running benchmarks](#running-benchmarks)
- [Differences from `sqlite3_rsync`](#differences-from-sqlite3_rsync)
- [License](#license)

## Features

- **Live origin** — ORIGIN can continue serving reads and writes during sync.
- **Bandwidth-efficient** — two-pass hash protocol; typically transfers < 1 %
  of the database size when diffs are small.
- **Versioned hashing** — protocol v2 uses BLAKE3 for page and group hashes
  (v1 compatibility uses SHA-256).
- **Local and remote** — ORIGIN or REPLICA can be `[user@]host:path` (via SSH).
- **Interactive SSH option** — choose fast-fail non-interactive auth or
  terminal-prompted interactive auth.
- **Operationally safe** — when REPLICA is not being modified by other
  processes, sync applies origin pages directly and can be retried after a
  failed run.
- **Batch mode** — sync many origin/replica pairs from one manifest, with
  per-entry retries, timeouts, and backoff.
- **HA control loop** — single-writer lease-based orchestration (file or
  Kubernetes Lease), with readiness probes for lease/promotion state.
- **SQL Gateway** — optional embedded gRPC server exposing SQL execution
  against the cluster's current writer, with a CLI client and a standalone
  Rust client crate supporting leader discovery, automatic failover, and
  bearer-token authentication (required by default; see
  [Security](#security)).
- **Pure Rust** — built on [`libsqlite3-sys`](https://crates.io/crates/libsqlite3-sys).

## Installation

Not yet published to crates.io — build from source:

```bash
git clone https://github.com/sria91/rsqlite-rsync.git
cd rsqlite-rsync
cargo install --path .
```

For remote sync, install the binary on both the local and remote machine and
ensure it is on the `$PATH` used by SSH (e.g. `/usr/local/bin`).

## Usage

### Local sync

```bash
rsqlite-rsync origin.db replica.db
```

### Push to remote

```bash
rsqlite-rsync origin.db user@server:/data/replica.db
```

### Pull from remote

```bash
rsqlite-rsync user@server:/data/origin.db replica.db
```

### Options

| Flag | Description |
|------|-------------|
| `-v, --verbose` | Show pages synced and bytes transferred |
| `-n, --dry-run` | Compute diff but do not write REPLICA |
| `--exe PATH` | Path to `rsqlite-rsync` on the remote machine |
| `--ssh-opt OPT` | Extra argument passed to `ssh` (repeatable) |
| `--ssh-auth <non-interactive\|interactive>` | SSH auth mode (default: `non-interactive`) |
| `--ssh-connect-timeout SECONDS` | SSH connect timeout (default: `10`) |

Notes:

- At least one endpoint must be local (remote-to-remote sync is not supported).
- `--dry-run` performs compatibility checks and reports origin size, but does
  not launch remote protocol sessions.

### Internal server modes

The binary also exposes hidden flags used by the SSH transport:

- `--server`
- `--server-origin`
- `--server-replica`

These are internal implementation details and are not intended for direct use.

### Batch mode (multiple databases)

Use `--batch-manifest` to run multiple origin/replica syncs in one invocation.

Example:

```bash
rsqlite-rsync --batch-manifest batch-sync.json --batch-jobs 4
```

#### Flags

- `--batch-manifest PATH` (required for batch mode)
- `--batch-format <auto|json|yaml|toml>` (default: `auto`)
- `--batch-jobs N` (default: `1`, must be greater than `0`)
- `--batch-retries N` (default: `0`; applies per entry attempt loop)
- `--batch-timeout-secs SECONDS` (default: `0`, disabled when `0`)
- `--batch-retry-backoff-ms MS` (default: `0`, disabled when `0`)
- `--batch-retry-backoff-max-ms MS` (default: `0`, no cap when `0`)
- `--batch-retry-jitter-pct PCT` (default: `0`, valid range `0..=100`)

#### Manifest schema

JSON:

```json
{
  "version": 1,
  "syncs": [
    {
      "name": "users-db",
      "origin": "/data/origin-users.db",
      "replica": "/data/replica-users.db"
    },
    {
      "name": "events-db",
      "origin": "db-primary:/var/lib/sqlite/events.db",
      "replica": "/var/lib/sqlite/events.db",
      "dry_run": false,
      "retries": 2,
      "timeout_secs": 20,
      "retry_backoff_ms": 100,
      "retry_backoff_max_ms": 1000,
      "retry_jitter_pct": 15
    }
  ]
}
```

YAML:

```yaml
version: 1
syncs:
  - name: users-db
    origin: /data/origin-users.db
    replica: /data/replica-users.db
    retries: 1
  - name: events-db
    origin: db-primary:/var/lib/sqlite/events.db
    replica: /var/lib/sqlite/events.db
    timeout_secs: 20
    retry_backoff_ms: 100
    retry_backoff_max_ms: 1000
    retry_jitter_pct: 15
```

TOML:

```toml
version = 1

[[syncs]]
name = "users-db"
origin = "/data/origin-users.db"
replica = "/data/replica-users.db"
retries = 1

[[syncs]]
name = "events-db"
origin = "db-primary:/var/lib/sqlite/events.db"
replica = "/var/lib/sqlite/events.db"
timeout_secs = 20
retry_backoff_ms = 100
retry_backoff_max_ms = 1000
retry_jitter_pct = 15
```

#### Semantics

- Best-effort: all entries are attempted, even if some fail.
- Exit code: non-zero when any entry fails.
- Summary: stderr includes total/succeeded/failed and each failed entry.
- Retries: effective attempts per entry are `1 + retries`.
- Timeout: may be set globally (`--batch-timeout-secs`) or per entry (`timeout_secs`).
- Backoff: retries can use exponential delay starting at `--batch-retry-backoff-ms`.
- Jitter: optional +/- percentage spread on retry delay via `--batch-retry-jitter-pct`.
- Overrides: per-entry values (for retries, timeout, backoff, jitter) take precedence over global CLI defaults.

#### Option precedence

| Setting | Global CLI default | Per-entry manifest key | Effective value |
|------|------|------|------|
| Retries | `--batch-retries` | `retries` | Entry value if set, else CLI default |
| Timeout | `--batch-timeout-secs` | `timeout_secs` | Entry value if set, else CLI default |
| Backoff base | `--batch-retry-backoff-ms` | `retry_backoff_ms` | Entry value if set, else CLI default |
| Backoff max | `--batch-retry-backoff-max-ms` | `retry_backoff_max_ms` | Entry value if set, else CLI default |
| Jitter % | `--batch-retry-jitter-pct` | `retry_jitter_pct` | Entry value if set, else CLI default |

### HA control loop mode

`rsqlite-rsync` also includes an HA control loop mode for single-writer
orchestration. In this mode, the binary continuously evaluates lease/freshness
inputs and writes role state and action audit outputs.

Example:

```bash
rsqlite-rsync --ha \
  --ha-node-id node-a \
  --ha-lease-file /var/run/rsqlite-rsync/lease.txt \
  --ha-freshness-file /var/run/rsqlite-rsync/freshness.txt \
  --ha-role-state-file /var/run/rsqlite-rsync/role_state.txt \
  --ha-audit-log-file /var/log/rsqlite-rsync/ha_audit.log \
  --ha-tick-interval-ms 1000 \
  --ha-min-source-generation 0 \
  --ha-max-freshness-age-secs 10 \
  --ha-max-future-skew-secs 2
```

#### Required flags

- `--ha-node-id`
- `--ha-role-state-file`
- `--ha-audit-log-file`

#### Lease source flags

- `--ha-lease-source` (`file` or `kubernetes`, default: `file`)
- For `file`: `--ha-lease-file`
- For `kubernetes`: `--ha-kube-lease-name`, plus:
  - `--ha-kube-namespace` (default: `default`)
  - `--ha-kube-context`
  - `--ha-kubeconfig`
  - `--ha-kubectl-path` (default: `kubectl`)

#### Optional flags

- `--ha-freshness-file`
- `--ha-readiness-file`
- `--ha-readiness-http-bind` (for example, `127.0.0.1:8088`)
- `--ha-tick-interval-ms` (minimum enforced tick: `50ms`)
- `--ha-min-source-generation`
- `--ha-max-freshness-age-secs`
- `--ha-max-future-skew-secs`
- `--ha-continue-on-error`
- `--ha-startup-fence-mode` (`permissive` or `require-writer`)

#### SQL Gateway flags

See [SQL Gateway and client](#sql-gateway-and-client) for how these are used.

- `--ha-grpc-bind` (for example, `0.0.0.0:50051`) — starts the gateway
- `--ha-grpc-auth-token` (env `RSQLITE_GRPC_AUTH_TOKEN`) — bearer token every
  gRPC request must present as `authorization: Bearer <token>`. **Required**
  whenever `--ha-grpc-bind` is set — the gateway executes arbitrary SQL
  (including `DropDatabase`) for any caller that reaches it, so it refuses to
  start unauthenticated by accident. Pass `--ha-grpc-insecure-no-auth`
  instead to explicitly opt out (local development, or a deployment that
  already authenticates callers via a trusted-network/mTLS boundary in front
  of the gateway).
- `--ha-data-dir` — directory of `.db` files the gateway serves (default `.`)
- `--ha-allow-replica-reads` — permit eventual-consistency reads on replicas
- `--ha-service-name` (default: `sqlite-ha`) — headless service name used to
  build the advertised leader endpoint
- `--ha-grpc-port` (default: `50051`) — gRPC port used in the advertised
  leader endpoint

#### File formats

Lease file format (`--ha-lease-file`):

```text
holder_node_id=node-a
generation=12
renewed_at_secs=1731000100
ttl_secs=15
```

- Use `none` (or an empty file) to represent no active lease.

Kubernetes Lease mapping (`--ha-lease-source kubernetes`):

- `spec.holderIdentity` -> `holder_node_id`
- `metadata.annotations["rsqlite-rsync.dev/generation"]` -> `generation`
- `spec.renewTime` -> `renewed_at_secs`
- `spec.leaseDurationSeconds` -> `ttl_secs`

Freshness file format (`--ha-freshness-file`):

```text
source_node_id=node-a
source_generation=12
synced_at_secs=1731000099
```

- Use `none` (or an empty file) to represent unknown freshness.
- Invalid lease or freshness content is treated as an error for that tick.

#### Behavior notes

- When `--ha-readiness-file` is set, readiness is written as `ready` for writer
  mode and `not-ready` otherwise.
- When `--ha-readiness-http-bind` is set, the endpoint returns HTTP `200` with
  `ready` on `/ready` while writer-active and HTTP `503` with `not-ready`
  otherwise. The `/live` endpoint always returns HTTP `200` with `live` while
  the process is running. `/healthz` returns HTTP `200` once the HA
  reconcile loop has completed at least one tick, regardless of writer or
  replica outcome — **use `/healthz`, not `/ready`, as the Kubernetes
  `readinessProbe` target**: since `/ready` only ever passes for the single
  current writer, gating Pod readiness (and therefore StatefulSet rollout
  progression) on it stalls any rolling update after the first pod.
- When `--ha-startup-fence-mode=require-writer`, process startup fails unless
  the first HA tick can promote/confirm writer state.
- Stop the loop cleanly with `Ctrl-C`.

#### Kubernetes deployment references

- [docs/ha-kubernetes.md](docs/ha-kubernetes.md)
- [docs/k3s-ha-runbook.md](docs/k3s-ha-runbook.md)
- [examples/k8s/ha-deployment.yaml](examples/k8s/ha-deployment.yaml) (lease-manager RBAC)
- [examples/k8s/ha-deployment-readonly.yaml](examples/k8s/ha-deployment-readonly.yaml) (read-only RBAC)
- [examples/k8s/k3s-ha-stack.yaml](examples/k8s/k3s-ha-stack.yaml) (k3s-oriented StatefulSet + lease updater + replica-sync contract)
- [examples/k8s/local-dev-file-lease.yaml](examples/k8s/local-dev-file-lease.yaml) (single-node local testing, file-based lease, no Kubernetes election required)
- [examples/k8s/client-pod.yaml](examples/k8s/client-pod.yaml) (debug/test client pod, pre-wired with cluster endpoints and the gRPC auth token)
- [scripts/apply-k3s-ha-stack.sh](scripts/apply-k3s-ha-stack.sh) (one-command apply with required image and replica sync command env vars)

### SQL Gateway and client

When `--ha-grpc-bind` is set on an `--ha` node, the process also serves a
gRPC **SQL Gateway** (`SqlGateway` service: `Execute`, `Query`, `StreamQuery`,
`Batch`, `DropDatabase`, `GetClusterStatus` — see
[`crates/rsqlite-rsync-proto`](crates/rsqlite-rsync-proto)) over the
`--ha-data-dir` directory of SQLite databases. Writes and strong-consistency
reads are only served by the current writer; a non-writer responds with a
`NOT_LEADER` status carrying the current leader's endpoint so clients can
redirect.

**Every RPC requires authentication by default** (see `--ha-grpc-auth-token`
above) — writer fencing controls *when* writes are accepted, not *who* is
allowed to call the gateway at all.

```bash
rsqlite-rsync --ha --ha-node-id node-a \
  --ha-lease-file lease.txt --ha-role-state-file role_state.txt \
  --ha-audit-log-file audit.log \
  --ha-grpc-bind 0.0.0.0:50051 --ha-grpc-auth-token "$RSQLITE_TOKEN" \
  --ha-data-dir ./data
```

Query it with the built-in `client`/`sql` CLI:

```bash
rsqlite-rsync sql --endpoint http://127.0.0.1:50051 --token "$RSQLITE_TOKEN" \
  -d app.db "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)"

rsqlite-rsync client --endpoint http://127.0.0.1:50051 --token "$RSQLITE_TOKEN" \
  exec -d app.db "INSERT INTO items (label) VALUES ('widget')"

rsqlite-rsync client --endpoint http://127.0.0.1:50051 --token "$RSQLITE_TOKEN" \
  query -d app.db "SELECT * FROM items" --format json

rsqlite-rsync client --endpoint http://127.0.0.1:50051 --token "$RSQLITE_TOKEN" status
rsqlite-rsync client --endpoint http://127.0.0.1:50051 --token "$RSQLITE_TOKEN" repl -d app.db
```

`--token` also reads from `RSQLITE_TOKEN`, so it can be left off the command
line entirely once that's set in the environment.

`client` subcommands:

| Subcommand | Purpose |
|------------|---------|
| `exec` | Run a write statement (INSERT/UPDATE/DELETE/DDL) |
| `query` | Run a read query and print the results |
| `batch` | Run multiple statements from `--sql` or `--file`, optionally in one transaction (`--tx-mode`) |
| `status` | Show cluster status (role, lease, generation, known databases) |
| `repl` | Interactive SQL shell (`.tables`, `.schema`, `.mode`, `.database`, ...) |
| `drop-database` | Delete a database file and its WAL/SHM/journal sidecars |

Connection flags (shared by `client` and `sql`): `--endpoint`, `--endpoints`
(comma-separated candidates, probed for the current writer), `--kube-lease` /
`--kube-namespace` / `--kube-service` (discover the writer via a Kubernetes
Lease), `--token` (env `RSQLITE_TOKEN`; required when the gateway enforces
authentication), `--max-retries`, `--timeout`. Output formatting: `--format
<table|json|csv|tsv|raw>`.

For embedding in another Rust service, the client is also published as a
standalone crate,
[`rsqlite-rsync-client`](crates/rsqlite-rsync-client), with no SQLite, HA, or CLI
dependencies:

```rust
use rsqlite_rsync_client::{ClientConfig, DiscoveryMode, SqlGatewayClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = SqlGatewayClient::new(
        ClientConfig::new(DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()))
            .with_auth_token(std::env::var("RSQLITE_TOKEN")?),
    );
    client.execute("app.db", "CREATE TABLE t (id INTEGER PRIMARY KEY)", None).await?;
    let rows = client.query("app.db", "SELECT * FROM t", None, 0, Default::default()).await?;
    Ok(())
}
```

It retries on `NOT_LEADER` by following the redirect endpoint, and supports
`DiscoveryMode::Candidates` (probe a fixed endpoint list for the writer) or a
pluggable `DiscoveryMode::Custom` resolver (used by the CLI's Kubernetes Lease
discovery). `with_auth_token` is only needed when the target gateway requires
authentication (the default — see [SQL Gateway and
client](#sql-gateway-and-client)); omit it to send no `authorization` header.

`--endpoint`/`--endpoints`/`--kube-lease` above work against any deployment,
independent of which example manifest you used. The
[`examples/k8s/k3s-ha-stack.yaml`](examples/k8s/k3s-ha-stack.yaml) reference
deployment specifically also provisions a `sqlite-ha-writer` Service as an
additional convenience — a label-updater sidecar tags the current writer's
pod so the Service's selector finds it — but it's a routing hint, not a
substitute for the mechanisms above: see [docs/k3s-ha-runbook.md](docs/k3s-ha-runbook.md#architecture)
for why, and note it isn't present in the other example manifests.

### Security

- **Authentication**: the gRPC SQL Gateway requires a bearer token on every
  request unless the operator explicitly opts out with
  `--ha-grpc-insecure-no-auth`. Writer fencing (which node accepts writes) is
  a *separate*, correctness-only mechanism — it does not authenticate
  callers, so auth is not optional by default.
- **Transport encryption**: the gateway does not terminate TLS itself.
  `--ha-grpc-bind`/`--endpoint` traffic (including the bearer token) is
  plaintext on the wire. For anything beyond local development, put the
  gateway behind a trusted network boundary, a reverse proxy, or a service
  mesh sidecar that terminates TLS/mTLS in front of it.

## Protocol

See [`docs/protocol.md`](docs/protocol.md) for the full message grammar and
state-machine description.

At a high level:

1. **Handshake** — version and page-size negotiation.
2. **Coarse pass** — replica sends protocol-version-negotiated hashes of
   64-page groups (BLAKE3 in v2, SHA-256 in v1); origin identifies changed
   groups.
3. **Fine pass** — per-page hashes exchanged for changed groups; only
   diverging page bytes are transferred.
4. **Done** — origin signals completion.

Current wire protocol version is `2`.

## Crate structure

This is a Cargo workspace. The main package (`rsqlite-rsync`, binary +
`rsqlite_rsync` library) contains:

| Module | Purpose |
|--------|---------|
| `db` | Safe FFI wrappers around `libsqlite3-sys` |
| `endpoint` | Parsing of local vs. `[user@]host:path` endpoints |
| `hash` | Page and page-group hashing (v2: BLAKE3, v1: SHA-256) |
| `protocol` | Wire messages, origin and replica state machines |
| `transport` | Pluggable I/O: in-process (`local`), stdio framing, or SSH subprocess |
| `snapshot` | Read-consistent snapshot via `BEGIN DEFERRED` |
| `ha` | Lease-based single-writer control loop (file and Kubernetes lease sources) |
| `gateway` | Embedded gRPC SQL Gateway (`DatabaseEngine`, `SqlGatewayServer`) |
| `error` | Unified `SyncError` type |
| `client` (re-export) | gRPC client with leader discovery/failover, from `rsqlite-rsync-client` |
| `proto` (re-export) | Generated gRPC types, from `rsqlite-rsync-proto` |

Workspace members:

| Crate | Purpose |
|-------|---------|
| [`crates/rsqlite-rsync-proto`](crates/rsqlite-rsync-proto) | Protobuf/tonic-generated `SqlGateway` service types, compiled from `proto/rsqlite/v1/sqlite.proto` |
| [`crates/rsqlite-rsync-client`](crates/rsqlite-rsync-client) | Standalone async gRPC client for the SQL Gateway, usable without depending on this crate |

## Performance tuning (optional)

Runtime hashing behavior can be tuned with environment variables:

- `RSQLITE_RSYNC_MAX_HASH_THREADS` — cap rayon worker threads for hash-heavy
  stages.
- `RSQLITE_RSYNC_PARALLEL_MIN_PAGES` — minimum page count before switching to
  parallel hashing.
- `RSQLITE_RSYNC_HASH_CHUNK_GROUPS` — number of coarse hash groups processed
  per chunk.

## Running tests

```bash
cargo test
```

Run focused suites:

```bash
cargo test --bin rsqlite-rsync
cargo test --test ha_mode
cargo test --test grpc_gateway
cargo test --test grpc_failover
cargo test -p rsqlite-rsync-client
```

Run Kubernetes manifest validator tests:

```bash
bash tests/integration/validate_k8s_manifests.sh
```

## Running benchmarks

```bash
cargo bench
```

## Differences from `sqlite3_rsync`

| Feature | `sqlite3_rsync` | `rsqlite-rsync` |
|---------|-----------------|-----------------|
| Language | C | Rust |
| SSH transport | built-in | `ssh` subprocess |
| Protocol hashing | SHA-256 | v2: BLAKE3 (v1: SHA-256) |
| Protocol versioning | n/a | negotiated (current: v2) |
| WAL requirement | removed in 3.50.0 | no requirement |
| Async I/O | no | tokio |

## License

MIT OR Apache-2.0
