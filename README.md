# rsqlite-rsync

[![CI](https://github.com/sria91/rsqlite-rsync/actions/workflows/ci.yml/badge.svg)](https://github.com/sria91/rsqlite-rsync/actions/workflows/ci.yml)

A bandwidth-efficient SQLite database synchronisation tool written in Rust,
inspired by the C utility
[`sqlite3_rsync`](https://www.sqlite.org/rsync.html).

## Overview

`rsqlite-rsync` makes **REPLICA** a consistent snapshot of **ORIGIN** by
exchanging cryptographic page hashes and transferring only the pages that
differ — much like `rsync` does for ordinary files, but with full awareness
of SQLite transaction boundaries.

```
rsqlite-rsync [OPTIONS] ORIGIN REPLICA
rsqlite-rsync --ha [HA OPTIONS]
rsqlite-rsync --batch-manifest PATH [BATCH OPTIONS]
```

ORIGIN may remain live while the tool runs. REPLICA should be treated as
exclusive to the sync process; concurrent writers are not coordinated. When
run in isolation, REPLICA ends up as a consistent snapshot of ORIGIN as it
existed when the command started.

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
- **Pure Rust** — built on [`libsqlite3-sys`](https://crates.io/crates/libsqlite3-sys).

## Installation

```bash
cargo install rsqlite-rsync
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

Batch options:

- `--batch-manifest PATH` (required for batch mode)
- `--batch-format <auto|json|yaml|toml>` (default: `auto`)
- `--batch-jobs N` (default: `1`, must be greater than `0`)
- `--batch-retries N` (default: `0`; applies per entry attempt loop)
- `--batch-timeout-secs SECONDS` (default: `0`, disabled when `0`)
- `--batch-retry-backoff-ms MS` (default: `0`, disabled when `0`)
- `--batch-retry-backoff-max-ms MS` (default: `0`, no cap when `0`)
- `--batch-retry-jitter-pct PCT` (default: `0`, valid range `0..=100`)

Manifest schema (JSON):

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

Manifest example (YAML):

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

Manifest example (TOML):

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

Batch semantics:

- Best-effort: all entries are attempted, even if some fail.
- Exit code: non-zero when any entry fails.
- Summary: stderr includes total/succeeded/failed and each failed entry.
- Retries: effective attempts per entry are `1 + retries`.
- Timeout: may be set globally (`--batch-timeout-secs`) or per entry (`timeout_secs`).
- Backoff: retries can use exponential delay starting at `--batch-retry-backoff-ms`.
- Jitter: optional +/- percentage spread on retry delay via `--batch-retry-jitter-pct`.
- Overrides: per-entry values (for retries, timeout, backoff, jitter) take precedence over global CLI defaults.

Batch option precedence:

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

Required HA flags:

- `--ha-node-id`
- `--ha-role-state-file`
- `--ha-audit-log-file`

Lease source flags:

- `--ha-lease-source` (`file` or `kubernetes`, default: `file`)
- For `file`: `--ha-lease-file`
- For `kubernetes`: `--ha-kube-lease-name`

Optional Kubernetes lease flags:

- `--ha-kube-namespace` (default: `default`)
- `--ha-kube-context`
- `--ha-kubeconfig`
- `--ha-kubectl-path` (default: `kubectl`)

Optional HA flags:

- `--ha-freshness-file`
- `--ha-readiness-file`
- `--ha-readiness-http-bind` (for example, `127.0.0.1:8088`)
- `--ha-tick-interval-ms` (minimum enforced tick: `50ms`)
- `--ha-min-source-generation`
- `--ha-max-freshness-age-secs`
- `--ha-max-future-skew-secs`
- `--ha-continue-on-error`
- `--ha-startup-fence-mode` (`permissive` or `require-writer`)

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
- When `--ha-readiness-file` is set, readiness is written as `ready` for writer
  mode and `not-ready` otherwise.
- When `--ha-readiness-http-bind` is set, the endpoint returns HTTP `200` with
  `ready` on `/ready` while writer-active and HTTP `503` with `not-ready`
  otherwise. The `/live` endpoint always returns HTTP `200` with `live` while
  the process is running.
- When `--ha-startup-fence-mode=require-writer`, process startup fails unless
  the first HA tick can promote/confirm writer state.
- Stop the loop cleanly with `Ctrl-C`.

Kubernetes deployment references:

- [docs/ha-kubernetes.md](docs/ha-kubernetes.md)
- [docs/k3s-ha-runbook.md](docs/k3s-ha-runbook.md)
- [examples/k8s/ha-deployment.yaml](examples/k8s/ha-deployment.yaml) (lease-manager RBAC)
- [examples/k8s/ha-deployment-readonly.yaml](examples/k8s/ha-deployment-readonly.yaml) (read-only RBAC)
- [examples/k8s/k3s-ha-stack.yaml](examples/k8s/k3s-ha-stack.yaml) (k3s-oriented StatefulSet + writer Service + lease updater + replica-sync contract)
- [scripts/apply-k3s-ha-stack.sh](scripts/apply-k3s-ha-stack.sh) (one-command apply with required image and replica sync command env vars)

## Protocol

See [`docs/protocol.md`](docs/protocol.md) for the full message grammar and
state-machine description.

At a high level:

1. **Handshake** — version and page-size negotiation.
2. **Coarse pass** — replica sends protocol-version-negotiated hashes of
  64-page groups (BLAKE3 in v2, SHA-256 in v1); origin
   identifies changed groups.
3. **Fine pass** — per-page hashes exchanged for changed groups; only diverging
   page bytes are transferred.
4. **Done** — origin signals completion.

Current wire protocol version is `2`.

## Crate structure

| Module | Purpose |
|--------|---------|
| `db` | Safe FFI wrappers around `libsqlite3-sys` |
| `hash` | Page and page-group hashing (v2: BLAKE3, v1: SHA-256) |
| `protocol` | Wire messages, origin and replica state machines |
| `transport` | Pluggable I/O: in-process (`local`), stdio framing, or SSH subprocess |
| `snapshot` | Read-consistent snapshot via `BEGIN DEFERRED` |
| `error` | Unified `SyncError` type |

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

Run focused HA suites:

```bash
cargo test --bin rsqlite-rsync
cargo test --test ha_mode
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
