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
