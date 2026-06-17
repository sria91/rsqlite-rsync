# rsqlite-rsync Wire Protocol

## Overview

The protocol is a simple request-response exchange between two endpoints:

- **Origin** — holds the authoritative database (read-only during sync).
- **Replica** — the destination that should become a copy of origin.

The replica always initiates the connection.

## Framing

Every message is length-prefixed:

```
+---+---+---+---+---- ... ----+
| len (u32 LE)  | bincode payload |
+---+---+---+---+---- ... ----+
```

The `len` field is the number of payload bytes (not including the 4-byte
length field itself).  Payload is encoded with
[bincode v2](https://docs.rs/bincode/latest/bincode/) using the standard
configuration (little-endian, variable-length integers).

## State machine

```
Replica                              Origin
───────                              ──────
Hello ──────────────────────────────►
                                    ◄──── HelloAck  (or Error)

GroupHashes ────────────────────────►
                                    ◄──── GroupsNeedFine

for each group in GroupsNeedFine:
  PageHashes ─────────────────────►
                                    ◄──── SendPages  (origin → replica)
  PagesAck ───────────────────────►

                                    ◄──── Done
```

If `GroupsNeedFine` is empty (databases are already in sync), the origin
sends `Done` immediately after `GroupsNeedFine`.

## Message reference

### `Hello`

Sent by the **replica** immediately after connecting.

| Field | Type | Description |
|-------|------|-------------|
| `version` | u32 | Protocol version offered |
| `page_size` | u32 | Replica page size in bytes (0 if replica is empty) |
| `page_count` | u32 | Current page count of replica |

### `HelloAck`

Sent by the **origin** in response to `Hello`.

| Field | Type | Description |
|-------|------|-------------|
| `version` | u32 | Negotiated version (≤ offered) |
| `page_size` | u32 | Origin page size in bytes |
| `page_count` | u32 | Total origin pages at start of session |

If the page sizes are incompatible the origin sends `Error` instead.

### `GroupHashes`

Sent by the **replica** to describe its current coarse state.

| Field | Type | Description |
|-------|------|-------------|
| `first_group` | u32 | 0-based index of the first group in `hashes` |
| `hashes` | `Vec<[u8; 32]>` | One group hash per group of 64 pages |

Current implementation constraint (protocol v2): `first_group` must be `0`.
Non-zero offsets are rejected as protocol violations.

A group hash is `H(concat(page_hash_0, page_hash_1, ..., page_hash_63))` where
`H` is determined by negotiated protocol version:

- v1: SHA-256
- v2: BLAKE3

### `GroupsNeedFine`

Sent by the **origin** after comparing group hashes.

| Field | Type | Description |
|-------|------|-------------|
| `group_indices` | `Vec<u32>` | 0-based group indices that differ |

### `PageHashes`

Sent by the **replica** for each group listed in `GroupsNeedFine`.

| Field | Type | Description |
|-------|------|-------------|
| `page_nos` | `Vec<u32>` | 1-based page numbers |
| `hashes` | `Vec<[u8; 32]>` | Per-page hash (version-dependent) |

Validation rules:

- `page_nos.len() == hashes.len()`
- page numbers must be strictly increasing and in-range for the requested group
- page numbers must form a contiguous prefix of that group
  (for example `first_page..=k`, or empty when the group is fully beyond the
  replica's page count)

### `SendPages`

Sent by the **origin** with the raw bytes of changed pages.

| Field | Type | Description |
|-------|------|-------------|
| `pages` | `Vec<PageData>` | Changed pages |

`PageData` is `{ page_no: u32, data: Vec<u8> }`.

### `PagesAck`

Sent by the **replica** after writing all pages from `SendPages`.

| Field | Type | Description |
|-------|------|-------------|
| `page_nos` | `Vec<u32>` | Page numbers that were written |

### `Done`

Sent by the **origin** when no more pages remain.  No fields.

`Done` is valid only after the expected fine-pass exchange is complete.
Receiving `Done` early while there are pending groups is a protocol violation.

### `Error`

Sent by either side to abort the session.

| Field | Type | Description |
|-------|------|-------------|
| `message` | String | Human-readable error description |

On protocol violations, the implementation attempts a best-effort `Error`
message before returning a local failure. If the transport is already broken,
the local error is still returned.

## Version negotiation

The replica offers its highest supported version in `Hello.version`.  The
origin responds with `min(offered, own_version)` in `HelloAck.version`.  Both
sides must use the negotiated version for the remainder of the session.

`Hello.version = 0` is invalid and causes the origin to return `Error`.
The replica must reject `HelloAck.version` values that are `0` or greater than
the version it offered.

Hash algorithm by version:

- Version `1`: SHA-256 page and group hashing.
- Version `2`: BLAKE3 page and group hashing.

Current version is `2`.

## Bandwidth analysis

| Scenario | Approximate bytes on wire |
|----------|--------------------------|
| No changes | `O(page_count / 64 × 32)` (just group hashes) |
| 1% pages changed | `~1% × db_size + hash_overhead` |
| All pages different | `~db_size × 1.01` (plus hash overhead) |
