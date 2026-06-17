//! # rsqlite-rsync
//!
//! A bandwidth-efficient SQLite database synchronisation tool, inspired by the
//! C utility [`sqlite3_rsync`](https://www.sqlite.org/rsync.html).
//!
//! ## Overview
//!
//! ```text
//!                    ┌──────────────┐        ┌──────────────┐
//!  rsqlite-rsync     │    ORIGIN    │◄──────►│   REPLICA    │
//!  (local or SSH)    │  (read-only) │  wire  │  (read-write)│
//!                    └──────────────┘        └──────────────┘
//! ```
//!
//! The protocol works in two passes:
//!
//! 1. **Coarse pass** — the replica sends protocol-version-negotiated hashes
//!    of groups of 64 pages (Blake3 in v2, SHA-256 in v1). The origin
//!    identifies which groups differ.
//! 2. **Fine pass** — per-page hashes are exchanged for changed groups; only
//!    diverging page bytes are transferred.
//!
//! This means a 500 MB database that has 1 % of pages changed will transfer
//! roughly 5 MB instead of 500 MB.
//!
//! ## Crate structure
//!
//! | Module | Purpose |
//! |--------|---------|
//! | [`db`] | Safe FFI wrappers around `libsqlite3-sys` |
//! | [`hash`] | Page and group hashing |
//! | [`protocol`] | Wire messages and origin/replica state machines |
//! | [`transport`] | Pluggable I/O: local duplex or SSH subprocess |
//! | [`snapshot`] | Read-consistent snapshot via `BEGIN DEFERRED` |
//! | [`error`] | Unified [`SyncError`](error::SyncError) type |
//!
//! ## Example: local sync
//!
//! ```rust,no_run
//! use rsqlite_rsync::{db::Connection, sync_local};
//! use std::path::Path;
//!
//! #[tokio::main]
//! async fn main() -> rsqlite_rsync::error::Result<()> {
//!     sync_local(Path::new("origin.db"), Path::new("replica.db")).await
//! }
//! ```

pub mod db;
pub mod error;
pub mod hash;
pub mod protocol;
pub mod snapshot;
pub mod transport;

use std::path::Path;
use std::time::Duration;

use libsqlite3_sys as ffi;

use crate::db::Connection;
use crate::error::Result;
use crate::protocol::{origin, replica};
use crate::snapshot::Snapshot;
use crate::transport::local::LocalTransport;

const LOCAL_SYNC_TIMEOUT: Duration = Duration::from_secs(300);

/// Runtime tuning knobs for CPU-sensitive hashing paths.
#[derive(Debug, Clone)]
pub struct SyncTuning {
    /// Optional cap for rayon worker threads used by hash-heavy stages.
    pub max_hash_threads: Option<usize>,
    /// Minimum page count before switching from sequential to parallel hashing.
    pub parallel_min_pages: u32,
    /// Number of coarse hash groups to process per chunk.
    pub hash_chunk_groups: u32,
}

impl Default for SyncTuning {
    fn default() -> Self {
        Self {
            max_hash_threads: None,
            parallel_min_pages: 4096,
            hash_chunk_groups: 16,
        }
    }
}

impl SyncTuning {
    /// Load tuning from environment variables.
    ///
    /// Invalid values are ignored and the default is used for that field.
    pub fn from_env() -> Self {
        let default = Self::default();

        let max_hash_threads = std::env::var("RSQLITE_RSYNC_MAX_HASH_THREADS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .or(default.max_hash_threads);

        let parallel_min_pages = std::env::var("RSQLITE_RSYNC_PARALLEL_MIN_PAGES")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default.parallel_min_pages);

        let hash_chunk_groups = std::env::var("RSQLITE_RSYNC_HASH_CHUNK_GROUPS")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default.hash_chunk_groups);

        Self {
            max_hash_threads,
            parallel_min_pages,
            hash_chunk_groups,
        }
    }

    pub fn should_parallelize(&self, pages: u32) -> bool {
        pages >= self.parallel_min_pages
    }
}

/// Synchronise two local SQLite databases: make `replica_path` a consistent
/// copy of `origin_path`.
///
/// This is the primary entry point for local-to-local synchronisation.  For
/// remote sync, use the CLI or build a custom [`transport::Transport`].
///
/// # Errors
///
/// Returns a [`error::SyncError`] if:
/// * Either database cannot be opened.
/// * The page sizes differ.
/// * Any underlying I/O or SQLite operation fails.
///
/// # Example
///
/// ```rust,no_run
/// # async fn demo() -> rsqlite_rsync::error::Result<()> {
/// rsqlite_rsync::sync_local(
///     std::path::Path::new("origin.db"),
///     std::path::Path::new("replica.db"),
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn sync_local(origin_path: &Path, replica_path: &Path) -> Result<()> {
    let tuning = SyncTuning::from_env();
    sync_local_with_tuning(origin_path, replica_path, &tuning).await
}

/// Synchronise two local SQLite databases with explicit runtime tuning.
pub async fn sync_local_with_tuning(
    origin_path: &Path,
    replica_path: &Path,
    tuning: &SyncTuning,
) -> Result<()> {
    let origin_conn = Connection::open(origin_path, ffi::SQLITE_OPEN_READONLY)?;
    let replica_conn = Connection::open(
        replica_path,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )?;

    // Begin a read-consistent snapshot on the origin.
    let snap = Snapshot::begin(&origin_conn)?;

    let (mut origin_transport, mut replica_transport) = LocalTransport::pair();

    // Run both sides concurrently using tokio.
    let origin_fut = origin::run_with_tuning(&snap, &mut origin_transport, tuning);
    let replica_fut = replica::run_with_tuning(&replica_conn, &mut replica_transport, tuning);

    let (origin_res, replica_res) = tokio::time::timeout(LOCAL_SYNC_TIMEOUT, async {
        tokio::join!(origin_fut, replica_fut)
    })
    .await
    .map_err(|_| crate::error::SyncError::Protocol("local sync timed out".into()))?;

    origin_res?;
    replica_res?;
    snap.commit()?;

    Ok(())
}
