//! Unified error type for `rsqlite-rsync`.

use thiserror::Error;

/// Every error that can occur during a sync operation.
#[derive(Debug, Error)]
pub enum SyncError {
    /// A SQLite API returned a non-`SQLITE_OK` result code.
    #[error("SQLite error (code {code}): {msg}")]
    Sqlite {
        /// Raw SQLite result code (e.g. `SQLITE_BUSY = 5`).
        code: i32,
        /// Human-readable message from SQLite.
        msg: String,
    },

    /// An I/O failure on the local filesystem or network stream.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The two endpoints could not agree on a protocol version, or a framing
    /// invariant was violated (truncated message, wrong magic, etc.).
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// The remote `rsqlite-rsync --server` process could not be started.
    #[error("Failed to launch remote endpoint: {0}")]
    RemoteLaunch(String),

    /// ORIGIN and REPLICA have different SQLite page sizes, which prevents
    /// in-place page transfer.
    #[error("Page-size mismatch: origin={origin}, replica={replica}")]
    PageSizeMismatch {
        /// Page size reported by the origin database.
        origin: u32,
        /// Page size reported by the replica database.
        replica: u32,
    },

    /// The database file is locked and could not be acquired after retries.
    #[error("Database busy / locked: {0}")]
    Busy(String),

    /// Message serialisation / deserialisation failed.
    #[error("Codec error: {0}")]
    Codec(String),
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, SyncError>;

impl SyncError {
    /// Construct a [`SyncError::Sqlite`] from a raw result code and an
    /// optional message string.
    pub fn sqlite(code: i32, msg: impl Into<String>) -> Self {
        SyncError::Sqlite {
            code,
            msg: msg.into(),
        }
    }
}
