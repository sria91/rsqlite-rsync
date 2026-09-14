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

    /// A network transport (e.g. gRPC channel) failed to connect or serve.
    #[error("Network error: {0}")]
    Network(String),

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

impl From<rsqlite_rsync_client::ClientError> for SyncError {
    fn from(error: rsqlite_rsync_client::ClientError) -> Self {
        match error {
            rsqlite_rsync_client::ClientError::Connect { .. } => {
                SyncError::Network(error.to_string())
            }
            rsqlite_rsync_client::ClientError::RetriesExhausted { ref source, .. }
                if matches!(**source, rsqlite_rsync_client::ClientError::Connect { .. }) =>
            {
                SyncError::Network(error.to_string())
            }
            other => SyncError::Protocol(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_error_display_and_constructors() {
        let sqlite_err = SyncError::sqlite(5, "database is locked");
        assert_eq!(sqlite_err.to_string(), "SQLite error (code 5): database is locked");

        let io_err: SyncError = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found").into();
        assert_eq!(io_err.to_string(), "I/O error: file not found");

        let proto_err = SyncError::Protocol("bad version".into());
        assert_eq!(proto_err.to_string(), "Protocol error: bad version");

        let remote_err = SyncError::RemoteLaunch("ssh died".into());
        assert_eq!(remote_err.to_string(), "Failed to launch remote endpoint: ssh died");

        let net_err = SyncError::Network("connection refused".into());
        assert_eq!(net_err.to_string(), "Network error: connection refused");

        let page_mismatch = SyncError::PageSizeMismatch { origin: 4096, replica: 1024 };
        assert_eq!(page_mismatch.to_string(), "Page-size mismatch: origin=4096, replica=1024");

        let busy_err = SyncError::Busy("locked by reader".into());
        assert_eq!(busy_err.to_string(), "Database busy / locked: locked by reader");

        let codec_err = SyncError::Codec("invalid frame".into());
        assert_eq!(codec_err.to_string(), "Codec error: invalid frame");
    }

    #[test]
    fn client_rpc_error_maps_to_protocol() {
        // `ClientError` is `#[non_exhaustive]` with private fields, so it
        // can't be literal-constructed from outside its crate; the `Rpc`
        // variant's `#[from] Box<Status>` impl is the one constructor this
        // crate can legitimately call to get a non-`Connect` variant.
        let status = tonic::Status::new(tonic::Code::Internal, "boom");
        let client_err: rsqlite_rsync_client::ClientError = Box::new(status).into();

        let sync_err: SyncError = client_err.into();
        assert!(matches!(sync_err, SyncError::Protocol(msg) if msg.contains("boom")));
    }

    #[tokio::test]
    async fn client_connect_error_maps_to_network() {
        // Bind an ephemeral port, then immediately drop the listener so
        // nothing is listening there. This gives a real, fast connection
        // failure (rather than a slow timeout) without depending on any
        // external network access, yielding a genuine
        // `rsqlite_rsync_client::ClientError::Connect` the same way a real
        // sync session would encounter one.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let config = rsqlite_rsync_client::ClientConfig::new(
            rsqlite_rsync_client::DiscoveryMode::Direct(format!("http://{addr}")),
        )
        .with_max_retries(1)
        .with_timeout(std::time::Duration::from_millis(500));
        let mut client = rsqlite_rsync_client::SqlGatewayClient::new(config);

        let err = client
            .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
            .await
            .expect_err("nothing is listening on the dropped port");

        // Convert the *outer* error directly — the production `From` impl
        // must handle `RetriesExhausted { source: Connect { .. } }` just
        // as well as a bare `Connect`.
        let sync_err: SyncError = err.into();
        assert!(
            matches!(sync_err, SyncError::Network(_)),
            "expected SyncError::Network, got {sync_err:?}"
        );
    }
}
