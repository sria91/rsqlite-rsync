//! Synchronous (blocking) wrapper for [`crate::SqlGatewayClient`].
//!
//! Enable with `features = ["blocking"]`.
//!
//! The blocking client owns an internal single-threaded tokio runtime and
//! drives the async client via [`tokio::runtime::Runtime::block_on`].
//!
//! # Panics
//!
//! [`SqlGatewayClient::new`] will **panic** if called from within an
//! active tokio runtime (i.e. inside `#[tokio::main]`, `#[tokio::test]`,
//! or a spawned task).  If you are already inside an async context, use
//! [`crate::SqlGatewayClient`] directly or run the blocking code on a
//! dedicated thread via [`tokio::task::spawn_blocking`].
//!
//! # Example
//!
//! ```no_run
//! use rsqlite_rsync_client::blocking::SqlGatewayClient;
//! use rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let mut client = SqlGatewayClient::new(ClientConfig::new(
//!         DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
//!     ));
//!
//!     client.execute("app.db", "CREATE TABLE t (id INTEGER PRIMARY KEY)", None)?;
//!
//!     let rows = client.query("app.db", "SELECT * FROM t", None, 0, Default::default())?;
//!     println!("{} rows", rows.total_rows);
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use crate::error::ClientResult;
use crate::proto::{
    BatchResponse, BatchTransactionMode, ClusterStatusResponse, ConsistencyLevel,
    DropDatabaseResponse, ExecuteResponse, Parameters, QueryChunk, QueryResponse, Statement,
};
use crate::ClientConfig;

// Re-export commonly used types so callers can pull everything from
// `rsqlite_rsync_client::blocking::*`.
pub use crate::config::{ClientTarget, RuntimeMode};
pub use crate::discovery::{DiscoveryMode, LeaderResolver};
pub use crate::error::{BoxError, ClientError as Error, ClientResult as Result};
pub use crate::proto;
pub use crate::tonic;
pub use crate::ClientConfig as Config;

/// Synchronous wrapper around [`crate::SqlGatewayClient`].
///
/// Each instance creates a lightweight, single-threaded tokio runtime.
/// The runtime is reference-counted so that [`BlockingStreaming`] handles
/// returned by [`SqlGatewayClient::stream_query`] can outlive individual
/// method calls.
pub struct SqlGatewayClient {
    inner: crate::SqlGatewayClient,
    rt: Arc<tokio::runtime::Runtime>,
}

impl SqlGatewayClient {
    /// Create a new blocking client with the given configuration.
    ///
    /// # Panics
    ///
    /// Panics when called from within an active tokio runtime.
    pub fn new(config: ClientConfig) -> Self {
        if tokio::runtime::Handle::try_current().is_ok() {
            panic!(
                "Cannot create a blocking SqlGatewayClient from within an async runtime. \
                 Use the async `SqlGatewayClient` directly, or run blocking code on a \
                 dedicated thread via `tokio::task::spawn_blocking`."
            );
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build tokio current-thread runtime");

        Self {
            inner: crate::SqlGatewayClient::new(config),
            rt: Arc::new(rt),
        }
    }

    /// Disconnect the current channel, forcing rediscovery and reconnect
    /// on the next call.
    pub fn reset_connection(&mut self) {
        self.inner.reset_connection();
    }

    /// Discover the current active writer endpoint using the configured
    /// [`DiscoveryMode`], without connecting.
    pub fn discover_leader(&mut self) -> ClientResult<String> {
        self.rt.block_on(self.inner.discover_leader())
    }

    /// Execute a write statement (DML/DDL) with transparent failover and
    /// retry.
    pub fn execute(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
    ) -> ClientResult<ExecuteResponse> {
        self.rt
            .block_on(self.inner.execute(database, sql, parameters))
    }

    /// Execute a read query with transparent failover and retry.
    pub fn query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        consistency: ConsistencyLevel,
    ) -> ClientResult<QueryResponse> {
        self.rt.block_on(
            self.inner
                .query(database, sql, parameters, max_rows, consistency),
        )
    }

    /// Stream query results in chunks.
    ///
    /// Returns a [`BlockingStreaming`] handle that implements [`Iterator`],
    /// yielding one [`QueryChunk`] per iteration.
    pub fn stream_query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        chunk_size: u32,
        consistency: ConsistencyLevel,
    ) -> ClientResult<BlockingStreaming<QueryChunk>> {
        let stream = self.rt.block_on(self.inner.stream_query(
            database,
            sql,
            parameters,
            max_rows,
            chunk_size,
            consistency,
        ))?;
        Ok(BlockingStreaming {
            inner: stream,
            rt: Arc::clone(&self.rt),
        })
    }

    /// Execute a batch of statements with transparent failover and retry.
    pub fn batch(
        &mut self,
        database: &str,
        statements: Vec<Statement>,
        tx_mode: BatchTransactionMode,
        stop_on_error: bool,
    ) -> ClientResult<BatchResponse> {
        self.rt.block_on(
            self.inner
                .batch(database, statements, tx_mode, stop_on_error),
        )
    }

    /// Delete a database file (and its WAL/SHM sidecars).
    pub fn drop_database(&mut self, database: &str) -> ClientResult<DropDatabaseResponse> {
        self.rt.block_on(self.inner.drop_database(database))
    }

    /// Get cluster status (role, generation, lease, known databases).
    pub fn get_cluster_status(&mut self) -> ClientResult<ClusterStatusResponse> {
        self.rt.block_on(self.inner.get_cluster_status())
    }
}

/// Synchronous iterator wrapper around [`tonic::Streaming`].
///
/// Obtained from [`SqlGatewayClient::stream_query`].  Each call to
/// [`Iterator::next`] blocks until the next chunk arrives or the stream
/// ends.
pub struct BlockingStreaming<T> {
    inner: tonic::Streaming<T>,
    rt: Arc<tokio::runtime::Runtime>,
}

impl<T> BlockingStreaming<T> {
    /// Blocking analog of [`tonic::Streaming::message`].
    ///
    /// Returns `Ok(Some(item))` for each chunk, `Ok(None)` when the
    /// stream ends, or `Err(status)` on a gRPC error.
    pub fn message(&mut self) -> std::result::Result<Option<T>, tonic::Status> {
        self.rt.block_on(self.inner.message())
    }
}

impl<T> Iterator for BlockingStreaming<T> {
    type Item = std::result::Result<T, tonic::Status>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.message() {
            Ok(Some(v)) => Some(Ok(v)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DiscoveryMode;

    #[test]
    fn blocking_client_is_send_sync() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<SqlGatewayClient>();
    }

    #[test]
    fn blocking_streaming_is_iterator() {
        fn assert_iter<T: Iterator>() {}
        assert_iter::<BlockingStreaming<QueryChunk>>();
    }

    #[test]
    fn blocking_client_constructs_and_resets() {
        let mut client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
            "http://127.0.0.1:50051".to_string(),
        )));
        // reset_connection is a pure in-memory operation — no network.
        client.reset_connection();
    }

    #[test]
    fn blocking_client_discover_leader_direct() {
        let mut client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
            "127.0.0.1:50051".to_string(),
        )));
        let ep = client.discover_leader().unwrap();
        assert_eq!(ep, "http://127.0.0.1:50051");
    }

    #[tokio::test]
    #[should_panic(expected = "Cannot create a blocking SqlGatewayClient")]
    async fn blocking_client_panics_inside_async_runtime() {
        let _client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
            "http://127.0.0.1:50051".to_string(),
        )));
    }
}
