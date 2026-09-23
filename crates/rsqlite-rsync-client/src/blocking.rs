//! Synchronous (blocking) wrapper for [`crate::SqlGatewayClient`].
//!
//! Enable with `features = ["blocking"]`.
//!
//! The blocking client owns an internal single-threaded tokio runtime and
//! drives the async client via [`tokio::runtime::Runtime::block_on`].
//!
//! # Panics
//!
//! Every blocking method (and [`BlockingStreaming::message`]) will **panic**
//! if called from within an active async execution context — i.e. inside
//! `#[tokio::main]`, `#[tokio::test]`, or a spawned async task.
//!
//! The client **can** be constructed and used on a
//! [`tokio::task::spawn_blocking`] thread; however, it must not be moved
//! back into an async task afterwards.
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

/// Panic if called from within an active async execution context.
///
/// `tokio::runtime::Handle::try_current()` succeeds both inside async
/// tasks **and** on `spawn_blocking` threads.  To distinguish the two we
/// attempt to build a *new* current-thread runtime — this succeeds on
/// `spawn_blocking` threads (where `block_on` is allowed) but panics
/// inside an async task (where `block_on` would deadlock).
///
/// The temporary runtime is dropped immediately; it only exists to
/// validate the execution context.
fn assert_not_in_async_context() {
    // If there is no handle at all we are definitely not in an async
    // context — skip the heavier check.
    if tokio::runtime::Handle::try_current().is_err() {
        return;
    }

    // A handle exists.  Try building + entering a throw-away runtime.
    // Inside an async task this panics with Tokio's own
    // "Cannot start a runtime from within a runtime" message, but we
    // surface a more domain-specific message first.
    let probe = std::panic::catch_unwind(|| {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("probe runtime");
        // `block_on` is what actually triggers the nested-runtime
        // panic when called from an async task.
        rt.block_on(async {});
    });

    if probe.is_err() {
        panic!(
            "Cannot use a blocking SqlGatewayClient from within an async runtime. \
             Use the async `SqlGatewayClient` directly, or run blocking code on a \
             dedicated thread via `tokio::task::spawn_blocking`."
        );
    }
}

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
    /// Panics when called from within an active async execution context
    /// (e.g. inside `#[tokio::main]` or a spawned async task).
    /// Construction inside [`tokio::task::spawn_blocking`] is allowed.
    pub fn new(config: ClientConfig) -> Self {
        assert_not_in_async_context();

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
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn discover_leader(&mut self) -> ClientResult<String> {
        assert_not_in_async_context();
        self.rt.block_on(self.inner.discover_leader())
    }

    /// Execute a write statement (DML/DDL) with transparent failover and
    /// retry.
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn execute(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
    ) -> ClientResult<ExecuteResponse> {
        assert_not_in_async_context();
        self.rt
            .block_on(self.inner.execute(database, sql, parameters))
    }

    /// Execute a read query with transparent failover and retry.
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        consistency: ConsistencyLevel,
    ) -> ClientResult<QueryResponse> {
        assert_not_in_async_context();
        self.rt.block_on(
            self.inner
                .query(database, sql, parameters, max_rows, consistency),
        )
    }

    /// Stream query results in chunks.
    ///
    /// Returns a [`BlockingStreaming`] handle that implements [`Iterator`],
    /// yielding one [`QueryChunk`] per iteration.
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn stream_query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        chunk_size: u32,
        consistency: ConsistencyLevel,
    ) -> ClientResult<BlockingStreaming<QueryChunk>> {
        assert_not_in_async_context();
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
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn batch(
        &mut self,
        database: &str,
        statements: Vec<Statement>,
        tx_mode: BatchTransactionMode,
        stop_on_error: bool,
    ) -> ClientResult<BatchResponse> {
        assert_not_in_async_context();
        self.rt.block_on(
            self.inner
                .batch(database, statements, tx_mode, stop_on_error),
        )
    }

    /// Delete a database file (and its WAL/SHM sidecars).
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn drop_database(&mut self, database: &str) -> ClientResult<DropDatabaseResponse> {
        assert_not_in_async_context();
        self.rt.block_on(self.inner.drop_database(database))
    }

    /// Get cluster status (role, generation, lease, known databases).
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn get_cluster_status(&mut self) -> ClientResult<ClusterStatusResponse> {
        assert_not_in_async_context();
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
    ///
    /// # Panics
    ///
    /// Panics when called from within an async execution context.
    pub fn message(&mut self) -> std::result::Result<Option<T>, tonic::Status> {
        assert_not_in_async_context();
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
    #[should_panic(expected = "Cannot use a blocking SqlGatewayClient")]
    async fn blocking_client_panics_inside_async_runtime() {
        let _client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
            "http://127.0.0.1:50051".to_string(),
        )));
    }

    #[tokio::test]
    async fn blocking_client_works_inside_spawn_blocking() {
        let result = tokio::task::spawn_blocking(|| {
            let mut client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
                "127.0.0.1:50051".to_string(),
            )));
            // discover_leader is an in-memory operation for Direct mode.
            client.discover_leader().unwrap()
        })
        .await
        .unwrap();
        assert_eq!(result, "http://127.0.0.1:50051");
    }
}
