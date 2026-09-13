//! The gRPC SQL Gateway client.

use std::future::Future;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

use rsqlite_rsync_proto::metadata::HEADER_RSQLITE_LEADER_ENDPOINT;
use rsqlite_rsync_proto::rsqlite::v1::sql_gateway_client::SqlGatewayClient as TonicSqlGatewayClient;
use rsqlite_rsync_proto::rsqlite::v1::{
    BatchRequest, BatchResponse, BatchTransactionMode, ClusterStatusRequest, ClusterStatusResponse,
    ConsistencyLevel, ExecuteRequest, ExecuteResponse, Parameters, QueryChunk, QueryRequest,
    QueryResponse, Statement,
};

use crate::config::ClientConfig;
use crate::discovery::{discover_leader, normalize_endpoint};
use crate::error::{is_not_leader_status, ClientError, ClientResult};

/// Client for executing SQL against the rsqlite-rsync HA cluster's gRPC SQL
/// Gateway, with automatic leader discovery and failover.
///
/// # Example
/// ```no_run
/// # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
/// use rsqlite_rsync_client::{ClientConfig, DiscoveryMode, SqlGatewayClient};
///
/// let mut client = SqlGatewayClient::new(ClientConfig::new(
///     DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
/// ));
/// let resp = client
///     .execute("app.db", "CREATE TABLE t (id INTEGER PRIMARY KEY)", None)
///     .await?;
/// println!("rows affected: {}", resp.rows_affected);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SqlGatewayClient {
    config: ClientConfig,
    current_endpoint: Option<String>,
    tonic_client: Option<TonicSqlGatewayClient<Channel>>,
}

impl SqlGatewayClient {
    /// Create a new client with the given configuration.
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            current_endpoint: None,
            tonic_client: None,
        }
    }

    /// Connect or retrieve the active channel.
    // clippy suggests `if let Some(client) = &mut self.tonic_client { return Ok(client); }`,
    // but that doesn't compile here: the `-> ClientResult<&mut ...>` return type ties the
    // borrow to the whole function body regardless of control flow (a known NLL limitation,
    // not yet fixed by Polonius), so the later `self.tonic_client = Some(...)` below would
    // conflict with it. The is_some()+unwrap() split is the actual necessary workaround.
    #[allow(clippy::unnecessary_unwrap)]
    async fn get_or_connect(&mut self) -> ClientResult<&mut TonicSqlGatewayClient<Channel>> {
        if self.tonic_client.is_some() {
            return Ok(self.tonic_client.as_mut().unwrap());
        }

        let endpoint_str = match &self.current_endpoint {
            Some(ep) => ep.clone(),
            None => discover_leader(&self.config.discovery).await?,
        };
        let endpoint = Endpoint::from_shared(endpoint_str.clone())
            .map_err(|source| ClientError::InvalidEndpoint {
                endpoint: endpoint_str.clone(),
                source,
            })?
            .timeout(self.config.timeout)
            .connect_timeout(self.config.timeout);

        let channel = endpoint
            .connect()
            .await
            .map_err(|source| ClientError::Connect {
                endpoint: endpoint_str.clone(),
                source,
            })?;

        self.current_endpoint = Some(endpoint_str);
        self.tonic_client = Some(TonicSqlGatewayClient::new(channel));
        Ok(self.tonic_client.as_mut().unwrap())
    }

    /// Disconnect the current channel, forcing rediscovery and reconnect on
    /// the next call.
    pub fn reset_connection(&mut self) {
        self.tonic_client = None;
        self.current_endpoint = None;
    }

    /// Discover the current active writer endpoint using the configured
    /// [`crate::DiscoveryMode`], without connecting.
    pub async fn discover_leader(&self) -> ClientResult<String> {
        discover_leader(&self.config.discovery).await
    }

    /// Execute a write statement (DML/DDL) with transparent failover and
    /// retry.
    ///
    /// Only retried on a definitive `NOT_LEADER` response — a transient
    /// `Unavailable`/`DeadlineExceeded` is surfaced immediately rather than
    /// blindly retried, since the write's outcome on the server is not
    /// known in that case and retrying could duplicate it.
    pub async fn execute(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
    ) -> ClientResult<ExecuteResponse> {
        let stmt = Statement {
            sql: sql.to_string(),
            parameters,
        };
        let req = ExecuteRequest {
            database: database.to_string(),
            statement: Some(stmt),
        };

        self.retry_loop(false, |mut client| {
            let req = req.clone();
            async move {
                client
                    .execute(Request::new(req))
                    .await
                    .map(|r| r.into_inner())
            }
        })
        .await
    }

    /// Execute a read query with transparent failover and retry.
    pub async fn query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        consistency: ConsistencyLevel,
    ) -> ClientResult<QueryResponse> {
        let stmt = Statement {
            sql: sql.to_string(),
            parameters,
        };
        let req = QueryRequest {
            database: database.to_string(),
            statement: Some(stmt),
            max_rows,
            chunk_size: 0,
            consistency: consistency as i32,
        };

        self.retry_loop(true, |mut client| {
            let req = req.clone();
            async move {
                client
                    .query(Request::new(req))
                    .await
                    .map(|r| r.into_inner())
            }
        })
        .await
    }

    /// Stream query results in chunks.
    ///
    /// Only the initial call that establishes the stream is retried; once
    /// streaming begins, any error while consuming it is surfaced to the
    /// caller as-is (not retried by this client).
    pub async fn stream_query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        chunk_size: u32,
        consistency: ConsistencyLevel,
    ) -> ClientResult<tonic::Streaming<QueryChunk>> {
        let stmt = Statement {
            sql: sql.to_string(),
            parameters,
        };
        let req = QueryRequest {
            database: database.to_string(),
            statement: Some(stmt),
            max_rows,
            chunk_size,
            consistency: consistency as i32,
        };

        self.retry_loop(true, |mut client| {
            let req = req.clone();
            async move {
                client
                    .stream_query(Request::new(req))
                    .await
                    .map(|r| r.into_inner())
            }
        })
        .await
    }

    /// Execute a batch of statements with transparent failover and retry.
    ///
    /// Like [`Self::execute`], only retried on a definitive `NOT_LEADER`
    /// response.
    pub async fn batch(
        &mut self,
        database: &str,
        statements: Vec<Statement>,
        tx_mode: BatchTransactionMode,
        stop_on_error: bool,
    ) -> ClientResult<BatchResponse> {
        let req = BatchRequest {
            database: database.to_string(),
            statements,
            transaction_mode: tx_mode as i32,
            stop_on_error,
        };

        self.retry_loop(false, |mut client| {
            let req = req.clone();
            async move {
                client
                    .batch(Request::new(req))
                    .await
                    .map(|r| r.into_inner())
            }
        })
        .await
    }

    /// Get cluster status (role, generation, lease, known databases).
    pub async fn get_cluster_status(&mut self) -> ClientResult<ClusterStatusResponse> {
        self.retry_loop(true, |mut client| async move {
            client
                .get_cluster_status(Request::new(ClusterStatusRequest {}))
                .await
                .map(|r| r.into_inner())
        })
        .await
    }

    /// General retry loop supporting automatic discovery, backoff, and
    /// redirection. `idempotent` controls whether a transient
    /// `Unavailable`/`DeadlineExceeded` (as opposed to a definitive
    /// `NOT_LEADER`) is retried — see [`Self::execute`].
    async fn retry_loop<T, F, Fut>(&mut self, idempotent: bool, mut op: F) -> ClientResult<T>
    where
        F: FnMut(TonicSqlGatewayClient<Channel>) -> Fut,
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let mut attempts = 0usize;
        let mut backoff_ms = self.config.initial_backoff_ms;

        loop {
            attempts += 1;
            let client = match self.get_or_connect().await {
                Ok(c) => c.clone(),
                Err(e) => {
                    if matches!(e, ClientError::InvalidEndpoint { .. }) {
                        // Permanent configuration error: never retry.
                        return Err(e);
                    }
                    if attempts >= self.config.max_retries {
                        return Err(ClientError::RetriesExhausted {
                            attempts,
                            source: Box::new(e),
                        });
                    }
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = next_backoff(backoff_ms, self.config.max_backoff_ms);
                    self.reset_connection();
                    continue;
                }
            };

            match op(client).await {
                Ok(res) => return Ok(res),
                Err(status) => {
                    let not_leader = is_not_leader_status(&status);
                    let should_retry = not_leader
                        || (idempotent
                            && matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded));

                    if !should_retry {
                        return Err(ClientError::Rpc(Box::new(status)));
                    }
                    if attempts >= self.config.max_retries {
                        return Err(ClientError::RetriesExhausted {
                            attempts,
                            source: Box::new(ClientError::Rpc(Box::new(status))),
                        });
                    }

                    // Check if the server suggested a new leader endpoint.
                    if let Some(leader_ep) = status.metadata().get(HEADER_RSQLITE_LEADER_ENDPOINT) {
                        if let Ok(ep_str) = leader_ep.to_str() {
                            if !ep_str.is_empty() {
                                self.current_endpoint = Some(normalize_endpoint(ep_str));
                                self.tonic_client = None;
                            }
                        }
                    } else {
                        // Reset connection to force rediscovery on next attempt.
                        self.reset_connection();
                    }

                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = next_backoff(backoff_ms, self.config.max_backoff_ms);
                }
            }
        }
    }
}

pub(crate) fn next_backoff(current_ms: u64, max_ms: u64) -> u64 {
    current_ms.saturating_mul(2).min(max_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_backoff_doubles_until_clamped_at_max() {
        assert_eq!(next_backoff(100, 2000), 200);
        assert_eq!(next_backoff(200, 2000), 400);
        assert_eq!(next_backoff(400, 2000), 800);
        assert_eq!(next_backoff(800, 2000), 1600);
        assert_eq!(next_backoff(1600, 2000), 2000);
        assert_eq!(next_backoff(2000, 2000), 2000);
    }

    #[test]
    fn next_backoff_saturates_instead_of_overflowing() {
        assert_eq!(next_backoff(u64::MAX, 2000), 2000);
        assert_eq!(next_backoff(u64::MAX / 2 + 1, 2000), 2000);
    }
}
