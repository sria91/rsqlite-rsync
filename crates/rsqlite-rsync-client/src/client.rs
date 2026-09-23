//! The gRPC SQL Gateway client.

use std::future::Future;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

use rsqlite_rsync_proto::metadata::HEADER_RSQLITE_LEADER_ENDPOINT;
use rsqlite_rsync_proto::rsqlite::v1::sql_gateway_client::SqlGatewayClient as TonicSqlGatewayClient;
use rsqlite_rsync_proto::rsqlite::v1::{
    BatchRequest, BatchResponse, BatchTransactionMode, ClusterStatusRequest, ClusterStatusResponse,
    ConsistencyLevel, DropDatabaseRequest, DropDatabaseResponse, ExecuteRequest, ExecuteResponse,
    Parameters, QueryChunk, QueryRequest, QueryResponse, Statement,
};

use crate::config::ClientConfig;
use crate::discovery::{discover_leader, normalize_endpoint};
use crate::error::{is_not_leader_status, ClientError, ClientResult};

/// Wrap `message` in a [`Request`], attaching `authorization: Bearer <token>`
/// when `auth_token` is set. Kept as a free function (rather than a method
/// taking `&self`) so it can be called from inside the `move` closures
/// passed to [`SqlGatewayClient::retry_loop`], which already borrow `self`
/// mutably for the surrounding call — and reused by
/// [`crate::discovery::discover_leader`]'s own candidate-probing RPCs, which
/// otherwise would silently fail (and mis-select) against an
/// authentication-requiring gateway.
pub(crate) fn authorized_request<T>(auth_token: &Option<String>, message: T) -> Request<T> {
    let mut request = Request::new(message);
    if let Some(token) = auth_token {
        if let Ok(value) = format!("Bearer {token}").parse() {
            request.metadata_mut().insert("authorization", value);
        }
    }
    request
}

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
            None => discover_leader(&self.config.discovery, &self.config.auth_token).await?,
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
        discover_leader(&self.config.discovery, &self.config.auth_token).await
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

        let auth_token = self.config.auth_token.clone();
        self.retry_loop(false, |mut client| {
            let req = req.clone();
            let auth_token = auth_token.clone();
            async move {
                client
                    .execute(authorized_request(&auth_token, req))
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

        let auth_token = self.config.auth_token.clone();
        self.retry_loop(true, |mut client| {
            let req = req.clone();
            let auth_token = auth_token.clone();
            async move {
                client
                    .query(authorized_request(&auth_token, req))
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

        let auth_token = self.config.auth_token.clone();
        self.retry_loop(true, |mut client| {
            let req = req.clone();
            let auth_token = auth_token.clone();
            async move {
                client
                    .stream_query(authorized_request(&auth_token, req))
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

        let auth_token = self.config.auth_token.clone();
        self.retry_loop(false, |mut client| {
            let req = req.clone();
            let auth_token = auth_token.clone();
            async move {
                client
                    .batch(authorized_request(&auth_token, req))
                    .await
                    .map(|r| r.into_inner())
            }
        })
        .await
    }

    /// Delete a database file (and its WAL/SHM sidecars) with transparent
    /// failover and retry.
    ///
    /// Deletion is idempotent — dropping an already-absent database just
    /// reports `existed: false` — so this is safe to retry on a transient
    /// `Unavailable`/`DeadlineExceeded`, unlike [`Self::execute`].
    pub async fn drop_database(&mut self, database: &str) -> ClientResult<DropDatabaseResponse> {
        let req = DropDatabaseRequest {
            database: database.to_string(),
        };

        let auth_token = self.config.auth_token.clone();
        self.retry_loop(true, |mut client| {
            let req = req.clone();
            let auth_token = auth_token.clone();
            async move {
                client
                    .drop_database(authorized_request(&auth_token, req))
                    .await
                    .map(|r| r.into_inner())
            }
        })
        .await
    }

    /// Get cluster status (role, generation, lease, known databases).
    pub async fn get_cluster_status(&mut self) -> ClientResult<ClusterStatusResponse> {
        let auth_token = self.config.auth_token.clone();
        self.retry_loop(true, |mut client| {
            let auth_token = auth_token.clone();
            async move {
                client
                    .get_cluster_status(authorized_request(&auth_token, ClusterStatusRequest {}))
                    .await
                    .map(|r| r.into_inner())
            }
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
                    self.process_leader_header(&status);

                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = next_backoff(backoff_ms, self.config.max_backoff_ms);
                }
            }
        }
    }

    /// Process the `HEADER_RSQLITE_LEADER_ENDPOINT` header from a gRPC status.
    ///
    /// If the header is present and contains a non-empty endpoint, update the
    /// current endpoint and reset the tonic client to force rediscovery.
    /// If the header is not present, reset the connection to force rediscovery.
    fn process_leader_header(&mut self, status: &tonic::Status) {
        // Check if the server suggested a new leader endpoint.
        if let Some(ep_str) = status
            .metadata()
            .get(HEADER_RSQLITE_LEADER_ENDPOINT)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
        {
            self.current_endpoint = Some(normalize_endpoint(ep_str));
            self.tonic_client = None;
        } else {
            // Header absent, invalid UTF-8, or empty: reset connection to force rediscovery on next attempt.
            self.reset_connection();
        }
    }
}

pub(crate) fn next_backoff(current_ms: u64, max_ms: u64) -> u64 {
    current_ms.saturating_mul(2).min(max_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Code, Status};

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

    #[test]
    fn test_sql_gateway_client_clone() {
        let client = SqlGatewayClient::new(ClientConfig::new(
            crate::discovery::DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
        ));
        let cloned = client.clone();
        assert_eq!(cloned.current_endpoint, None);
    }

    #[tokio::test]
    async fn test_client_discover_leader_direct() {
        use crate::discovery::DiscoveryMode;
        let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
            "127.0.0.1:50051".to_string(),
        )));
        let ep = client.discover_leader().await.unwrap();
        assert_eq!(ep, "http://127.0.0.1:50051");
    }

    #[test]
    fn test_authorized_request() {
        let req_none = authorized_request(&None, ());
        assert!(req_none.metadata().get("authorization").is_none());

        let req_some = authorized_request(&Some("token123".to_string()), ());
        assert_eq!(
            req_some
                .metadata()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer token123"
        );

        let req_invalid =
            authorized_request(&Some("token\nwith\ninvalid\x00chars".to_string()), ());
        assert!(req_invalid.metadata().get("authorization").is_none());
    }

    #[tokio::test]
    async fn test_process_leader_header_present_and_non_empty() {
        let mut client = SqlGatewayClient::new(ClientConfig::new(
            crate::discovery::DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
        ));
        client.current_endpoint = Some("http://old.endpoint".to_string());
        // connect_lazy() still requires a tokio runtime to construct the channel
        let channel = Endpoint::from_shared("http://dummy")
            .unwrap()
            .connect_lazy();
        client.tonic_client = Some(TonicSqlGatewayClient::new(channel));

        // Create a status with a non-empty leader endpoint header
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            HEADER_RSQLITE_LEADER_ENDPOINT,
            "http://new.endpoint".parse().unwrap(),
        );
        let status = Status::with_metadata(Code::Unknown, "test", metadata);

        client.process_leader_header(&status);
        assert_eq!(
            client.current_endpoint,
            Some("http://new.endpoint".to_string())
        );
        assert!(client.tonic_client.is_none());
    }

    #[tokio::test]
    async fn test_process_leader_header_present_but_empty() {
        let mut client = SqlGatewayClient::new(ClientConfig::new(
            crate::discovery::DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
        ));
        client.current_endpoint = Some("http://old.endpoint".to_string());
        // connect_lazy() still requires a tokio runtime to construct the channel
        let channel = Endpoint::from_shared("http://dummy")
            .unwrap()
            .connect_lazy();
        client.tonic_client = Some(TonicSqlGatewayClient::new(channel));

        // Create a status with an empty leader endpoint header
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(HEADER_RSQLITE_LEADER_ENDPOINT, "".parse().unwrap());
        let status = Status::with_metadata(Code::Unknown, "test", metadata);

        client.process_leader_header(&status);
        // Should reset the connection because the header value is empty
        assert!(client.current_endpoint.is_none());
        assert!(client.tonic_client.is_none());
    }

    #[tokio::test]
    async fn test_process_leader_header_absent() {
        let mut client = SqlGatewayClient::new(ClientConfig::new(
            crate::discovery::DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
        ));
        client.current_endpoint = Some("http://old.endpoint".to_string());
        // connect_lazy() still requires a tokio runtime to construct the channel
        let channel = Endpoint::from_shared("http://dummy")
            .unwrap()
            .connect_lazy();
        client.tonic_client = Some(TonicSqlGatewayClient::new(channel));

        // Create a status without the leader endpoint header
        let metadata = tonic::metadata::MetadataMap::new();
        let status = Status::with_metadata(Code::Unknown, "test", metadata);

        client.process_leader_header(&status);
        // Should reset the connection
        assert!(client.current_endpoint.is_none());
        assert!(client.tonic_client.is_none());
    }
}
