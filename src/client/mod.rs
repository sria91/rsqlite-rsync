//! Client library for communicating with the rsqlite-rsync SQL Gateway with automatic leader discovery and failover.

use std::path::PathBuf;
use std::time::Duration;

use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request};

use crate::error::{Result, SyncError};
use crate::gateway::server::{
    CODE_NOT_LEADER, HEADER_RSQLITE_CODE, HEADER_RSQLITE_LEADER_ENDPOINT,
};
use crate::ha::{KubectlLeaseReader, LeaseReader};
use crate::proto::rsqlite::v1::{
    BatchRequest, BatchResponse, BatchTransactionMode, ClusterStatusRequest,
    ClusterStatusResponse, ConsistencyLevel, ExecuteRequest, ExecuteResponse,
    NodeRole, Parameters, QueryChunk, QueryRequest, QueryResponse, Statement,
    sql_gateway_client::SqlGatewayClient as TonicSqlGatewayClient,
};

/// Discovery mode for locating the active writer node.
#[derive(Debug, Clone)]
pub enum DiscoveryMode {
    /// Single static endpoint.
    Direct(String),
    /// Multiple candidate endpoints to probe for the active writer.
    Candidates(Vec<String>),
    /// Kubernetes Lease inspection via kubectl.
    KubernetesLease {
        namespace: String,
        lease_name: String,
        service_name: String,
        grpc_port: u16,
        kube_context: Option<String>,
        kubeconfig: Option<PathBuf>,
        kubectl_path: PathBuf,
    },
}

/// Client configuration options.
#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub discovery: DiscoveryMode,
    pub max_retries: usize,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            discovery: DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
            max_retries: 5,
            initial_backoff_ms: 100,
            max_backoff_ms: 2000,
            timeout: Duration::from_secs(15),
        }
    }
}

/// Client for executing SQL against the rsqlite-rsync HA cluster.
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
    async fn get_or_connect(&mut self) -> Result<&mut TonicSqlGatewayClient<Channel>> {
        if self.tonic_client.is_some() {
            return Ok(self.tonic_client.as_mut().unwrap());
        }

        let endpoint_str = match &self.current_endpoint {
            Some(ep) => ep.clone(),
            None => self.discover_leader().await?,
        };
        let endpoint = Endpoint::from_shared(endpoint_str.clone())
            .map_err(|e| SyncError::Protocol(format!("invalid endpoint '{endpoint_str}': {e}")))?
            .timeout(self.config.timeout)
            .connect_timeout(self.config.timeout);

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| SyncError::Network(format!("failed to connect to '{endpoint_str}': {e}")))?;

        self.current_endpoint = Some(endpoint_str);
        self.tonic_client = Some(TonicSqlGatewayClient::new(channel));
        Ok(self.tonic_client.as_mut().unwrap())
    }

    /// Disconnect current client to force reconnect on next call.
    pub fn reset_connection(&mut self) {
        self.tonic_client = None;
        self.current_endpoint = None;
    }

    /// Discover the current active writer endpoint.
    pub async fn discover_leader(&self) -> Result<String> {
        match &self.config.discovery {
            DiscoveryMode::Direct(ep) => Ok(normalize_endpoint(ep)),
            DiscoveryMode::Candidates(candidates) => {
                for candidate in candidates {
                    let norm = normalize_endpoint(candidate);
                    if let Ok(ep) = Endpoint::from_shared(norm.clone()) {
                        let timeout = Duration::from_millis(1500);
                        if let Ok(channel) = ep.timeout(timeout).connect_timeout(timeout).connect().await {
                            let mut probe_client = TonicSqlGatewayClient::new(channel);
                            if let Ok(resp) = probe_client.get_cluster_status(Request::new(ClusterStatusRequest {})).await {
                                let status = resp.into_inner();
                                if NodeRole::try_from(status.role) == Ok(NodeRole::Writer) {
                                    return Ok(norm);
                                }
                            }
                        }
                    }
                }
                // If no writer found among candidates, return first candidate as fallback
                candidates
                    .first()
                    .map(|s| normalize_endpoint(s))
                    .ok_or_else(|| SyncError::Protocol("no candidates configured for discovery".into()))
            }
            DiscoveryMode::KubernetesLease {
                namespace,
                lease_name,
                service_name,
                grpc_port,
                kube_context,
                kubeconfig,
                kubectl_path,
            } => {
                let mut reader = KubectlLeaseReader::new(kubectl_path, namespace, lease_name);
                reader.set_kube_context(kube_context.clone());
                reader.set_kubeconfig(kubeconfig.clone());

                let lease_opt = reader
                    .read_lease()
                    .map_err(|e| SyncError::Protocol(format!("failed to read k8s lease: {e}")))?;

                if let Some(lease) = lease_opt {
                    // E.g. sqlite-ha-0.sqlite-ha.sqlite-ha.svc.cluster.local:50051
                    let host = if service_name.is_empty() {
                        lease.holder_node_id
                    } else {
                        format!("{}.{}", lease.holder_node_id, service_name)
                    };
                    Ok(format!("http://{host}:{grpc_port}"))
                } else {
                    Err(SyncError::Protocol("k8s lease has no active holder".into()))
                }
            }
        }
    }

    /// Execute a write statement with transparent failover and retry.
    pub async fn execute(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
    ) -> Result<ExecuteResponse> {
        let stmt = Statement {
            sql: sql.to_string(),
            parameters,
        };
        let req = ExecuteRequest {
            database: database.to_string(),
            statement: Some(stmt),
        };

        self.retry_loop(|mut client| {
            let req = req.clone();
            async move { client.execute(Request::new(req)).await.map(|r| r.into_inner()) }
        })
        .await
    }

    /// Execute a query with transparent failover and retry.
    pub async fn query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        consistency: ConsistencyLevel,
    ) -> Result<QueryResponse> {
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

        self.retry_loop(|mut client| {
            let req = req.clone();
            async move { client.query(Request::new(req)).await.map(|r| r.into_inner()) }
        })
        .await
    }

    /// Stream query results in chunks.
    pub async fn stream_query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        chunk_size: u32,
        consistency: ConsistencyLevel,
    ) -> Result<tonic::Streaming<QueryChunk>> {
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

        self.retry_loop(|mut client| {
            let req = req.clone();
            async move { client.stream_query(Request::new(req)).await.map(|r| r.into_inner()) }
        })
        .await
    }

    /// Execute a batch of statements with transparent failover and retry.
    pub async fn batch(
        &mut self,
        database: &str,
        statements: Vec<Statement>,
        tx_mode: BatchTransactionMode,
        stop_on_error: bool,
    ) -> Result<BatchResponse> {
        let req = BatchRequest {
            database: database.to_string(),
            statements,
            transaction_mode: tx_mode as i32,
            stop_on_error,
        };

        self.retry_loop(|mut client| {
            let req = req.clone();
            async move { client.batch(Request::new(req)).await.map(|r| r.into_inner()) }
        })
        .await
    }

    /// Get cluster status.
    pub async fn get_cluster_status(&mut self) -> Result<ClusterStatusResponse> {
        self.retry_loop(|mut client| async move {
            client
                .get_cluster_status(Request::new(ClusterStatusRequest {}))
                .await
                .map(|r| r.into_inner())
        })
        .await
    }

    /// General retry loop supporting automatic discovery, backoff, and redirection.
    async fn retry_loop<T, F, Fut>(&mut self, mut op: F) -> Result<T>
    where
        F: FnMut(TonicSqlGatewayClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, tonic::Status>>,
    {
        let mut attempts = 0;
        let mut backoff_ms = self.config.initial_backoff_ms;

        loop {
            attempts += 1;
            let client = match self.get_or_connect().await {
                Ok(c) => c.clone(),
                Err(e) => {
                    if attempts >= self.config.max_retries {
                        return Err(e);
                    }
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(self.config.max_backoff_ms);
                    self.reset_connection();
                    continue;
                }
            };

            match op(client).await {
                Ok(res) => return Ok(res),
                Err(status) => {
                    let is_not_leader = status.code() == Code::FailedPrecondition
                        && status.metadata().get(HEADER_RSQLITE_CODE).map(|v| v == CODE_NOT_LEADER).unwrap_or(false);

                    let is_transient = is_not_leader
                        || status.code() == Code::Unavailable
                        || status.code() == Code::DeadlineExceeded;

                    if !is_transient || attempts >= self.config.max_retries {
                        return Err(SyncError::Protocol(format!(
                            "gRPC call failed (code: {:?}): {}",
                            status.code(),
                            status.message()
                        )));
                    }

                    // Check if the server suggested a new leader endpoint
                    if let Some(leader_ep) = status.metadata().get(HEADER_RSQLITE_LEADER_ENDPOINT) {
                        if let Ok(ep_str) = leader_ep.to_str()
                            && !ep_str.is_empty()
                        {
                            self.current_endpoint = Some(normalize_endpoint(ep_str));
                            self.tonic_client = None;
                        }
                    } else {
                        // Reset connection to force rediscovery on next attempt
                        self.reset_connection();
                    }

                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(self.config.max_backoff_ms);
                }
            }
        }
    }
}

fn normalize_endpoint(ep: &str) -> String {
    let trimmed = ep.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}
