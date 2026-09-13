//! gRPC SqlGateway server implementation with writer fencing.

use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::gateway::engine::DatabaseEngine;
use crate::ha::HaSharedState;
use crate::proto::rsqlite::v1::{
    BatchRequest, BatchResponse, BatchTransactionMode, ClusterStatusRequest, ClusterStatusResponse,
    ConsistencyLevel, ExecuteRequest, ExecuteResponse, LeaseStatus, NodeRole, QueryChunk,
    QueryRequest, QueryResponse, sql_gateway_server::SqlGateway,
};

const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

pub use rsqlite_rsync_proto::metadata::{
    CODE_NOT_LEADER, HEADER_RSQLITE_CODE, HEADER_RSQLITE_GENERATION,
    HEADER_RSQLITE_LEADER_ENDPOINT, HEADER_RSQLITE_LEADER_ID,
};

/// Implementation of the `SqlGateway` gRPC service.
#[derive(Clone)]
pub struct SqlGatewayServer {
    engine: DatabaseEngine,
    ha_state: Arc<RwLock<HaSharedState>>,
    started_at: Instant,
}

impl SqlGatewayServer {
    /// Create a new SqlGatewayServer.
    pub fn new(engine: DatabaseEngine, ha_state: Arc<RwLock<HaSharedState>>) -> Self {
        Self {
            engine,
            ha_state,
            started_at: Instant::now(),
        }
    }

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn not_leader_status(state: &HaSharedState) -> Status {
        let mut metadata = tonic::metadata::MetadataMap::new();
        if let Ok(v) = CODE_NOT_LEADER.parse() {
            metadata.insert(HEADER_RSQLITE_CODE, v);
        }
        if let Some(ref leader) = state.active_leader_id
            && let Ok(v) = leader.parse()
        {
            metadata.insert(HEADER_RSQLITE_LEADER_ID, v);
        }
        if let Ok(v) = state.generation.to_string().parse() {
            metadata.insert(HEADER_RSQLITE_GENERATION, v);
        }
        if let Some(ref ep) = state.active_leader_endpoint
            && let Ok(v) = ep.parse()
        {
            metadata.insert(HEADER_RSQLITE_LEADER_ENDPOINT, v);
        }

        Status::with_metadata(
            tonic::Code::FailedPrecondition,
            "node is not the active writer",
            metadata,
        )
    }

    // `Status` is mandated by the generated `SqlGateway` trait's error type at
    // every call site (`self.check_write_access()?` inside a method returning
    // `Result<_, Status>`); boxing it here would just add an unbox step per
    // call for no benefit on this non-hot-path.
    #[allow(clippy::result_large_err)]
    fn check_write_access(&self) -> std::result::Result<u64, Status> {
        let state = self.ha_state.read().unwrap();
        let now = Self::now_secs();
        if !state.is_writer(now) {
            return Err(Self::not_leader_status(&state));
        }
        Ok(state.generation)
    }

    #[allow(clippy::result_large_err)]
    fn check_read_access(
        &self,
        consistency: ConsistencyLevel,
    ) -> std::result::Result<(u64, bool), Status> {
        let state = self.ha_state.read().unwrap();
        let now = Self::now_secs();
        let is_writer = state.is_writer(now);

        match consistency {
            ConsistencyLevel::Strong => {
                if !is_writer {
                    return Err(Self::not_leader_status(&state));
                }
                Ok((state.generation, false))
            }
            ConsistencyLevel::Eventual => {
                if is_writer {
                    Ok((state.generation, false))
                } else if state.allow_replica_reads {
                    Ok((state.generation, true))
                } else {
                    Err(Self::not_leader_status(&state))
                }
            }
        }
    }
}

#[tonic::async_trait]
impl SqlGateway for SqlGatewayServer {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> std::result::Result<Response<ExecuteResponse>, Status> {
        let generation = self.check_write_access()?;
        let req = request.into_inner();

        let stmt = req
            .statement
            .ok_or_else(|| Status::invalid_argument("missing statement in execute request"))?;

        let engine = self.engine.clone();
        let resp =
            tokio::task::spawn_blocking(move || engine.execute(&req.database, &stmt, generation))
                .await
                .map_err(|e| Status::internal(format!("task join error: {e}")))?
                .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(resp))
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        let consistency =
            ConsistencyLevel::try_from(req.consistency).unwrap_or(ConsistencyLevel::Strong);
        let (generation, is_replica) = self.check_read_access(consistency)?;

        let stmt = req
            .statement
            .ok_or_else(|| Status::invalid_argument("missing statement in query request"))?;

        let engine = self.engine.clone();
        let max_rows = req.max_rows;
        let resp = tokio::task::spawn_blocking(move || {
            engine.query(&req.database, &stmt, max_rows, generation, is_replica)
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(resp))
    }

    type StreamQueryStream =
        Pin<Box<dyn Stream<Item = std::result::Result<QueryChunk, Status>> + Send + 'static>>;

    async fn stream_query(
        &self,
        request: Request<QueryRequest>,
    ) -> std::result::Result<Response<Self::StreamQueryStream>, Status> {
        let req = request.into_inner();
        let consistency =
            ConsistencyLevel::try_from(req.consistency).unwrap_or(ConsistencyLevel::Strong);
        let (_generation, _is_replica) = self.check_read_access(consistency)?;

        let stmt = req
            .statement
            .ok_or_else(|| Status::invalid_argument("missing statement in stream query request"))?;

        let engine = self.engine.clone();
        let max_rows = req.max_rows;
        let chunk_size = if req.chunk_size > 0 {
            req.chunk_size as usize
        } else {
            100
        };

        let chunks = tokio::task::spawn_blocking(move || {
            engine.stream_query_chunks(&req.database, &stmt, max_rows, chunk_size)
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = tokio::sync::mpsc::channel(16);
        tokio::spawn(async move {
            for chunk in chunks {
                if tx.send(Ok(chunk)).await.is_err() {
                    break;
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::StreamQueryStream
        ))
    }

    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> std::result::Result<Response<BatchResponse>, Status> {
        let req = request.into_inner();
        let tx_mode = BatchTransactionMode::try_from(req.transaction_mode)
            .unwrap_or(BatchTransactionMode::Deferred);

        // For batch operations containing potential writes, require writer access
        let generation = self.check_write_access()?;

        let engine = self.engine.clone();
        let stop_on_error = req.stop_on_error;
        let resp = tokio::task::spawn_blocking(move || {
            engine.batch(
                &req.database,
                &req.statements,
                tx_mode,
                stop_on_error,
                generation,
            )
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(resp))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> std::result::Result<Response<ClusterStatusResponse>, Status> {
        let now = Self::now_secs();
        let (node_id, role, local_generation, lease, current_leader_id, current_leader_endpoint) = {
            let state = self.ha_state.read().unwrap();
            let is_writer = state.is_writer(now);
            let role = if is_writer {
                NodeRole::Writer
            } else {
                NodeRole::Replica
            };
            let lease = state.lease_record.as_ref().map(|l| LeaseStatus {
                is_held: true,
                holder_node_id: l.holder_node_id.clone(),
                generation: l.generation,
                renewed_at_secs: l.renewed_at_secs,
                ttl_secs: l.ttl_secs,
                is_expired: l.is_expired(now),
            });
            (
                state.node_id.clone(),
                role,
                state.generation,
                lease,
                state.active_leader_id.clone().unwrap_or_default(),
                state.active_leader_endpoint.clone().unwrap_or_default(),
            )
        };

        let engine = self.engine.clone();
        let databases = tokio::task::spawn_blocking(move || engine.list_databases())
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .unwrap_or_default();

        Ok(Response::new(ClusterStatusResponse {
            node_id,
            role: role as i32,
            local_generation,
            lease,
            current_leader_id,
            current_leader_endpoint,
            databases,
            uptime_secs: self.started_at.elapsed().as_secs(),
            version: SERVER_VERSION.to_string(),
        }))
    }
}
