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
    ConsistencyLevel, DropDatabaseRequest, DropDatabaseResponse, ExecuteRequest, ExecuteResponse,
    LeaseStatus, NodeRole, QueryChunk, QueryRequest, QueryResponse, sql_gateway_server::SqlGateway,
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
        let state = self
            .ha_state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
        let state = self
            .ha_state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> std::result::Result<Response<DropDatabaseResponse>, Status> {
        let generation = self.check_write_access()?;
        let req = request.into_inner();

        let engine = self.engine.clone();
        let existed = tokio::task::spawn_blocking(move || engine.drop_database(&req.database))
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DropDatabaseResponse {
            existed,
            generation,
        }))
    }

    async fn get_cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> std::result::Result<Response<ClusterStatusResponse>, Status> {
        let now = Self::now_secs();
        let (node_id, role, local_generation, lease, current_leader_id, current_leader_endpoint) = {
            let state = self
                .ha_state
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ha::LeaseRecord;
    use crate::proto::rsqlite::v1::Statement;
    use tempfile::tempdir;
    use tokio_stream::StreamExt;

    fn create_test_server(
        is_writer: bool,
        allow_replica_reads: bool,
    ) -> (SqlGatewayServer, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();
        let now = SqlGatewayServer::now_secs();
        let mut state = HaSharedState::new("node-1".to_string(), allow_replica_reads);
        state.generation = 42;

        if is_writer {
            state.role = crate::ha::NodeRole::Writer;
            state.lease_record = Some(LeaseRecord {
                holder_node_id: "node-1".to_string(),
                generation: 42,
                renewed_at_secs: now,
                ttl_secs: 60,
            });
        } else {
            state.role = crate::ha::NodeRole::Replica;
            state.active_leader_id = Some("node-writer".to_string());
            state.active_leader_endpoint = Some("http://127.0.0.1:9090".to_string());
        }

        let ha_state = Arc::new(RwLock::new(state));
        let server = SqlGatewayServer::new(engine, ha_state);
        (server, dir)
    }

    #[tokio::test]
    async fn execute_on_writer_succeeds() {
        let (server, _dir) = create_test_server(true, false);

        let req = Request::new(ExecuteRequest {
            database: "test.db".into(),
            statement: Some(Statement {
                sql: "CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT);".into(),
                parameters: None,
            }),
        });

        let resp = server.execute(req).await.unwrap().into_inner();
        assert_eq!(resp.generation, 42);

        let insert_req = Request::new(ExecuteRequest {
            database: "test.db".into(),
            statement: Some(Statement {
                sql: "INSERT INTO kv (k, v) VALUES ('hello', 'world');".into(),
                parameters: None,
            }),
        });
        let insert_resp = server.execute(insert_req).await.unwrap().into_inner();
        assert_eq!(insert_resp.rows_affected, 1);
        assert_eq!(insert_resp.last_insert_rowid, 1);
    }

    #[tokio::test]
    async fn execute_on_replica_fails_precondition_with_leader_metadata() {
        let (server, _dir) = create_test_server(false, false);

        let req = Request::new(ExecuteRequest {
            database: "test.db".into(),
            statement: Some(Statement {
                sql: "CREATE TABLE t (id INT);".into(),
                parameters: None,
            }),
        });

        let err = server.execute(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
        let meta = err.metadata();
        assert_eq!(meta.get(HEADER_RSQLITE_CODE).unwrap(), CODE_NOT_LEADER);
        assert_eq!(meta.get(HEADER_RSQLITE_LEADER_ID).unwrap(), "node-writer");
        assert_eq!(
            meta.get(HEADER_RSQLITE_LEADER_ENDPOINT).unwrap(),
            "http://127.0.0.1:9090"
        );
        assert_eq!(meta.get(HEADER_RSQLITE_GENERATION).unwrap(), "42");
    }

    #[tokio::test]
    async fn execute_missing_statement_returns_invalid_argument() {
        let (server, _dir) = create_test_server(true, false);

        let req = Request::new(ExecuteRequest {
            database: "test.db".into(),
            statement: None,
        });

        let err = server.execute(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn execute_invalid_sql_returns_internal() {
        let (server, _dir) = create_test_server(true, false);

        let req = Request::new(ExecuteRequest {
            database: "test.db".into(),
            statement: Some(Statement {
                sql: "INVALID SQL STATEMENT;".into(),
                parameters: None,
            }),
        });

        let err = server.execute(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Internal);
    }

    #[tokio::test]
    async fn query_strong_consistency_on_writer_succeeds() {
        let (server, _dir) = create_test_server(true, false);

        server
            .execute(Request::new(ExecuteRequest {
                database: "query.db".into(),
                statement: Some(Statement {
                    sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT);".into(),
                    parameters: None,
                }),
            }))
            .await
            .unwrap();

        server
            .execute(Request::new(ExecuteRequest {
                database: "query.db".into(),
                statement: Some(Statement {
                    sql: "INSERT INTO items VALUES (1, 'item1');".into(),
                    parameters: None,
                }),
            }))
            .await
            .unwrap();

        let req = Request::new(QueryRequest {
            database: "query.db".into(),
            statement: Some(Statement {
                sql: "SELECT id, name FROM items;".into(),
                parameters: None,
            }),
            max_rows: 10,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 0,
        });

        let resp = server.query(req).await.unwrap().into_inner();
        assert_eq!(resp.total_rows, 1);
        assert_eq!(resp.rows.len(), 1);
        assert_eq!(resp.columns.len(), 2);
        assert!(!resp.is_replica_read);
    }

    #[tokio::test]
    async fn query_strong_consistency_on_replica_fails() {
        let (server, _dir) = create_test_server(false, true);

        let req = Request::new(QueryRequest {
            database: "query.db".into(),
            statement: Some(Statement {
                sql: "SELECT 1;".into(),
                parameters: None,
            }),
            max_rows: 10,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 0,
        });

        let err = server.query(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn query_eventual_consistency_on_replica_with_reads_allowed() {
        let (server, _dir) = create_test_server(false, true);
        server.engine.open_connection("query.db", false).unwrap();

        let req = Request::new(QueryRequest {
            database: "query.db".into(),
            statement: Some(Statement {
                sql: "SELECT 42 as num;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: ConsistencyLevel::Eventual as i32,
            chunk_size: 0,
        });

        let resp = server.query(req).await.unwrap().into_inner();
        assert_eq!(resp.total_rows, 1);
        assert!(resp.is_replica_read);
    }

    #[tokio::test]
    async fn query_eventual_consistency_on_replica_disallowed_fails() {
        let (server, _dir) = create_test_server(false, false);

        let req = Request::new(QueryRequest {
            database: "query.db".into(),
            statement: Some(Statement {
                sql: "SELECT 42 as num;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: ConsistencyLevel::Eventual as i32,
            chunk_size: 0,
        });

        let err = server.query(req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn query_missing_statement_and_invalid_sql() {
        let (server, _dir) = create_test_server(true, false);

        let req_missing = Request::new(QueryRequest {
            database: "q.db".into(),
            statement: None,
            max_rows: 0,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 0,
        });
        assert_eq!(
            server.query(req_missing).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let req_invalid = Request::new(QueryRequest {
            database: "q.db".into(),
            statement: Some(Statement {
                sql: "SELECT FROM WHERE;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 0,
        });
        assert_eq!(
            server.query(req_invalid).await.unwrap_err().code(),
            tonic::Code::Internal
        );
    }

    #[tokio::test]
    async fn stream_query_on_writer_and_replica() {
        let (server, _dir) = create_test_server(true, false);

        server
            .execute(Request::new(ExecuteRequest {
                database: "stream.db".into(),
                statement: Some(Statement {
                    sql: "CREATE TABLE nums (n INT);".into(),
                    parameters: None,
                }),
            }))
            .await
            .unwrap();

        for i in 1..=5 {
            server
                .execute(Request::new(ExecuteRequest {
                    database: "stream.db".into(),
                    statement: Some(Statement {
                        sql: format!("INSERT INTO nums VALUES ({i});"),
                        parameters: None,
                    }),
                }))
                .await
                .unwrap();
        }

        // Query with chunk_size 2
        let req = Request::new(QueryRequest {
            database: "stream.db".into(),
            statement: Some(Statement {
                sql: "SELECT n FROM nums ORDER BY n;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 2,
        });

        let resp = server.stream_query(req).await.unwrap();
        let mut stream = resp.into_inner();
        let mut total_chunks = 0;
        let mut total_rows = 0;

        while let Some(chunk_res) = stream.next().await {
            let chunk = chunk_res.unwrap();
            total_chunks += 1;
            total_rows += chunk.rows.len();
        }

        assert!(total_chunks >= 3);
        assert_eq!(total_rows, 5);

        // Missing statement
        let req_missing = Request::new(QueryRequest {
            database: "stream.db".into(),
            statement: None,
            max_rows: 0,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 0,
        });
        // `Response<Self::StreamQueryStream>` isn't `Debug` (it boxes a
        // `dyn Stream`), so extract the error via `.err()` rather than
        // `unwrap_err()`.
        let missing_err = server.stream_query(req_missing).await.err().unwrap();
        assert_eq!(missing_err.code(), tonic::Code::InvalidArgument);

        // Replica failure
        let (replica_server, _dir2) = create_test_server(false, false);
        let req_replica = Request::new(QueryRequest {
            database: "stream.db".into(),
            statement: Some(Statement {
                sql: "SELECT 1;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 0,
        });
        let replica_err = replica_server
            .stream_query(req_replica)
            .await
            .err()
            .unwrap();
        assert_eq!(replica_err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn batch_on_writer_and_replica() {
        let (server, _dir) = create_test_server(true, false);

        let stmts = vec![
            Statement {
                sql: "CREATE TABLE b (id INT);".into(),
                parameters: None,
            },
            Statement {
                sql: "INSERT INTO b VALUES (100);".into(),
                parameters: None,
            },
            Statement {
                sql: "SELECT id FROM b;".into(),
                parameters: None,
            },
        ];

        let req = Request::new(BatchRequest {
            database: "batch.db".into(),
            statements: stmts,
            transaction_mode: BatchTransactionMode::Immediate as i32,
            stop_on_error: true,
        });

        let resp = server.batch(req).await.unwrap().into_inner();
        assert_eq!(resp.results.len(), 3);
        assert!(resp.committed);
        assert_eq!(resp.generation, 42);

        // Replica rejects batch
        let (replica_server, _dir2) = create_test_server(false, false);
        let rep_req = Request::new(BatchRequest {
            database: "batch.db".into(),
            statements: vec![],
            transaction_mode: BatchTransactionMode::None as i32,
            stop_on_error: false,
        });
        assert_eq!(
            replica_server.batch(rep_req).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn drop_database_on_writer_and_replica() {
        let (server, _dir) = create_test_server(true, false);

        // Create db first
        server
            .execute(Request::new(ExecuteRequest {
                database: "dropme.db".into(),
                statement: Some(Statement {
                    sql: "CREATE TABLE t (x INT);".into(),
                    parameters: None,
                }),
            }))
            .await
            .unwrap();

        let req = Request::new(DropDatabaseRequest {
            database: "dropme.db".into(),
        });
        let resp = server.drop_database(req).await.unwrap().into_inner();
        assert!(resp.existed);
        assert_eq!(resp.generation, 42);

        // Dropping non-existent
        let req_missing = Request::new(DropDatabaseRequest {
            database: "nonexistent.db".into(),
        });
        let resp_missing = server
            .drop_database(req_missing)
            .await
            .unwrap()
            .into_inner();
        assert!(!resp_missing.existed);

        // Replica rejects drop
        let (replica_server, _dir2) = create_test_server(false, false);
        let rep_req = Request::new(DropDatabaseRequest {
            database: "dropme.db".into(),
        });
        assert_eq!(
            replica_server
                .drop_database(rep_req)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn get_cluster_status_writer_and_replica() {
        let (writer_server, _dir) = create_test_server(true, false);

        writer_server
            .execute(Request::new(ExecuteRequest {
                database: "stat.db".into(),
                statement: Some(Statement {
                    sql: "CREATE TABLE s (id INT);".into(),
                    parameters: None,
                }),
            }))
            .await
            .unwrap();

        let req = Request::new(ClusterStatusRequest {});
        let resp = writer_server
            .get_cluster_status(req)
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.node_id, "node-1");
        assert_eq!(resp.role, NodeRole::Writer as i32);
        assert_eq!(resp.local_generation, 42);
        assert!(resp.lease.is_some());
        let lease = resp.lease.unwrap();
        assert!(lease.is_held);
        assert_eq!(lease.holder_node_id, "node-1");
        assert!(!resp.databases.is_empty());
        assert!(!resp.version.is_empty());

        let (replica_server, _dir2) = create_test_server(false, false);
        let rep_resp = replica_server
            .get_cluster_status(Request::new(ClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(rep_resp.role, NodeRole::Replica as i32);
        assert_eq!(rep_resp.current_leader_id, "node-writer");
        assert_eq!(rep_resp.current_leader_endpoint, "http://127.0.0.1:9090");
    }

    #[tokio::test]
    async fn query_eventual_consistency_on_writer_uses_writer_generation() {
        // `check_read_access` has a distinct branch for `Eventual` consistency
        // when the local node *is* the writer (it should behave like a
        // non-replica read rather than falling through to the
        // `allow_replica_reads` check).
        let (server, _dir) = create_test_server(true, false);

        server
            .execute(Request::new(ExecuteRequest {
                database: "eventual_writer.db".into(),
                statement: Some(Statement {
                    sql: "CREATE TABLE t (id INT);".into(),
                    parameters: None,
                }),
            }))
            .await
            .unwrap();

        let req = Request::new(QueryRequest {
            database: "eventual_writer.db".into(),
            statement: Some(Statement {
                sql: "SELECT 1;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: ConsistencyLevel::Eventual as i32,
            chunk_size: 0,
        });

        let resp = server.query(req).await.unwrap().into_inner();
        assert_eq!(resp.total_rows, 1);
        assert_eq!(resp.generation, 42);
        assert!(!resp.is_replica_read);
    }

    #[tokio::test]
    async fn query_out_of_range_consistency_falls_back_to_strong() {
        let (server, _dir) = create_test_server(true, false);
        // `query` always opens the database read-only, so the file must
        // already exist even for a query that doesn't touch a table.
        server
            .engine
            .open_connection("consistency_fallback.db", false)
            .unwrap();

        let req = Request::new(QueryRequest {
            database: "consistency_fallback.db".into(),
            statement: Some(Statement {
                sql: "SELECT 1;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: 999, // not a valid ConsistencyLevel variant
            chunk_size: 0,
        });

        // Falls back to `Strong`, which succeeds on the writer.
        let resp = server.query(req).await.unwrap().into_inner();
        assert!(!resp.is_replica_read);

        // On a replica, the same out-of-range value should fail exactly like
        // an explicit `Strong` request would.
        let (replica_server, _dir2) = create_test_server(false, true);
        let replica_req = Request::new(QueryRequest {
            database: "consistency_fallback.db".into(),
            statement: Some(Statement {
                sql: "SELECT 1;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: -1,
            chunk_size: 0,
        });
        let err = replica_server.query(replica_req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn batch_out_of_range_transaction_mode_falls_back_to_deferred() {
        let (server, _dir) = create_test_server(true, false);

        let req = Request::new(BatchRequest {
            database: "batch_fallback.db".into(),
            statements: vec![Statement {
                sql: "CREATE TABLE t (id INT);".into(),
                parameters: None,
            }],
            transaction_mode: 999, // not a valid BatchTransactionMode variant
            stop_on_error: true,
        });

        let resp = server.batch(req).await.unwrap().into_inner();
        assert_eq!(resp.results.len(), 1);
        assert!(resp.committed);
    }

    #[tokio::test]
    async fn stream_query_handles_dropped_receiver_without_panic() {
        // Exercises the `if tx.send(Ok(chunk)).await.is_err() { break; }` path:
        // when the client drops the response stream before it is fully
        // drained, the forwarding task spawned by `stream_query` must observe
        // the closed channel and exit cleanly instead of panicking or hanging.
        let (server, _dir) = create_test_server(true, false);

        server
            .execute(Request::new(ExecuteRequest {
                database: "dropped.db".into(),
                statement: Some(Statement {
                    sql: "CREATE TABLE nums (n INT);".into(),
                    parameters: None,
                }),
            }))
            .await
            .unwrap();

        for i in 1..=3 {
            server
                .execute(Request::new(ExecuteRequest {
                    database: "dropped.db".into(),
                    statement: Some(Statement {
                        sql: format!("INSERT INTO nums VALUES ({i});"),
                        parameters: None,
                    }),
                }))
                .await
                .unwrap();
        }

        let req = Request::new(QueryRequest {
            database: "dropped.db".into(),
            statement: Some(Statement {
                sql: "SELECT n FROM nums ORDER BY n;".into(),
                parameters: None,
            }),
            max_rows: 0,
            consistency: ConsistencyLevel::Strong as i32,
            chunk_size: 1,
        });

        let resp = server.stream_query(req).await.unwrap();
        let stream = resp.into_inner();
        // Drop before the spawned forwarding task has a chance to run its
        // first `tx.send`, so that send observes a closed channel.
        drop(stream);

        // Yield repeatedly so the spawned task actually runs to completion.
        // If it panicked or hung, this test would hang or the runtime would
        // report a panic on shutdown.
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn get_cluster_status_recovers_from_poisoned_ha_lock() {
        // Exercises the `unwrap_or_else(|poisoned| poisoned.into_inner())`
        // recovery path: if some other thread panics while holding the write
        // lock on `ha_state`, the lock becomes poisoned, but readers here must
        // still recover the inner state rather than propagating a panic.
        let (server, _dir) = create_test_server(true, false);

        let ha_state = server.ha_state.clone();
        let handle = std::thread::spawn(move || {
            let _guard = ha_state.write().unwrap_or_else(|e| e.into_inner());
            panic!("intentional poison for test");
        });
        assert!(handle.join().is_err(), "helper thread should have panicked");
        assert!(server.ha_state.is_poisoned());

        let resp = server
            .get_cluster_status(Request::new(ClusterStatusRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.node_id, "node-1");
        assert_eq!(resp.role, NodeRole::Writer as i32);
    }
}
