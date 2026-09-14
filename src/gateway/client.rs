//! Unified database client supporting both remote gRPC clusters and standalone local engines.

use std::path::Path;

use crate::error::{Result, SyncError};
use crate::gateway::engine::DatabaseEngine;
use rsqlite_rsync_client::proto::{
    BatchResponse, BatchTransactionMode, ClusterStatusResponse, ConsistencyLevel,
    DropDatabaseResponse, ExecuteResponse, NodeRole, Parameters, QueryResponse, Statement,
};
use rsqlite_rsync_client::{ClientTarget, SqlGatewayClient};

/// Backend engine for the unified client.
pub enum ClientBackend {
    /// Remote gRPC client communicating with a SQL Gateway cluster.
    Remote(Box<SqlGatewayClient>),
    /// In-process SQLite engine executing directly against a local data directory.
    Local(DatabaseEngine),
}

/// A unified SQLite client that operates seamlessly either against a remote
/// HA cluster via gRPC or directly in-process on a local standalone edge device.
pub struct Client {
    backend: ClientBackend,
}

impl Client {
    /// Create a client targeting either a remote cluster or a local directory.
    pub fn new(target: ClientTarget) -> Result<Self> {
        match target {
            ClientTarget::Remote { config } => Ok(Self {
                backend: ClientBackend::Remote(Box::new(SqlGatewayClient::new(config))),
            }),
            ClientTarget::Local { data_dir } => {
                let engine = DatabaseEngine::new(data_dir)?;
                Ok(Self {
                    backend: ClientBackend::Local(engine),
                })
            }
        }
    }

    /// Construct a client directly from a `SqlGatewayClient`.
    pub fn from_remote(remote: SqlGatewayClient) -> Self {
        Self {
            backend: ClientBackend::Remote(Box::new(remote)),
        }
    }

    /// Construct a client directly from a local data directory.
    pub fn from_local_dir(data_dir: impl AsRef<Path>) -> Result<Self> {
        let engine = DatabaseEngine::new(data_dir)?;
        Ok(Self {
            backend: ClientBackend::Local(engine),
        })
    }

    /// Returns whether this client is running in local standalone mode.
    pub fn is_local(&self) -> bool {
        matches!(self.backend, ClientBackend::Local(_))
    }

    /// Returns whether this client is communicating with a remote cluster.
    pub fn is_remote(&self) -> bool {
        matches!(self.backend, ClientBackend::Remote(_))
    }

    /// Execute a write statement (DML/DDL).
    pub async fn execute(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
    ) -> Result<ExecuteResponse> {
        match &mut self.backend {
            ClientBackend::Remote(client) => client
                .execute(database, sql, parameters)
                .await
                .map_err(Into::into),
            ClientBackend::Local(engine) => {
                let stmt = Statement {
                    sql: sql.to_string(),
                    parameters,
                };
                let engine = engine.clone();
                let db_name = database.to_string();
                tokio::task::spawn_blocking(move || engine.execute(&db_name, &stmt, 1))
                    .await
                    .map_err(|e| SyncError::Protocol(format!("task join error: {e}")))?
            }
        }
    }

    /// Execute a read query (SELECT / read pragma).
    pub async fn query(
        &mut self,
        database: &str,
        sql: &str,
        parameters: Option<Parameters>,
        max_rows: u32,
        consistency: ConsistencyLevel,
    ) -> Result<QueryResponse> {
        match &mut self.backend {
            ClientBackend::Remote(client) => client
                .query(database, sql, parameters, max_rows, consistency)
                .await
                .map_err(Into::into),
            ClientBackend::Local(engine) => {
                let stmt = Statement {
                    sql: sql.to_string(),
                    parameters,
                };
                let engine = engine.clone();
                let db_name = database.to_string();
                tokio::task::spawn_blocking(move || {
                    engine.query(&db_name, &stmt, max_rows, 1, false)
                })
                .await
                .map_err(|e| SyncError::Protocol(format!("task join error: {e}")))?
            }
        }
    }

    /// Execute a batch of statements atomically within an optional transaction.
    pub async fn batch(
        &mut self,
        database: &str,
        statements: Vec<Statement>,
        tx_mode: BatchTransactionMode,
        stop_on_error: bool,
    ) -> Result<BatchResponse> {
        match &mut self.backend {
            ClientBackend::Remote(client) => client
                .batch(database, statements, tx_mode, stop_on_error)
                .await
                .map_err(Into::into),
            ClientBackend::Local(engine) => {
                let engine = engine.clone();
                let db_name = database.to_string();
                tokio::task::spawn_blocking(move || {
                    engine.batch(&db_name, &statements, tx_mode, stop_on_error, 1)
                })
                .await
                .map_err(|e| SyncError::Protocol(format!("task join error: {e}")))?
            }
        }
    }

    /// Delete a database file (and its WAL/SHM sidecars).
    pub async fn drop_database(&mut self, database: &str) -> Result<DropDatabaseResponse> {
        match &mut self.backend {
            ClientBackend::Remote(client) => {
                client.drop_database(database).await.map_err(Into::into)
            }
            ClientBackend::Local(engine) => {
                let engine = engine.clone();
                let db_name = database.to_string();
                let existed = tokio::task::spawn_blocking(move || engine.drop_database(&db_name))
                    .await
                    .map_err(|e| SyncError::Protocol(format!("task join error: {e}")))??;
                Ok(DropDatabaseResponse {
                    existed,
                    generation: 1,
                })
            }
        }
    }

    /// Get cluster status (or local standalone status).
    pub async fn get_cluster_status(&mut self) -> Result<ClusterStatusResponse> {
        match &mut self.backend {
            ClientBackend::Remote(client) => client.get_cluster_status().await.map_err(Into::into),
            ClientBackend::Local(engine) => {
                let engine = engine.clone();
                let databases = tokio::task::spawn_blocking(move || engine.list_databases())
                    .await
                    .map_err(|e| SyncError::Protocol(format!("task join error: {e}")))?
                    .unwrap_or_default();
                Ok(ClusterStatusResponse {
                    node_id: "standalone-edge".to_string(),
                    role: NodeRole::Writer as i32,
                    local_generation: 1,
                    lease: None,
                    current_leader_id: "standalone-edge".to_string(),
                    current_leader_endpoint: "local://in-process".to_string(),
                    databases,
                    uptime_secs: 0,
                    version: env!("CARGO_PKG_VERSION").to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsqlite_rsync_client::proto::Value;
    use rsqlite_rsync_client::proto::value::Value as ProtoValueInner;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_client_crud_and_status() {
        let dir = tempdir().unwrap();
        let mut client = Client::from_local_dir(dir.path()).unwrap();

        assert!(client.is_local());
        assert!(!client.is_remote());

        // Create table
        let exec_res = client
            .execute(
                "test.db",
                "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, score REAL);",
                None,
            )
            .await
            .unwrap();
        assert_eq!(exec_res.rows_affected, 0);

        // Insert row with parameters
        let params = Parameters {
            positional: vec![
                Value {
                    value: Some(ProtoValueInner::TextValue("Alice".to_string())),
                },
                Value {
                    value: Some(ProtoValueInner::FloatValue(95.5)),
                },
            ],
            named: Vec::new(),
        };
        let insert_res = client
            .execute(
                "test.db",
                "INSERT INTO users (name, score) VALUES (?, ?);",
                Some(params),
            )
            .await
            .unwrap();
        assert_eq!(insert_res.rows_affected, 1);
        assert_eq!(insert_res.last_insert_rowid, 1);

        // Query rows
        let query_res = client
            .query(
                "test.db",
                "SELECT id, name, score FROM users;",
                None,
                10,
                ConsistencyLevel::Strong,
            )
            .await
            .unwrap();
        assert_eq!(query_res.total_rows, 1);
        let col_names: Vec<&str> = query_res.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(col_names, vec!["id", "name", "score"]);
        let row = &query_res.rows[0];
        assert_eq!(
            row.values[1].value,
            Some(ProtoValueInner::TextValue("Alice".to_string()))
        );

        // Batch execution
        let stmts = vec![
            Statement {
                sql: "INSERT INTO users (name, score) VALUES ('Bob', 88.0);".to_string(),
                parameters: None,
            },
            Statement {
                sql: "INSERT INTO users (name, score) VALUES ('Charlie', 72.5);".to_string(),
                parameters: None,
            },
        ];
        let batch_res = client
            .batch("test.db", stmts, BatchTransactionMode::Immediate, true)
            .await
            .unwrap();
        assert_eq!(batch_res.results.len(), 2);

        // Cluster status in standalone
        let status = client.get_cluster_status().await.unwrap();
        assert_eq!(status.node_id, "standalone-edge");
        assert_eq!(status.role, NodeRole::Writer as i32);
        assert!(status.databases.iter().any(|d| d.name == "test.db"));

        // Drop database
        let drop_res = client.drop_database("test.db").await.unwrap();
        assert!(drop_res.existed);

        let drop_res2 = client.drop_database("test.db").await.unwrap();
        assert!(!drop_res2.existed);
    }
}
