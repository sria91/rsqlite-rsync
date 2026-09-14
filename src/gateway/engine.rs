//! Database Engine managing SQLite databases with WAL mode and statement execution.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use libsqlite3_sys as ffi;

use crate::db::{Connection, PreparedStatement, SqlValue, StepResult};
use crate::error::{Result, SyncError};
use crate::proto::rsqlite::v1::{
    BatchResponse, BatchTransactionMode, ColumnHeader, ColumnType, DatabaseInfo, ExecuteResponse,
    NamedParameter, QueryChunk, QueryResponse, Row, Statement, StatementResult, Value,
    value::Value as ProtoValueInner,
};

/// Thread-safe SQLite database manager for a root data directory.
#[derive(Clone)]
pub struct DatabaseEngine {
    data_dir: PathBuf,
    db_mutexes: Arc<Mutex<HashMap<String, Arc<Mutex<()>>>>>,
}

impl DatabaseEngine {
    /// Create a new DatabaseEngine rooted at `data_dir`.
    pub fn new(data_dir: impl AsRef<Path>) -> Result<Self> {
        let path = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;
        Ok(Self {
            data_dir: path,
            db_mutexes: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Resolve and validate a database path under the data directory.
    pub fn resolve_db_path(&self, db_name: &str) -> Result<PathBuf> {
        let trimmed = db_name.trim();
        if trimmed.is_empty() {
            return Err(SyncError::Protocol("database name cannot be empty".into()));
        }

        // Prevent path traversal
        let p = Path::new(trimmed);
        for comp in p.components() {
            match comp {
                std::path::Component::Normal(_) => {}
                _ => {
                    return Err(SyncError::Protocol(format!(
                        "invalid database name '{trimmed}': path traversal not permitted"
                    )));
                }
            }
        }

        let full_path = self.data_dir.join(p);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(full_path)
    }

    fn get_db_lock(&self, db_name: &str) -> Arc<Mutex<()>> {
        let trimmed = db_name.trim();
        let mut map = self.db_mutexes.lock().unwrap();
        map.entry(trimmed.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    /// Open a connection with WAL mode and busy timeout configured.
    pub fn open_connection(&self, db_name: &str, read_only: bool) -> Result<Connection> {
        let path = self.resolve_db_path(db_name)?;
        let flags = if read_only {
            ffi::SQLITE_OPEN_READONLY
        } else {
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE
        };
        let conn = Connection::open(&path, flags)?;

        if !read_only {
            // Configure WAL mode and sane defaults for concurrent access
            conn.exec("PRAGMA journal_mode=WAL;")?;
            conn.exec("PRAGMA synchronous=NORMAL;")?;
            conn.exec("PRAGMA busy_timeout=5000;")?;
        }
        Ok(conn)
    }

    /// Execute a write statement (DML/DDL).
    pub fn execute(
        &self,
        db_name: &str,
        stmt_proto: &Statement,
        generation: u64,
    ) -> Result<ExecuteResponse> {
        let lock = self.get_db_lock(db_name);
        let _guard = lock.lock().unwrap();

        let start = Instant::now();
        let conn = self.open_connection(db_name, false)?;

        let mut prepared = conn.prepare(&stmt_proto.sql)?;
        bind_parameters(&mut prepared, stmt_proto.parameters.as_ref())?;

        let _ = prepared.step()?;
        let rows_affected = conn.changes();
        let last_insert_rowid = conn.last_insert_rowid();
        let execution_time_us = start.elapsed().as_micros() as u64;

        Ok(ExecuteResponse {
            rows_affected,
            last_insert_rowid,
            execution_time_us,
            generation,
        })
    }

    /// Execute a read query (SELECT / read pragma).
    pub fn query(
        &self,
        db_name: &str,
        stmt_proto: &Statement,
        max_rows: u32,
        generation: u64,
        is_replica_read: bool,
    ) -> Result<QueryResponse> {
        let start = Instant::now();
        let conn = self.open_connection(db_name, true)?;

        let mut prepared = conn.prepare(&stmt_proto.sql)?;
        bind_parameters(&mut prepared, stmt_proto.parameters.as_ref())?;

        let col_count = prepared.column_count();
        let mut columns = Vec::with_capacity(col_count as usize);
        for idx in 0..col_count {
            let name = prepared.column_name(idx);
            let declared_type = prepared.column_decltype(idx).unwrap_or_default();
            let column_type = map_decltype_to_proto(&declared_type);
            columns.push(ColumnHeader {
                name,
                column_type: column_type as i32,
                declared_type,
            });
        }

        let mut rows = Vec::new();
        let mut total_rows = 0;

        while let StepResult::Row = prepared.step()? {
            total_rows += 1;
            let mut values = Vec::with_capacity(col_count as usize);
            for idx in 0..col_count {
                let sql_val = prepared.column_value(idx);
                values.push(sql_value_to_proto(sql_val));
            }
            rows.push(Row { values });

            if max_rows > 0 && total_rows >= max_rows as u64 {
                break;
            }
        }

        let execution_time_us = start.elapsed().as_micros() as u64;

        Ok(QueryResponse {
            columns,
            rows,
            total_rows,
            execution_time_us,
            generation,
            is_replica_read,
        })
    }

    /// Stream rows from a query in chunks.
    pub fn stream_query_chunks(
        &self,
        db_name: &str,
        stmt_proto: &Statement,
        max_rows: u32,
        chunk_size: usize,
    ) -> Result<Vec<QueryChunk>> {
        let start = Instant::now();
        let conn = self.open_connection(db_name, true)?;

        let mut prepared = conn.prepare(&stmt_proto.sql)?;
        bind_parameters(&mut prepared, stmt_proto.parameters.as_ref())?;

        let col_count = prepared.column_count();
        let mut columns = Vec::with_capacity(col_count as usize);
        for idx in 0..col_count {
            let name = prepared.column_name(idx);
            let declared_type = prepared.column_decltype(idx).unwrap_or_default();
            let column_type = map_decltype_to_proto(&declared_type);
            columns.push(ColumnHeader {
                name,
                column_type: column_type as i32,
                declared_type,
            });
        }

        let mut chunks = Vec::new();
        let mut current_chunk_rows = Vec::new();
        let mut total_rows = 0;
        let mut is_first_chunk = true;

        while let StepResult::Row = prepared.step()? {
            total_rows += 1;
            let mut values = Vec::with_capacity(col_count as usize);
            for idx in 0..col_count {
                let sql_val = prepared.column_value(idx);
                values.push(sql_value_to_proto(sql_val));
            }
            current_chunk_rows.push(Row { values });

            if current_chunk_rows.len() >= chunk_size {
                let chunk_cols = if is_first_chunk {
                    is_first_chunk = false;
                    columns.clone()
                } else {
                    Vec::new()
                };
                chunks.push(QueryChunk {
                    columns: chunk_cols,
                    rows: std::mem::take(&mut current_chunk_rows),
                    is_last: false,
                    total_rows,
                    execution_time_us: start.elapsed().as_micros() as u64,
                });
            }

            if max_rows > 0 && total_rows >= max_rows as u64 {
                break;
            }
        }

        // Final chunk
        let chunk_cols = if is_first_chunk { columns } else { Vec::new() };
        chunks.push(QueryChunk {
            columns: chunk_cols,
            rows: current_chunk_rows,
            is_last: true,
            total_rows,
            execution_time_us: start.elapsed().as_micros() as u64,
        });

        Ok(chunks)
    }

    /// Execute a batch of statements atomically within an optional transaction.
    pub fn batch(
        &self,
        db_name: &str,
        statements: &[Statement],
        tx_mode: BatchTransactionMode,
        stop_on_error: bool,
        generation: u64,
    ) -> Result<BatchResponse> {
        let lock = self.get_db_lock(db_name);
        let _guard = lock.lock().unwrap();

        let start = Instant::now();
        let conn = self.open_connection(db_name, false)?;

        let use_tx = !matches!(tx_mode, BatchTransactionMode::None);
        if use_tx {
            let begin_sql = match tx_mode {
                BatchTransactionMode::Immediate => "BEGIN IMMEDIATE",
                BatchTransactionMode::Exclusive => "BEGIN EXCLUSIVE",
                _ => "BEGIN DEFERRED",
            };
            conn.exec(begin_sql)?;
        }

        let mut results = Vec::with_capacity(statements.len());
        let mut has_error = false;

        for stmt_proto in statements {
            let stmt_start = Instant::now();
            match conn.prepare(&stmt_proto.sql) {
                Ok(mut prepared) => {
                    let is_read = prepared.is_readonly();
                    let bind_res = bind_parameters(&mut prepared, stmt_proto.parameters.as_ref());
                    if let Err(e) = bind_res {
                        results.push(StatementResult {
                            result: None,
                            error: e.to_string(),
                        });
                        has_error = true;
                        if stop_on_error {
                            break;
                        }
                        continue;
                    }

                    if is_read {
                        let col_count = prepared.column_count();
                        let mut columns = Vec::with_capacity(col_count as usize);
                        for idx in 0..col_count {
                            let name = prepared.column_name(idx);
                            let declared_type = prepared.column_decltype(idx).unwrap_or_default();
                            columns.push(ColumnHeader {
                                name,
                                column_type: map_decltype_to_proto(&declared_type) as i32,
                                declared_type,
                            });
                        }
                        let mut rows = Vec::new();
                        let mut step_err = None;
                        loop {
                            match prepared.step() {
                                Ok(StepResult::Done) => break,
                                Ok(StepResult::Row) => {
                                    let mut values = Vec::with_capacity(col_count as usize);
                                    for idx in 0..col_count {
                                        values.push(sql_value_to_proto(prepared.column_value(idx)));
                                    }
                                    rows.push(Row { values });
                                }
                                Err(e) => {
                                    step_err = Some(e.to_string());
                                    break;
                                }
                            }
                        }

                        if let Some(err) = step_err {
                            results.push(StatementResult {
                                result: None,
                                error: err,
                            });
                            has_error = true;
                            if stop_on_error {
                                break;
                            }
                        } else {
                            let total_rows = rows.len() as u64;
                            results.push(StatementResult {
                                result: Some(crate::proto::rsqlite::v1::statement_result::Result::QueryResult(
                                    QueryResponse {
                                        columns,
                                        rows,
                                        total_rows,
                                        execution_time_us: stmt_start.elapsed().as_micros() as u64,
                                        generation,
                                        is_replica_read: false,
                                    },
                                )),
                                error: String::new(),
                            });
                        }
                    } else {
                        // Write statement
                        match prepared.step() {
                            Ok(_) => {
                                results.push(StatementResult {
                                    result: Some(
                                        crate::proto::rsqlite::v1::statement_result::Result::ExecuteResult(
                                            ExecuteResponse {
                                                rows_affected: conn.changes(),
                                                last_insert_rowid: conn.last_insert_rowid(),
                                                execution_time_us: stmt_start.elapsed().as_micros() as u64,
                                                generation,
                                            },
                                        ),
                                    ),
                                    error: String::new(),
                                });
                            }
                            Err(e) => {
                                results.push(StatementResult {
                                    result: None,
                                    error: e.to_string(),
                                });
                                has_error = true;
                                if stop_on_error {
                                    break;
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    results.push(StatementResult {
                        result: None,
                        error: e.to_string(),
                    });
                    has_error = true;
                    if stop_on_error {
                        break;
                    }
                }
            }
        }

        let committed = if use_tx {
            if has_error && stop_on_error {
                let _ = conn.exec("ROLLBACK");
                false
            } else {
                conn.exec("COMMIT").is_ok()
            }
        } else {
            !has_error
        };

        Ok(BatchResponse {
            results,
            total_execution_time_us: start.elapsed().as_micros() as u64,
            generation,
            committed,
        })
    }

    /// Delete a database file and its WAL/SHM/journal sidecars.
    ///
    /// Returns whether the primary database file existed prior to deletion.
    pub fn drop_database(&self, db_name: &str) -> Result<bool> {
        let lock = self.get_db_lock(db_name);
        let _guard = lock.lock().unwrap();

        let path = self.resolve_db_path(db_name)?;
        let existed = path.exists();

        for suffix in ["", "-wal", "-shm", "-journal"] {
            let sidecar = if suffix.is_empty() {
                path.clone()
            } else {
                let mut name = path.clone().into_os_string();
                name.push(suffix);
                PathBuf::from(name)
            };
            match fs::remove_file(&sidecar) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }

        self.db_mutexes.lock().unwrap().remove(db_name.trim());
        Ok(existed)
    }

    /// Introspect all databases in the data directory recursively.
    pub fn list_databases(&self) -> Result<Vec<DatabaseInfo>> {
        let mut list = Vec::new();
        if !self.data_dir.exists() {
            return Ok(list);
        }

        self.collect_databases(&self.data_dir, &mut list)?;
        list.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(list)
    }

    fn collect_databases(&self, dir: &Path, list: &mut Vec<DatabaseInfo>) -> Result<()> {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                self.collect_databases(&path, list)?;
            } else if path.is_file() {
                let file_name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or_default();

                // Exclude journal/WAL files
                if file_name.ends_with("-wal")
                    || file_name.ends_with("-shm")
                    || file_name.ends_with("-journal")
                {
                    continue;
                }

                let rel_name = path
                    .strip_prefix(&self.data_dir)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();

                match Connection::open(&path, ffi::SQLITE_OPEN_READONLY) {
                    Ok(conn) => {
                        let page_size = conn.page_size();
                        let page_count = conn.page_count().unwrap_or(0);
                        let file_size_bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

                        list.push(DatabaseInfo {
                            name: rel_name,
                            page_size,
                            page_count,
                            file_size_bytes,
                            journal_mode: "wal".into(),
                        });
                    }
                    Err(error) => {
                        tracing::warn!(
                            database = %rel_name,
                            error = %error,
                            "skipping unreadable database file while listing databases"
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Parameter and Value Mapping Helpers
// ─────────────────────────────────────────────────────────────────────────────

pub fn bind_parameters(
    stmt: &mut PreparedStatement,
    params: Option<&crate::proto::rsqlite::v1::Parameters>,
) -> Result<()> {
    let Some(params) = params else {
        return Ok(());
    };

    // 1. Positional parameters (1-indexed)
    for (idx, val) in params.positional.iter().enumerate() {
        bind_proto_value(stmt, (idx + 1) as i32, val)?;
    }

    // 2. Named parameters
    for NamedParameter { name, value } in &params.named {
        if let Some(val) = value {
            if let Some(idx) = stmt.bind_parameter_index(name) {
                bind_proto_value(stmt, idx, val)?;
            } else {
                return Err(SyncError::Protocol(format!(
                    "named parameter '{name}' not found in prepared statement"
                )));
            }
        }
    }

    Ok(())
}

fn bind_proto_value(stmt: &mut PreparedStatement, idx: i32, val: &Value) -> Result<()> {
    match &val.value {
        None | Some(ProtoValueInner::NullValue(_)) => stmt.bind_null(idx),
        Some(ProtoValueInner::IntValue(v)) => stmt.bind_int64(idx, *v),
        Some(ProtoValueInner::FloatValue(v)) => stmt.bind_double(idx, *v),
        Some(ProtoValueInner::TextValue(v)) => stmt.bind_text(idx, v),
        Some(ProtoValueInner::BlobValue(v)) => stmt.bind_blob(idx, v),
    }
}

pub fn sql_value_to_proto(val: SqlValue) -> Value {
    match val {
        SqlValue::Null => Value {
            value: Some(ProtoValueInner::NullValue(true)),
        },
        SqlValue::Integer(i) => Value {
            value: Some(ProtoValueInner::IntValue(i)),
        },
        SqlValue::Float(f) => Value {
            value: Some(ProtoValueInner::FloatValue(f)),
        },
        SqlValue::Text(t) => Value {
            value: Some(ProtoValueInner::TextValue(t)),
        },
        SqlValue::Blob(b) => Value {
            value: Some(ProtoValueInner::BlobValue(b)),
        },
    }
}

pub fn proto_value_to_sql(val: &Value) -> SqlValue {
    match &val.value {
        None | Some(ProtoValueInner::NullValue(_)) => SqlValue::Null,
        Some(ProtoValueInner::IntValue(i)) => SqlValue::Integer(*i),
        Some(ProtoValueInner::FloatValue(f)) => SqlValue::Float(*f),
        Some(ProtoValueInner::TextValue(t)) => SqlValue::Text(t.clone()),
        Some(ProtoValueInner::BlobValue(b)) => SqlValue::Blob(b.clone()),
    }
}

fn map_decltype_to_proto(decltype: &str) -> ColumnType {
    let upper = decltype.to_ascii_uppercase();
    if upper.contains("INT") {
        ColumnType::Integer
    } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
        ColumnType::Text
    } else if upper.contains("BLOB") {
        ColumnType::Blob
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        ColumnType::Float
    } else {
        ColumnType::Unspecified
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_lock_normalization() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        let lock1 = engine.get_db_lock("test.db");
        let lock2 = engine.get_db_lock("  test.db  \n");
        assert!(Arc::ptr_eq(&lock1, &lock2));
    }

    #[test]
    fn test_recursive_list_and_drop_databases() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        // Create root database
        engine
            .execute(
                "root.db",
                &Statement {
                    sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(),
                    parameters: None,
                },
                1,
            )
            .unwrap();

        // Create nested database
        engine
            .execute(
                "tenants/tenant1.db",
                &Statement {
                    sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(),
                    parameters: None,
                },
                1,
            )
            .unwrap();

        let dbs = engine.list_databases().unwrap();
        let db_names: Vec<&str> = dbs.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(db_names, vec!["root.db", "tenants/tenant1.db"]);

        // Drop nested database with whitespace in name
        assert!(engine.drop_database("  tenants/tenant1.db  ").unwrap());
        let dbs_after = engine.list_databases().unwrap();
        let db_names_after: Vec<&str> = dbs_after.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(db_names_after, vec!["root.db"]);
    }
}
