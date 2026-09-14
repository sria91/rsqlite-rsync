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

                // `Connection::open` alone doesn't validate the SQLite file
                // header — `PRAGMA page_size` (read during open) returns the
                // default page size without touching page 1, so opening a
                // file containing arbitrary garbage bytes succeeds. The
                // header is only actually checked once something reads the
                // schema, e.g. `PRAGMA page_count`. So both `Connection::open`
                // and `page_count()` must be treated as "is this a real
                // database" checks for a corrupt/non-database file to be
                // skipped here rather than silently listed as a bogus
                // zero-page database.
                let opened = Connection::open(&path, ffi::SQLITE_OPEN_READONLY)
                    .and_then(|conn| conn.page_count().map(|page_count| (conn, page_count)));

                match opened {
                    Ok((conn, page_count)) => {
                        let page_size = conn.page_size();
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
    use crate::proto::rsqlite::v1::Parameters;
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

    // ── resolve_db_path ────────────────────────────────────────────────────

    #[test]
    fn test_resolve_db_path_rejects_empty_name() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        let err = engine.resolve_db_path("   ").unwrap_err();
        assert!(err.to_string().contains("cannot be empty"));
    }

    #[test]
    fn test_resolve_db_path_rejects_path_traversal_and_absolute() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        let err = engine.resolve_db_path("../escape.db").unwrap_err();
        assert!(err.to_string().contains("path traversal not permitted"));

        let err = engine.resolve_db_path("./current.db").unwrap_err();
        assert!(err.to_string().contains("path traversal not permitted"));

        #[cfg(unix)]
        {
            let err = engine.resolve_db_path("/etc/evil.db").unwrap_err();
            assert!(err.to_string().contains("path traversal not permitted"));
        }
    }

    #[test]
    fn test_resolve_db_path_creates_nested_parent_dirs() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        let resolved = engine.resolve_db_path("a/b/c.db").unwrap();
        assert_eq!(resolved, dir.path().join("a/b/c.db"));
        assert!(resolved.parent().unwrap().is_dir());
    }

    // ── execute / query / bind_parameters ──────────────────────────────────

    #[test]
    fn test_execute_and_query_with_positional_and_named_parameters() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute(
                "app.db",
                &Statement {
                    sql: "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price REAL, blob_col BLOB, note TEXT);".to_string(),
                    parameters: None,
                },
                1,
            )
            .unwrap();

        // Positional parameters, including a blob and a null.
        let insert_positional = Statement {
            sql: "INSERT INTO items (name, price, blob_col, note) VALUES (?, ?, ?, ?);".to_string(),
            parameters: Some(Parameters {
                positional: vec![
                    Value { value: Some(ProtoValueInner::TextValue("widget".to_string())) },
                    Value { value: Some(ProtoValueInner::FloatValue(9.99)) },
                    Value { value: Some(ProtoValueInner::BlobValue(vec![1, 2, 3])) },
                    Value { value: None },
                ],
                named: vec![],
            }),
        };
        let resp = engine.execute("app.db", &insert_positional, 7).unwrap();
        assert_eq!(resp.rows_affected, 1);
        assert_eq!(resp.last_insert_rowid, 1);
        assert_eq!(resp.generation, 7);

        // Named parameters, successfully resolved.
        let insert_named = Statement {
            sql: "INSERT INTO items (name, price, blob_col, note) VALUES (:name, :price, :blob, :note);".to_string(),
            parameters: Some(Parameters {
                positional: vec![],
                named: vec![
                    NamedParameter {
                        name: ":name".to_string(),
                        value: Some(Value { value: Some(ProtoValueInner::TextValue("gadget".to_string())) }),
                    },
                    NamedParameter {
                        name: ":price".to_string(),
                        value: Some(Value { value: Some(ProtoValueInner::IntValue(5)) }),
                    },
                    NamedParameter {
                        name: ":blob".to_string(),
                        value: Some(Value { value: Some(ProtoValueInner::NullValue(true)) }),
                    },
                    NamedParameter {
                        name: ":note".to_string(),
                        value: Some(Value { value: None }),
                    },
                ],
            }),
        };
        engine.execute("app.db", &insert_named, 1).unwrap();

        let query_stmt = Statement {
            sql: "SELECT id, name, price, blob_col, note FROM items ORDER BY id;".to_string(),
            parameters: None,
        };
        let resp = engine.query("app.db", &query_stmt, 0, 3, true).unwrap();
        assert_eq!(resp.total_rows, 2);
        assert!(resp.is_replica_read);
        assert_eq!(resp.generation, 3);
        assert_eq!(resp.columns[0].column_type, ColumnType::Integer as i32);
        assert_eq!(resp.columns[1].column_type, ColumnType::Text as i32);
        assert_eq!(resp.columns[2].column_type, ColumnType::Float as i32);
        assert_eq!(resp.columns[3].column_type, ColumnType::Blob as i32);

        // First row: blob_col carries the bound blob, note is NULL (bound as `None`).
        let first_row = &resp.rows[0];
        assert!(matches!(first_row.values[3].value, Some(ProtoValueInner::BlobValue(_))));
        assert!(matches!(first_row.values[4].value, Some(ProtoValueInner::NullValue(_))));
    }

    #[test]
    fn test_execute_named_parameter_not_found_errors() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute(
                "app.db",
                &Statement { sql: "CREATE TABLE t (id INTEGER, v TEXT);".to_string(), parameters: None },
                1,
            )
            .unwrap();

        let stmt = Statement {
            sql: "INSERT INTO t (id, v) VALUES (:id, :v);".to_string(),
            parameters: Some(Parameters {
                positional: vec![],
                named: vec![NamedParameter {
                    name: ":missing".to_string(),
                    value: Some(Value { value: Some(ProtoValueInner::IntValue(1)) }),
                }],
            }),
        };

        let err = engine.execute("app.db", &stmt, 1).unwrap_err();
        assert!(err.to_string().contains("not found in prepared statement"));
    }

    #[test]
    fn test_query_respects_max_rows_limit() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(), parameters: None }, 1)
            .unwrap();
        for _ in 0..5 {
            engine
                .execute("app.db", &Statement { sql: "INSERT INTO t DEFAULT VALUES;".to_string(), parameters: None }, 1)
                .unwrap();
        }

        let resp = engine
            .query("app.db", &Statement { sql: "SELECT id FROM t ORDER BY id;".to_string(), parameters: None }, 2, 1, false)
            .unwrap();
        assert_eq!(resp.total_rows, 2);
        assert_eq!(resp.rows.len(), 2);
    }

    // ── stream_query_chunks ─────────────────────────────────────────────────

    #[test]
    fn test_stream_query_chunks_splits_into_multiple_chunks() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(), parameters: None }, 1)
            .unwrap();
        for _ in 0..5 {
            engine
                .execute("app.db", &Statement { sql: "INSERT INTO t DEFAULT VALUES;".to_string(), parameters: None }, 1)
                .unwrap();
        }

        let chunks = engine
            .stream_query_chunks("app.db", &Statement { sql: "SELECT id FROM t ORDER BY id;".to_string(), parameters: None }, 0, 2)
            .unwrap();

        // 5 rows with chunk_size 2 -> [2, 2, 1] rows across 3 chunks.
        assert_eq!(chunks.len(), 3);
        assert!(!chunks[0].columns.is_empty(), "first chunk should carry column headers");
        assert!(chunks[1].columns.is_empty(), "later chunks should not repeat column headers");
        assert!(chunks[2].columns.is_empty());
        assert!(!chunks[0].is_last);
        assert!(!chunks[1].is_last);
        assert!(chunks[2].is_last);
        assert_eq!(chunks[2].rows.len(), 1);
        assert_eq!(chunks[2].total_rows, 5);
    }

    #[test]
    fn test_stream_query_chunks_respects_max_rows() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(), parameters: None }, 1)
            .unwrap();
        for _ in 0..10 {
            engine
                .execute("app.db", &Statement { sql: "INSERT INTO t DEFAULT VALUES;".to_string(), parameters: None }, 1)
                .unwrap();
        }

        // chunk_size is never reached (10), so only the max_rows cutoff (3) governs.
        let chunks = engine
            .stream_query_chunks("app.db", &Statement { sql: "SELECT id FROM t ORDER BY id;".to_string(), parameters: None }, 3, 10)
            .unwrap();

        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_last);
        assert_eq!(chunks[0].total_rows, 3);
        assert_eq!(chunks[0].rows.len(), 3);
        assert!(!chunks[0].columns.is_empty());
    }

    // ── batch ───────────────────────────────────────────────────────────────

    #[test]
    fn test_batch_commits_across_transaction_modes() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);".to_string(), parameters: None }, 1)
            .unwrap();

        for (mode, val) in [
            (BatchTransactionMode::None, "none"),
            (BatchTransactionMode::Deferred, "deferred"),
            (BatchTransactionMode::Immediate, "immediate"),
            (BatchTransactionMode::Exclusive, "exclusive"),
        ] {
            let stmts = vec![Statement { sql: format!("INSERT INTO t (v) VALUES ('{val}');"), parameters: None }];
            let resp = engine.batch("app.db", &stmts, mode, true, 9).unwrap();
            assert!(resp.committed, "mode {mode:?} should commit");
            assert_eq!(resp.results.len(), 1);
            assert_eq!(resp.generation, 9);
        }

        let count = engine
            .query("app.db", &Statement { sql: "SELECT COUNT(*) FROM t;".to_string(), parameters: None }, 0, 1, false)
            .unwrap();
        assert_eq!(count.rows[0].values[0].value, Some(ProtoValueInner::IntValue(4)));
    }

    #[test]
    fn test_batch_stop_on_error_true_halts_on_prepare_error() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);".to_string(), parameters: None }, 1)
            .unwrap();

        let stmts = vec![
            Statement { sql: "NOT VALID SQL".to_string(), parameters: None },
            Statement { sql: "INSERT INTO t (v) VALUES ('after');".to_string(), parameters: None },
        ];
        let resp = engine.batch("app.db", &stmts, BatchTransactionMode::Deferred, true, 1).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert!(!resp.results[0].error.is_empty());
        assert!(!resp.committed);

        let count = engine
            .query("app.db", &Statement { sql: "SELECT COUNT(*) FROM t;".to_string(), parameters: None }, 0, 1, false)
            .unwrap();
        assert_eq!(count.rows[0].values[0].value, Some(ProtoValueInner::IntValue(0)));
    }

    #[test]
    fn test_batch_continues_past_prepare_error_when_stop_on_error_false() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);".to_string(), parameters: None }, 1)
            .unwrap();

        let stmts = vec![
            Statement { sql: "INSERT INTO t (v) VALUES ('first');".to_string(), parameters: None },
            Statement { sql: "NOT VALID SQL".to_string(), parameters: None },
            Statement { sql: "INSERT INTO t (v) VALUES ('third');".to_string(), parameters: None },
        ];
        let resp = engine.batch("app.db", &stmts, BatchTransactionMode::None, false, 1).unwrap();
        assert_eq!(resp.results.len(), 3);
        assert!(resp.results[0].error.is_empty());
        assert!(!resp.results[1].error.is_empty());
        assert!(resp.results[2].error.is_empty());
        // BatchTransactionMode::None never opens a transaction, so `committed`
        // just reflects whether any statement failed.
        assert!(!resp.committed);

        let count = engine
            .query("app.db", &Statement { sql: "SELECT COUNT(*) FROM t;".to_string(), parameters: None }, 0, 1, false)
            .unwrap();
        assert_eq!(count.rows[0].values[0].value, Some(ProtoValueInner::IntValue(2)));
    }

    #[test]
    fn test_batch_bind_error_stop_on_error_variants() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);".to_string(), parameters: None }, 1)
            .unwrap();

        let bad_bind = Statement {
            sql: "INSERT INTO t (v) VALUES (:v);".to_string(),
            parameters: Some(Parameters {
                positional: vec![],
                named: vec![NamedParameter {
                    name: ":missing".to_string(),
                    value: Some(Value { value: Some(ProtoValueInner::TextValue("x".to_string())) }),
                }],
            }),
        };
        let stmts = vec![bad_bind, Statement { sql: "INSERT INTO t (v) VALUES ('after');".to_string(), parameters: None }];

        let resp = engine.batch("app.db", &stmts, BatchTransactionMode::Deferred, true, 1).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert!(resp.results[0].error.contains("not found in prepared statement"));
        assert!(!resp.committed);

        let resp2 = engine.batch("app.db", &stmts, BatchTransactionMode::Deferred, false, 1).unwrap();
        assert_eq!(resp2.results.len(), 2);
        assert!(resp2.results[0].error.contains("not found in prepared statement"));
        assert!(resp2.results[1].error.is_empty());
    }

    #[test]
    fn test_batch_write_step_error_stop_on_error_variants() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT UNIQUE);".to_string(), parameters: None }, 1)
            .unwrap();
        engine
            .execute("app.db", &Statement { sql: "INSERT INTO t (v) VALUES ('dup');".to_string(), parameters: None }, 1)
            .unwrap();

        let stmts = vec![
            Statement { sql: "INSERT INTO t (v) VALUES ('dup');".to_string(), parameters: None },
            Statement { sql: "INSERT INTO t (v) VALUES ('unique-2');".to_string(), parameters: None },
        ];

        let resp = engine.batch("app.db", &stmts, BatchTransactionMode::Immediate, true, 1).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert!(!resp.results[0].error.is_empty());
        assert!(!resp.committed);

        let resp2 = engine.batch("app.db", &stmts, BatchTransactionMode::Immediate, false, 1).unwrap();
        assert_eq!(resp2.results.len(), 2);
        assert!(!resp2.results[0].error.is_empty());
        assert!(resp2.results[1].error.is_empty());
    }

    // ── drop_database ───────────────────────────────────────────────────────

    #[test]
    fn test_drop_database_removes_existing_and_tolerates_missing_sidecars() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(), parameters: None }, 1)
            .unwrap();

        let db_path = engine.resolve_db_path("app.db").unwrap();
        // Only the WAL sidecar is present; -shm and -journal are absent and
        // must be tolerated (NotFound).
        fs::write(format!("{}-wal", db_path.display()), b"wal-bytes").unwrap();

        let existed = engine.drop_database("app.db").unwrap();
        assert!(existed);
        assert!(!db_path.exists());
        assert!(!Path::new(&format!("{}-wal", db_path.display())).exists());

        // Dropping again: the primary file is now missing too, so every
        // removal hits the tolerated NotFound branch.
        let existed_again = engine.drop_database("app.db").unwrap();
        assert!(!existed_again);
    }

    #[test]
    fn test_drop_database_propagates_non_notfound_errors() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("app.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(), parameters: None }, 1)
            .unwrap();

        let db_path = engine.resolve_db_path("app.db").unwrap();
        let wal_path = format!("{}-wal", db_path.display());
        // Replace the WAL sidecar with a directory so `fs::remove_file` fails
        // with a non-NotFound error, exercising the propagation branch.
        let _ = fs::remove_file(&wal_path);
        fs::create_dir(&wal_path).unwrap();

        let result = engine.drop_database("app.db");
        assert!(result.is_err());
    }

    // ── list_databases ──────────────────────────────────────────────────────

    #[test]
    fn test_list_databases_returns_empty_when_data_dir_missing() {
        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        fs::remove_dir_all(dir.path()).unwrap();

        let dbs = engine.list_databases().unwrap();
        assert!(dbs.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn test_list_databases_skips_unreadable_database_files() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempdir().unwrap();
        let engine = DatabaseEngine::new(dir.path()).unwrap();

        engine
            .execute("good.db", &Statement { sql: "CREATE TABLE t (id INTEGER PRIMARY KEY);".to_string(), parameters: None }, 1)
            .unwrap();

        // A file SQLite cannot open at all (permission denied at the OS
        // level) is skipped (with a warning) rather than aborting the whole
        // listing. Merely malformed *content* is not enough to reproduce
        // this: SQLite's `PRAGMA page_size` (used by `Connection::open`)
        // defers full header validation past open, so only a hard OS-level
        // failure reliably reaches the `Err` branch in `collect_databases`.
        let bad_path = dir.path().join("bad.db");
        fs::write(&bad_path, b"not a sqlite database").unwrap();
        fs::set_permissions(&bad_path, fs::Permissions::from_mode(0o000)).unwrap();

        let dbs = engine.list_databases().unwrap();
        let names: Vec<&str> = dbs.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(names, vec!["good.db"]);
    }

    // ── value / decltype mapping helpers ─────────────────────────────────────

    #[test]
    fn test_map_decltype_to_proto_all_branches() {
        assert_eq!(map_decltype_to_proto("INTEGER"), ColumnType::Integer);
        assert_eq!(map_decltype_to_proto("VARCHAR(32)"), ColumnType::Text);
        assert_eq!(map_decltype_to_proto("TEXT"), ColumnType::Text);
        assert_eq!(map_decltype_to_proto("CLOB"), ColumnType::Text);
        assert_eq!(map_decltype_to_proto("BLOB"), ColumnType::Blob);
        assert_eq!(map_decltype_to_proto("REAL"), ColumnType::Float);
        assert_eq!(map_decltype_to_proto("FLOAT"), ColumnType::Float);
        assert_eq!(map_decltype_to_proto("DOUBLE"), ColumnType::Float);
        assert_eq!(map_decltype_to_proto(""), ColumnType::Unspecified);
    }

    #[test]
    fn test_sql_value_to_proto_all_variants() {
        assert_eq!(sql_value_to_proto(SqlValue::Null).value, Some(ProtoValueInner::NullValue(true)));
        assert_eq!(sql_value_to_proto(SqlValue::Integer(42)).value, Some(ProtoValueInner::IntValue(42)));
        assert_eq!(sql_value_to_proto(SqlValue::Float(1.5)).value, Some(ProtoValueInner::FloatValue(1.5)));
        assert_eq!(
            sql_value_to_proto(SqlValue::Text("hi".to_string())).value,
            Some(ProtoValueInner::TextValue("hi".to_string()))
        );
        assert_eq!(
            sql_value_to_proto(SqlValue::Blob(vec![1, 2, 3])).value,
            Some(ProtoValueInner::BlobValue(vec![1, 2, 3]))
        );
    }

    #[test]
    fn test_proto_value_to_sql_all_variants() {
        assert_eq!(proto_value_to_sql(&Value { value: None }), SqlValue::Null);
        assert_eq!(proto_value_to_sql(&Value { value: Some(ProtoValueInner::NullValue(true)) }), SqlValue::Null);
        assert_eq!(proto_value_to_sql(&Value { value: Some(ProtoValueInner::IntValue(7)) }), SqlValue::Integer(7));
        assert_eq!(proto_value_to_sql(&Value { value: Some(ProtoValueInner::FloatValue(2.5)) }), SqlValue::Float(2.5));
        assert_eq!(
            proto_value_to_sql(&Value { value: Some(ProtoValueInner::TextValue("x".to_string())) }),
            SqlValue::Text("x".to_string())
        );
        assert_eq!(
            proto_value_to_sql(&Value { value: Some(ProtoValueInner::BlobValue(vec![9, 8])) }),
            SqlValue::Blob(vec![9, 8])
        );
    }
}
