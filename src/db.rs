//! Safe Rust wrappers around the raw `libsqlite3-sys` FFI.
//!
//! This module provides:
//!
//! * [`Connection`] — an open SQLite database file with page-level read/write
//!   access needed by the sync protocol.
//! * [`Backup`] — a thin wrapper around the `sqlite3_backup_*` API used to
//!   apply a consistent snapshot from origin to replica.
//!
//! # Safety
//!
//! All functions in this module perform the necessary FFI calls and translate
//! non-`SQLITE_OK` result codes into [`SyncError::Sqlite`].  Callers do **not**
//! need to use `unsafe` code.

use std::ffi::{CStr, CString};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::ptr;

pub use libsqlite3_sys as ffi;
use std::sync::Mutex;

use crate::error::{Result, SyncError};

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Convert a raw SQLite result code into a [`SyncError`] if it is not
/// `SQLITE_OK` / `SQLITE_DONE` / `SQLITE_ROW`.
fn check(db: *mut ffi::sqlite3, rc: i32) -> Result<()> {
    match rc {
        ffi::SQLITE_OK | ffi::SQLITE_DONE | ffi::SQLITE_ROW => Ok(()),
        _ => {
            let msg = if db.is_null() {
                format!("SQLite error code {rc}")
            } else {
                unsafe {
                    let ptr = ffi::sqlite3_errmsg(db);
                    CStr::from_ptr(ptr).to_string_lossy().into_owned()
                }
            };
            Err(SyncError::sqlite(rc, msg))
        }
    }
}

fn validate_page_no(page_no: u32) -> Result<()> {
    if page_no == 0 {
        return Err(SyncError::Protocol(
            "page numbers are 1-indexed; page 0 is invalid".into(),
        ));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Connection
// ─────────────────────────────────────────────────────────────────────────────

/// An open SQLite database connection.
///
/// The connection is closed automatically when this value is dropped.
pub struct Connection {
    db: *mut ffi::sqlite3,
    /// Cached page size in bytes (set once on open).
    page_size: u32,
    /// Cached read file handle — reused across all `read_page` calls.
    read_fd: Mutex<File>,
    /// Cached write file handle — present only for read-write connections.
    write_fd: Option<Mutex<File>>,
}

// SAFETY: `sqlite3` can be used from a single thread at a time.  We never
// share a `Connection` across threads without synchronisation.
unsafe impl Send for Connection {}

impl Connection {
    /// Open an existing database file (or create a new one) at `path`.
    ///
    /// `flags` is a bitwise-OR of `SQLITE_OPEN_*` constants from
    /// [`libsqlite3_sys`].  Use `SQLITE_OPEN_READONLY` for origin reads and
    /// `SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE` for the replica.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::Sqlite`] if `sqlite3_open_v2` fails.
    pub fn open(path: &Path, flags: i32) -> Result<Self> {
        let c_path = CString::new(path.to_string_lossy().as_bytes())
            .map_err(|e| SyncError::Protocol(e.to_string()))?;

        let mut db: *mut ffi::sqlite3 = ptr::null_mut();
        let rc = unsafe { ffi::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, ptr::null()) };

        // sqlite3_open_v2 may set `db` to a non-null handle even on failure;
        // the SQLite docs require closing it in all cases.
        if let Err(e) = check(db, rc) {
            if !db.is_null() {
                unsafe { ffi::sqlite3_close(db) };
            }
            return Err(e);
        }

        // `db` is now a live, valid handle.  Use a guard so it is closed if
        // any subsequent setup step fails before `Connection` takes ownership.
        struct DbGuard(*mut ffi::sqlite3);
        impl Drop for DbGuard {
            fn drop(&mut self) {
                if !self.0.is_null() {
                    unsafe { ffi::sqlite3_close(self.0) };
                }
            }
        }
        let guard = DbGuard(db);

        // Allow SQLite to wait briefly on transient file locks instead of
        // immediately returning SQLITE_BUSY under concurrent activity.
        unsafe {
            check(db, ffi::sqlite3_busy_timeout(db, 5_000))?;
        }

        // Read the page size from the database header (PRAGMA page_size).
        let page_size = Self::query_pragma_u32(db, "page_size")?;

        let read_fd = Mutex::new(OpenOptions::new().read(true).open(path)?);
        let write_fd = if flags & ffi::SQLITE_OPEN_READWRITE != 0 {
            Some(Mutex::new(
                OpenOptions::new().read(true).write(true).open(path)?,
            ))
        } else {
            None
        };

        // All setup succeeded — disarm the guard and hand ownership to Connection.
        std::mem::forget(guard);
        Ok(Connection {
            db,
            page_size,
            read_fd,
            write_fd,
        })
    }

    /// The page size of this database in bytes.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// The total number of pages in the database.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::Sqlite`] if the query fails.
    pub fn page_count(&self) -> Result<u32> {
        Self::query_pragma_u32(self.db, "page_count")
    }

    /// Serialise the entire `main` database to a byte vector.
    ///
    /// This captures the current read view of the connection, which is useful
    /// for snapshotting the origin at sync start.
    pub fn serialize(&self) -> Result<Vec<u8>> {
        let main = CString::new("main").unwrap();
        let mut size: i64 = 0;
        unsafe {
            let ptr = ffi::sqlite3_serialize(self.db, main.as_ptr(), &mut size, 0);
            if ptr.is_null() {
                return Err(SyncError::sqlite(
                    ffi::SQLITE_NOMEM,
                    "sqlite3_serialize returned NULL",
                ));
            }
            let bytes = std::slice::from_raw_parts(ptr, size as usize).to_vec();
            ffi::sqlite3_free(ptr as *mut _);
            Ok(bytes)
        }
    }

    /// Read a single database page (1-indexed) into a freshly allocated buffer.
    ///
    /// The returned `Vec<u8>` has exactly `page_size` bytes.
    ///
    /// Reads directly from the underlying database file at the page offset.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::Sqlite`] if the page number is out of range or the
    /// file cannot be read.
    pub fn read_page(&self, page_no: u32) -> Result<Vec<u8>> {
        validate_page_no(page_no)?;
        let ps = self.page_size as usize;
        let offset = (page_no - 1) as u64 * self.page_size as u64;
        let mut file = self.read_fd.lock().unwrap();
        let mut buf = vec![0u8; ps];
        file.seek(SeekFrom::Start(offset))?;
        let n = file.read(&mut buf)?;
        if n != ps {
            return Err(SyncError::sqlite(
                1,
                format!("page {page_no} is out of range (read {n} of {ps} bytes)"),
            ));
        }
        Ok(buf)
    }

    /// Write raw page data for page `page_no` (1-indexed).
    ///
    /// `data` must be exactly `page_size` bytes.  The page is written directly
    /// into the database file at the corresponding byte offset.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::Sqlite`] if the write fails.
    pub fn write_page(&self, page_no: u32, data: &[u8]) -> Result<()> {
        validate_page_no(page_no)?;
        if data.len() != self.page_size as usize {
            return Err(SyncError::Protocol(format!(
                "page {page_no} has {} bytes, expected {}",
                data.len(),
                self.page_size
            )));
        }
        let write_fd = self.write_fd.as_ref().ok_or_else(|| {
            SyncError::Protocol("connection is read-only; cannot write pages".into())
        })?;
        let offset = (page_no - 1) as u64 * self.page_size as u64;
        let mut file = write_fd.lock().unwrap();
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.flush()?;
        Ok(())
    }

    /// Execute `PRAGMA wal_checkpoint(TRUNCATE)` to flush the WAL after sync.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::Sqlite`] if the checkpoint fails.
    pub fn wal_checkpoint(&self) -> Result<()> {
        self.exec("PRAGMA wal_checkpoint(TRUNCATE)")
    }

    /// Execute a SQL statement that returns no rows.
    pub fn exec(&self, sql: &str) -> Result<()> {
        let c_sql = CString::new(sql).map_err(|e| SyncError::Protocol(e.to_string()))?;
        unsafe {
            check(
                self.db,
                ffi::sqlite3_exec(
                    self.db,
                    c_sql.as_ptr(),
                    None,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
            )
        }
    }

    /// Return the raw `*mut sqlite3` pointer.
    pub fn as_ptr(&self) -> *mut ffi::sqlite3 {
        self.db
    }

    /// Number of rows modified, inserted or deleted by the most recent statement.
    pub fn changes(&self) -> u64 {
        unsafe { ffi::sqlite3_changes(self.db) as u64 }
    }

    /// Rowid of the most recent successful INSERT into a rowid table.
    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db) }
    }

    /// Prepare a SQL statement for execution and parameter binding.
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        PreparedStatement::new(self, sql)
    }

    // ── private helpers ──────────────────────────────────────────────────────

    fn query_pragma_u32(db: *mut ffi::sqlite3, pragma: &str) -> Result<u32> {
        let sql = CString::new(format!("PRAGMA {pragma}")).unwrap();
        const MAX_RETRIES: usize = 12;
        for attempt in 0..=MAX_RETRIES {
            let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
            unsafe {
                check(
                    db,
                    ffi::sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, ptr::null_mut()),
                )?;
                let rc = ffi::sqlite3_step(stmt);
                if rc == ffi::SQLITE_ROW {
                    let val = ffi::sqlite3_column_int(stmt, 0) as u32;
                    ffi::sqlite3_finalize(stmt);
                    return Ok(val);
                }
                ffi::sqlite3_finalize(stmt);

                // Under concurrent writers, PRAGMA reads can occasionally hit
                // transient lock/no-row outcomes. Retry briefly before surfacing
                // an error to callers.
                if (rc == ffi::SQLITE_BUSY || rc == ffi::SQLITE_LOCKED || rc == ffi::SQLITE_DONE)
                    && attempt < MAX_RETRIES
                {
                    let delay_ms = (attempt as u64 + 1).min(10);
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                    continue;
                }

                let msg = if rc == ffi::SQLITE_DONE {
                    format!("PRAGMA {pragma} returned no rows")
                } else {
                    format!("PRAGMA {pragma} failed")
                };
                return Err(SyncError::sqlite(rc, msg));
            }
        }
        Err(SyncError::Protocol(format!(
            "PRAGMA {pragma} exceeded retry budget"
        )))
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe {
                let rc = ffi::sqlite3_close(self.db);
                if rc != ffi::SQLITE_OK {
                    eprintln!("warning: sqlite3_close returned non-OK status {rc}");
                }
            }
            self.db = ptr::null_mut();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Backup
// ─────────────────────────────────────────────────────────────────────────────

/// Wrapper around the `sqlite3_backup_*` incremental backup API.
///
/// Use [`Backup::new`] to initialise a backup from a source to a destination
/// database, then call [`Backup::step`] to copy pages incrementally, and
/// finally [`Backup::finish`] to clean up.
pub struct Backup {
    inner: *mut ffi::sqlite3_backup,
    /// Keep references alive for the duration of the backup.
    _dst: *mut ffi::sqlite3,
}

// SAFETY: same single-thread contract as `Connection`.
unsafe impl Send for Backup {}

impl Backup {
    /// Create a new backup that will copy **from** `src.main` **into**
    /// `dst.main`.
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::Sqlite`] if `sqlite3_backup_init` fails.
    pub fn new(dst: &Connection, src: &Connection) -> Result<Self> {
        let main = CString::new("main").unwrap();
        let inner = unsafe {
            ffi::sqlite3_backup_init(dst.as_ptr(), main.as_ptr(), src.as_ptr(), main.as_ptr())
        };
        if inner.is_null() {
            let msg = unsafe {
                let ptr = ffi::sqlite3_errmsg(dst.as_ptr());
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            };
            return Err(SyncError::sqlite(ffi::SQLITE_ERROR, msg));
        }
        Ok(Backup {
            inner,
            _dst: dst.as_ptr(),
        })
    }

    /// Copy up to `n_pages` pages from source to destination.
    ///
    /// Pass `-1` to copy everything in one step.
    ///
    /// Returns `true` if the backup is complete (`SQLITE_DONE`), `false` if
    /// more pages remain (`SQLITE_OK`).
    ///
    /// # Errors
    ///
    /// Returns [`SyncError::Sqlite`] on any other result code.
    pub fn step(&self, n_pages: i32) -> Result<bool> {
        let rc = unsafe { ffi::sqlite3_backup_step(self.inner, n_pages) };
        match rc {
            ffi::SQLITE_DONE => Ok(true),
            ffi::SQLITE_OK => Ok(false),
            _ => Err(SyncError::sqlite(rc, "sqlite3_backup_step failed")),
        }
    }

    /// Return the number of pages remaining to be backed up.
    pub fn remaining(&self) -> i32 {
        unsafe { ffi::sqlite3_backup_remaining(self.inner) }
    }

    /// Return the total number of pages in the source database.
    pub fn pagecount(&self) -> i32 {
        unsafe { ffi::sqlite3_backup_pagecount(self.inner) }
    }

    /// Release resources associated with this backup object.
    ///
    /// If `sqlite3_backup_step` previously returned `SQLITE_DONE`, this
    /// completes the transaction; otherwise it rolls back.
    ///
    /// Consumes `self` so it cannot be used after finishing.
    pub fn finish(self) -> Result<()> {
        let rc = unsafe { ffi::sqlite3_backup_finish(self.inner) };
        // SQLITE_OK or SQLITE_DONE are both success here.
        match rc {
            ffi::SQLITE_OK | ffi::SQLITE_DONE => Ok(()),
            _ => Err(SyncError::sqlite(rc, "sqlite3_backup_finish failed")),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Prepared Statement & Typed SQL Values
// ─────────────────────────────────────────────────────────────────────────────

/// SQLite column data type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnDataType {
    Integer,
    Float,
    Text,
    Blob,
    Null,
}

/// Dynamically typed SQLite value.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

/// Result of stepping a prepared statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepResult {
    Row,
    Done,
}

/// A prepared SQLite statement.
pub struct PreparedStatement {
    stmt: *mut ffi::sqlite3_stmt,
    db: *mut ffi::sqlite3,
}

// SAFETY: `sqlite3_stmt` is owned and accessed from a single thread.
unsafe impl Send for PreparedStatement {}

impl PreparedStatement {
    /// Prepare a new SQL statement on the connection.
    pub fn new(conn: &Connection, sql: &str) -> Result<Self> {
        let c_sql = CString::new(sql).map_err(|e| SyncError::Protocol(e.to_string()))?;
        let mut stmt: *mut ffi::sqlite3_stmt = ptr::null_mut();
        unsafe {
            check(
                conn.as_ptr(),
                ffi::sqlite3_prepare_v2(
                    conn.as_ptr(),
                    c_sql.as_ptr(),
                    -1,
                    &mut stmt,
                    ptr::null_mut(),
                ),
            )?;
        }
        Ok(PreparedStatement {
            stmt,
            db: conn.as_ptr(),
        })
    }

    /// Returns `true` if this statement is guaranteed to not change the database.
    pub fn is_readonly(&self) -> bool {
        unsafe { ffi::sqlite3_stmt_readonly(self.stmt) != 0 }
    }

    /// Step the statement execution.
    pub fn step(&mut self) -> Result<StepResult> {
        let rc = unsafe { ffi::sqlite3_step(self.stmt) };
        match rc {
            ffi::SQLITE_ROW => Ok(StepResult::Row),
            ffi::SQLITE_DONE => Ok(StepResult::Done),
            _ => {
                check(self.db, rc)?;
                Ok(StepResult::Done)
            }
        }
    }

    /// Reset the prepared statement back to its initial state for re-execution.
    pub fn reset(&mut self) -> Result<()> {
        unsafe { check(self.db, ffi::sqlite3_reset(self.stmt)) }
    }

    /// Reset all parameter bindings back to NULL.
    pub fn clear_bindings(&mut self) -> Result<()> {
        unsafe { check(self.db, ffi::sqlite3_clear_bindings(self.stmt)) }
    }

    /// Look up the 1-based index of a named parameter (e.g., ":id", "@name", "$val", or bare "id").
    pub fn bind_parameter_index(&self, name: &str) -> Option<i32> {
        let candidates = [
            name.to_string(),
            format!(":{name}"),
            format!("@{name}"),
            format!("${name}"),
        ];
        for cand in candidates {
            if let Ok(c_name) = CString::new(cand) {
                let idx = unsafe { ffi::sqlite3_bind_parameter_index(self.stmt, c_name.as_ptr()) };
                if idx > 0 {
                    return Some(idx);
                }
            }
        }
        None
    }

    /// Bind a NULL value to 1-indexed parameter `idx`.
    pub fn bind_null(&mut self, idx: i32) -> Result<()> {
        unsafe { check(self.db, ffi::sqlite3_bind_null(self.stmt, idx)) }
    }

    /// Bind a 64-bit integer value to 1-indexed parameter `idx`.
    pub fn bind_int64(&mut self, idx: i32, val: i64) -> Result<()> {
        unsafe { check(self.db, ffi::sqlite3_bind_int64(self.stmt, idx, val)) }
    }

    /// Bind a 64-bit float value to 1-indexed parameter `idx`.
    pub fn bind_double(&mut self, idx: i32, val: f64) -> Result<()> {
        unsafe { check(self.db, ffi::sqlite3_bind_double(self.stmt, idx, val)) }
    }

    /// Bind a UTF-8 text string to 1-indexed parameter `idx`.
    pub fn bind_text(&mut self, idx: i32, val: &str) -> Result<()> {
        unsafe {
            check(
                self.db,
                ffi::sqlite3_bind_text(
                    self.stmt,
                    idx,
                    val.as_ptr() as *const std::os::raw::c_char,
                    val.len() as i32,
                    ffi::SQLITE_TRANSIENT(),
                ),
            )
        }
    }

    /// Bind raw blob bytes to 1-indexed parameter `idx`.
    pub fn bind_blob(&mut self, idx: i32, val: &[u8]) -> Result<()> {
        unsafe {
            check(
                self.db,
                ffi::sqlite3_bind_blob(
                    self.stmt,
                    idx,
                    val.as_ptr() as *const std::os::raw::c_void,
                    val.len() as i32,
                    ffi::SQLITE_TRANSIENT(),
                ),
            )
        }
    }

    /// Return the number of columns in the result set.
    pub fn column_count(&self) -> i32 {
        unsafe { ffi::sqlite3_column_count(self.stmt) }
    }

    /// Return the name of 0-indexed column `idx`.
    pub fn column_name(&self, idx: i32) -> String {
        unsafe {
            let ptr = ffi::sqlite3_column_name(self.stmt, idx);
            if ptr.is_null() {
                String::new()
            } else {
                CStr::from_ptr(ptr).to_string_lossy().into_owned()
            }
        }
    }

    /// Return the declared datatype of 0-indexed column `idx`, if any.
    pub fn column_decltype(&self, idx: i32) -> Option<String> {
        unsafe {
            let ptr = ffi::sqlite3_column_decltype(self.stmt, idx);
            if ptr.is_null() {
                None
            } else {
                Some(CStr::from_ptr(ptr).to_string_lossy().into_owned())
            }
        }
    }

    /// Return the SQLite storage class of 0-indexed column `idx` for the current row.
    pub fn column_type(&self, idx: i32) -> ColumnDataType {
        let code = unsafe { ffi::sqlite3_column_type(self.stmt, idx) };
        match code {
            ffi::SQLITE_INTEGER => ColumnDataType::Integer,
            ffi::SQLITE_FLOAT => ColumnDataType::Float,
            ffi::SQLITE_TEXT => ColumnDataType::Text,
            ffi::SQLITE_BLOB => ColumnDataType::Blob,
            _ => ColumnDataType::Null,
        }
    }

    /// Extract the typed value of 0-indexed column `idx` for the current row.
    pub fn column_value(&self, idx: i32) -> SqlValue {
        match self.column_type(idx) {
            ColumnDataType::Null => SqlValue::Null,
            ColumnDataType::Integer => {
                let val = unsafe { ffi::sqlite3_column_int64(self.stmt, idx) };
                SqlValue::Integer(val)
            }
            ColumnDataType::Float => {
                let val = unsafe { ffi::sqlite3_column_double(self.stmt, idx) };
                SqlValue::Float(val)
            }
            ColumnDataType::Text => {
                let ptr = unsafe { ffi::sqlite3_column_text(self.stmt, idx) };
                let len = unsafe { ffi::sqlite3_column_bytes(self.stmt, idx) as usize };
                if ptr.is_null() {
                    SqlValue::Text(String::new())
                } else {
                    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
                    SqlValue::Text(String::from_utf8_lossy(bytes).into_owned())
                }
            }
            ColumnDataType::Blob => {
                let ptr = unsafe { ffi::sqlite3_column_blob(self.stmt, idx) };
                let len = unsafe { ffi::sqlite3_column_bytes(self.stmt, idx) as usize };
                if ptr.is_null() {
                    SqlValue::Blob(Vec::new())
                } else {
                    let bytes = unsafe { std::slice::from_raw_parts(ptr as *const u8, len) };
                    SqlValue::Blob(bytes.to_vec())
                }
            }
        }
    }
}

impl Drop for PreparedStatement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            unsafe {
                ffi::sqlite3_finalize(self.stmt);
            }
            self.stmt = ptr::null_mut();
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    fn open_rw(path: &Path) -> Connection {
        Connection::open(path, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE)
            .expect("open rw")
    }

    fn seed_db(conn: &Connection) {
        conn.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();
        for i in 0..100 {
            conn.exec(&format!("INSERT INTO t VALUES ({i}, 'row{i}')"))
                .unwrap();
        }
    }

    #[test]
    fn open_creates_database() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        // A fresh database may report 0 pages until a write forces the header
        // to disk.  After a DDL statement the page count must be ≥ 1.
        conn.exec("CREATE TABLE _init (x INTEGER)").unwrap();
        assert!(conn.page_size() > 0);
        assert!(conn.page_count().unwrap() >= 1);
    }

    #[test]
    fn page_count_grows_with_data() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        seed_db(&conn);
        let pages = conn.page_count().unwrap();
        assert!(pages >= 2, "expected at least 2 pages, got {pages}");
    }

    #[test]
    fn read_page_returns_correct_size() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        seed_db(&conn);
        let data = conn.read_page(1).unwrap();
        assert_eq!(data.len(), conn.page_size() as usize);
    }

    #[test]
    fn read_page_out_of_range_errors() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        // An empty (just created) DB has 1 page.
        let result = conn.read_page(9999);
        assert!(result.is_err());
    }

    #[test]
    fn read_page_zero_errors() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        let result = conn.read_page(0);
        assert!(result.is_err());
    }

    #[test]
    fn write_page_zero_errors() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        seed_db(&conn);
        let result = conn.write_page(0, &vec![0u8; conn.page_size() as usize]);
        assert!(result.is_err());
    }

    #[test]
    fn backup_copies_all_data() {
        let src_f = NamedTempFile::new().unwrap();
        let dst_f = NamedTempFile::new().unwrap();

        let src = open_rw(src_f.path());
        seed_db(&src);

        let dst = open_rw(dst_f.path());
        let bk = Backup::new(&dst, &src).unwrap();
        let done = bk.step(-1).unwrap();
        assert!(done);
        bk.finish().unwrap();

        // Verify replica has same page count.
        assert_eq!(src.page_count().unwrap(), dst.page_count().unwrap());
    }

    #[test]
    fn prepared_statement_crud_and_introspection() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());

        conn.exec("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB, extra TEXT)")
            .unwrap();

        // Check is_readonly
        let insert_stmt = conn
            .prepare("INSERT INTO items (id, name, score, data, extra) VALUES (?, :name, ?, ?, ?)")
            .unwrap();
        assert!(!insert_stmt.is_readonly());

        let select_stmt = conn
            .prepare("SELECT id, name, score, data, extra FROM items")
            .unwrap();
        assert!(select_stmt.is_readonly());

        drop(select_stmt);
        drop(insert_stmt);

        // Insert with parameters
        let mut stmt = conn
            .prepare("INSERT INTO items (id, name, score, data, extra) VALUES (?, :name, ?, ?, ?)")
            .unwrap();

        let name_idx = stmt.bind_parameter_index("name").unwrap();
        assert_eq!(name_idx, 2);

        stmt.bind_int64(1, 42).unwrap();
        stmt.bind_text(2, "apple").unwrap();
        stmt.bind_double(3, 98.5).unwrap();
        stmt.bind_blob(4, b"binary_data").unwrap();
        stmt.bind_null(5).unwrap();

        let step_res = stmt.step().unwrap();
        assert_eq!(step_res, StepResult::Done);
        assert_eq!(conn.changes(), 1);
        assert_eq!(conn.last_insert_rowid(), 42);

        // Query row
        let mut query = conn
            .prepare("SELECT id, name, score, data, extra FROM items WHERE id = ?")
            .unwrap();
        query.bind_int64(1, 42).unwrap();

        assert_eq!(query.column_count(), 5);
        assert_eq!(query.column_name(0), "id");
        assert_eq!(query.column_name(1), "name");
        assert_eq!(query.column_name(2), "score");

        let step_res = query.step().unwrap();
        assert_eq!(step_res, StepResult::Row);

        assert_eq!(query.column_type(0), ColumnDataType::Integer);
        assert_eq!(query.column_value(0), SqlValue::Integer(42));

        assert_eq!(query.column_type(1), ColumnDataType::Text);
        assert_eq!(query.column_value(1), SqlValue::Text("apple".into()));

        assert_eq!(query.column_type(2), ColumnDataType::Float);
        if let SqlValue::Float(f) = query.column_value(2) {
            assert!((f - 98.5).abs() < f64::EPSILON);
        } else {
            panic!("expected float");
        }

        assert_eq!(query.column_type(3), ColumnDataType::Blob);
        assert_eq!(
            query.column_value(3),
            SqlValue::Blob(b"binary_data".to_vec())
        );

        assert_eq!(query.column_type(4), ColumnDataType::Null);
        assert_eq!(query.column_value(4), SqlValue::Null);

        assert_eq!(query.step().unwrap(), StepResult::Done);
    }

    #[test]
    fn db_additional_coverage_tests() {
        // 1. Connection::open with null byte in path
        let res1 = Connection::open(Path::new("test\0bad.db"), ffi::SQLITE_OPEN_READONLY);
        assert!(res1.is_err());
        let err = match res1 {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(err.to_string().contains("nul byte"));

        // 2. Connection::open on non-existent file in readonly mode
        let tmpdir = tempfile::tempdir().unwrap();
        let non_existent = tmpdir.path().join("no_such_subdir/db.sqlite");
        let res2 = Connection::open(&non_existent, ffi::SQLITE_OPEN_READONLY);
        assert!(res2.is_err());
        let err = match res2 {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(matches!(err, SyncError::Sqlite { .. }));

        // 3. Connection write on read-only connection
        let f = NamedTempFile::new().unwrap();
        let conn_rw = open_rw(f.path());
        seed_db(&conn_rw);
        let ps = conn_rw.page_size() as usize;
        drop(conn_rw);

        let conn_ro = Connection::open(f.path(), ffi::SQLITE_OPEN_READONLY).unwrap();
        let err_ro_write = conn_ro.write_page(1, &vec![0u8; ps]).unwrap_err();
        assert!(err_ro_write.to_string().contains("connection is read-only"));

        // 4. Connection write page with invalid length
        let conn_rw2 = open_rw(f.path());
        let err_bad_len = conn_rw2.write_page(1, &[0u8; 10]).unwrap_err();
        assert!(err_bad_len.to_string().contains("bytes, expected"));

        // 5. wal_checkpoint execution
        conn_rw2.wal_checkpoint().unwrap();

        // 6. Backup incremental stepping, remaining(), and pagecount()
        let dst_f = NamedTempFile::new().unwrap();
        let dst_conn = open_rw(dst_f.path());
        let bk = Backup::new(&dst_conn, &conn_rw2).unwrap();
        let is_done = bk.step(1).unwrap();
        assert!(bk.pagecount() > 0);
        if !is_done {
            assert!(bk.remaining() >= 0);
            let done_final = bk.step(-1).unwrap();
            assert!(done_final);
        }
        bk.finish().unwrap();

        // 7. PreparedStatement reset and clear_bindings
        let mut stmt = conn_rw2.prepare("SELECT ?").unwrap();
        stmt.bind_int64(1, 100).unwrap();
        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_value(0), SqlValue::Integer(100));
        stmt.reset().unwrap();
        stmt.clear_bindings().unwrap();
        assert_eq!(stmt.step().unwrap(), StepResult::Row);
        assert_eq!(stmt.column_value(0), SqlValue::Null);

        // 8. Named parameter lookup variations
        let stmt_named = conn_rw2.prepare("SELECT :id, @name, $val, ?").unwrap();
        assert_eq!(stmt_named.bind_parameter_index("id"), Some(1));
        assert_eq!(stmt_named.bind_parameter_index(":id"), Some(1));
        assert_eq!(stmt_named.bind_parameter_index("name"), Some(2));
        assert_eq!(stmt_named.bind_parameter_index("@name"), Some(2));
        assert_eq!(stmt_named.bind_parameter_index("val"), Some(3));
        assert_eq!(stmt_named.bind_parameter_index("$val"), Some(3));
        assert_eq!(stmt_named.bind_parameter_index("nonexistent"), None);

        // 9. Column decltype
        conn_rw2.exec("CREATE TABLE types_t (i INTEGER, s TEXT, b BLOB)").unwrap();
        let query_types = conn_rw2.prepare("SELECT i, s, b FROM types_t").unwrap();
        assert_eq!(query_types.column_decltype(0), Some("INTEGER".to_string()));
        assert_eq!(query_types.column_decltype(1), Some("TEXT".to_string()));
        assert_eq!(query_types.column_decltype(2), Some("BLOB".to_string()));
        assert_eq!(query_types.column_decltype(999), None);
    }
}
