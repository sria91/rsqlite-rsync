//! Consistent read snapshot via `BEGIN DEFERRED` and the backup API.
//!
//! The [`Snapshot`] type opens a read transaction on the origin database so
//! that all page reads during a sync see a single consistent state, even if
//! other writers commit to the origin while the sync is in progress.

use crate::db::Connection;
use crate::error::Result;

/// A read-consistent view of a SQLite database.
///
/// Created via [`Snapshot::begin`]; the transaction is released when this
/// value is dropped.
pub struct Snapshot<'a> {
    conn: &'a Connection,
    bytes: Vec<u8>,
    page_size: u32,
    page_count: u32,
}

impl<'a> Snapshot<'a> {
    /// Begin a deferred (read) transaction on `conn`, creating a consistent
    /// snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::SyncError::Sqlite`] if the transaction cannot
    /// be started.
    pub fn begin(conn: &'a Connection) -> Result<Self> {
        conn.exec("BEGIN DEFERRED")?;
        let page_size = conn.page_size();
        let page_count = conn.page_count()?;
        let bytes = if page_count == 0 {
            Vec::new()
        } else {
            conn.serialize()?
        };
        Ok(Snapshot {
            conn,
            bytes,
            page_size,
            page_count,
        })
    }

    /// The underlying connection (read-only within this snapshot).
    pub fn connection(&self) -> &Connection {
        self.conn
    }

    /// Page size of the captured snapshot.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Page count of the captured snapshot.
    pub fn page_count(&self) -> u32 {
        self.page_count
    }

    /// Return a reference to the raw snapshot bytes.
    ///
    /// Callers can index into this directly to avoid per-page allocation.
    /// Byte offset for 1-indexed page `p` is `(p - 1) * page_size`.
    pub fn all_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Read a page from the captured snapshot bytes.
    pub fn read_page(&self, page_no: u32) -> Result<Vec<u8>> {
        if page_no == 0 {
            return Err(crate::error::SyncError::Protocol(
                "page numbers are 1-indexed; page 0 is invalid".into(),
            ));
        }
        let ps = self.page_size as usize;
        let offset = (page_no - 1) as usize * ps;
        if offset + ps > self.bytes.len() {
            return Err(crate::error::SyncError::sqlite(
                1,
                format!(
                    "page {page_no} is out of range (snapshot has {} pages)",
                    self.page_count
                ),
            ));
        }
        Ok(self.bytes[offset..offset + ps].to_vec())
    }

    /// Explicitly commit the read transaction (no-op for read-only, but keeps
    /// the code symmetric with write transactions).
    pub fn commit(self) -> Result<()> {
        self.conn.exec("COMMIT")?;
        // Prevent the DROP impl from issuing a redundant ROLLBACK.
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for Snapshot<'_> {
    fn drop(&mut self) {
        // Best-effort rollback; ignore errors.
        let _ = self.conn.exec("ROLLBACK");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use libsqlite3_sys as ffi;
    use tempfile::NamedTempFile;

    fn open_rw(path: &std::path::Path) -> Connection {
        Connection::open(path, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE).unwrap()
    }

    #[test]
    fn snapshot_drop_allows_new_snapshot() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());

        conn.exec("CREATE TABLE t (v INTEGER)").unwrap();
        conn.exec("INSERT INTO t VALUES (1)").unwrap();

        let snap = Snapshot::begin(&conn).unwrap();

        // This test validates snapshot lifecycle behavior, not visibility
        // guarantees across concurrent writes.
        drop(snap); // rolls back cleanly

        // Can start a new snapshot after drop.
        let snap2 = Snapshot::begin(&conn).unwrap();
        snap2.commit().unwrap();
    }

    #[test]
    fn snapshot_commit_then_extra_commit_does_not_panic() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        let snap = Snapshot::begin(&conn).unwrap();
        snap.commit().unwrap();
        // A second COMMIT at connection level may fail, but should not panic.
        let _ = conn.exec("COMMIT"); // may error — that's fine
    }

    #[test]
    fn snapshot_read_page_zero_errors() {
        let f = NamedTempFile::new().unwrap();
        let conn = open_rw(f.path());
        conn.exec("CREATE TABLE t (v INTEGER)").unwrap();
        let snap = Snapshot::begin(&conn).unwrap();
        let result = snap.read_page(0);
        assert!(result.is_err());
    }
}
