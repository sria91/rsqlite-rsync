// Helper utilities for creating seeded SQLite databases in tests.

use std::path::Path;

use libsqlite3_sys as ffi;
use rsqlite_rsync::db::Connection;

/// Open a read-write (create-if-absent) connection.
#[allow(dead_code)]
pub fn open_rw(path: &Path) -> Connection {
    Connection::open(
        path,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )
    .expect("open rw")
}

/// Create a database with a single table containing `rows` rows.
#[allow(dead_code)]
pub fn seed(path: &Path, rows: usize) {
    let conn = open_rw(path);
    conn.exec("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, data TEXT)")
        .unwrap();
    for i in 0..rows {
        conn.exec(&format!("INSERT INTO items VALUES ({i}, 'value_{i}')"))
            .unwrap();
    }
}

/// Modify every `step`-th row starting at `offset`.
#[allow(dead_code)]
pub fn modify_rows(path: &Path, offset: usize, step: usize, total: usize) {
    let conn = open_rw(path);
    let mut i = offset;
    while i < total {
        conn.exec(&format!(
            "UPDATE items SET data = 'modified_{i}' WHERE id = {i}"
        ))
        .unwrap();
        i += step;
    }
}

/// Append `count` new rows starting at `start_id`.
#[allow(dead_code)]
pub fn append_rows(path: &Path, start_id: usize, count: usize) {
    let conn = open_rw(path);
    for i in 0..count {
        let id = start_id + i;
        conn.exec(&format!(
            "INSERT INTO items VALUES ({id}, 'value_{id}')"
        ))
        .unwrap();
    }
}

/// Return the current SQLite page count for the database.
#[allow(dead_code)]
pub fn count_pages(path: &Path) -> u64 {
    let conn = open_rw(path);
    conn.page_count().unwrap() as u64
}
