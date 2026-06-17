// Error-case integration tests.

mod fixtures {
    include!("../fixtures/gen_db.rs");
}

use rsqlite_rsync::error::SyncError;
use rsqlite_rsync::sync_local;
use tempfile::NamedTempFile;

/// Opening a non-existent path read-only must return an error.
#[tokio::test]
async fn nonexistent_origin_returns_error() {
    let replica_f = NamedTempFile::new().unwrap();
    let result = sync_local(
        std::path::Path::new("/nonexistent/origin.db"),
        replica_f.path(),
    )
    .await;
    assert!(
        result.is_err(),
        "expected error for nonexistent origin, got Ok"
    );
}

/// Protocol error when replica receives an unexpected message.
#[tokio::test]
async fn protocol_error_on_unexpected_message() {
    use libsqlite3_sys as ffi;
    use rsqlite_rsync::db::Connection;
    use rsqlite_rsync::protocol::messages::Message;
    use rsqlite_rsync::protocol::origin;
    use rsqlite_rsync::snapshot::Snapshot;
    use rsqlite_rsync::transport::Transport;
    use rsqlite_rsync::transport::local::LocalTransport;

    let origin_f = NamedTempFile::new().unwrap();
    let conn = Connection::open(
        origin_f.path(),
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )
    .unwrap();
    conn.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    let snap = Snapshot::begin(&conn).unwrap();

    let (mut origin_side, mut peer_side) = LocalTransport::pair();
    peer_side.send(&Message::Done).await.unwrap();

    let result = origin::run(&snap, &mut origin_side).await;
    assert!(matches!(
        result,
        Err(SyncError::Protocol(message)) if message.contains("expected Hello")
    ));
}

/// [`SyncError`] display strings are human-readable.
#[test]
fn sync_error_display() {
    let e = SyncError::sqlite(5, "database is locked");
    assert!(e.to_string().contains("locked"));

    let e = SyncError::PageSizeMismatch {
        origin: 4096,
        replica: 8192,
    };
    assert!(e.to_string().contains("4096"));
    assert!(e.to_string().contains("8192"));

    let e = SyncError::Protocol("bad magic".into());
    assert!(e.to_string().contains("bad magic"));
}

/// Sync from a valid origin to a valid replica path that doesn't yet exist.
#[tokio::test]
async fn replica_created_if_not_exists() {
    let origin_f = NamedTempFile::new().unwrap();
    fixtures::seed(origin_f.path(), 50);

    // Use a path that does not exist yet.
    let dir = tempfile::tempdir().unwrap();
    let replica_path = dir.path().join("new_replica.db");
    assert!(!replica_path.exists());

    sync_local(origin_f.path(), &replica_path).await.unwrap();
    assert!(replica_path.exists(), "replica should have been created");
}
