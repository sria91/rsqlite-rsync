// Local-to-local sync integration tests.

mod fixtures {
    include!("../fixtures/gen_db.rs");
}

use std::fs;

use rsqlite_rsync::sync_local;
use tempfile::NamedTempFile;

/// Verify that syncing a populated origin onto an empty replica produces
/// identical page counts and file sizes.
#[tokio::test]
async fn empty_replica_becomes_copy_of_origin() {
    let origin_f = NamedTempFile::new().unwrap();
    let replica_f = NamedTempFile::new().unwrap();

    fixtures::seed(origin_f.path(), 500);

    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    let origin_bytes = fs::read(origin_f.path()).unwrap();
    let replica_bytes = fs::read(replica_f.path()).unwrap();
    assert_eq!(
        origin_bytes, replica_bytes,
        "replica bytes should equal origin bytes"
    );
}

/// When origin and replica are already identical, no pages should be
/// transferred (protocol should complete immediately after coarse pass).
#[tokio::test]
async fn identical_databases_no_error() {
    let origin_f = NamedTempFile::new().unwrap();
    let replica_f = NamedTempFile::new().unwrap();

    fixtures::seed(origin_f.path(), 200);

    // First sync — makes them identical.
    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    // Second sync — they are already in sync.
    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    let o = fs::read(origin_f.path()).unwrap();
    let r = fs::read(replica_f.path()).unwrap();
    assert_eq!(o, r, "replica content should still match after second sync");
}

/// Modify a fraction of origin pages and re-sync; replica should become
/// identical to origin again.
#[tokio::test]
async fn partial_diff_synced_correctly() {
    let origin_f = NamedTempFile::new().unwrap();
    let replica_f = NamedTempFile::new().unwrap();

    fixtures::seed(origin_f.path(), 1000);
    // Initial sync.
    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    // Modify ~10% of rows on origin.
    fixtures::modify_rows(origin_f.path(), 0, 10, 1000);

    // Re-sync.
    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    let o = fs::read(origin_f.path()).unwrap();
    let r = fs::read(replica_f.path()).unwrap();
    assert_eq!(o, r, "replica bytes must equal origin after partial sync");
}

/// Sync a brand-new (completely different) database onto an existing replica.
#[tokio::test]
async fn completely_different_databases() {
    let origin_f = NamedTempFile::new().unwrap();
    let replica_f = NamedTempFile::new().unwrap();

    // Seed replica with different data first.
    fixtures::seed(replica_f.path(), 100);
    // Seed origin with more rows.
    fixtures::seed(origin_f.path(), 800);

    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    let o = fs::read(origin_f.path()).unwrap();
    let r = fs::read(replica_f.path()).unwrap();
    assert_eq!(o, r, "replica bytes must equal origin bytes");
}

/// Syncing a minimal (1-page) database works.
#[tokio::test]
async fn minimal_database_sync() {
    let origin_f = NamedTempFile::new().unwrap();
    let replica_f = NamedTempFile::new().unwrap();

    // Just create an empty origin DB.
    {
        use libsqlite3_sys as ffi;
        let _ = rsqlite_rsync::db::Connection::open(
            origin_f.path(),
            ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
        )
        .unwrap();
    }

    sync_local(origin_f.path(), replica_f.path()).await.unwrap();
}
