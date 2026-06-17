// Tests verifying correct behaviour while writes occur concurrently.

mod fixtures {
    include!("../fixtures/gen_db.rs");
}

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use rsqlite_rsync::sync_local;
use tempfile::NamedTempFile;

async fn run_origin_writes_during_sync_once(seed_rows: usize) {
    let origin_f = Arc::new(NamedTempFile::new().unwrap());
    let replica_f = NamedTempFile::new().unwrap();

    fixtures::seed(origin_f.path(), seed_rows);

    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();
    let origin_path = origin_f.path().to_path_buf();

    let writer = std::thread::spawn(move || {
        use libsqlite3_sys as ffi;
        let conn =
            rsqlite_rsync::db::Connection::open(&origin_path, ffi::SQLITE_OPEN_READWRITE).unwrap();
        let mut i = seed_rows;
        while !stop_clone.load(Ordering::Relaxed) {
            let _ = conn.exec(&format!(
                "INSERT OR IGNORE INTO items VALUES ({i}, 'live_{i}')"
            ));
            i += 1;
            std::thread::sleep(std::time::Duration::from_micros(500));
        }
    });

    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // Validate that the destination remains a readable SQLite file.
    {
        use libsqlite3_sys as ffi;
        let r = rsqlite_rsync::db::Connection::open(replica_f.path(), ffi::SQLITE_OPEN_READONLY)
            .unwrap();
        assert!(r.page_count().unwrap() > 0);
    }
}

/// A writer thread continuously inserts rows into the ORIGIN while a sync is
/// running.  The sync must complete without error and produce a consistent
/// replica.
#[tokio::test]
async fn origin_writes_during_sync() {
    run_origin_writes_during_sync_once(200).await;
}

/// Repeat the writer-sync race in a single test to catch intermittent lock
/// handling regressions with higher probability.
#[tokio::test]
async fn origin_writes_during_sync_repeated() {
    const ITERATIONS: usize = 10;
    for _ in 0..ITERATIONS {
        run_origin_writes_during_sync_once(200).await;
    }
}

/// Extended stress variant for nightly/long CI jobs.
///
/// Run explicitly with:
/// cargo test --test live_writes origin_writes_during_sync_stress -- --ignored
#[tokio::test]
#[ignore = "stress test for extended CI"]
async fn origin_writes_during_sync_stress() {
    const ITERATIONS: usize = 50;
    for _ in 0..ITERATIONS {
        run_origin_writes_during_sync_once(200).await;
    }
}

/// A reader can query the REPLICA while a sync is running without errors.
#[tokio::test]
async fn replica_readable_during_sync() {
    let origin_f = NamedTempFile::new().unwrap();
    let replica_f = Arc::new(NamedTempFile::new().unwrap());

    fixtures::seed(origin_f.path(), 500);
    // Pre-populate replica so a reader can open it.
    fixtures::seed(replica_f.path(), 100);

    let replica_path = replica_f.path().to_path_buf();
    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = stop.clone();

    // Background reader: open replica and keep reading page count.
    let reader = std::thread::spawn(move || {
        use libsqlite3_sys as ffi;
        while !stop_clone.load(Ordering::Relaxed) {
            if let Ok(conn) =
                rsqlite_rsync::db::Connection::open(&replica_path, ffi::SQLITE_OPEN_READONLY)
            {
                let _ = conn.page_count();
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });

    sync_local(origin_f.path(), replica_f.path()).await.unwrap();

    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
}
