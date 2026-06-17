//! Criterion benchmarks for local sync throughput and bandwidth efficiency.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::NamedTempFile;

use rsqlite_rsync::hash::{HashAlgorithm, hash_group_for, hash_page_for};

fn seed_db(path: &std::path::Path, rows: usize) {
    use libsqlite3_sys as ffi;
    let conn = rsqlite_rsync::db::Connection::open(
        path,
        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
    )
    .unwrap();
    conn.exec("CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    for i in 0..rows {
        conn.exec(&format!("INSERT INTO t VALUES ({i}, 'v{i}')"))
            .unwrap();
    }
}

fn modify_rows(path: &std::path::Path, every: usize, rows: usize) {
    use libsqlite3_sys as ffi;
    let conn = rsqlite_rsync::db::Connection::open(path, ffi::SQLITE_OPEN_READWRITE).unwrap();
    for i in (0..rows).step_by(every.max(1)) {
        conn.exec(&format!("UPDATE t SET v = 'mod_{i}' WHERE id = {i}"))
            .unwrap();
    }
}

fn bench_hashing(c: &mut Criterion) {
    let mut group = c.benchmark_group("hash");
    let page = vec![0xABu8; 4096];
    let page_hashes = vec![hash_page_for(&page, HashAlgorithm::Blake3V2); 64];

    group.bench_function("page_sha256_v1", |b| {
        b.iter(|| hash_page_for(black_box(&page), HashAlgorithm::Sha256V1))
    });

    group.bench_function("page_blake3_v2", |b| {
        b.iter(|| hash_page_for(black_box(&page), HashAlgorithm::Blake3V2))
    });

    group.bench_function("group_sha256_v1", |b| {
        b.iter(|| hash_group_for(black_box(&page_hashes), HashAlgorithm::Sha256V1))
    });

    group.bench_function("group_blake3_v2", |b| {
        b.iter(|| hash_group_for(black_box(&page_hashes), HashAlgorithm::Blake3V2))
    });

    group.finish();
}

fn bench_local_sync(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("local_sync_full");
    for rows in [500usize, 2000, 5000] {
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, &rows| {
            b.iter(|| {
                let origin_f = NamedTempFile::new().unwrap();
                let replica_f = NamedTempFile::new().unwrap();
                seed_db(origin_f.path(), rows);
                rt.block_on(rsqlite_rsync::sync_local(origin_f.path(), replica_f.path()))
                    .unwrap();
            });
        });
    }
    group.finish();
}

fn bench_incremental_sync(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("local_sync_incremental");
    let rows = 10_000usize;

    group.bench_function("diff_0pct", |b| {
        b.iter_batched(
            || {
                let origin_f = NamedTempFile::new().unwrap();
                let replica_f = NamedTempFile::new().unwrap();
                seed_db(origin_f.path(), rows);
                rt.block_on(rsqlite_rsync::sync_local(origin_f.path(), replica_f.path()))
                    .unwrap();
                (origin_f, replica_f)
            },
            |(origin_f, replica_f)| {
                rt.block_on(rsqlite_rsync::sync_local(origin_f.path(), replica_f.path()))
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("diff_1pct", |b| {
        b.iter_batched(
            || {
                let origin_f = NamedTempFile::new().unwrap();
                let replica_f = NamedTempFile::new().unwrap();
                seed_db(origin_f.path(), rows);
                rt.block_on(rsqlite_rsync::sync_local(origin_f.path(), replica_f.path()))
                    .unwrap();
                modify_rows(origin_f.path(), 100, rows);
                (origin_f, replica_f)
            },
            |(origin_f, replica_f)| {
                rt.block_on(rsqlite_rsync::sync_local(origin_f.path(), replica_f.path()))
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.bench_function("diff_50pct", |b| {
        b.iter_batched(
            || {
                let origin_f = NamedTempFile::new().unwrap();
                let replica_f = NamedTempFile::new().unwrap();
                seed_db(origin_f.path(), rows);
                rt.block_on(rsqlite_rsync::sync_local(origin_f.path(), replica_f.path()))
                    .unwrap();
                modify_rows(origin_f.path(), 2, rows);
                (origin_f, replica_f)
            },
            |(origin_f, replica_f)| {
                rt.block_on(rsqlite_rsync::sync_local(origin_f.path(), replica_f.path()))
                    .unwrap();
            },
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

criterion_group!(benches, bench_hashing, bench_local_sync, bench_incremental_sync);
criterion_main!(benches);
