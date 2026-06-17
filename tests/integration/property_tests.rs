// Property-based tests using `proptest`.
//
// These tests verify that for arbitrary database contents, a sync always
// produces a replica with the same page count and page size as the origin.

mod fixtures {
    include!("../fixtures/gen_db.rs");
}

use proptest::prelude::*;
use rsqlite_rsync::sync_local;
use tempfile::NamedTempFile;

proptest! {
    /// For any number of rows between 1 and 2000, syncing always succeeds and
    /// replica ends up with the same page count as origin.
    #[test]
    fn arbitrary_row_count_syncs_correctly(rows in 1usize..2000) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let origin_f = NamedTempFile::new().unwrap();
            let replica_f = NamedTempFile::new().unwrap();

            fixtures::seed(origin_f.path(), rows);
            sync_local(origin_f.path(), replica_f.path()).await.unwrap();

            let o = std::fs::read(origin_f.path()).unwrap();
            let r = std::fs::read(replica_f.path()).unwrap();
            prop_assert_eq!(o, r);
            Ok(())
        }).unwrap();
    }

    /// After two consecutive syncs, the replica is still consistent.
    #[test]
    fn double_sync_is_idempotent(rows in 1usize..500, extra in 0usize..200) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let origin_f = NamedTempFile::new().unwrap();
            let replica_f = NamedTempFile::new().unwrap();

            fixtures::seed(origin_f.path(), rows);
            sync_local(origin_f.path(), replica_f.path()).await.unwrap();

            // Add more rows to origin without reusing primary keys.
            fixtures::append_rows(origin_f.path(), rows, extra);
            sync_local(origin_f.path(), replica_f.path()).await.unwrap();

            let o = std::fs::read(origin_f.path()).unwrap();
            let r = std::fs::read(replica_f.path()).unwrap();
            prop_assert_eq!(o, r);
            Ok(())
        }).unwrap();
    }
}
