//! Protocol module: message types and origin/replica state machines.

pub mod messages;
pub mod origin;
pub mod replica;

use crate::SyncTuning;

pub(crate) async fn with_configured_pool<T>(
    tuning: &SyncTuning,
    f: impl FnOnce() -> T + Send + 'static,
) -> T
where
    T: Send + 'static,
{
    if let Some(threads) = tuning.max_hash_threads
        && threads > 0
        && let Ok(pool) = rayon::ThreadPoolBuilder::new().num_threads(threads).build()
    {
        return tokio::task::spawn_blocking(move || pool.install(f))
            .await
            .expect("blocking task should not be cancelled");
    }
    f()
}

/// Compute a result using parallelism when appropriate, falling back to serial execution.
///
/// This helper encapsulates the common pattern of checking tuning parameters and branching
/// between parallel and serial computation. Both `parallel_fn` and `serial_fn` must compute
/// the same result; they differ only in execution strategy.
pub(crate) async fn compute_with_parallelism<T: Send + 'static>(
    tuning: &SyncTuning,
    item_count: u32,
    parallel_fn: impl FnOnce() -> Vec<T> + Send + 'static,
    serial_fn: impl FnOnce() -> Vec<T>,
) -> Vec<T> {
    if tuning.should_parallelize(item_count) {
        with_configured_pool(tuning, parallel_fn).await
    } else {
        serial_fn()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_with_configured_pool_custom_threads() {
        let tuning = SyncTuning {
            max_hash_threads: Some(2),
            parallel_min_pages: 10,
            hash_chunk_groups: 16,
        };
        let res = with_configured_pool(&tuning, || 42).await;
        assert_eq!(res, 42);
    }

    #[tokio::test]
    async fn test_with_configured_pool_fallback_when_none_or_zero() {
        let tuning_none = SyncTuning {
            max_hash_threads: None,
            parallel_min_pages: 10,
            hash_chunk_groups: 16,
        };
        let res1 = with_configured_pool(&tuning_none, || 100).await;
        assert_eq!(res1, 100);

        let tuning_zero = SyncTuning {
            max_hash_threads: Some(0),
            parallel_min_pages: 10,
            hash_chunk_groups: 16,
        };
        let res2 = with_configured_pool(&tuning_zero, || 200).await;
        assert_eq!(res2, 200);
    }

    // Named (rather than inline) so that whichever call below actually
    // invokes a given one, its body is exercised at least once: passing a
    // fresh `|| vec![...]` closure literal at each call site would leave
    // whichever slot isn't taken (parallel_fn on the serial branch, or vice
    // versa) permanently unexecuted, since a closure argument that is never
    // called never runs its body.
    fn parallel_result() -> Vec<i32> {
        vec![1, 2, 3]
    }

    fn serial_result() -> Vec<i32> {
        vec![9, 9, 9]
    }

    #[tokio::test]
    async fn test_compute_with_parallelism_branches() {
        let tuning = SyncTuning {
            max_hash_threads: Some(2),
            parallel_min_pages: 100,
            hash_chunk_groups: 16,
        };

        // Parallel branch: item_count >= 100
        let res_par =
            compute_with_parallelism(&tuning, 150, parallel_result, serial_result).await;
        assert_eq!(res_par, vec![1, 2, 3]);

        // Serial branch: item_count < 100
        let res_ser = compute_with_parallelism(&tuning, 50, parallel_result, serial_result).await;
        assert_eq!(res_ser, vec![9, 9, 9]);
    }
}
