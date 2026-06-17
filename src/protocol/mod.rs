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
            .unwrap();
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
