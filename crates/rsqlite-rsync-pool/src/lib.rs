//! Async connection pool adapters for [`rsqlite_rsync_client::SqlGatewayClient`].
//!
//! This crate provides pool-manager implementations that let you use
//! [`rsqlite_rsync_client::SqlGatewayClient`] with popular async connection pools instead of
//! managing individual client instances by hand.
//!
//! Two backends are supported behind Cargo feature flags (the `bb8` feature
//! is enabled by default):
//!
//! | Feature    | Pool crate                | Module              |
//! |------------|---------------------------|---------------------|
//! | **`bb8`**  | [`bb8`]                   | [`pool_bb8`]        |
//! | `deadpool` | [`deadpool::managed`]     | [`pool_deadpool`]   |
//!
//! Both backends share the same [`SqlGatewayManager`], which holds a
//! [`ClientConfig`] and knows how to create and health-check clients.
//!
//! # Example (bb8)
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
//! use rsqlite_rsync_pool::SqlGatewayManager;
//!
//! let manager = SqlGatewayManager::new(ClientConfig::new(
//!     DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
//! ));
//!
//! let pool = bb8::Pool::builder()
//!     .max_size(8)
//!     .build(manager)
//!     .await?;
//!
//! let mut conn = pool.get().await?;
//! let rows = conn
//!     .query("app.db", "SELECT 1", None, 0, Default::default())
//!     .await?;
//! println!("{} rows", rows.total_rows);
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "bb8")]
pub mod pool_bb8;

#[cfg(feature = "deadpool")]
pub mod pool_deadpool;

// Re-export the client crate so users don't need to pin it independently.
pub use rsqlite_rsync_client;

use rsqlite_rsync_client::ClientConfig;

/// Connection manager for pooling [`rsqlite_rsync_client::SqlGatewayClient`]
/// instances.
///
/// Holds a [`ClientConfig`] that is cloned into each new client the pool
/// creates. The same `SqlGatewayManager` works with every supported pool
/// backend — just enable the corresponding Cargo feature.
#[derive(Clone, Debug)]
pub struct SqlGatewayManager {
    config: ClientConfig,
}

impl SqlGatewayManager {
    /// Create a manager that will produce clients configured with `config`.
    pub fn new(config: ClientConfig) -> Self {
        Self { config }
    }

    /// Return a reference to the underlying [`ClientConfig`].
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsqlite_rsync_client::DiscoveryMode;

    #[test]
    fn manager_is_send_sync_clone() {
        fn assert_send_sync_clone<T: Send + Sync + Clone + 'static>() {}
        assert_send_sync_clone::<SqlGatewayManager>();
    }

    #[test]
    fn manager_debug_output_is_stable() {
        let manager = SqlGatewayManager::new(ClientConfig::new(DiscoveryMode::Direct(
            "http://127.0.0.1:50051".to_string(),
        )));
        let dbg = format!("{manager:?}");
        assert!(dbg.contains("SqlGatewayManager"));
    }
}
