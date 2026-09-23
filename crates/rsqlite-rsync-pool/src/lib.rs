//! Connection pool adapters for [`rsqlite_rsync_client::SqlGatewayClient`].
//!
//! This crate provides pool-manager implementations that let you use
//! [`rsqlite_rsync_client::SqlGatewayClient`] (async) or
//! [`rsqlite_rsync_client::blocking::SqlGatewayClient`] (sync) with popular connection pools
//! instead of managing individual client instances by hand.
//!
//! Three backends are supported behind Cargo feature flags (the `bb8` feature
//! is enabled by default):
//!
//! | Feature    | Pool crate                | Mode   | Module              |
//! |------------|---------------------------|--------|---------------------|
//! | **`bb8`**  | [`bb8`]                   | Async  | [`pool_bb8`]        |
//! | `deadpool` | [`deadpool::managed`]     | Async  | [`pool_deadpool`]   |
//! | `r2d2`     | [`r2d2`]                  | Sync   | [`pool_r2d2`]       |
//!
//! All backends share the same [`SqlGatewayManager`], which holds a
//! [`ClientConfig`] and knows how to create and health-check clients.
//!
//! # Example (bb8 - Async)
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rsqlite_rsync_pool::pool_bb8::Pool;
//! use rsqlite_rsync_pool::rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
//! use rsqlite_rsync_pool::SqlGatewayManager;
//!
//! let manager = SqlGatewayManager::new(ClientConfig::new(
//!     DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
//! ));
//!
//! let pool = Pool::builder()
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
//!
//! # Example (r2d2 - Sync)
//!
//! ```no_run
//! # fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rsqlite_rsync_pool::pool_r2d2::Pool;
//! use rsqlite_rsync_pool::rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
//! use rsqlite_rsync_pool::SqlGatewayManager;
//!
//! let manager = SqlGatewayManager::new(ClientConfig::new(
//!     DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
//! ));
//!
//! let pool = Pool::builder()
//!     .max_size(8)
//!     .build(manager)?;
//!
//! let mut conn = pool.get()?;
//! let rows = conn.query("app.db", "SELECT 1", None, 0, Default::default())?;
//! println!("{} rows", rows.total_rows);
//! # Ok(())
//! # }
//! ```

#[cfg(feature = "bb8")]
pub mod pool_bb8;

#[cfg(feature = "deadpool")]
pub mod pool_deadpool;

#[cfg(feature = "r2d2")]
pub mod pool_r2d2;

// Re-export the client crate so users don't need to pin it independently.
pub use rsqlite_rsync_client;

use rsqlite_rsync_client::ClientConfig;

/// Connection manager for pooling [`rsqlite_rsync_client::SqlGatewayClient`]
/// instances.
///
/// Holds a [`ClientConfig`] that is cloned into each new client the pool
/// creates. The same `SqlGatewayManager` works with every supported pool
/// backend — just enable the corresponding Cargo feature.
#[derive(Clone)]
pub struct SqlGatewayManager {
    config: ClientConfig,
}

impl std::fmt::Debug for SqlGatewayManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlGatewayManager").finish()
    }
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
        let manager = SqlGatewayManager::new(
            ClientConfig::new(DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()))
                .with_auth_token("secret-token-12345"),
        );
        let dbg = format!("{manager:?}");
        assert_eq!(dbg, "SqlGatewayManager");
        assert!(!dbg.contains("secret-token-12345"));
    }
}
