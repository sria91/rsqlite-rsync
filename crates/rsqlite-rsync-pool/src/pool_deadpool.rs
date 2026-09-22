//! [`deadpool::managed::Manager`] implementation for
//! [`rsqlite_rsync_client::SqlGatewayClient`].
//!
//! Enable with `features = ["deadpool"]`.
//!
//! # Example
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
//! use rsqlite_rsync_pool::SqlGatewayManager;
//! use rsqlite_rsync_pool::pool_deadpool::Pool;
//!
//! let manager = SqlGatewayManager::new(ClientConfig::new(
//!     DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
//! ));
//!
//! let pool = Pool::builder(manager)
//!     .max_size(8)
//!     .build()
//!     .expect("pool build");
//!
//! let mut conn = pool.get().await?;
//! conn.execute("app.db", "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)", None)
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::future::Future;

use deadpool::managed::{Metrics, RecycleError, RecycleResult};
use rsqlite_rsync_client::{ClientError, SqlGatewayClient};

use crate::SqlGatewayManager;

/// Convenience alias for a deadpool pool of [`SqlGatewayClient`] connections.
pub type Pool = deadpool::managed::Pool<SqlGatewayManager>;

/// Convenience alias for a pooled [`SqlGatewayClient`] object.
pub type Object = deadpool::managed::Object<SqlGatewayManager>;

/// Convenience alias for the deadpool builder configured with
/// [`SqlGatewayManager`].
pub type Builder = deadpool::managed::PoolBuilder<SqlGatewayManager>;

impl deadpool::managed::Manager for SqlGatewayManager {
    type Type = SqlGatewayClient;
    type Error = ClientError;

    fn create(
        &self,
    ) -> impl Future<Output = Result<Self::Type, Self::Error>> + Send {
        let config = self.config.clone();
        async move {
            let mut client = SqlGatewayClient::new(config);
            // Eagerly validate so the pool never hands out a client that
            // cannot reach its cluster.
            client.get_cluster_status().await?;
            Ok(client)
        }
    }

    fn recycle(
        &self,
        conn: &mut Self::Type,
        _metrics: &Metrics,
    ) -> impl Future<Output = RecycleResult<Self::Error>> + Send {
        async move {
            conn.get_cluster_status()
                .await
                .map_err(|e| RecycleError::Backend(e))?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[allow(unused_imports)]
    use rsqlite_rsync_client::{ClientConfig, DiscoveryMode};

    #[test]
    fn deadpool_manager_impl_compiles() {
        // Type-level check: Manager is implemented and the associated
        // types resolve correctly.
        fn assert_manager<T: deadpool::managed::Manager<Type = SqlGatewayClient, Error = ClientError>>() {}
        assert_manager::<SqlGatewayManager>();
    }

    #[test]
    fn pool_type_aliases_resolve() {
        fn _accept_pool(_p: Pool) {}
        fn _accept_object(_o: Object) {}
    }
}
