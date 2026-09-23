//! [`r2d2::ManageConnection`] implementation for
//! [`rsqlite_rsync_client::blocking::SqlGatewayClient`].
//!
//! Enable with `features = ["r2d2"]`.
//!
//! # Example
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
//! conn.execute("app.db", "CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY)", None)?;
//! # Ok(())
//! # }
//! ```

use r2d2::ManageConnection;
use rsqlite_rsync_client::blocking::SqlGatewayClient;
use rsqlite_rsync_client::ClientError;

use crate::SqlGatewayManager;

/// Convenience alias for an r2d2 pool of [`SqlGatewayClient`] connections.
pub type Pool = r2d2::Pool<SqlGatewayManager>;

/// Convenience alias for a pooled [`SqlGatewayClient`] connection.
pub type PooledConnection = r2d2::PooledConnection<SqlGatewayManager>;

/// Convenience alias for the r2d2 pool builder configured with
/// [`SqlGatewayManager`].
pub type Builder = r2d2::Builder<SqlGatewayManager>;

impl ManageConnection for SqlGatewayManager {
    type Connection = SqlGatewayClient;
    type Error = ClientError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let mut client = SqlGatewayClient::new(self.config.clone());
        // Eagerly validate so the pool never hands out a client that
        // cannot reach its cluster.
        client.get_cluster_status()?;
        Ok(client)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.get_cluster_status()?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // SqlGatewayClient self-heals through its internal retry loop and
        // leader redirection, so there is no synchronously-detectable
        // "broken" state.
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use r2d2::ManageConnection;
    use rsqlite_rsync_client::{ClientConfig, DiscoveryMode};

    #[test]
    fn r2d2_manage_connection_impl_compiles() {
        // Type-level check: ManageConnection is implemented and the
        // associated types resolve correctly.
        fn assert_manage<
            T: ManageConnection<Connection = SqlGatewayClient, Error = ClientError>,
        >() {
        }
        assert_manage::<SqlGatewayManager>();
    }

    #[test]
    fn pool_type_aliases_resolve() {
        // Verify the convenience type aliases are well-formed.
        fn _accept_pool(_p: Pool) {}
        fn _accept_conn(_c: PooledConnection) {}
        fn _accept_builder(_b: Builder) {}
    }

    #[test]
    fn has_broken_always_returns_false() {
        let manager = SqlGatewayManager::new(ClientConfig::new(DiscoveryMode::Direct(
            "http://127.0.0.1:50051".to_string(),
        )));
        let mut client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
            "http://127.0.0.1:50051".to_string(),
        )));
        assert!(!manager.has_broken(&mut client));
    }
}
