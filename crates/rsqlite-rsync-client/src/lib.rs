//! Async gRPC client for the rsqlite-rsync HA SQL Gateway.
//!
//! This crate is deliberately lean: it depends only on `tonic`, a slim
//! `tokio`, and the generated wire types in [`rsqlite_rsync_proto`] — no
//! SQLite FFI, no HA controller, no CLI dependencies — so any Rust service
//! can depend on it to talk to an rsqlite-rsync HA cluster.
//!
//! # Quickstart
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! use rsqlite_rsync_client::{ClientConfig, DiscoveryMode, SqlGatewayClient};
//!
//! let mut client = SqlGatewayClient::new(ClientConfig::new(
//!     DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
//! ));
//!
//! client
//!     .execute("app.db", "CREATE TABLE t (id INTEGER PRIMARY KEY)", None)
//!     .await?;
//! let rows = client
//!     .query("app.db", "SELECT * FROM t", None, 0, Default::default())
//!     .await?;
//! println!("{} rows", rows.total_rows);
//! # Ok(())
//! # }
//! ```
//!
//! # Leader discovery
//!
//! [`DiscoveryMode`] supports a single static endpoint, a list of
//! candidates to probe, or a fully custom [`LeaderResolver`] for plugging in
//! any external discovery mechanism (Kubernetes, Consul, etcd, a service
//! mesh header, ...) without this crate needing to know about it.
//!
//! # A note on retries
//!
//! [`SqlGatewayClient::execute`]/[`SqlGatewayClient::batch`] (writes) are
//! only automatically retried on a definitive `NOT_LEADER` response; a
//! transient `Unavailable` or `DeadlineExceeded` is surfaced immediately
//! rather than blindly retried, since the write's outcome on the server
//! side is not known in that case. Reads (`query`/`stream_query`/
//! `get_cluster_status`) retry on all three.

mod client;
mod config;
mod discovery;
mod error;

pub use client::SqlGatewayClient;
pub use config::{ClientConfig, ClientTarget, RuntimeMode};
pub use discovery::{DiscoveryMode, LeaderResolver};
pub use error::{BoxError, ClientError, ClientResult};

/// Re-exported so callers can name [`tonic::Streaming`], [`tonic::Status`],
/// and [`tonic::Code`] without independently version-pinning `tonic`.
pub use tonic;

/// Re-exported generated protobuf/gRPC request, response, and enum types.
pub use rsqlite_rsync_proto::rsqlite::v1 as proto;
