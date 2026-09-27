//! Embedded gRPC SQL Gateway.

pub mod auth;
pub mod client;
pub mod engine;
pub mod replication;
pub mod server;

pub use auth::AuthConfig;
pub use client::{Client, ClientBackend};
pub use engine::DatabaseEngine;
pub use replication::ReplicationServer;
pub use server::SqlGatewayServer;
