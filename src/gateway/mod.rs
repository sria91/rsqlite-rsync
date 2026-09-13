//! Embedded gRPC SQL Gateway.

pub mod engine;
pub mod server;

pub use engine::DatabaseEngine;
pub use server::SqlGatewayServer;
