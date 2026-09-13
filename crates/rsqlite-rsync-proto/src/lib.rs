//! Generated protobuf/gRPC types for the `rsqlite-rsync` SQL Gateway, plus
//! the small set of gRPC metadata keys both the server and any client
//! implementation need to agree on out-of-band (leader redirection).
//!
//! This crate has no business logic — it is the shared wire contract that
//! both `rsqlite-rsync` (the server) and `rsqlite-rsync-client` (or any
//! other client implementation) depend on.

pub mod rsqlite {
    pub mod v1 {
        tonic::include_proto!("rsqlite.v1");
    }
}

/// gRPC metadata keys used by the `SqlGateway` service to signal that a node
/// is not the current writer, and where the writer can currently be reached.
pub mod metadata {
    /// Metadata key carrying a machine-readable status code (see
    /// [`CODE_NOT_LEADER`]) on error responses.
    pub const HEADER_RSQLITE_CODE: &str = "x-rsqlite-code";
    /// Metadata key carrying the current writer's node id, when known.
    pub const HEADER_RSQLITE_LEADER_ID: &str = "x-rsqlite-leader-id";
    /// Metadata key carrying the responding node's local generation number.
    pub const HEADER_RSQLITE_GENERATION: &str = "x-rsqlite-generation";
    /// Metadata key carrying a gRPC endpoint URL for the current writer,
    /// when known. Clients use this to transparently redirect a request.
    pub const HEADER_RSQLITE_LEADER_ENDPOINT: &str = "x-rsqlite-leader-endpoint";
    /// Value of [`HEADER_RSQLITE_CODE`] indicating the responding node is
    /// not the active writer.
    pub const CODE_NOT_LEADER: &str = "NOT_LEADER";
}
