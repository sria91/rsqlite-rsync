//! Error type for [`crate::SqlGatewayClient`].

use tonic::{Code, Status};

/// A type-erased error, used by [`crate::LeaderResolver`] implementations so
/// they aren't forced to depend on this crate's error type.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Convenience alias for results returned by this crate.
pub type ClientResult<T> = Result<T, ClientError>;

/// Errors that can occur while using [`crate::SqlGatewayClient`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientError {
    /// The configured endpoint string could not be parsed as a URI. This is
    /// treated as a permanent configuration error and is never retried.
    #[error("invalid endpoint '{endpoint}': {source}")]
    InvalidEndpoint {
        /// The offending endpoint string.
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },

    /// Failed to establish a connection to an endpoint.
    #[error("failed to connect to '{endpoint}': {source}")]
    Connect {
        /// The endpoint that could not be reached.
        endpoint: String,
        #[source]
        source: tonic::transport::Error,
    },

    /// Leader discovery failed: no reachable/writer candidate was found, or
    /// a [`crate::LeaderResolver`] returned an error.
    #[error("leader discovery failed: {message}")]
    Discovery {
        /// Human-readable description of the failure.
        message: String,
        #[source]
        source: Option<BoxError>,
    },

    /// The gRPC call itself returned an error status. Boxed because
    /// [`Status`] is 176+ bytes and would otherwise dominate the size of
    /// every [`ClientResult`], even on the common non-error path.
    #[error("gRPC call failed (code: {:?}): {}", .0.code(), .0.message())]
    Rpc(#[from] Box<Status>),

    /// Retries were exhausted without a successful response.
    #[error("gave up after {attempts} attempt(s): {source}")]
    RetriesExhausted {
        /// Total number of attempts made, including the first.
        attempts: usize,
        #[source]
        source: Box<ClientError>,
    },
}

impl ClientError {
    pub(crate) fn discovery(message: impl Into<String>) -> Self {
        ClientError::Discovery {
            message: message.into(),
            source: None,
        }
    }

    pub(crate) fn discovery_with_source(message: impl Into<String>, source: BoxError) -> Self {
        ClientError::Discovery {
            message: message.into(),
            source: Some(source),
        }
    }

    /// The underlying [`tonic::Status`], if this error originated from (or,
    /// through [`ClientError::RetriesExhausted`], wraps) a gRPC response.
    pub fn status(&self) -> Option<&Status> {
        match self {
            ClientError::Rpc(status) => Some(status.as_ref()),
            ClientError::RetriesExhausted { source, .. } => source.status(),
            _ => None,
        }
    }

    /// The gRPC status code, if any (see [`ClientError::status`]).
    pub fn code(&self) -> Option<Code> {
        self.status().map(|s| s.code())
    }

    /// True if this error indicates the node that was called is not the
    /// current writer (a `NOT_LEADER`-coded `FailedPrecondition`).
    pub fn is_not_leader(&self) -> bool {
        self.status().map(is_not_leader_status).unwrap_or(false)
    }

    /// True if this kind of error is generally worth retrying: a definitive
    /// `NOT_LEADER`, or a transient `Unavailable`/`DeadlineExceeded`.
    pub fn is_retryable(&self) -> bool {
        self.is_not_leader()
            || matches!(
                self.code(),
                Some(Code::Unavailable) | Some(Code::DeadlineExceeded)
            )
    }
}

/// True if `status` is a `NOT_LEADER`-coded `FailedPrecondition`, per the
/// [`rsqlite_rsync_proto::metadata`] wire contract.
pub(crate) fn is_not_leader_status(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition
        && status
            .metadata()
            .get(rsqlite_rsync_proto::metadata::HEADER_RSQLITE_CODE)
            .map(|v| v == rsqlite_rsync_proto::metadata::CODE_NOT_LEADER)
            .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn not_leader_status() -> Status {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            rsqlite_rsync_proto::metadata::HEADER_RSQLITE_CODE,
            rsqlite_rsync_proto::metadata::CODE_NOT_LEADER
                .parse()
                .unwrap(),
        );
        Status::with_metadata(
            Code::FailedPrecondition,
            "node is not the active writer",
            metadata,
        )
    }

    #[test]
    fn client_error_display_contains_grpc_code_through_retry_wrapper() {
        // Guards the exact substring `tests/integration/grpc_gateway.rs`
        // (in the main workspace crate) asserts on for an unmodified
        // `SyncError`-mapped client error.
        let err = ClientError::RetriesExhausted {
            attempts: 1,
            source: Box::new(ClientError::Rpc(Box::new(not_leader_status()))),
        };
        assert!(err.to_string().contains("FailedPrecondition"));
    }

    #[test]
    fn client_error_accessors_expose_code_and_not_leader_through_wrapper() {
        let err = ClientError::RetriesExhausted {
            attempts: 3,
            source: Box::new(ClientError::Rpc(Box::new(not_leader_status()))),
        };
        assert_eq!(err.code(), Some(Code::FailedPrecondition));
        assert!(err.is_not_leader());
        assert!(err.is_retryable());
    }

    #[test]
    fn non_rpc_errors_have_no_status_or_code() {
        let err = ClientError::discovery("no candidates configured for discovery");
        assert!(err.status().is_none());
        assert!(err.code().is_none());
        assert!(!err.is_not_leader());
        assert!(!err.is_retryable());
    }

    #[test]
    fn client_is_send_sync_and_spawnable() {
        fn assert_send_sync<T: Send + Sync + 'static>() {}
        assert_send_sync::<crate::SqlGatewayClient>();
        assert_send_sync::<ClientError>();
    }
}
