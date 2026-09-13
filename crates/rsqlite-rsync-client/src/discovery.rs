//! Leader discovery: locating the current SQL Gateway writer.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Endpoint;
use tonic::Request;

use rsqlite_rsync_proto::rsqlite::v1::sql_gateway_client::SqlGatewayClient as TonicSqlGatewayClient;
use rsqlite_rsync_proto::rsqlite::v1::{ClusterStatusRequest, NodeRole};

use crate::error::{BoxError, ClientError, ClientResult};

/// Timeout used to probe each [`DiscoveryMode::Candidates`] endpoint while
/// looking for the current writer. Not currently configurable.
const CANDIDATE_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

/// A pluggable strategy for resolving the current writer's gRPC endpoint.
///
/// Implement this to plug in any leader-discovery mechanism — a Kubernetes
/// Lease, Consul, etcd, a service mesh header, or anything else — without
/// this crate needing to know about it. A plain `Fn() -> Future<Output =
/// Result<String, BoxError>>` closure works too, via the blanket impl below.
#[async_trait]
pub trait LeaderResolver: Send + Sync + 'static {
    /// Resolve and return the current writer's endpoint, e.g.
    /// `"http://10.0.0.7:50051"`. A missing scheme is normalized to
    /// `http://` by the caller.
    async fn resolve(&self) -> Result<String, BoxError>;
}

#[async_trait]
impl<F, Fut> LeaderResolver for F
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<String, BoxError>> + Send,
{
    async fn resolve(&self) -> Result<String, BoxError> {
        (self)().await
    }
}

/// Discovery mode for locating the active writer node.
#[derive(Clone)]
#[non_exhaustive]
pub enum DiscoveryMode {
    /// Single static endpoint.
    Direct(String),
    /// Multiple candidate endpoints, probed in order via `GetClusterStatus`
    /// for whichever currently reports itself as the writer. If none do
    /// (or none are reachable), falls back to the first candidate.
    Candidates(Vec<String>),
    /// A caller-supplied strategy for resolving the current writer.
    Custom(Arc<dyn LeaderResolver>),
}

impl std::fmt::Debug for DiscoveryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscoveryMode::Direct(ep) => f.debug_tuple("Direct").field(ep).finish(),
            DiscoveryMode::Candidates(candidates) => {
                f.debug_tuple("Candidates").field(candidates).finish()
            }
            DiscoveryMode::Custom(_) => f.write_str("Custom(<resolver>)"),
        }
    }
}

/// Normalize an endpoint string, prefixing `http://` if no scheme is present.
pub(crate) fn normalize_endpoint(ep: &str) -> String {
    let trimmed = ep.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

/// Resolve the current writer endpoint for the given [`DiscoveryMode`].
pub(crate) async fn discover_leader(mode: &DiscoveryMode) -> ClientResult<String> {
    match mode {
        DiscoveryMode::Direct(ep) => Ok(normalize_endpoint(ep)),
        DiscoveryMode::Candidates(candidates) => {
            for candidate in candidates {
                let norm = normalize_endpoint(candidate);
                if let Ok(ep) = Endpoint::from_shared(norm.clone()) {
                    let probe = ep
                        .timeout(CANDIDATE_PROBE_TIMEOUT)
                        .connect_timeout(CANDIDATE_PROBE_TIMEOUT)
                        .connect()
                        .await;
                    if let Ok(channel) = probe {
                        let mut probe_client = TonicSqlGatewayClient::new(channel);
                        let resp = probe_client
                            .get_cluster_status(Request::new(ClusterStatusRequest {}))
                            .await;
                        if let Ok(resp) = resp {
                            let status = resp.into_inner();
                            if NodeRole::try_from(status.role) == Ok(NodeRole::Writer) {
                                return Ok(norm);
                            }
                        }
                    }
                }
            }
            // No candidate reported itself as writer (or none were
            // reachable): fall back to the first candidate. Callers relying
            // on automatic failover should expect a subsequent NOT_LEADER
            // redirect or connection error in this case, not necessarily a
            // successful call.
            candidates
                .first()
                .map(|s| normalize_endpoint(s))
                .ok_or_else(|| ClientError::discovery("no candidates configured for discovery"))
        }
        DiscoveryMode::Custom(resolver) => resolver
            .resolve()
            .await
            .map(|ep| normalize_endpoint(&ep))
            .map_err(|source| ClientError::discovery_with_source("custom resolver failed", source)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_endpoint_adds_http_scheme_when_missing() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:50051"),
            "http://127.0.0.1:50051"
        );
    }

    #[test]
    fn normalize_endpoint_preserves_http_and_https_schemes() {
        assert_eq!(normalize_endpoint("http://host:1"), "http://host:1");
        assert_eq!(normalize_endpoint("https://host:1"), "https://host:1");
    }

    #[test]
    fn normalize_endpoint_trims_whitespace() {
        assert_eq!(normalize_endpoint("  127.0.0.1:1  "), "http://127.0.0.1:1");
    }

    #[test]
    fn normalize_endpoint_prefixes_non_http_schemes() {
        // Documents current behavior: any non-http(s)-prefixed string is
        // treated as schemeless and gets `http://` prepended verbatim.
        assert_eq!(normalize_endpoint("unix:/run/x"), "http://unix:/run/x");
    }

    #[test]
    fn discovery_mode_debug_is_stable_for_custom_resolver() {
        struct NoopResolver;
        #[async_trait]
        impl LeaderResolver for NoopResolver {
            async fn resolve(&self) -> Result<String, BoxError> {
                Ok(String::new())
            }
        }
        let mode = DiscoveryMode::Custom(Arc::new(NoopResolver));
        assert_eq!(format!("{mode:?}"), "Custom(<resolver>)");

        assert_eq!(
            format!("{:?}", DiscoveryMode::Direct("x".to_string())),
            "Direct(\"x\")"
        );
    }
}
