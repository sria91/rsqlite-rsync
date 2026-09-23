//! Leader discovery: locating the current SQL Gateway writer.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tonic::transport::Endpoint;

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
///
/// `auth_token` is threaded through so [`DiscoveryMode::Candidates`]' own
/// `GetClusterStatus` probes authenticate the same way the client's regular
/// RPCs do (see [`crate::client::authorized_request`]) — otherwise every
/// probe against an authentication-requiring gateway would fail uniformly,
/// silently collapsing this into "fall back to the first candidate"
/// regardless of which one is actually the writer.
pub(crate) async fn discover_leader(
    mode: &DiscoveryMode,
    auth_token: &Option<String>,
) -> ClientResult<String> {
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
                            .get_cluster_status(crate::client::authorized_request(
                                auth_token,
                                ClusterStatusRequest {},
                            ))
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
            // reachable): fall back to the first *syntactically valid*
            // candidate. Callers relying on automatic failover should
            // expect a subsequent NOT_LEADER redirect or connection error
            // in this case, not necessarily a successful call.
            candidates
                .iter()
                .map(|s| normalize_endpoint(s))
                .find(|norm| Endpoint::from_shared(norm.clone()).is_ok())
                .ok_or_else(|| {
                    ClientError::discovery(
                        "no syntactically valid candidates configured for discovery",
                    )
                })
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

    #[tokio::test]
    async fn discovery_mode_debug_is_stable_for_custom_resolver() {
        struct NoopResolver;
        #[async_trait]
        impl LeaderResolver for NoopResolver {
            async fn resolve(&self) -> Result<String, BoxError> {
                Ok("resolved".to_string())
            }
        }
        let resolver = Arc::new(NoopResolver);
        assert_eq!(resolver.resolve().await.unwrap(), "resolved");
        let mode = DiscoveryMode::Custom(resolver);
        assert_eq!(format!("{mode:?}"), "Custom(<resolver>)");

        assert_eq!(
            format!("{:?}", DiscoveryMode::Direct("x".to_string())),
            "Direct(\"x\")"
        );
        assert_eq!(
            format!(
                "{:?}",
                DiscoveryMode::Candidates(vec!["http://a".to_string()])
            ),
            "Candidates([\"http://a\"])"
        );
    }

    #[tokio::test]
    async fn test_discover_leader_invalid_candidate_url_skips_and_falls_back_to_valid() {
        // A candidate with invalid endpoint syntax that fails
        // `Endpoint::from_shared` is skipped; the first syntactically valid
        // candidate is used as the fallback instead.
        let mode = DiscoveryMode::Candidates(vec![
            "://invalid url without scheme/host".to_string(),
            "127.0.0.1:50051".to_string(),
        ]);
        let res = discover_leader(&mode, &Some("token".to_string())).await;
        assert_eq!(res.unwrap(), "http://127.0.0.1:50051");
    }

    #[tokio::test]
    async fn test_discover_leader_all_invalid_candidates_returns_error() {
        let mode = DiscoveryMode::Candidates(vec![
            "://invalid url 1".to_string(),
            "://invalid url 2".to_string(),
        ]);
        let res = discover_leader(&mode, &None).await;
        assert!(res.is_err());
        let err_msg = res.unwrap_err().to_string();
        assert!(err_msg.contains("no syntactically valid candidates"));
    }

    #[tokio::test]
    async fn test_discover_leader_custom_resolver() {
        struct CustomRes;
        #[async_trait]
        impl LeaderResolver for CustomRes {
            async fn resolve(&self) -> Result<String, BoxError> {
                Ok("127.0.0.1:60000".to_string())
            }
        }
        let mode = DiscoveryMode::Custom(Arc::new(CustomRes));
        let res = discover_leader(&mode, &None).await.unwrap();
        assert_eq!(res, "http://127.0.0.1:60000");
    }

    // Test the internal logic of handling non-Writer responses from candidates
    #[test]
    fn test_node_role_conversion() {
        // Test that NodeRole::try_from works correctly for different role values
        use rsqlite_rsync_proto::rsqlite::v1::NodeRole;

        // Test Writer role (should succeed)
        let writer_role = rsqlite_rsync_proto::rsqlite::v1::NodeRole::Writer as i32;
        assert_eq!(NodeRole::try_from(writer_role), Ok(NodeRole::Writer));

        // Test Reader role (should succeed but not be Writer)
        let reader_role = rsqlite_rsync_proto::rsqlite::v1::NodeRole::Replica as i32;
        assert_eq!(NodeRole::try_from(reader_role), Ok(NodeRole::Replica));
        assert_ne!(NodeRole::try_from(reader_role), Ok(NodeRole::Writer));

        // Test unknown role (should fail)
        let unknown_role = 999;
        assert!(NodeRole::try_from(unknown_role).is_err());
    }
}
