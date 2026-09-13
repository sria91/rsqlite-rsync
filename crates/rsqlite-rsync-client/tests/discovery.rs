//! Tests for `DiscoveryMode` — Direct, Candidates, and Custom resolvers.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rsqlite_rsync_client::{ClientConfig, ClientError, DiscoveryMode, SqlGatewayClient};
use support::{closed_endpoint, MockGateway, MockServer};

#[tokio::test]
async fn direct_mode_returns_normalized_endpoint_without_network() {
    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Direct(
        "127.0.0.1:9999".to_string(),
    )));
    let endpoint = client.discover_leader().await.unwrap();
    assert_eq!(endpoint, "http://127.0.0.1:9999");
}

#[tokio::test]
async fn candidates_empty_returns_discovery_error() {
    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Candidates(vec![])));
    let err = client.discover_leader().await.unwrap_err();
    assert!(matches!(err, ClientError::Discovery { .. }));
}

#[tokio::test]
async fn candidates_picks_writer_not_first_candidate() {
    let replica = MockServer::start(MockGateway::replica()).await;
    let writer = MockServer::start(MockGateway::writer()).await;

    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Candidates(vec![
        replica.endpoint(),
        writer.endpoint(),
    ])));
    let endpoint = client.discover_leader().await.unwrap();
    assert_eq!(endpoint, writer.endpoint());
}

#[tokio::test]
async fn candidates_skips_unreachable_candidate() {
    let writer = MockServer::start(MockGateway::writer()).await;
    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Candidates(vec![
        closed_endpoint(),
        writer.endpoint(),
    ])));
    let endpoint = client.discover_leader().await.unwrap();
    assert_eq!(endpoint, writer.endpoint());
}

#[tokio::test]
async fn candidates_falls_back_to_first_when_no_writer_found() {
    let a = MockServer::start(MockGateway::replica()).await;
    let b = MockServer::start(MockGateway::replica()).await;

    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Candidates(vec![
        a.endpoint(),
        b.endpoint(),
    ])));
    let endpoint = client.discover_leader().await.unwrap();
    assert_eq!(
        endpoint,
        a.endpoint(),
        "documented fallback: first candidate wins when nobody is writer"
    );
}

#[tokio::test]
async fn candidates_falls_back_to_first_when_all_unreachable() {
    let a = closed_endpoint();
    let b = closed_endpoint();
    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Candidates(vec![
        a.clone(),
        b,
    ])));
    let endpoint = client.discover_leader().await.unwrap();
    assert_eq!(endpoint, a);
}

#[tokio::test]
async fn custom_resolver_endpoint_is_used_and_normalized() {
    let writer = MockServer::start(MockGateway::writer()).await;
    let addr = writer.endpoint().trim_start_matches("http://").to_string();

    struct FixedResolver(String);
    #[async_trait::async_trait]
    impl rsqlite_rsync_client::LeaderResolver for FixedResolver {
        async fn resolve(&self) -> Result<String, rsqlite_rsync_client::BoxError> {
            Ok(self.0.clone())
        }
    }

    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Custom(Arc::new(
        FixedResolver(addr),
    ))));
    let endpoint = client.discover_leader().await.unwrap();
    assert_eq!(endpoint, writer.endpoint());
}

#[tokio::test]
async fn custom_resolver_error_maps_to_discovery_error_with_source() {
    struct FailingResolver;
    #[async_trait::async_trait]
    impl rsqlite_rsync_client::LeaderResolver for FailingResolver {
        async fn resolve(&self) -> Result<String, rsqlite_rsync_client::BoxError> {
            Err("boom".into())
        }
    }

    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Custom(Arc::new(
        FailingResolver,
    ))));
    let err = client.discover_leader().await.unwrap_err();
    match err {
        ClientError::Discovery { message, source } => {
            assert!(message.contains("custom resolver"));
            assert!(source.is_some());
            assert!(source.unwrap().to_string().contains("boom"));
        }
        other => panic!("expected Discovery error, got: {other:?}"),
    }
}

#[tokio::test]
async fn custom_resolver_is_invoked_via_closure_blanket_impl() {
    let writer = MockServer::start(MockGateway::writer()).await;
    let endpoint_for_closure = writer.endpoint();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_closure = calls.clone();

    let resolver = Arc::new(move || {
        calls_for_closure.fetch_add(1, Ordering::SeqCst);
        let endpoint = endpoint_for_closure.clone();
        async move { Ok::<_, rsqlite_rsync_client::BoxError>(endpoint) }
    });

    let client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Custom(resolver)));
    let resolved = client.discover_leader().await.unwrap();
    assert_eq!(resolved, writer.endpoint());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
