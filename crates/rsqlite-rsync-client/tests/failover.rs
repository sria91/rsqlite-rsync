//! Tests for retry, backoff, and NOT_LEADER redirect/rediscovery behavior.

mod support;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::Code;

use rsqlite_rsync_client::{ClientConfig, ClientError, DiscoveryMode, SqlGatewayClient};
use support::{closed_endpoint, Behavior, MockGateway, MockServer};

fn fast_config(discovery: DiscoveryMode, max_retries: usize) -> ClientConfig {
    let mut config = ClientConfig::new(discovery).with_max_retries(max_retries);
    config.initial_backoff_ms = 10;
    config.max_backoff_ms = 40;
    config
}

#[tokio::test]
async fn successful_call_makes_exactly_one_rpc() {
    let gw = MockGateway::writer();
    let server = MockServer::start(gw.clone()).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 5));

    client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap();
    assert_eq!(gw.call_count(), 1);
}

#[tokio::test]
async fn redirect_header_is_followed_to_new_leader() {
    let writer_gw = MockGateway::writer();
    let writer = MockServer::start(writer_gw.clone()).await;

    let stale_gw = MockGateway::replica().script([Behavior::NotLeader {
        leader_endpoint: Some(writer.endpoint()),
    }]);
    let stale = MockServer::start(stale_gw.clone()).await;

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(stale.endpoint()), 5));
    client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap();

    assert_eq!(stale_gw.call_count(), 1);
    assert_eq!(writer_gw.call_count(), 1);
}

#[tokio::test]
async fn redirect_pins_subsequent_calls_to_new_leader() {
    let writer_gw = MockGateway::writer();
    let writer = MockServer::start(writer_gw.clone()).await;

    let stale_gw = MockGateway::replica().script([Behavior::NotLeader {
        leader_endpoint: Some(writer.endpoint()),
    }]);
    let stale = MockServer::start(stale_gw.clone()).await;

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(stale.endpoint()), 5));
    client
        .execute("app.db", "INSERT INTO t VALUES (1)", None)
        .await
        .unwrap();
    client
        .execute("app.db", "INSERT INTO t VALUES (2)", None)
        .await
        .unwrap();

    assert_eq!(
        stale_gw.call_count(),
        1,
        "second call should stay pinned to the discovered writer"
    );
    assert_eq!(writer_gw.call_count(), 2);
}

#[tokio::test]
async fn not_leader_without_header_triggers_rediscovery() {
    // The mock fails NOT_LEADER (no redirect header) on its first call, then
    // succeeds. A counting Custom resolver proves the client re-resolves
    // the leader (rather than blindly retrying the same connection) when
    // no redirect endpoint is given.
    let gw = MockGateway::writer().script([Behavior::NotLeader {
        leader_endpoint: None,
    }]);
    let server = MockServer::start(gw.clone()).await;
    let endpoint = server.endpoint();

    let resolve_count = Arc::new(AtomicUsize::new(0));
    let resolve_count_for_closure = resolve_count.clone();
    let resolver = Arc::new(move || {
        resolve_count_for_closure.fetch_add(1, Ordering::SeqCst);
        let endpoint = endpoint.clone();
        async move { Ok::<_, rsqlite_rsync_client::BoxError>(endpoint) }
    });

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Custom(resolver), 5));
    client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap();

    assert_eq!(
        resolve_count.load(Ordering::SeqCst),
        2,
        "resolver should run again after a headerless NOT_LEADER"
    );
    assert_eq!(gw.call_count(), 2);
}

#[tokio::test]
async fn not_leader_with_empty_header_falls_back_to_rediscovery() {
    let gw = MockGateway::writer().script([Behavior::NotLeaderRaw { header_value: "" }]);
    let server = MockServer::start(gw.clone()).await;

    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 5));
    client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap();
    assert_eq!(
        gw.call_count(),
        2,
        "empty header should fall back to reset+rediscover, then succeed"
    );
}

#[tokio::test]
async fn not_leader_in_direct_mode_retries_same_endpoint_then_exhausts() {
    let gw = MockGateway::replica().default_behavior(Behavior::NotLeader {
        leader_endpoint: None,
    });
    let server = MockServer::start(gw.clone()).await;

    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 3));
    let err = client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap_err();

    assert_eq!(gw.call_count(), 3);
    match err {
        ClientError::RetriesExhausted { attempts, .. } => assert_eq!(attempts, 3),
        other => panic!("expected RetriesExhausted, got: {other:?}"),
    }
}

#[tokio::test]
async fn retries_exhausted_preserves_last_status_code() {
    let gw = MockGateway::replica().default_behavior(Behavior::NotLeader {
        leader_endpoint: None,
    });
    let server = MockServer::start(gw).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 2));

    let err = client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some(Code::FailedPrecondition));
    assert!(err.to_string().contains("FailedPrecondition"));
}

#[tokio::test]
async fn unavailable_status_is_retried_then_succeeds_for_reads() {
    let gw = MockGateway::writer().script([Behavior::Fail(Code::Unavailable, "transient")]);
    let server = MockServer::start(gw.clone()).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 5));

    client
        .query("app.db", "SELECT 1", None, 0, Default::default())
        .await
        .unwrap();
    assert_eq!(gw.call_count(), 2);
}

#[tokio::test]
async fn deadline_exceeded_status_is_retried_then_succeeds_for_reads() {
    let gw = MockGateway::writer().script([Behavior::Fail(Code::DeadlineExceeded, "slow")]);
    let server = MockServer::start(gw.clone()).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 5));

    client.get_cluster_status().await.unwrap();
    assert_eq!(gw.call_count(), 2);
}

#[tokio::test]
async fn drop_database_is_retried_on_unavailable() {
    // Unlike execute/batch, dropping a database is idempotent — deleting an
    // already-absent file is a no-op — so it's safe to retry on a transient
    // Unavailable, unlike ambiguous-outcome writes.
    let gw = MockGateway::writer().script([Behavior::Fail(Code::Unavailable, "transient")]);
    let server = MockServer::start(gw.clone()).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 5));

    client.drop_database("app.db").await.unwrap();
    assert_eq!(gw.call_count(), 2);
}

#[tokio::test]
async fn write_does_not_retry_on_unavailable() {
    // Writes are not retried on ambiguous-outcome transient errors, only on
    // definitive NOT_LEADER — retrying execute/batch on Unavailable risks
    // duplicating a write whose outcome on the server is unknown.
    let gw = MockGateway::writer().default_behavior(Behavior::Fail(Code::Unavailable, "transient"));
    let server = MockServer::start(gw.clone()).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 5));

    let err = client
        .execute("app.db", "INSERT INTO t VALUES (1)", None)
        .await
        .unwrap_err();
    assert_eq!(gw.call_count(), 1, "execute must not retry on Unavailable");
    assert_eq!(err.code(), Some(Code::Unavailable));
}

#[tokio::test]
async fn non_transient_status_returns_immediately() {
    let gw =
        MockGateway::writer().default_behavior(Behavior::Fail(Code::InvalidArgument, "bad sql"));
    let server = MockServer::start(gw.clone()).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 5));

    let err = client.execute("app.db", "NOT SQL", None).await.unwrap_err();
    assert_eq!(gw.call_count(), 1);
    assert_eq!(err.code(), Some(Code::InvalidArgument));
    assert!(!matches!(err, ClientError::RetriesExhausted { .. }));
}

#[tokio::test]
async fn connect_failure_to_closed_port_exhausts_retries() {
    let dead = closed_endpoint();
    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(dead), 3));

    let err = client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap_err();
    match err {
        ClientError::RetriesExhausted { attempts, source } => {
            assert_eq!(attempts, 3);
            assert!(matches!(*source, ClientError::Connect { .. }));
        }
        other => panic!("expected RetriesExhausted(Connect), got: {other:?}"),
    }
}

#[tokio::test]
async fn connect_failure_then_resolver_switch_recovers() {
    let writer = MockServer::start(MockGateway::writer()).await;
    let writer_endpoint = writer.endpoint();
    let dead = closed_endpoint();

    let attempt = Arc::new(AtomicUsize::new(0));
    let attempt_for_resolver = attempt.clone();
    let resolver = Arc::new(move || {
        let n = attempt_for_resolver.fetch_add(1, Ordering::SeqCst);
        let endpoint = if n == 0 {
            dead.clone()
        } else {
            writer_endpoint.clone()
        };
        async move { Ok::<_, rsqlite_rsync_client::BoxError>(endpoint) }
    });

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Custom(resolver), 5));
    client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_endpoint_string_fails_fast_without_retry() {
    let mut client = SqlGatewayClient::new(fast_config(
        DiscoveryMode::Direct("::: not a url".to_string()),
        5,
    ));
    let start = Instant::now();
    let err = client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::InvalidEndpoint { .. }));
    assert!(
        start.elapsed() < Duration::from_millis(200),
        "invalid endpoint must fail immediately, not after backoff"
    );
}

#[tokio::test]
async fn max_retries_one_makes_exactly_one_attempt() {
    let gw = MockGateway::replica().default_behavior(Behavior::NotLeader {
        leader_endpoint: None,
    });
    let server = MockServer::start(gw.clone()).await;
    let mut client =
        SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint()), 1));

    let _ = client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await;
    assert_eq!(gw.call_count(), 1);
}

#[tokio::test]
async fn reset_connection_forces_reconnect_and_rediscovery() {
    let a = MockServer::start(MockGateway::writer()).await;
    let b = MockServer::start(MockGateway::writer()).await;
    let a_endpoint = a.endpoint();
    let b_endpoint = b.endpoint();

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_for_resolver = call_count.clone();
    let resolver = Arc::new(move || {
        let n = call_count_for_resolver.fetch_add(1, Ordering::SeqCst);
        let endpoint = if n == 0 {
            a_endpoint.clone()
        } else {
            b_endpoint.clone()
        };
        async move { Ok::<_, rsqlite_rsync_client::BoxError>(endpoint) }
    });

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Custom(resolver), 5));
    client.get_cluster_status().await.unwrap();
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    client.reset_connection();
    client.get_cluster_status().await.unwrap();
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        2,
        "reset_connection should force the resolver to run again"
    );
}

#[tokio::test]
async fn cached_connection_is_reused_across_calls() {
    let gw = MockGateway::writer();
    let server = MockServer::start(gw.clone()).await;

    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_for_resolver = call_count.clone();
    let endpoint = server.endpoint();
    let resolver = Arc::new(move || {
        call_count_for_resolver.fetch_add(1, Ordering::SeqCst);
        let endpoint = endpoint.clone();
        async move { Ok::<_, rsqlite_rsync_client::BoxError>(endpoint) }
    });

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Custom(resolver), 5));
    for _ in 0..5 {
        client.get_cluster_status().await.unwrap();
    }
    assert_eq!(
        call_count.load(Ordering::SeqCst),
        1,
        "resolver should only run once; connection is cached"
    );
    assert_eq!(gw.call_count(), 5);
}

#[tokio::test]
async fn backoff_delays_grow_between_attempts() {
    let gw = MockGateway::replica().default_behavior(Behavior::NotLeader {
        leader_endpoint: None,
    });
    let server = MockServer::start(gw).await;
    let mut config =
        ClientConfig::new(DiscoveryMode::Direct(server.endpoint())).with_max_retries(4);
    config.initial_backoff_ms = 40;
    config.max_backoff_ms = 500;
    let mut client = SqlGatewayClient::new(config);

    let start = Instant::now();
    let _ = client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await;
    let elapsed = start.elapsed();
    // 3 sleeps between 4 attempts: ~40 + ~80 + ~160 = ~280ms lower bound.
    assert!(
        elapsed >= Duration::from_millis(250),
        "expected backoff sleeps to accumulate, elapsed was {elapsed:?}"
    );
}
