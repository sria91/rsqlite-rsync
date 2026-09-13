//! Per-RPC coverage: batch/stream_query under failover (previously
//! untested), and request-field round-tripping.

mod support;

use rsqlite_rsync_client::proto::{
    BatchTransactionMode, ConsistencyLevel, NamedParameter, Parameters, Statement, Value,
};
use rsqlite_rsync_client::{ClientConfig, DiscoveryMode, SqlGatewayClient};
use support::{Behavior, MockGateway, MockServer, RequestSnapshot};

fn fast_config(discovery: DiscoveryMode) -> ClientConfig {
    let mut config = ClientConfig::new(discovery).with_max_retries(5);
    config.initial_backoff_ms = 10;
    config.max_backoff_ms = 40;
    config
}

#[tokio::test]
async fn batch_follows_not_leader_redirect() {
    let writer_gw = MockGateway::writer();
    let writer = MockServer::start(writer_gw.clone()).await;

    let stale_gw = MockGateway::replica().script([Behavior::NotLeader {
        leader_endpoint: Some(writer.endpoint()),
    }]);
    let stale = MockServer::start(stale_gw.clone()).await;

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(stale.endpoint())));
    client
        .batch(
            "app.db",
            vec![Statement {
                sql: "INSERT INTO t VALUES (1)".into(),
                parameters: None,
            }],
            BatchTransactionMode::Immediate,
            true,
        )
        .await
        .unwrap();

    assert_eq!(stale_gw.call_count(), 1);
    assert_eq!(writer_gw.call_count(), 1);
}

#[tokio::test]
async fn stream_query_follows_not_leader_redirect_and_yields_a_chunk() {
    let writer_gw = MockGateway::writer();
    let writer = MockServer::start(writer_gw.clone()).await;

    let stale_gw = MockGateway::replica().script([Behavior::NotLeader {
        leader_endpoint: Some(writer.endpoint()),
    }]);
    let stale = MockServer::start(stale_gw.clone()).await;

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(stale.endpoint())));
    let mut stream = client
        .stream_query(
            "app.db",
            "SELECT * FROM t",
            None,
            0,
            100,
            ConsistencyLevel::Strong,
        )
        .await
        .unwrap();

    let mut chunks = 0;
    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        chunk.unwrap();
        chunks += 1;
    }
    assert_eq!(chunks, 1);
    assert_eq!(writer_gw.call_count(), 1);
}

#[tokio::test]
async fn get_cluster_status_follows_not_leader_redirect() {
    let writer_gw = MockGateway::writer();
    let writer = MockServer::start(writer_gw.clone()).await;

    let stale_gw = MockGateway::replica().script([Behavior::NotLeader {
        leader_endpoint: Some(writer.endpoint()),
    }]);
    let stale = MockServer::start(stale_gw.clone()).await;

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(stale.endpoint())));
    let status = client.get_cluster_status().await.unwrap();
    assert_eq!(status.node_id, "mock-node");
    assert_eq!(writer_gw.call_count(), 1);
}

#[tokio::test]
async fn drop_database_follows_not_leader_redirect() {
    let writer_gw = MockGateway::writer();
    let writer = MockServer::start(writer_gw.clone()).await;

    let stale_gw = MockGateway::replica().script([Behavior::NotLeader {
        leader_endpoint: Some(writer.endpoint()),
    }]);
    let stale = MockServer::start(stale_gw.clone()).await;

    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(stale.endpoint())));
    let resp = client.drop_database("app.db").await.unwrap();
    assert!(resp.existed);
    assert_eq!(stale_gw.call_count(), 1);
    assert_eq!(writer_gw.call_count(), 1);
}

#[tokio::test]
async fn drop_database_sends_database_name_verbatim() {
    let gw = MockGateway::writer();
    let server = MockServer::start(gw.clone()).await;
    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint())));

    client.drop_database("app.db").await.unwrap();

    match gw.last_request().unwrap() {
        RequestSnapshot::DropDatabase(req) => {
            assert_eq!(req.database, "app.db");
        }
        other => panic!("expected DropDatabase snapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn execute_sends_database_sql_and_parameters_verbatim() {
    let gw = MockGateway::writer();
    let server = MockServer::start(gw.clone()).await;
    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint())));

    let params = Parameters {
        positional: vec![Value {
            value: Some(rsqlite_rsync_client::proto::value::Value::IntValue(7)),
        }],
        named: vec![NamedParameter {
            name: "name".into(),
            value: Some(Value {
                value: Some(rsqlite_rsync_client::proto::value::Value::TextValue(
                    "alice".into(),
                )),
            }),
        }],
    };
    client
        .execute(
            "app.db",
            "INSERT INTO t VALUES (?, :name)",
            Some(params.clone()),
        )
        .await
        .unwrap();

    match gw.last_request().unwrap() {
        RequestSnapshot::Execute(req) => {
            assert_eq!(req.database, "app.db");
            assert_eq!(
                req.statement.as_ref().unwrap().sql,
                "INSERT INTO t VALUES (?, :name)"
            );
            assert_eq!(req.statement.unwrap().parameters, Some(params));
        }
        other => panic!("expected Execute snapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn query_sends_max_rows_consistency_and_zero_chunk_size() {
    let gw = MockGateway::writer();
    let server = MockServer::start(gw.clone()).await;
    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint())));

    client
        .query(
            "app.db",
            "SELECT * FROM t",
            None,
            42,
            ConsistencyLevel::Eventual,
        )
        .await
        .unwrap();

    match gw.last_request().unwrap() {
        RequestSnapshot::Query(req) => {
            assert_eq!(req.max_rows, 42);
            assert_eq!(req.consistency, ConsistencyLevel::Eventual as i32);
            assert_eq!(req.chunk_size, 0, "unary query always sends chunk_size 0");
        }
        other => panic!("expected Query snapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn batch_sends_transaction_mode_and_stop_on_error() {
    let gw = MockGateway::writer();
    let server = MockServer::start(gw.clone()).await;
    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint())));

    client
        .batch(
            "app.db",
            vec![Statement {
                sql: "DELETE FROM t".into(),
                parameters: None,
            }],
            BatchTransactionMode::Exclusive,
            false,
        )
        .await
        .unwrap();

    match gw.last_request().unwrap() {
        RequestSnapshot::Batch(req) => {
            assert_eq!(req.transaction_mode, BatchTransactionMode::Exclusive as i32);
            assert!(!req.stop_on_error);
            assert_eq!(req.statements.len(), 1);
        }
        other => panic!("expected Batch snapshot, got {other:?}"),
    }
}

#[tokio::test]
async fn stream_query_sends_requested_chunk_size() {
    let gw = MockGateway::writer();
    let server = MockServer::start(gw.clone()).await;
    let mut client = SqlGatewayClient::new(fast_config(DiscoveryMode::Direct(server.endpoint())));

    let mut stream = client
        .stream_query(
            "app.db",
            "SELECT * FROM t",
            None,
            0,
            250,
            ConsistencyLevel::Strong,
        )
        .await
        .unwrap();
    while tokio_stream::StreamExt::next(&mut stream).await.is_some() {}

    match gw.last_request().unwrap() {
        RequestSnapshot::StreamQuery(req) => assert_eq!(req.chunk_size, 250),
        other => panic!("expected StreamQuery snapshot, got {other:?}"),
    }
}
