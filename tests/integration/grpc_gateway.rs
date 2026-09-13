//! Integration tests for the embedded gRPC SQL Gateway: an external client
//! (both the `rsqlite-rsync client`/`sql` CLI and the `SqlGatewayClient`
//! library) talking to an HA cluster writer over gRPC.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rsqlite_rsync::client::{ClientConfig, DiscoveryMode, SqlGatewayClient};
use rsqlite_rsync::proto::rsqlite::v1::ConsistencyLevel;

/// Fixed token every test node in this file is started with, so tests only
/// need to exercise the SQL Gateway's actual behavior — not its (mandatory)
/// authentication — and any client built by these helpers can talk to it.
const TEST_AUTH_TOKEN: &str = "integration-test-token";

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn spawn(command: &mut Command) -> std::io::Result<Self> {
        Ok(Self {
            child: command.spawn()?,
        })
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

fn write_lease(path: &Path, node_id: &str, generation: u64, renewed_at_secs: u64, ttl_secs: u64) {
    std::fs::write(
        path,
        format!(
            "holder_node_id={node_id}\ngeneration={generation}\nrenewed_at_secs={renewed_at_secs}\nttl_secs={ttl_secs}\n"
        ),
    )
    .unwrap();
}

fn write_freshness(path: &Path, node_id: &str, generation: u64, synced_at_secs: u64) {
    std::fs::write(
        path,
        format!(
            "source_node_id={node_id}\nsource_generation={generation}\nsynced_at_secs={synced_at_secs}\n"
        ),
    )
    .unwrap();
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn wait_until(timeout: Duration, mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    predicate()
}

fn read_http_status_code(bind_addr: &str, path: &str) -> Option<u16> {
    let mut stream = std::net::TcpStream::connect(bind_addr).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
    stream
        .set_write_timeout(Some(Duration::from_secs(1)))
        .ok()?;
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .as_bytes(),
        )
        .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let status_line = response.lines().next()?;
    status_line.split_whitespace().nth(1)?.parse().ok()
}

/// A single-node HA cluster where this node is always the writer, with an
/// embedded gRPC SQL Gateway bound and ready to serve external clients.
struct SingleWriterCluster {
    _guard: ChildGuard,
    _temp: tempfile::TempDir,
    grpc_endpoint: String,
    data_dir: std::path::PathBuf,
}

impl SingleWriterCluster {
    fn start() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let lease_path = temp.path().join("lease.txt");
        let freshness_path = temp.path().join("freshness.txt");
        let role_state_path = temp.path().join("role_state.txt");
        let audit_log_path = temp.path().join("audit.log");
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        let readiness_port = free_port();
        let readiness_bind = format!("127.0.0.1:{readiness_port}");
        let grpc_port = free_port();
        let grpc_bind = format!("127.0.0.1:{grpc_port}");

        let now = now_secs();
        write_lease(&lease_path, "writer-node", 1, now, 60);
        write_freshness(&freshness_path, "writer-node", 1, now);

        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
        cmd.arg("--ha")
            .arg("--ha-node-id")
            .arg("writer-node")
            .arg("--ha-lease-file")
            .arg(&lease_path)
            .arg("--ha-freshness-file")
            .arg(&freshness_path)
            .arg("--ha-role-state-file")
            .arg(&role_state_path)
            .arg("--ha-audit-log-file")
            .arg(&audit_log_path)
            .arg("--ha-readiness-http-bind")
            .arg(&readiness_bind)
            .arg("--ha-grpc-bind")
            .arg(&grpc_bind)
            .arg("--ha-grpc-auth-token")
            .arg(TEST_AUTH_TOKEN)
            .arg("--ha-data-dir")
            .arg(&data_dir)
            .arg("--ha-allow-replica-reads")
            .arg("--ha-tick-interval-ms")
            .arg("50")
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        let guard = ChildGuard::spawn(&mut cmd).expect("failed to start HA gateway process");

        let became_ready = wait_until(Duration::from_secs(5), || {
            read_http_status_code(&readiness_bind, "/ready") == Some(200)
        });
        assert!(became_ready, "writer node never became ready");

        Self {
            _guard: guard,
            _temp: temp,
            grpc_endpoint: format!("http://{grpc_bind}"),
            data_dir,
        }
    }

    fn client(&self) -> SqlGatewayClient {
        SqlGatewayClient::new(ClientConfig {
            discovery: DiscoveryMode::Direct(self.grpc_endpoint.clone()),
            max_retries: 5,
            initial_backoff_ms: 20,
            max_backoff_ms: 200,
            timeout: Duration::from_secs(5),
            auth_token: Some(TEST_AUTH_TOKEN.to_string()),
        })
    }
}

#[tokio::test]
async fn grpc_gateway_execute_query_and_batch_roundtrip() {
    let cluster = SingleWriterCluster::start();
    let mut client = cluster.client();

    let create = client
        .execute(
            "app.db",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            None,
        )
        .await
        .expect("create table should succeed on writer");
    assert!(create.generation >= 1);

    let insert = client
        .execute("app.db", "INSERT INTO users (name) VALUES ('alice')", None)
        .await
        .expect("insert should succeed on writer");
    assert_eq!(insert.rows_affected, 1);
    assert_eq!(insert.last_insert_rowid, 1);

    let query = client
        .query(
            "app.db",
            "SELECT id, name FROM users ORDER BY id",
            None,
            0,
            ConsistencyLevel::Strong,
        )
        .await
        .expect("query should succeed on writer");
    assert_eq!(query.total_rows, 1);
    assert_eq!(query.columns.len(), 2);
    assert!(!query.is_replica_read);

    let batch_sql = vec![
        rsqlite_rsync::proto::rsqlite::v1::Statement {
            sql: "INSERT INTO users (name) VALUES ('bob')".to_string(),
            parameters: None,
        },
        rsqlite_rsync::proto::rsqlite::v1::Statement {
            sql: "INSERT INTO users (name) VALUES ('carol')".to_string(),
            parameters: None,
        },
    ];
    let batch_resp = client
        .batch(
            "app.db",
            batch_sql,
            rsqlite_rsync::proto::rsqlite::v1::BatchTransactionMode::Immediate,
            true,
        )
        .await
        .expect("batch should succeed on writer");
    assert!(batch_resp.committed);
    assert_eq!(batch_resp.results.len(), 2);

    let recount = client
        .query(
            "app.db",
            "SELECT COUNT(*) FROM users",
            None,
            0,
            ConsistencyLevel::Eventual,
        )
        .await
        .expect("eventual read should be served locally by the writer");
    assert_eq!(recount.total_rows, 1);

    let status = client
        .get_cluster_status()
        .await
        .expect("cluster status should succeed");
    assert_eq!(status.node_id, "writer-node");
    assert_eq!(
        status.role,
        rsqlite_rsync::proto::rsqlite::v1::NodeRole::Writer as i32
    );
    assert!(status.databases.iter().any(|db| db.name == "app.db"));

    assert!(cluster.data_dir.join("app.db").exists());
}

#[tokio::test]
async fn grpc_gateway_drop_database_removes_file_and_wal_shm() {
    let cluster = SingleWriterCluster::start();
    let mut client = cluster.client();

    client
        .execute("app.db", "CREATE TABLE t (id INTEGER PRIMARY KEY)", None)
        .await
        .expect("create table should succeed on writer");
    assert!(cluster.data_dir.join("app.db").exists());

    // Force a WAL/SHM sidecar to exist alongside the primary file.
    client
        .execute("app.db", "INSERT INTO t VALUES (1)", None)
        .await
        .expect("insert should succeed on writer");

    let resp = client
        .drop_database("app.db")
        .await
        .expect("drop_database should succeed on writer");
    assert!(resp.existed);
    assert!(!cluster.data_dir.join("app.db").exists());
    assert!(!cluster.data_dir.join("app.db-wal").exists());
    assert!(!cluster.data_dir.join("app.db-shm").exists());

    let status = client
        .get_cluster_status()
        .await
        .expect("cluster status should succeed");
    assert!(!status.databases.iter().any(|db| db.name == "app.db"));

    let second = client
        .drop_database("app.db")
        .await
        .expect("dropping an already-absent database should not error");
    assert!(!second.existed);
}

#[tokio::test]
async fn grpc_gateway_streams_query_rows_in_chunks() {
    let cluster = SingleWriterCluster::start();
    let mut client = cluster.client();

    client
        .execute("stream.db", "CREATE TABLE t (v INTEGER)", None)
        .await
        .unwrap();
    for i in 0..25 {
        client
            .execute(
                "stream.db",
                &format!("INSERT INTO t (v) VALUES ({i})"),
                None,
            )
            .await
            .unwrap();
    }

    let mut stream = client
        .stream_query(
            "stream.db",
            "SELECT v FROM t ORDER BY v",
            None,
            0,
            10,
            ConsistencyLevel::Strong,
        )
        .await
        .expect("stream_query should succeed");

    let mut total_rows = 0u64;
    let mut chunk_count = 0;
    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk = chunk.expect("chunk should not error");
        total_rows += chunk.rows.len() as u64;
        chunk_count += 1;
        if chunk.is_last {
            assert_eq!(chunk.total_rows, 25);
        }
    }
    assert_eq!(total_rows, 25);
    assert!(
        chunk_count >= 3,
        "expected rows split across multiple chunks, got {chunk_count}"
    );
}

#[test]
fn grpc_gateway_cli_client_and_sql_shorthand_roundtrip() {
    let cluster = SingleWriterCluster::start();

    let run = |args: &[&str]| -> (bool, String, String) {
        let output = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"))
            .args(args)
            .output()
            .expect("failed to run CLI subcommand");
        (
            output.status.success(),
            String::from_utf8_lossy(&output.stdout).to_string(),
            String::from_utf8_lossy(&output.stderr).to_string(),
        )
    };

    let (ok, _out, err) = run(&[
        "sql",
        "--endpoint",
        &cluster.grpc_endpoint,
        "--token",
        TEST_AUTH_TOKEN,
        "-d",
        "cli.db",
        "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)",
    ]);
    assert!(ok, "CREATE TABLE via `sql` shorthand failed: {err}");

    let (ok, _out, err) = run(&[
        "client",
        "--endpoint",
        &cluster.grpc_endpoint,
        "--token",
        TEST_AUTH_TOKEN,
        "exec",
        "-d",
        "cli.db",
        "INSERT INTO items (label) VALUES ('widget')",
    ]);
    assert!(ok, "`client exec` failed: {err}");

    let (ok, out, err) = run(&[
        "client",
        "--endpoint",
        &cluster.grpc_endpoint,
        "--token",
        TEST_AUTH_TOKEN,
        "query",
        "-d",
        "cli.db",
        "SELECT label FROM items",
        "--format",
        "json",
    ]);
    assert!(ok, "`client query` failed: {err}");
    assert!(
        out.contains("widget"),
        "expected query output to contain inserted row, got: {out}"
    );

    let (ok, out, err) = run(&[
        "client",
        "--endpoint",
        &cluster.grpc_endpoint,
        "--token",
        TEST_AUTH_TOKEN,
        "status",
        "--format",
        "json",
    ]);
    assert!(ok, "`client status` failed: {err}");
    assert!(
        out.contains("\"role\": \"writer\""),
        "expected status to report writer role, got: {out}"
    );
}

/// A node that never holds the lease and therefore always stays a replica,
/// with an embedded gRPC gateway available for read-only / rejection tests.
struct ReplicaOnlyNode {
    _guard: ChildGuard,
    _temp: tempfile::TempDir,
    grpc_endpoint: String,
}

impl ReplicaOnlyNode {
    fn start(allow_replica_reads: bool) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let lease_path = temp.path().join("lease.txt");
        let freshness_path = temp.path().join("freshness.txt");
        let role_state_path = temp.path().join("role_state.txt");
        let audit_log_path = temp.path().join("audit.log");
        let data_dir = temp.path().join("data");
        std::fs::create_dir_all(&data_dir).unwrap();

        // Lease is held by some *other* node, so this node can never promote.
        let now = now_secs();
        write_lease(&lease_path, "some-other-node", 9, now, 60);
        write_freshness(&freshness_path, "some-other-node", 9, now);

        // A replica can never create a database itself (it can never write),
        // so pre-create an empty one for read-only opens to succeed against.
        let _ = rsqlite_rsync::db::Connection::open(
            &data_dir.join("app.db"),
            libsqlite3_sys::SQLITE_OPEN_READWRITE | libsqlite3_sys::SQLITE_OPEN_CREATE,
        )
        .expect("failed to pre-create replica database file");

        let grpc_port = free_port();
        let grpc_bind = format!("127.0.0.1:{grpc_port}");

        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
        cmd.arg("--ha")
            .arg("--ha-node-id")
            .arg("replica-node")
            .arg("--ha-lease-file")
            .arg(&lease_path)
            .arg("--ha-freshness-file")
            .arg(&freshness_path)
            .arg("--ha-role-state-file")
            .arg(&role_state_path)
            .arg("--ha-audit-log-file")
            .arg(&audit_log_path)
            .arg("--ha-grpc-bind")
            .arg(&grpc_bind)
            .arg("--ha-grpc-auth-token")
            .arg(TEST_AUTH_TOKEN)
            .arg("--ha-data-dir")
            .arg(&data_dir)
            .arg("--ha-tick-interval-ms")
            .arg("50")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if allow_replica_reads {
            cmd.arg("--ha-allow-replica-reads");
        }

        let guard = ChildGuard::spawn(&mut cmd).expect("failed to start replica gateway process");

        let listening = wait_until(Duration::from_secs(5), || {
            std::net::TcpStream::connect(&grpc_bind).is_ok()
        });
        assert!(
            listening,
            "replica node's gRPC gateway never started listening"
        );
        // Give the control loop a couple of ticks to settle into Replica role.
        thread::sleep(Duration::from_millis(150));

        Self {
            _guard: guard,
            _temp: temp,
            grpc_endpoint: format!("http://{grpc_bind}"),
        }
    }

    fn client(&self) -> SqlGatewayClient {
        // max_retries=1: the lease holder ("some-other-node") isn't a real
        // process in this test, so a NOT_LEADER response's redirect would
        // point at an unreachable host. Keeping retries at 1 means these
        // assertions see the gateway's own FailedPrecondition status
        // directly, rather than a subsequent connection failure.
        SqlGatewayClient::new(ClientConfig {
            discovery: DiscoveryMode::Direct(self.grpc_endpoint.clone()),
            max_retries: 1,
            initial_backoff_ms: 10,
            max_backoff_ms: 50,
            timeout: Duration::from_secs(5),
            auth_token: Some(TEST_AUTH_TOKEN.to_string()),
        })
    }
}

#[tokio::test]
async fn grpc_gateway_rejects_writes_and_strong_reads_on_replica() {
    let node = ReplicaOnlyNode::start(false);
    let mut client = node.client();

    let write_err = client
        .execute("app.db", "CREATE TABLE t (id INTEGER)", None)
        .await
        .expect_err("write against a replica-only node must fail");
    assert!(
        write_err.to_string().contains("FailedPrecondition"),
        "expected FailedPrecondition error, got: {write_err}"
    );

    let read_err = client
        .query("app.db", "SELECT 1", None, 0, ConsistencyLevel::Strong)
        .await
        .expect_err("strong read against a replica-only node must fail");
    assert!(
        read_err.to_string().contains("FailedPrecondition"),
        "expected FailedPrecondition error, got: {read_err}"
    );

    let drop_err = client
        .drop_database("app.db")
        .await
        .expect_err("drop_database against a replica-only node must fail");
    assert!(
        drop_err.to_string().contains("FailedPrecondition"),
        "expected FailedPrecondition error, got: {drop_err}"
    );
}

#[tokio::test]
async fn grpc_gateway_allows_eventual_reads_on_replica_when_enabled() {
    let node = ReplicaOnlyNode::start(true);
    let mut client = node.client();

    let resp = client
        .query(
            "app.db",
            "SELECT 1 AS one",
            None,
            0,
            ConsistencyLevel::Eventual,
        )
        .await
        .expect("eventual read should be permitted on a replica when enabled");
    assert!(resp.is_replica_read);
    assert_eq!(resp.total_rows, 1);
}
