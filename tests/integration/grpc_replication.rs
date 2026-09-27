//! End-to-end integration tests for native gRPC SQLite replication transport.
//!
//! Exercises push and pull synchronization over HTTP/2 gRPC streams,
//! validating authentication, incremental deltas, data consistency,
//! CLI argument parsing, and error handling.

mod fixtures {
    include!("../fixtures/gen_db.rs");
}

use std::fs;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rsqlite_rsync::gateway::{AuthConfig, DatabaseEngine, ReplicationServer};
use rsqlite_rsync::proto::rsqlite::v1::replication_service_server::ReplicationServiceServer as TonicReplicationServiceServer;
use rsqlite_rsync::{SyncTuning, grpc_pull_sync, grpc_push_sync};
use tempfile::{NamedTempFile, TempDir};

const TEST_TOKEN: &str = "test-secret-token-12345";

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

struct TestServer {
    url: String,
    data_dir: PathBuf,
    _temp: TempDir,
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl TestServer {
    async fn start(auth: AuthConfig) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let data_dir = temp.path().join("databases");
        fs::create_dir_all(&data_dir).unwrap();

        let port = free_port();
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let engine = DatabaseEngine::new(data_dir.clone()).unwrap();
        let repl_server = ReplicationServer::new(engine, auth, SyncTuning::default());
        let svc = TonicReplicationServiceServer::new(repl_server);

        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(svc)
                .serve_with_shutdown(addr, async move {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        // Wait until server port is accepting connections
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut connected = false;
        while Instant::now() < deadline {
            if std::net::TcpStream::connect(addr).is_ok() {
                connected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(connected, "test gRPC server failed to start on {addr}");

        Self {
            url: format!("http://{addr}"),
            data_dir,
            _temp: temp,
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// In-process gRPC Push & Pull Replication Tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn grpc_push_sync_creates_identical_remote_replica() {
    let server = TestServer::start(AuthConfig::required(TEST_TOKEN.to_string())).await;
    let local_origin = NamedTempFile::new().unwrap();
    fixtures::seed(local_origin.path(), 500);

    let remote_db_name = "push_target.db";
    grpc_push_sync(
        local_origin.path(),
        &server.url,
        remote_db_name,
        Some(TEST_TOKEN),
    )
    .await
    .expect("gRPC push sync should succeed");

    let remote_db_path = server.data_dir.join(remote_db_name);
    assert!(remote_db_path.exists(), "remote replica file must exist");

    let origin_bytes = fs::read(local_origin.path()).unwrap();
    let replica_bytes = fs::read(&remote_db_path).unwrap();
    assert_eq!(
        origin_bytes, replica_bytes,
        "remote database content must match local origin exactly"
    );
}

#[tokio::test]
async fn grpc_pull_sync_creates_identical_local_replica() {
    let server = TestServer::start(AuthConfig::required(TEST_TOKEN.to_string())).await;
    let remote_db_name = "pull_source.db";
    let remote_db_path = server.data_dir.join(remote_db_name);
    fixtures::seed(&remote_db_path, 600);

    let local_replica = NamedTempFile::new().unwrap();

    grpc_pull_sync(
        local_replica.path(),
        &server.url,
        remote_db_name,
        Some(TEST_TOKEN),
    )
    .await
    .expect("gRPC pull sync should succeed");

    let remote_bytes = fs::read(&remote_db_path).unwrap();
    let local_bytes = fs::read(local_replica.path()).unwrap();
    assert_eq!(
        remote_bytes, local_bytes,
        "local database content must match remote origin exactly"
    );
}

#[tokio::test]
async fn grpc_incremental_push_sync_updates_modified_pages() {
    let server = TestServer::start(AuthConfig::required(TEST_TOKEN.to_string())).await;
    let local_origin = NamedTempFile::new().unwrap();
    fixtures::seed(local_origin.path(), 800);

    let remote_db_name = "incremental.db";
    let remote_db_path = server.data_dir.join(remote_db_name);

    // Initial sync
    grpc_push_sync(
        local_origin.path(),
        &server.url,
        remote_db_name,
        Some(TEST_TOKEN),
    )
    .await
    .expect("initial push sync should succeed");

    // Modify rows in local origin
    fixtures::modify_rows(local_origin.path(), 10, 5, 800);
    fixtures::append_rows(local_origin.path(), 800, 200);

    // Incremental push sync
    grpc_push_sync(
        local_origin.path(),
        &server.url,
        remote_db_name,
        Some(TEST_TOKEN),
    )
    .await
    .expect("incremental push sync should succeed");

    let origin_bytes = fs::read(local_origin.path()).unwrap();
    let replica_bytes = fs::read(&remote_db_path).unwrap();
    assert_eq!(
        origin_bytes, replica_bytes,
        "remote database content must match updated local origin"
    );
}

#[tokio::test]
async fn grpc_auth_failures_are_rejected() {
    let server = TestServer::start(AuthConfig::required(TEST_TOKEN.to_string())).await;
    let local_db = NamedTempFile::new().unwrap();
    fixtures::seed(local_db.path(), 50);

    // 1. Missing token
    let res_no_token = grpc_push_sync(local_db.path(), &server.url, "auth_test.db", None).await;
    assert!(
        res_no_token.is_err(),
        "push sync without auth token must be rejected"
    );

    // 2. Wrong token
    let res_wrong_token = grpc_push_sync(
        local_db.path(),
        &server.url,
        "auth_test.db",
        Some("wrong-secret"),
    )
    .await;
    assert!(
        res_wrong_token.is_err(),
        "push sync with wrong auth token must be rejected"
    );

    // 3. Pull sync with wrong token
    let res_pull_wrong = grpc_pull_sync(
        local_db.path(),
        &server.url,
        "auth_test.db",
        Some("wrong-secret"),
    )
    .await;
    assert!(
        res_pull_wrong.is_err(),
        "pull sync with wrong auth token must be rejected"
    );
}

#[tokio::test]
async fn grpc_insecure_server_accepts_unauthenticated_sync() {
    let server = TestServer::start(AuthConfig::disabled()).await;
    let local_origin = NamedTempFile::new().unwrap();
    fixtures::seed(local_origin.path(), 150);

    let remote_db_name = "insecure.db";
    grpc_push_sync(local_origin.path(), &server.url, remote_db_name, None)
        .await
        .expect("insecure server should accept sync without token");

    let remote_db_path = server.data_dir.join(remote_db_name);
    let origin_bytes = fs::read(local_origin.path()).unwrap();
    let replica_bytes = fs::read(&remote_db_path).unwrap();
    assert_eq!(origin_bytes, replica_bytes);
}

#[tokio::test]
async fn grpc_invalid_database_name_rejected() {
    let server = TestServer::start(AuthConfig::required(TEST_TOKEN.to_string())).await;
    let local_db = NamedTempFile::new().unwrap();
    fixtures::seed(local_db.path(), 50);

    // Path traversal attempt
    let res = grpc_push_sync(
        local_db.path(),
        &server.url,
        "../traversal.db",
        Some(TEST_TOKEN),
    )
    .await;
    assert!(
        res.is_err(),
        "database name with path traversal should be rejected"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// CLI Integration Tests (exercising CLI argument parsing and gRPC dispatch)
// ─────────────────────────────────────────────────────────────────────────────

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

struct HaDaemonNode {
    _guard: ChildGuard,
    _temp: TempDir,
    grpc_bind: String,
    data_dir: PathBuf,
}

impl HaDaemonNode {
    fn start(auth_token: Option<&str>) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let lease_path = temp.path().join("lease.txt");
        let freshness_path = temp.path().join("freshness.txt");
        let role_state_path = temp.path().join("role_state.txt");
        let audit_log_path = temp.path().join("audit.log");
        let data_dir = temp.path().join("data");
        fs::create_dir_all(&data_dir).unwrap();

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
            .arg("--ha-data-dir")
            .arg(&data_dir)
            .arg("--ha-allow-replica-reads")
            .arg("--ha-tick-interval-ms")
            .arg("50")
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        if let Some(token) = auth_token {
            cmd.arg("--ha-grpc-auth-token").arg(token);
        } else {
            cmd.arg("--ha-grpc-insecure-no-auth");
        }

        let guard = ChildGuard::spawn(&mut cmd).expect("failed to start HA daemon process");

        let became_ready = wait_until(Duration::from_secs(5), || {
            std::net::TcpStream::connect(&grpc_bind).is_ok()
        });
        assert!(became_ready, "HA daemon gRPC server never became ready");

        Self {
            _guard: guard,
            _temp: temp,
            grpc_bind,
            data_dir,
        }
    }
}

#[test]
fn cli_grpc_push_and_pull_sync() {
    let daemon = HaDaemonNode::start(Some(TEST_TOKEN));

    // 1. Push sync via CLI: local DB -> grpc://...
    let local_origin = NamedTempFile::new().unwrap();
    fixtures::seed(local_origin.path(), 350);

    let remote_grpc_target = format!("grpc://{}/cli_push.db", daemon.grpc_bind);
    let mut push_cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    let push_status = push_cmd
        .arg(local_origin.path())
        .arg(&remote_grpc_target)
        .arg("--grpc-auth-token")
        .arg(TEST_TOKEN)
        .status()
        .expect("CLI push sync execution failed");

    assert!(push_status.success(), "CLI push sync should exit 0");

    let remote_db_path = daemon.data_dir.join("cli_push.db");
    assert!(remote_db_path.exists());
    let origin_bytes = fs::read(local_origin.path()).unwrap();
    let remote_bytes = fs::read(&remote_db_path).unwrap();
    assert_eq!(origin_bytes, remote_bytes);

    // 2. Pull sync via CLI: grpc://... -> local DB
    let local_pull_dest = NamedTempFile::new().unwrap();
    let mut pull_cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    let pull_status = pull_cmd
        .arg(&remote_grpc_target)
        .arg(local_pull_dest.path())
        .arg("--grpc-auth-token")
        .arg(TEST_TOKEN)
        .status()
        .expect("CLI pull sync execution failed");

    assert!(pull_status.success(), "CLI pull sync should exit 0");

    let pulled_bytes = fs::read(local_pull_dest.path()).unwrap();
    assert_eq!(origin_bytes, pulled_bytes);
}

#[test]
fn cli_grpc_batch_manifest_sync() {
    let daemon = HaDaemonNode::start(Some(TEST_TOKEN));

    let local_db1 = NamedTempFile::new().unwrap();
    let local_db2 = NamedTempFile::new().unwrap();
    fixtures::seed(local_db1.path(), 100);
    fixtures::seed(local_db2.path(), 200);

    let temp_manifest = NamedTempFile::new().unwrap();
    let manifest_content = format!(
        r#"{{
  "syncs": [
    {{
      "origin": "{}",
      "replica": "grpc://{}/batch1.db"
    }},
    {{
      "origin": "{}",
      "replica": "grpc://{}/batch2.db"
    }}
  ]
}}"#,
        local_db1.path().display(),
        daemon.grpc_bind,
        local_db2.path().display(),
        daemon.grpc_bind,
    );
    fs::write(temp_manifest.path(), manifest_content).unwrap();

    let mut batch_cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
    let batch_status = batch_cmd
        .arg("--batch-manifest")
        .arg(temp_manifest.path())
        .arg("--grpc-auth-token")
        .arg(TEST_TOKEN)
        .status()
        .expect("CLI batch sync execution failed");

    assert!(batch_status.success(), "CLI batch sync should exit 0");

    let remote1_path = daemon.data_dir.join("batch1.db");
    let remote2_path = daemon.data_dir.join("batch2.db");
    assert!(remote1_path.exists());
    assert!(remote2_path.exists());

    assert_eq!(
        fs::read(local_db1.path()).unwrap(),
        fs::read(&remote1_path).unwrap()
    );
    assert_eq!(
        fs::read(local_db2.path()).unwrap(),
        fs::read(&remote2_path).unwrap()
    );
}
