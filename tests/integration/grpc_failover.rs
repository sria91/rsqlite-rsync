//! Integration tests for transparent client failover against the gRPC SQL
//! Gateway: an external client must discover the active cluster writer and
//! automatically redirect away from a node that answers `NOT_LEADER`.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rsqlite_rsync::client::{ClientConfig, DiscoveryMode, SqlGatewayClient};
use rsqlite_rsync::proto::rsqlite::v1::ConsistencyLevel;

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
    stream.set_write_timeout(Some(Duration::from_secs(1))).ok()?;
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

/// A two-node cluster sharing one lease file: `writer_endpoint` always holds
/// the lease and is promoted to writer, while `stale_endpoint` never holds
/// the lease and stays a replica forever. Both nodes are configured (via
/// `--ha-grpc-port`) so that whichever node observes the lease computes the
/// *same*, actually-reachable, endpoint for the current holder — mirroring
/// how a real deployment's headless-service DNS would resolve, but pinned to
/// localhost ports for the test.
struct TwoNodeCluster {
    _writer_guard: ChildGuard,
    _stale_guard: ChildGuard,
    _temp: tempfile::TempDir,
    writer_endpoint: String,
    stale_endpoint: String,
}

impl TwoNodeCluster {
    async fn start() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let lease_path = temp.path().join("lease.txt");
        let writer_freshness_path = temp.path().join("writer_freshness.txt");
        let writer_role_state = temp.path().join("writer_role_state.txt");
        let writer_audit_log = temp.path().join("writer_audit.log");
        let writer_readiness_bind = format!("127.0.0.1:{}", free_port());
        let writer_data_dir = temp.path().join("writer_data");
        std::fs::create_dir_all(&writer_data_dir).unwrap();

        let stale_freshness_path = temp.path().join("stale_freshness.txt");
        let stale_role_state = temp.path().join("stale_role_state.txt");
        let stale_audit_log = temp.path().join("stale_audit.log");
        let stale_data_dir = temp.path().join("stale_data");
        std::fs::create_dir_all(&stale_data_dir).unwrap();

        let writer_port = free_port();
        let stale_port = free_port();

        let now = now_secs();
        write_lease(&lease_path, "127.0.0.1", 7, now, 120);
        write_freshness(&writer_freshness_path, "127.0.0.1", 7, now);

        let mut writer_cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
        writer_cmd
            .arg("--ha")
            .arg("--ha-node-id")
            .arg("127.0.0.1")
            .arg("--ha-lease-file")
            .arg(&lease_path)
            .arg("--ha-freshness-file")
            .arg(&writer_freshness_path)
            .arg("--ha-role-state-file")
            .arg(&writer_role_state)
            .arg("--ha-audit-log-file")
            .arg(&writer_audit_log)
            .arg("--ha-readiness-http-bind")
            .arg(&writer_readiness_bind)
            .arg("--ha-grpc-bind")
            .arg(format!("127.0.0.1:{writer_port}"))
            .arg("--ha-grpc-port")
            .arg(writer_port.to_string())
            .arg("--ha-service-name")
            .arg("")
            .arg("--ha-data-dir")
            .arg(&writer_data_dir)
            .arg("--ha-tick-interval-ms")
            .arg("50")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let writer_guard =
            ChildGuard::spawn(&mut writer_cmd).expect("failed to start writer node");

        let became_ready = wait_until(Duration::from_secs(5), || {
            read_http_status_code(&writer_readiness_bind, "/ready") == Some(200)
        });
        assert!(became_ready, "writer node never became ready");

        // The stale node never holds the lease, but is told (via
        // --ha-grpc-port) how to reach whoever *does* hold it.
        write_freshness(&stale_freshness_path, "127.0.0.1", 7, now);
        let mut stale_cmd = Command::new(env!("CARGO_BIN_EXE_rsqlite-rsync"));
        stale_cmd
            .arg("--ha")
            .arg("--ha-node-id")
            .arg("stale-node")
            .arg("--ha-lease-file")
            .arg(&lease_path)
            .arg("--ha-freshness-file")
            .arg(&stale_freshness_path)
            .arg("--ha-role-state-file")
            .arg(&stale_role_state)
            .arg("--ha-audit-log-file")
            .arg(&stale_audit_log)
            .arg("--ha-grpc-bind")
            .arg(format!("127.0.0.1:{stale_port}"))
            .arg("--ha-grpc-port")
            .arg(writer_port.to_string())
            .arg("--ha-service-name")
            .arg("")
            .arg("--ha-data-dir")
            .arg(&stale_data_dir)
            .arg("--ha-tick-interval-ms")
            .arg("50")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let stale_guard = ChildGuard::spawn(&mut stale_cmd).expect("failed to start stale node");

        let writer_endpoint = format!("http://127.0.0.1:{writer_port}");
        let stale_endpoint = format!("http://127.0.0.1:{stale_port}");

        // Wait until the stale node has observed the lease and correctly
        // computed the writer's endpoint, so redirect-based failover has
        // something valid to redirect to.
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut observed = false;
        while Instant::now() < deadline {
            let mut probe = SqlGatewayClient::new(ClientConfig {
                discovery: DiscoveryMode::Direct(stale_endpoint.clone()),
                max_retries: 1,
                initial_backoff_ms: 10,
                max_backoff_ms: 20,
                timeout: Duration::from_secs(2),
            });
            if let Ok(status) = probe.get_cluster_status().await
                && status.current_leader_endpoint == writer_endpoint
            {
                observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        assert!(
            observed,
            "stale node never observed the lease holder's endpoint"
        );

        Self {
            _writer_guard: writer_guard,
            _stale_guard: stale_guard,
            _temp: temp,
            writer_endpoint,
            stale_endpoint,
        }
    }
}

#[tokio::test]
async fn client_fails_over_from_stale_node_to_active_writer() {
    let cluster = TwoNodeCluster::start().await;

    // Pin the client directly at the *stale* node. A naive client would
    // simply fail; ours must read the NOT_LEADER response's leader-endpoint
    // metadata and transparently redirect to the real writer.
    let mut client = SqlGatewayClient::new(ClientConfig {
        discovery: DiscoveryMode::Direct(cluster.stale_endpoint.clone()),
        max_retries: 5,
        initial_backoff_ms: 20,
        max_backoff_ms: 200,
        timeout: Duration::from_secs(5),
    });

    let resp = client
        .execute(
            "failover.db",
            "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)",
            None,
        )
        .await
        .expect("client should transparently fail over to the active writer");
    assert!(resp.generation >= 1);

    let insert = client
        .execute("failover.db", "INSERT INTO t (v) VALUES ('ok')", None)
        .await
        .expect("subsequent writes should stay pinned to the discovered writer");
    assert_eq!(insert.rows_affected, 1);

    // Verify directly against the writer's own endpoint that the write
    // actually landed there (and not, say, silently on the stale node).
    let mut direct = SqlGatewayClient::new(ClientConfig {
        discovery: DiscoveryMode::Direct(cluster.writer_endpoint.clone()),
        max_retries: 3,
        initial_backoff_ms: 20,
        max_backoff_ms: 200,
        timeout: Duration::from_secs(5),
    });
    let query = direct
        .query(
            "failover.db",
            "SELECT v FROM t",
            None,
            0,
            ConsistencyLevel::Strong,
        )
        .await
        .expect("query directly against the writer should succeed");
    assert_eq!(query.total_rows, 1);
}

#[tokio::test]
async fn client_discovers_writer_among_multiple_candidates() {
    let cluster = TwoNodeCluster::start().await;

    // List the stale node first to prove discovery doesn't just pick the
    // first candidate; it must probe each and pick the one reporting Writer.
    let mut client = SqlGatewayClient::new(ClientConfig {
        discovery: DiscoveryMode::Candidates(vec![
            cluster.stale_endpoint.clone(),
            cluster.writer_endpoint.clone(),
        ]),
        max_retries: 5,
        initial_backoff_ms: 20,
        max_backoff_ms: 200,
        timeout: Duration::from_secs(5),
    });

    let status = client
        .get_cluster_status()
        .await
        .expect("candidate discovery should find the active writer");
    assert_eq!(status.node_id, "127.0.0.1");
    assert_eq!(
        status.role,
        rsqlite_rsync::proto::rsqlite::v1::NodeRole::Writer as i32
    );
}
