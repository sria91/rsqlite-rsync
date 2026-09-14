//! Walkthrough of the `rsqlite_rsync::client` API (the standalone
//! `rsqlite-rsync-client` crate, re-exported here), exercising every
//! `SqlGateway` RPC, all three leader-discovery modes, parameter binding,
//! and both the happy and error paths.
//!
//! It is fully self-contained: it spawns a single-writer HA `rsqlite-rsync`
//! server as a child process, talks to it exactly the way an external
//! service would over real gRPC, and tears the server down on exit.
//!
//! Run with:
//! ```text
//! cargo run --example client_demo
//! ```

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use comfy_table::{Cell, ContentArrangement, Row, Table};

use rsqlite_rsync::client::{BoxError, ClientConfig, ClientError, DiscoveryMode, SqlGatewayClient};
use rsqlite_rsync::proto::rsqlite::v1::{
    BatchTransactionMode, ConsistencyLevel, DatabaseInfo, NamedParameter, NodeRole, Parameters,
    QueryResponse, Statement, StatementResult, Value, statement_result, value,
};

// ── Self-contained server bootstrap (mirrors tests/integration/grpc_gateway.rs) ──

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_secs()
}

fn write_lease(path: &Path, node_id: &str) {
    let now = now_secs();
    std::fs::write(
        path,
        format!("holder_node_id={node_id}\ngeneration=1\nrenewed_at_secs={now}\nttl_secs=86400\n"),
    )
    .expect("failed to write lease file");
}

/// Writes a freshness ledger claiming this node is fully caught up as of
/// now. Promotion to writer is denied (`MissingFreshness`) without one, even
/// under the permissive startup fence mode.
fn write_freshness(path: &Path, node_id: &str) {
    let now = now_secs();
    std::fs::write(
        path,
        format!("source_node_id={node_id}\nsource_generation=1\nsynced_at_secs={now}\n"),
    )
    .expect("failed to write freshness file");
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind ephemeral port");
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

fn http_status_code(bind_addr: &str, path: &str) -> Option<u16> {
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
    response
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

/// Locates the sibling `rsqlite-rsync` server binary next to this example's
/// own executable under `target/<profile>/`. Avoids relying on
/// `CARGO_BIN_EXE_*`, whose support is documented for integration tests and
/// benchmarks but not consistently for `[[example]]` targets.
fn locate_server_binary() -> PathBuf {
    let mut path = std::env::current_exe().expect("failed to resolve current_exe");
    path.pop(); // .../target/<profile>/examples/
    path.pop(); // .../target/<profile>/
    path.push(if cfg!(windows) {
        "rsqlite-rsync.exe"
    } else {
        "rsqlite-rsync"
    });
    assert!(
        path.exists(),
        "server binary not found at {path:?} — build it first with `cargo build`"
    );
    path
}

/// Starts a single-node HA `rsqlite-rsync` server that is always the writer
/// (a pre-seeded file lease names it the permanent holder), with an embedded
/// gRPC SQL Gateway ready to serve. Returns the gRPC endpoint, a guard that
/// kills the server process on drop, and the backing temp dir.
fn start_demo_server() -> (String, ChildGuard, tempfile::TempDir) {
    let temp = tempfile::tempdir().expect("failed to create temp dir");
    let lease_path = temp.path().join("lease.txt");
    let freshness_path = temp.path().join("freshness.txt");
    let role_state_path = temp.path().join("role_state.txt");
    let audit_log_path = temp.path().join("audit.log");
    let data_dir = temp.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    write_lease(&lease_path, "demo-node");
    write_freshness(&freshness_path, "demo-node");

    let readiness_bind = format!("127.0.0.1:{}", free_port());
    let grpc_bind = format!("127.0.0.1:{}", free_port());

    let mut cmd = Command::new(locate_server_binary());
    cmd.arg("--ha")
        .arg("--ha-node-id")
        .arg("demo-node")
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

    let child = ChildGuard(cmd.spawn().expect("failed to spawn rsqlite-rsync server"));

    let ready = wait_until(Duration::from_secs(10), || {
        http_status_code(&readiness_bind, "/ready") == Some(200)
    });
    assert!(ready, "demo server never became ready");

    (format!("http://{grpc_bind}"), child, temp)
}

// ── Tiny Value/Parameters builders — no such helper ships in the client crate ──

fn val_text(s: impl Into<String>) -> Value {
    Value {
        value: Some(value::Value::TextValue(s.into())),
    }
}

fn val_int(i: i64) -> Value {
    Value {
        value: Some(value::Value::IntValue(i)),
    }
}

fn val_null() -> Value {
    Value {
        value: Some(value::Value::NullValue(true)),
    }
}

fn positional(values: Vec<Value>) -> Parameters {
    Parameters {
        positional: values,
        named: Vec::new(),
    }
}

fn named(pairs: Vec<(&str, Value)>) -> Parameters {
    Parameters {
        positional: Vec::new(),
        named: pairs
            .into_iter()
            .map(|(name, value)| NamedParameter {
                name: name.to_string(),
                value: Some(value),
            })
            .collect(),
    }
}

// ── Output helpers ──

fn section(n: u32, title: &str) {
    println!("\n=== {n}. {title} ===");
}

fn format_value(v: &Value) -> String {
    match &v.value {
        Some(value::Value::NullValue(_)) | None => "NULL".to_string(),
        Some(value::Value::IntValue(i)) => i.to_string(),
        Some(value::Value::FloatValue(f)) => f.to_string(),
        Some(value::Value::TextValue(s)) => s.clone(),
        Some(value::Value::BlobValue(b)) => format!("<{} byte blob>", b.len()),
    }
}

fn print_rows(resp: &QueryResponse) {
    let mut table = Table::new();
    table.set_content_arrangement(ContentArrangement::Dynamic);

    let header: Vec<Cell> = resp.columns.iter().map(|c| Cell::new(&c.name)).collect();
    table.set_header(header);

    for row in &resp.rows {
        let cells: Vec<Cell> = row
            .values
            .iter()
            .map(|v| Cell::new(format_value(v)))
            .collect();
        table.add_row(Row::from(cells));
    }

    println!("{table}");
    println!(
        "({} row(s), {:.2} ms)",
        resp.total_rows,
        resp.execution_time_us as f64 / 1000.0
    );
}

fn describe_statement_result(i: usize, result: &StatementResult) {
    if !result.error.is_empty() {
        println!("  [{i}] error: {}", result.error);
        return;
    }
    match &result.result {
        Some(statement_result::Result::ExecuteResult(exec)) => {
            println!("  [{i}] execute: rows_affected={}", exec.rows_affected)
        }
        Some(statement_result::Result::QueryResult(q)) => {
            println!("  [{i}] query: total_rows={}", q.total_rows)
        }
        None => println!("  [{i}] (no result)"),
    }
}

fn describe_database(db: &DatabaseInfo) {
    println!(
        "  database: {} ({} pages x {} bytes = {} bytes total, journal_mode={})",
        db.name, db.page_count, db.page_size, db.file_size_bytes, db.journal_mode
    );
}

fn describe_error(err: &ClientError) {
    println!("execute() failed as expected: {err}");
    println!(
        "  code={:?} is_not_leader={} is_retryable={}",
        err.code(),
        err.is_not_leader(),
        err.is_retryable()
    );
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("rsqlite-rsync client API walkthrough");
    println!("Starting a self-contained single-writer demo server...");
    let (endpoint, _server_guard, _temp) = start_demo_server();
    println!("Server ready at {endpoint}");

    // 1. Client construction & configuration ------------------------------
    section(1, "Client construction (ClientConfig builder)");
    let mut client = SqlGatewayClient::new(
        ClientConfig::new(DiscoveryMode::Direct(endpoint.clone()))
            .with_max_retries(5)
            .with_timeout(Duration::from_secs(5)),
    );
    println!("Built SqlGatewayClient with DiscoveryMode::Direct({endpoint})");

    // 2. discover_leader() --------------------------------------------------
    section(2, "discover_leader()");
    let leader = client.discover_leader().await?;
    println!("Current writer: {leader}");

    // 3. execute(): DDL -------------------------------------------------
    section(3, "execute() — DDL");
    let created = client
        .execute(
            "demo.db",
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, bio TEXT)",
            None,
        )
        .await?;
    println!("CREATE TABLE -> generation {}", created.generation);

    // 4. execute() with bound parameters ------------------------------
    section(4, "execute() — positional and named parameters");
    let inserted = client
        .execute(
            "demo.db",
            "INSERT INTO users (name, age, bio) VALUES (?, ?, ?)",
            Some(positional(vec![val_text("alice"), val_int(30), val_null()])),
        )
        .await?;
    println!(
        "positional insert -> rows_affected={}, last_insert_rowid={}",
        inserted.rows_affected, inserted.last_insert_rowid
    );

    let inserted_named = client
        .execute(
            "demo.db",
            "INSERT INTO users (name, age, bio) VALUES (:name, :age, :bio)",
            Some(named(vec![
                (":name", val_text("bob")),
                (":age", val_int(41)),
                (":bio", val_text("likes gRPC")),
            ])),
        )
        .await?;
    println!(
        "named insert -> rows_affected={}, last_insert_rowid={}",
        inserted_named.rows_affected, inserted_named.last_insert_rowid
    );

    // 5. query(): strong consistency -----------------------------------
    section(5, "query() — CONSISTENCY_LEVEL_STRONG");
    let rows = client
        .query(
            "demo.db",
            "SELECT id, name, age, bio FROM users ORDER BY id",
            None,
            0,
            ConsistencyLevel::Strong,
        )
        .await?;
    print_rows(&rows);

    // 6. query(): eventual consistency ---------------------------------
    section(6, "query() — CONSISTENCY_LEVEL_EVENTUAL");
    println!("Eventual consistency permits reading from a local replica instead of the");
    println!("writer; on this single-node demo it is served identically.");
    let rows = client
        .query(
            "demo.db",
            "SELECT COUNT(*) AS n FROM users",
            None,
            0,
            ConsistencyLevel::Eventual,
        )
        .await?;
    print_rows(&rows);

    // 7. stream_query() ---------------------------------------------------
    section(7, "stream_query()");
    client
        .execute("demo.db", "CREATE TABLE nums (v INTEGER)", None)
        .await?;
    for i in 0..25 {
        client
            .execute(
                "demo.db",
                &format!("INSERT INTO nums (v) VALUES ({i})"),
                None,
            )
            .await?;
    }
    let mut stream = client
        .stream_query(
            "demo.db",
            "SELECT v FROM nums ORDER BY v",
            None,
            0,
            10,
            ConsistencyLevel::Strong,
        )
        .await?;
    let mut chunk_no = 0;
    while let Some(chunk) = tokio_stream::StreamExt::next(&mut stream).await {
        let chunk = chunk?;
        chunk_no += 1;
        println!(
            "chunk {chunk_no}: {} row(s), is_last={}, total_rows={}",
            chunk.rows.len(),
            chunk.is_last,
            chunk.total_rows
        );
    }

    // 8. batch(): happy path ------------------------------------------
    section(8, "batch() — happy path (IMMEDIATE, stop_on_error=true)");
    let batch_ok = client
        .batch(
            "demo.db",
            vec![
                Statement {
                    sql: "INSERT INTO users (name, age) VALUES ('carol', 22)".to_string(),
                    parameters: None,
                },
                Statement {
                    sql: "SELECT COUNT(*) AS n FROM users".to_string(),
                    parameters: None,
                },
            ],
            BatchTransactionMode::Immediate,
            true,
        )
        .await?;
    println!("committed={}", batch_ok.committed);
    for (i, result) in batch_ok.results.iter().enumerate() {
        describe_statement_result(i, result);
    }

    // 9. batch(): failure path -----------------------------------------
    section(
        9,
        "batch() — failure path (bad statement mid-batch, stop_on_error=true)",
    );
    let batch_err = client
        .batch(
            "demo.db",
            vec![
                Statement {
                    sql: "INSERT INTO users (name, age) VALUES ('dave', 19)".to_string(),
                    parameters: None,
                },
                Statement {
                    sql: "INSERT INTO no_such_table (x) VALUES (1)".to_string(),
                    parameters: None,
                },
                Statement {
                    sql: "INSERT INTO users (name, age) VALUES ('erin', 27)".to_string(),
                    parameters: None,
                },
            ],
            BatchTransactionMode::Immediate,
            true,
        )
        .await?;
    println!(
        "committed={} (stop_on_error aborted the transaction: {} statement(s) even attempted)",
        batch_err.committed,
        batch_err.results.len()
    );
    for (i, result) in batch_err.results.iter().enumerate() {
        describe_statement_result(i, result);
    }

    // 10. get_cluster_status() -----------------------------------------
    section(10, "get_cluster_status()");
    let status = client.get_cluster_status().await?;
    let role = NodeRole::try_from(status.role).unwrap_or(NodeRole::Unspecified);
    println!(
        "node_id={} role={role:?} local_generation={}",
        status.node_id, status.local_generation
    );
    if let Some(lease) = &status.lease {
        println!(
            "lease: held={} holder={} generation={} ttl_secs={}",
            lease.is_held, lease.holder_node_id, lease.generation, lease.ttl_secs
        );
    }
    for db in &status.databases {
        describe_database(db);
    }

    // 11. drop_database(): idempotency ----------------------------------
    section(11, "drop_database() — idempotency");
    let first = client.drop_database("demo.db").await?;
    println!("first drop -> existed={}", first.existed);
    let second = client.drop_database("demo.db").await?;
    println!("second drop -> existed={} (already absent)", second.existed);

    // 12. Error introspection -------------------------------------------
    section(12, "Error introspection");
    match client
        .execute("demo.db", "THIS IS NOT VALID SQL", None)
        .await
    {
        Ok(_) => println!("(unexpected success)"),
        Err(err) => describe_error(&err),
    }

    // 13. Alternate discovery modes --------------------------------------
    section(13, "Alternate discovery modes");
    let mut candidates_client =
        SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Candidates(vec![
            endpoint.clone(),
        ])));
    let status = candidates_client.get_cluster_status().await?;
    println!(
        "DiscoveryMode::Candidates([...]) resolved to writer node_id={}",
        status.node_id
    );

    let custom_endpoint = endpoint.clone();
    let mut custom_client = SqlGatewayClient::new(ClientConfig::new(DiscoveryMode::Custom(
        Arc::new(move || {
            let endpoint = custom_endpoint.clone();
            async move { Ok::<_, BoxError>(endpoint) }
        }),
    )));
    let status = custom_client.get_cluster_status().await?;
    println!(
        "DiscoveryMode::Custom(resolver closure) resolved to writer node_id={}",
        status.node_id
    );

    // 14. reset_connection() ----------------------------------------------
    section(14, "reset_connection()");
    client.reset_connection();
    let status = client.get_cluster_status().await?;
    println!(
        "Reconnected after reset_connection(); node_id={}",
        status.node_id
    );

    // 15. Teardown -----------------------------------------------------
    section(15, "Teardown");
    println!("Walkthrough complete. Stopping demo server...");
    Ok(())
}
