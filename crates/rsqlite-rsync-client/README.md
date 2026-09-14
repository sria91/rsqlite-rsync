# rsqlite-rsync-client

Async gRPC client for the [rsqlite-rsync](https://github.com/sria91/rsqlite-rsync)
HA SQL Gateway, with leader discovery, bearer authentication, and transparent failover.

This crate is deliberately lean: it depends only on `tonic`, a slim `tokio`,
`async-trait`, `thiserror`, and the generated wire types in `rsqlite-rsync-proto` — **no SQLite C FFI, no HA
controller, no CLI dependencies** — so any Rust service can add it as a
lightweight dependency to interact with an rsqlite-rsync HA cluster.

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
rsqlite-rsync-client = { path = "crates/rsqlite-rsync-client" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
async-trait = "0.1" # required if implementing a custom LeaderResolver
```

## Quickstart

```rust,no_run
use rsqlite_rsync_client::{ClientConfig, DiscoveryMode, SqlGatewayClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Configure leader discovery and authentication
    let config = ClientConfig::new(
        DiscoveryMode::Direct("http://127.0.0.1:50051".to_string())
    )
    .with_auth_token(std::env::var("RSQLITE_TOKEN")?)
    .with_max_retries(5);

    let mut client = SqlGatewayClient::new(config);

    // 2. Execute DDL / DML writes
    client
        .execute(
            "app.db",
            "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
            None,
        )
        .await?;

    let exec_res = client
        .execute(
            "app.db",
            "INSERT INTO users (name) VALUES ('Alice')",
            None,
        )
        .await?;
    println!("Inserted {} row, last_insert_id={}", exec_res.rows_affected, exec_res.last_insert_rowid);

    // 3. Query records
    let rows = client
        .query("app.db", "SELECT id, name FROM users", None, 100, Default::default())
        .await?;
    println!("Fetched {} rows:", rows.total_rows);
    for row in rows.rows {
        println!("{:?}", row.values);
    }

    Ok(())
}
```

## Discovery Modes

The client supports three discovery strategies via `DiscoveryMode`:

1. **Direct endpoint**:
   ```rust
   let discovery = DiscoveryMode::Direct("http://10.0.0.1:50051".to_string());
   ```
   Connects to a single designated host. If that host returns a `NOT_LEADER` gRPC status with a redirect endpoint in the response metadata (`x-rsqlite-leader-endpoint`), the client automatically reconnects to the redirected leader.

2. **Candidate probing**:
   ```rust
   let discovery = DiscoveryMode::Candidates(vec![
       "http://10.0.0.1:50051".to_string(),
       "http://10.0.0.2:50051".to_string(),
       "http://10.0.0.3:50051".to_string(),
   ]);
   ```
   Probes the candidate nodes on connect to discover which one currently holds the active writer role.

3. **Custom resolver**:
   ```rust
   use rsqlite_rsync_client::{BoxError, LeaderResolver};
   use std::sync::Arc;

   struct ConsulResolver;

   #[async_trait::async_trait]
   impl LeaderResolver for ConsulResolver {
       async fn resolve(&self) -> Result<String, BoxError> {
           // Look up leader endpoint dynamically from Consul, etcd, DNS, etc.
           Ok("http://10.0.0.5:50051".to_string())
       }
   }

   let discovery = DiscoveryMode::Custom(Arc::new(ConsulResolver));
   ```

## Configuration Options

`ClientConfig` provides builder methods for fine-grained tuning:

```rust
use std::time::Duration;
use rsqlite_rsync_client::{ClientConfig, DiscoveryMode};

let config = ClientConfig::new(DiscoveryMode::Direct("http://127.0.0.1:50051".into()))
    .with_auth_token("my-secret-token")     // Sets `authorization: Bearer <token>`
    .with_max_retries(5)                   // Maximum attempts per call (default: 5)
    .with_initial_backoff(Duration::from_millis(100)) // Initial retry backoff
    .with_max_backoff(Duration::from_secs(2))         // Maximum retry backoff cap
    .with_timeout(Duration::from_secs(15));           // Request & connect timeout
```

## Client Operations

### 1. DDL and DML Execution (`execute`)
Executes single statements (e.g., `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`).

```rust
let response = client
    .execute("app.db", "UPDATE users SET name = 'Bob' WHERE id = 1", None)
    .await?;
```

### 2. Read Queries (`query`)
Runs read queries with pagination and consistency level configuration.

```rust
use rsqlite_rsync_client::proto::ConsistencyLevel;

let response = client
    .query(
        "app.db",
        "SELECT id, name FROM users WHERE id > 0",
        None,
        1000,                      // max_rows (0 = server default / unlimited)
        ConsistencyLevel::Strong,  // ConsistencyLevel::Strong or ConsistencyLevel::Eventual
    )
    .await?;
```

### 3. Chunked Streaming Queries (`stream_query`)
Streams large result sets incrementally in chunks without buffering the full dataset in memory.

```rust
use rsqlite_rsync_client::proto::ConsistencyLevel;

let mut stream = client
    .stream_query(
        "app.db",
        "SELECT * FROM large_log_table",
        None,
        0,                         // max_rows (0 = unlimited)
        500,                       // chunk_size (rows per chunk)
        ConsistencyLevel::Strong,
    )
    .await?;

while let Some(chunk) = stream.message().await? {
    println!("Received chunk with {} rows", chunk.rows.len());
}
```

### 4. Transactional Batches (`batch`)
Executes multiple statements with an optional transaction. Statements are executed atomically within a single transaction when using `Deferred`, `Immediate`, or `Exclusive` mode. Selecting `BatchTransactionMode::None` executes statements individually in autocommit mode without a transaction.

```rust
use rsqlite_rsync_client::proto::{BatchTransactionMode, Statement};

let stmts = vec![
    Statement { sql: "INSERT INTO users (name) VALUES ('Charlie')".into(), parameters: None },
    Statement { sql: "INSERT INTO users (name) VALUES ('Diana')".into(), parameters: None },
];

let batch_res = client
    .batch(
        "app.db",
        stmts,
        BatchTransactionMode::Immediate, // Deferred, Immediate, Exclusive, or None (autocommit)
        true,                            // stop_on_error
    )
    .await?;
```

### 5. Cluster Status and Database Management
```rust
// Inspect cluster role, lease holder, and active generation
let status = client.get_cluster_status().await?;
println!("Current role: {:?}, generation: {}", status.role(), status.local_generation);

// Drop database and associated WAL/SHM sidecars
let drop_res = client.drop_database("temp.db").await?;
println!("Database dropped: {}", drop_res.existed);
```

## Failover and Retry Semantics

- **Writes (`execute`, `batch`)**: Automatically retried **only** on a definitive `NOT_LEADER` response (following the leader redirect metadata). Ambiguous network failures (`Unavailable`, `DeadlineExceeded`) are **not** retried blindly, preventing duplicate executions of non-idempotent statements.
- **Reads (`query`, `get_cluster_status`)**: Safe and idempotent; automatically retried across transient network drops, deadline timeouts, and leader transitions.
- **Database management (`drop_database`)**: Deletes the database file and its associated WAL/SHM sidecars. Treated as idempotent (an absent database returns `existed: false`). Automatically retried across transient network drops (`Unavailable`, `DeadlineExceeded`) and leader transitions (`NOT_LEADER`).
- **Streams (`stream_query`)**: The initial call establishing the stream is retried. Once streaming begins, mid-stream disconnections are surfaced directly to the caller.
