# rsqlite-rsync-pool

Connection pool adapters for the
[rsqlite-rsync-client](../rsqlite-rsync-client) gRPC SQL Gateway client.

Three pool backends are supported behind Cargo feature flags:

| Feature      | Pool crate   | Mode   | Default |
|--------------|--------------|--------|---------|
| **`bb8`**    | [bb8][]      | Async  | ✓       |
| `deadpool`   | [deadpool][] | Async  |         |
| `r2d2`       | [r2d2][]     | Sync   |         |

[bb8]: https://crates.io/crates/bb8
[deadpool]: https://crates.io/crates/deadpool
[r2d2]: https://crates.io/crates/r2d2

All backends share the same `SqlGatewayManager`, which holds a `ClientConfig`
and knows how to create and health-check `SqlGatewayClient` instances.

## Installation

```toml
[dependencies]
# bb8 backend (default, async)
rsqlite-rsync-pool = { path = "crates/rsqlite-rsync-pool" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

# deadpool backend (async)
rsqlite-rsync-pool = { path = "crates/rsqlite-rsync-pool", default-features = false, features = ["deadpool"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }

# r2d2 backend (sync / blocking)
rsqlite-rsync-pool = { path = "crates/rsqlite-rsync-pool", default-features = false, features = ["r2d2"] }
```

## Quickstart (bb8 - Async)

```rust,no_run
use rsqlite_rsync_pool::pool_bb8::Pool;
use rsqlite_rsync_pool::rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
use rsqlite_rsync_pool::SqlGatewayManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = SqlGatewayManager::new(
        ClientConfig::new(DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()))
            .with_auth_token(std::env::var("RSQLITE_TOKEN")?),
    );

    let pool = Pool::builder()
        .max_size(8)
        .build(manager)
        .await?;

    // Check out a connection from the pool
    let mut conn = pool.get().await?;

    conn.execute(
        "app.db",
        "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)",
        None,
    )
    .await?;

    let rows = conn
        .query("app.db", "SELECT id, name FROM users", None, 100, Default::default())
        .await?;
    println!("Fetched {} rows", rows.total_rows);

    Ok(())
}
```

## Quickstart (deadpool - Async)

```rust,no_run
use rsqlite_rsync_pool::pool_deadpool::Pool;
use rsqlite_rsync_pool::rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
use rsqlite_rsync_pool::SqlGatewayManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = SqlGatewayManager::new(
        ClientConfig::new(DiscoveryMode::Direct("http://127.0.0.1:50051".to_string())),
    );

    let pool = Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("pool build");

    let mut conn = pool.get().await?;
    conn.execute("app.db", "INSERT INTO users (name) VALUES ('Alice')", None)
        .await?;

    Ok(())
}
```

## Quickstart (r2d2 - Sync / Blocking)

```rust,no_run
use rsqlite_rsync_pool::pool_r2d2::Pool;
use rsqlite_rsync_pool::rsqlite_rsync_client::{ClientConfig, DiscoveryMode};
use rsqlite_rsync_pool::SqlGatewayManager;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = SqlGatewayManager::new(
        ClientConfig::new(DiscoveryMode::Direct("http://127.0.0.1:50051".to_string())),
    );

    let pool = Pool::builder()
        .max_size(8)
        .build(manager)?;

    let mut conn = pool.get()?;
    conn.execute("app.db", "INSERT INTO users (name) VALUES ('Alice')", None)?;

    let rows = conn.query("app.db", "SELECT * FROM users", None, 100, Default::default())?;
    println!("Fetched {} rows", rows.total_rows);

    Ok(())
}
```

## How it works

- **`connect` / `create`**: Creates a fresh `SqlGatewayClient` from the
  manager's `ClientConfig` and eagerly validates it with a
  `get_cluster_status()` health-check RPC. The pool never hands out a client
  that cannot reach its cluster.

- **`is_valid` / `recycle`**: Runs `get_cluster_status()` against the
  existing connection as a lightweight liveness probe.

- **`has_broken`**: Always returns `false`. `SqlGatewayClient` self-heals
  through its internal retry loop and NOT_LEADER leader redirection, so there
  is no synchronously-detectable "broken" state the pool needs to act on.

## License

MIT OR Apache-2.0
