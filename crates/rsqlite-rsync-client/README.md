# rsqlite-rsync-client

Async gRPC client for the [rsqlite-rsync](https://github.com/sria91/rsqlite-rsync)
HA SQL Gateway, with leader discovery and automatic failover.

This crate is deliberately lean: it depends only on `tonic`, a slim `tokio`,
and the generated wire types in `rsqlite-rsync-proto` — no SQLite FFI, no HA
controller, no CLI dependencies — so any Rust service can add it as a
dependency to talk to an rsqlite-rsync HA cluster.

```rust,no_run
use rsqlite_rsync_client::{ClientConfig, DiscoveryMode, SqlGatewayClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = SqlGatewayClient::new(ClientConfig::new(
        DiscoveryMode::Direct("http://127.0.0.1:50051".to_string()),
    ));

    client
        .execute("app.db", "CREATE TABLE t (id INTEGER PRIMARY KEY)", None)
        .await?;
    let rows = client
        .query("app.db", "SELECT * FROM t", None, 0, Default::default())
        .await?;
    println!("{} rows", rows.total_rows);
    Ok(())
}
```

See the crate documentation for discovery modes (`Direct`, `Candidates`,
`Custom`), retry semantics, and failover behavior.
