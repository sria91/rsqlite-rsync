//! Stdio-backed transport for `rsqlite-rsync --server`.

use tokio::io::{self, AsyncReadExt, AsyncWriteExt, BufReader, Stdin, Stdout};

use crate::error::{Result, SyncError};
use crate::protocol::messages::{Message, encode};
use crate::transport::{Transport, try_take_framed_message};

/// A [`Transport`] that exchanges framed protocol messages over stdin/stdout.
pub struct StdioTransport {
    stdin: BufReader<Stdin>,
    stdout: Stdout,
    buf: Vec<u8>,
}

impl StdioTransport {
    /// Create a transport connected to the current process stdin/stdout.
    pub fn new() -> Self {
        StdioTransport {
            stdin: BufReader::new(io::stdin()),
            stdout: io::stdout(),
            buf: Vec::new(),
        }
    }
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Transport for StdioTransport {
    async fn send(&mut self, msg: &Message) -> Result<()> {
        let bytes = encode(msg)?;
        self.stdout.write_all(&bytes).await.map_err(SyncError::Io)?;
        self.stdout.flush().await.map_err(SyncError::Io)
    }

    async fn recv(&mut self) -> Result<Message> {
        loop {
            if let Some(msg) = try_take_framed_message(&mut self.buf)? {
                return Ok(msg);
            }

            let mut tmp = [0u8; 8192];
            let n = self.stdin.read(&mut tmp).await.map_err(SyncError::Io)?;
            if n == 0 {
                return Err(SyncError::Protocol("connection closed unexpectedly".into()));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }
}
