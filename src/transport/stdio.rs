//! Stdio-backed transport for `rsqlite-rsync --server`.

use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, Stdin, Stdout};

use crate::error::{Result, SyncError};
use crate::protocol::messages::{Message, encode};
use crate::transport::{Transport, try_take_framed_message};

/// A [`Transport`] that exchanges framed protocol messages over stdin/stdout.
pub struct StdioTransport<R = BufReader<Stdin>, W = Stdout> {
    stdin: R,
    stdout: W,
    buf: Vec<u8>,
}

impl StdioTransport<BufReader<Stdin>, Stdout> {
    /// Create a transport connected to the current process stdin/stdout.
    pub fn new() -> Self {
        StdioTransport {
            stdin: BufReader::new(io::stdin()),
            stdout: io::stdout(),
            buf: Vec::new(),
        }
    }
}

impl<R, W> StdioTransport<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
    /// Create a transport from custom reader and writer streams.
    pub fn from_io(reader: R, writer: W) -> Self {
        StdioTransport {
            stdin: reader,
            stdout: writer,
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
impl<R, W> Transport for StdioTransport<R, W>
where
    R: AsyncRead + Unpin + Send,
    W: AsyncWrite + Unpin + Send,
{
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::PROTOCOL_VERSION;

    #[tokio::test]
    async fn stdio_transport_default_constructs() {
        let _ = StdioTransport::default();
    }

    #[tokio::test]
    async fn stdio_transport_round_trip_via_duplex() {
        let (client_read, server_write) = tokio::io::duplex(1024);
        let (server_read, client_write) = tokio::io::duplex(1024);

        let mut client = StdioTransport::from_io(client_read, client_write);
        let mut server = StdioTransport::from_io(server_read, server_write);

        let msg = Message::Hello {
            version: PROTOCOL_VERSION,
            page_size: 4096,
            page_count: 42,
        };

        client.send(&msg).await.unwrap();
        let received = server.recv().await.unwrap();
        assert_eq!(received, msg);

        let reply = Message::Done;
        server.send(&reply).await.unwrap();
        let client_received = client.recv().await.unwrap();
        assert_eq!(client_received, reply);
    }

    #[tokio::test]
    async fn stdio_transport_recv_unexpected_eof() {
        let (read, write) = tokio::io::duplex(64);
        drop(write); // Close write end immediately

        let mut transport = StdioTransport::from_io(read, tokio::io::sink());
        let err = transport.recv().await.unwrap_err();
        assert!(matches!(err, SyncError::Protocol(_)));
    }

    #[tokio::test]
    async fn stdio_transport_send_failure() {
        let (read, write) = tokio::io::duplex(64);
        drop(read); // Close read end so write fails

        let mut transport = StdioTransport::from_io(tokio::io::empty(), write);
        let err = transport.send(&Message::Done).await.unwrap_err();
        assert!(matches!(err, SyncError::Io(_)));
    }
}
