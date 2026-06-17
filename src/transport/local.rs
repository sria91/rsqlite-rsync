//! In-process transport using `tokio` duplex channels.
//!
//! [`LocalTransport`] is the default transport for local-to-local sync and is
//! used extensively in tests because it requires no external processes.
//!
//! # Usage
//!
//! ```rust,no_run
//! use rsqlite_rsync::transport::local::LocalTransport;
//!
//! let (mut origin_side, mut replica_side) = LocalTransport::pair();
//! // origin_side sends/receives from the origin's perspective.
//! // replica_side sends/receives from the replica's perspective.
//! ```

use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

use crate::error::{Result, SyncError};
use crate::protocol::messages::{Message, encode};
use crate::transport::{Transport, try_take_framed_message};

/// A [`Transport`] backed by a pair of in-memory `tokio` duplex byte streams.
pub struct LocalTransport {
    stream: DuplexStream,
    /// Partial-read buffer — accumulates bytes until a full message is present.
    buf: Vec<u8>,
}

impl LocalTransport {
    /// Create a connected pair of transports.
    ///
    /// The first element should be given to the origin handler and the second
    /// to the replica handler.
    pub fn pair() -> (Self, Self) {
        let (a, b) = tokio::io::duplex(8 * 1024 * 1024); // 8 MiB internal buffer
        (
            LocalTransport {
                stream: a,
                buf: Vec::new(),
            },
            LocalTransport {
                stream: b,
                buf: Vec::new(),
            },
        )
    }
}

#[async_trait::async_trait]
impl Transport for LocalTransport {
    async fn send(&mut self, msg: &Message) -> Result<()> {
        let bytes = encode(msg)?;
        self.stream.write_all(&bytes).await.map_err(SyncError::Io)
    }

    async fn recv(&mut self) -> Result<Message> {
        loop {
            // Try to decode from what is already buffered.
            if let Some(msg) = try_take_framed_message(&mut self.buf)? {
                return Ok(msg);
            }

            // Read more bytes from the stream.
            let mut tmp = [0u8; 8192];
            let n = self.stream.read(&mut tmp).await.map_err(SyncError::Io)?;
            if n == 0 {
                return Err(SyncError::Protocol("connection closed unexpectedly".into()));
            }
            self.buf.extend_from_slice(&tmp[..n]);
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::{Message, PROTOCOL_VERSION};

    #[tokio::test]
    async fn send_recv_single_message() {
        let (mut origin, mut replica) = LocalTransport::pair();

        let msg = Message::Hello {
            version: PROTOCOL_VERSION,
            page_size: 4096,
            page_count: 10,
        };

        origin.send(&msg).await.unwrap();
        let received = replica.recv().await.unwrap();
        assert_eq!(received, msg);
    }

    #[tokio::test]
    async fn send_recv_multiple_messages() {
        let (mut a, mut b) = LocalTransport::pair();

        let msgs = vec![
            Message::Hello {
                version: 1,
                page_size: 4096,
                page_count: 1,
            },
            Message::Done,
            Message::Error {
                message: "oops".into(),
            },
        ];

        for m in &msgs {
            a.send(m).await.unwrap();
        }
        for expected in &msgs {
            let got = b.recv().await.unwrap();
            assert_eq!(&got, expected);
        }
    }

    #[tokio::test]
    async fn large_page_data_survives_transport() {
        let (mut a, mut b) = LocalTransport::pair();

        let page_data = vec![0xABu8; 65536]; // 64 KiB page
        let msg = Message::SendPages {
            pages: vec![crate::protocol::messages::PageData {
                page_no: 1,
                data: page_data.clone(),
            }],
        };
        a.send(&msg).await.unwrap();
        let got = b.recv().await.unwrap();
        assert_eq!(got, msg);
    }
}
