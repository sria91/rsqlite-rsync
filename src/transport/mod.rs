//! Transport abstraction: how messages travel between origin and replica.
//!
//! The [`Transport`] trait decouples the protocol state machines from the
//! underlying I/O mechanism.  Two implementations are provided:
//!
//! * [`local::LocalTransport`] — in-process `tokio` channels (used for
//!   local-to-local sync and in tests).
//! * [`ssh::SshTransport`] — an SSH subprocess bridge (used for remote sync).

use crate::error::{Result, SyncError};
use crate::protocol::messages::{MAX_MESSAGE_SIZE, Message, decode};

pub mod local;
pub mod ssh;
pub mod stdio;

pub(crate) fn try_take_framed_message(buf: &mut Vec<u8>) -> Result<Option<Message>> {
    if buf.len() < 4 {
        return Ok(None);
    }

    let payload_len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if payload_len > MAX_MESSAGE_SIZE {
        return Err(SyncError::Codec(format!(
            "message frame exceeds max size: {payload_len} > {MAX_MESSAGE_SIZE}"
        )));
    }

    if buf.len() < 4 + payload_len {
        return Ok(None);
    }

    let (msg, consumed) = decode(buf)?;
    buf.drain(..consumed);
    Ok(Some(msg))
}

/// A bidirectional, ordered, reliable message channel.
///
/// Implementations must be `Send` so they can be used across `tokio` tasks.
#[async_trait::async_trait]
pub trait Transport: Send {
    /// Send a single message to the remote endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying I/O fails or the message cannot be
    /// encoded.
    async fn send(&mut self, msg: &Message) -> Result<()>;

    /// Receive the next message from the remote endpoint.
    ///
    /// Blocks (asynchronously) until a complete message is available.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying I/O fails or the incoming bytes
    /// cannot be decoded.
    async fn recv(&mut self) -> Result<Message>;

    /// Gracefully close the transport and release underlying resources.
    ///
    /// Implementations that manage child processes should use this to ensure
    /// children are reaped to avoid zombie processes.
    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::messages::{encode, Message, PROTOCOL_VERSION};

    #[test]
    fn framed_message_round_trip() {
        let msg = Message::Hello {
            version: PROTOCOL_VERSION,
            page_size: 4096,
            page_count: 1,
        };
        let mut buf = encode(&msg).unwrap();

        let decoded = try_take_framed_message(&mut buf).unwrap();
        assert_eq!(decoded, Some(msg));
        assert!(buf.is_empty());
    }

    #[test]
    fn framed_message_waits_for_complete_payload() {
        let msg = Message::Done;
        let encoded = encode(&msg).unwrap();
        let mut buf = encoded[..encoded.len() - 1].to_vec();

        let decoded = try_take_framed_message(&mut buf).unwrap();
        assert_eq!(decoded, None);
        assert_eq!(buf.len(), encoded.len() - 1);
    }

    #[test]
    fn oversized_frame_is_rejected() {
        let len = (MAX_MESSAGE_SIZE as u32).saturating_add(1);
        let mut buf = len.to_le_bytes().to_vec();

        let err = try_take_framed_message(&mut buf).unwrap_err();
        assert!(matches!(err, SyncError::Codec(_)));
    }
}
