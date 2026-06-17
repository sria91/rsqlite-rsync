//! Protocol message types and their binary codec (bincode v2).
//!
//! Every message exchanged between origin and replica is one variant of
//! [`Message`].  Messages are length-prefixed on the wire:
//!
//! ```text
//! +--------+--------+-------- ... --------+
//! | len u32 (LE)    |  bincode payload     |
//! +--------+--------+-------- ... --------+
//! ```
//!
//! The length field encodes the number of payload bytes that follow (not
//! including the 4-byte length field itself).

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::hash::PageHash;

// ─────────────────────────────────────────────────────────────────────────────
// Wire messages
// ─────────────────────────────────────────────────────────────────────────────

/// Protocol version spoken by this build.
pub const PROTOCOL_VERSION: u32 = 2;

/// Hard upper bound for a single framed protocol message.
pub const MAX_MESSAGE_SIZE: usize = 256 * 1024 * 1024;

/// Every message that can travel over the wire in either direction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Message {
    // ── Handshake ────────────────────────────────────────────────────────────
    /// Sent by the replica immediately after connecting.
    Hello {
        /// Protocol version offered by the replica.
        version: u32,
        /// Page size of the replica database (bytes).
        page_size: u32,
        /// Number of pages currently in the replica.
        page_count: u32,
    },

    /// Sent by the origin in response to [`Message::Hello`].
    HelloAck {
        /// Negotiated protocol version (≤ offered version).
        version: u32,
        /// Page size of the origin database.
        page_size: u32,
        /// Total pages in the origin database at the start of this session.
        page_count: u32,
    },

    // ── Coarse pass ──────────────────────────────────────────────────────────
    /// Replica → Origin: group hashes for a contiguous range of page groups.
    ///
    /// `first_group` is the 0-based group index of `hashes[0]`.
    GroupHashes {
        /// Index of the first group in this batch.
        first_group: u32,
        /// One protocol-version-negotiated hash per page group.
        hashes: Vec<PageHash>,
    },

    /// Origin → Replica: which groups need fine-grained inspection.
    GroupsNeedFine {
        /// 0-based group indices that did not match.
        group_indices: Vec<u32>,
    },

    // ── Fine pass ────────────────────────────────────────────────────────────
    /// Replica → Origin: per-page hashes for pages inside a specific group.
    PageHashes {
        /// 1-based page numbers.
        page_nos: Arc<[u32]>,
        /// Hash for each page number listed in `page_nos`.
        hashes: Arc<[PageHash]>,
    },

    /// Origin → Replica: deliver these pages (origin found them different).
    SendPages {
        /// Pages to write, each paired with its 1-based page number and raw
        /// bytes.
        pages: Vec<PageData>,
    },

    // ── Acknowledgement ──────────────────────────────────────────────────────
    /// Replica → Origin: page writes confirmed; ready for next batch.
    PagesAck {
        /// 1-based page numbers that were successfully written.
        page_nos: Vec<u32>,
    },

    // ── Termination ──────────────────────────────────────────────────────────
    /// Origin → Replica: sync complete, no more pages to send.
    Done,

    /// Either side: unrecoverable error — abort with explanation.
    Error { message: String },
}

/// A single database page's content as exchanged during the fine pass.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageData {
    /// 1-based page number.
    pub page_no: u32,
    /// Raw page bytes (exactly `page_size` bytes).
    pub data: Vec<u8>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Codec helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Encode a [`Message`] into a length-prefixed byte buffer.
///
/// # Errors
///
/// Returns [`crate::error::SyncError::Codec`] if serialisation fails.
pub fn encode(msg: &Message) -> crate::error::Result<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(msg, bincode::config::standard())
        .map_err(|e| crate::error::SyncError::Codec(e.to_string()))?;
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(&payload);
    Ok(buf)
}

/// Decode a [`Message`] from a length-prefixed byte slice.
///
/// `buf` must contain *at least* 4 bytes (the length prefix) plus the payload.
/// Returns the decoded message and the number of bytes consumed.
///
/// # Errors
///
/// Returns [`crate::error::SyncError::Codec`] if the length prefix is
/// truncated or deserialisation fails.
pub fn decode(buf: &[u8]) -> crate::error::Result<(Message, usize)> {
    if buf.len() < 4 {
        return Err(crate::error::SyncError::Codec(
            "buffer too short for length prefix".into(),
        ));
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    if len > MAX_MESSAGE_SIZE {
        return Err(crate::error::SyncError::Codec(format!(
            "message frame exceeds max size: {len} > {MAX_MESSAGE_SIZE}"
        )));
    }
    if buf.len() < 4 + len {
        return Err(crate::error::SyncError::Codec(format!(
            "buffer has {} bytes but expected {} payload bytes",
            buf.len() - 4,
            len
        )));
    }
    let (msg, _) = bincode::serde::decode_from_slice(&buf[4..4 + len], bincode::config::standard())
        .map_err(|e| crate::error::SyncError::Codec(e.to_string()))?;
    Ok((msg, 4 + len))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(msg: Message) {
        let encoded = encode(&msg).expect("encode");
        let (decoded, consumed) = decode(&encoded).expect("decode");
        assert_eq!(decoded, msg);
        assert_eq!(consumed, encoded.len());
    }

    #[test]
    fn hello_round_trip() {
        round_trip(Message::Hello {
            version: PROTOCOL_VERSION,
            page_size: 4096,
            page_count: 100,
        });
    }

    #[test]
    fn hello_ack_round_trip() {
        round_trip(Message::HelloAck {
            version: PROTOCOL_VERSION,
            page_size: 4096,
            page_count: 200,
        });
    }

    #[test]
    fn group_hashes_round_trip() {
        round_trip(Message::GroupHashes {
            first_group: 0,
            hashes: vec![[0u8; 32], [1u8; 32]],
        });
    }

    #[test]
    fn send_pages_round_trip() {
        round_trip(Message::SendPages {
            pages: vec![PageData {
                page_no: 1,
                data: vec![42u8; 4096],
            }],
        });
    }

    #[test]
    fn done_round_trip() {
        round_trip(Message::Done);
    }

    #[test]
    fn error_round_trip() {
        round_trip(Message::Error {
            message: "something broke".into(),
        });
    }

    #[test]
    fn decode_truncated_length_prefix_errors() {
        let result = decode(&[0, 0]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_truncated_payload_errors() {
        // 4-byte length saying 100 bytes follow, but nothing actually follows.
        let mut buf = vec![100u8, 0, 0, 0];
        buf.extend_from_slice(&[0u8; 10]); // only 10 bytes of payload
        let result = decode(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn decode_oversized_payload_errors() {
        let len = (MAX_MESSAGE_SIZE as u32).saturating_add(1);
        let buf = len.to_le_bytes().to_vec();
        let result = decode(&buf);
        assert!(result.is_err());
    }
}
