//! gRPC-based bidirectional streaming transport for replication.
//!
//! [`GrpcTransport`] implements [`Transport`] over a bidirectional gRPC stream
//! (`ReplicationService.Sync`), translating between domain [`Message`] instances
//! and Protobuf [`SyncMessage`] types.

use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::Streaming;

use crate::error::{Result, SyncError};
use crate::proto::rsqlite::v1::{
    SyncDone, SyncError as ProtoSyncError, SyncGroupHashes, SyncGroupsNeedFine, SyncHello,
    SyncHelloAck, SyncMessage, SyncPageData, SyncPageHashes, SyncPagesAck, SyncSendPages,
    sync_message::Payload,
};
use crate::protocol::messages::{Message, PageData};
use crate::transport::Transport;

/// Default buffer capacity for outbound gRPC message channel.
pub const DEFAULT_GRPC_CHANNEL_CAPACITY: usize = 64;

/// A bidirectional streaming transport backed by gRPC.
pub struct GrpcTransport {
    tx: mpsc::Sender<SyncMessage>,
    rx: Streaming<SyncMessage>,
}

impl GrpcTransport {
    /// Create a new `GrpcTransport` from an outbound channel sender and an inbound gRPC stream.
    pub fn new(tx: mpsc::Sender<SyncMessage>, rx: Streaming<SyncMessage>) -> Self {
        Self { tx, rx }
    }

    /// Split into raw sender and receiver streams.
    pub fn into_parts(self) -> (mpsc::Sender<SyncMessage>, Streaming<SyncMessage>) {
        (self.tx, self.rx)
    }
}

#[async_trait::async_trait]
impl Transport for GrpcTransport {
    async fn send(&mut self, msg: &Message) -> Result<()> {
        let proto_msg: SyncMessage = msg.into();
        self.tx
            .send(proto_msg)
            .await
            .map_err(|e| SyncError::Network(format!("failed to send gRPC message: {e}")))
    }

    async fn recv(&mut self) -> Result<Message> {
        let item = self
            .rx
            .message()
            .await
            .map_err(|status| SyncError::Network(format!("gRPC streaming error: {status}")))?;

        match item {
            Some(proto_msg) => Message::try_from(proto_msg),
            None => Err(SyncError::Protocol(
                "gRPC stream closed unexpectedly by remote peer".into(),
            )),
        }
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Message <-> SyncMessage Conversions
// ─────────────────────────────────────────────────────────────────────────────

impl From<&Message> for SyncMessage {
    fn from(msg: &Message) -> Self {
        let payload = match msg {
            Message::Hello {
                version,
                page_size,
                page_count,
            } => Payload::Hello(SyncHello {
                version: *version,
                page_size: *page_size,
                page_count: *page_count,
            }),
            Message::HelloAck {
                version,
                page_size,
                page_count,
            } => Payload::HelloAck(SyncHelloAck {
                version: *version,
                page_size: *page_size,
                page_count: *page_count,
            }),
            Message::GroupHashes {
                first_group,
                hashes,
            } => {
                let mut packed = Vec::with_capacity(hashes.len() * 32);
                for h in hashes {
                    packed.extend_from_slice(h);
                }
                Payload::GroupHashes(SyncGroupHashes {
                    first_group: *first_group,
                    hashes: packed,
                })
            }
            Message::GroupsNeedFine { group_indices } => {
                Payload::GroupsNeedFine(SyncGroupsNeedFine {
                    group_indices: group_indices.clone(),
                })
            }
            Message::PageHashes { page_nos, hashes } => {
                let mut packed = Vec::with_capacity(hashes.len() * 32);
                for h in hashes.iter() {
                    packed.extend_from_slice(h);
                }
                Payload::PageHashes(SyncPageHashes {
                    page_nos: page_nos.to_vec(),
                    hashes: packed,
                })
            }
            Message::SendPages { pages } => Payload::SendPages(SyncSendPages {
                pages: pages
                    .iter()
                    .map(|p| SyncPageData {
                        page_no: p.page_no,
                        data: p.data.clone(),
                    })
                    .collect(),
            }),
            Message::PagesAck { page_nos } => Payload::PagesAck(SyncPagesAck {
                page_nos: page_nos.clone(),
            }),
            Message::Done => Payload::Done(SyncDone {}),
            Message::Error { message } => Payload::Error(ProtoSyncError {
                message: message.clone(),
            }),
        };

        SyncMessage {
            payload: Some(payload),
        }
    }
}

impl From<Message> for SyncMessage {
    fn from(msg: Message) -> Self {
        SyncMessage::from(&msg)
    }
}

impl TryFrom<SyncMessage> for Message {
    type Error = SyncError;

    fn try_from(proto_msg: SyncMessage) -> Result<Self> {
        let payload = proto_msg
            .payload
            .ok_or_else(|| SyncError::Protocol("received SyncMessage with empty payload".into()))?;

        match payload {
            Payload::Init(_) => Err(SyncError::Protocol(
                "unexpected SyncInit message during active sync stream".into(),
            )),
            Payload::Hello(h) => Ok(Message::Hello {
                version: h.version,
                page_size: h.page_size,
                page_count: h.page_count,
            }),
            Payload::HelloAck(h) => Ok(Message::HelloAck {
                version: h.version,
                page_size: h.page_size,
                page_count: h.page_count,
            }),
            Payload::GroupHashes(gh) => {
                if gh.hashes.len() % 32 != 0 {
                    return Err(SyncError::Codec(format!(
                        "invalid group hashes length: {} is not a multiple of 32",
                        gh.hashes.len()
                    )));
                }
                let (chunks, _) = gh.hashes.as_chunks::<32>();
                let hashes = chunks.to_vec();
                Ok(Message::GroupHashes {
                    first_group: gh.first_group,
                    hashes,
                })
            }
            Payload::GroupsNeedFine(gnf) => Ok(Message::GroupsNeedFine {
                group_indices: gnf.group_indices,
            }),
            Payload::PageHashes(ph) => {
                if ph.hashes.len() % 32 != 0 {
                    return Err(SyncError::Codec(format!(
                        "invalid page hashes length: {} is not a multiple of 32",
                        ph.hashes.len()
                    )));
                }
                let count = ph.hashes.len() / 32;
                if count != ph.page_nos.len() {
                    return Err(SyncError::Codec(format!(
                        "page count mismatch: {count} hashes for {} page numbers",
                        ph.page_nos.len()
                    )));
                }
                let (chunks, _) = ph.hashes.as_chunks::<32>();
                let hashes = chunks.to_vec();
                Ok(Message::PageHashes {
                    page_nos: Arc::from(ph.page_nos),
                    hashes: Arc::from(hashes),
                })
            }
            Payload::SendPages(sp) => {
                let pages = sp
                    .pages
                    .into_iter()
                    .map(|p| PageData {
                        page_no: p.page_no,
                        data: p.data,
                    })
                    .collect();
                Ok(Message::SendPages { pages })
            }
            Payload::PagesAck(pa) => Ok(Message::PagesAck {
                page_nos: pa.page_nos,
            }),
            Payload::Done(_) => Ok(Message::Done),
            Payload::Error(e) => Ok(Message::Error { message: e.message }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_round_trip(msg: Message) {
        let proto_msg: SyncMessage = (&msg).into();
        let decoded: Message = proto_msg.try_into().expect("conversion to Message");
        assert_eq!(msg, decoded);
    }

    #[test]
    fn round_trip_all_message_types() {
        assert_round_trip(Message::Hello {
            version: 2,
            page_size: 4096,
            page_count: 100,
        });

        assert_round_trip(Message::HelloAck {
            version: 2,
            page_size: 4096,
            page_count: 100,
        });

        assert_round_trip(Message::GroupHashes {
            first_group: 0,
            hashes: vec![[1u8; 32], [2u8; 32]],
        });

        assert_round_trip(Message::GroupsNeedFine {
            group_indices: vec![0, 1, 5],
        });

        assert_round_trip(Message::PageHashes {
            page_nos: Arc::from(vec![1, 2, 3]),
            hashes: Arc::from(vec![[10u8; 32], [20u8; 32], [30u8; 32]]),
        });

        assert_round_trip(Message::SendPages {
            pages: vec![
                PageData {
                    page_no: 1,
                    data: vec![123; 4096],
                },
                PageData {
                    page_no: 2,
                    data: vec![234; 4096],
                },
            ],
        });

        assert_round_trip(Message::PagesAck {
            page_nos: vec![1, 2],
        });

        assert_round_trip(Message::Done);

        assert_round_trip(Message::Error {
            message: "something went wrong".into(),
        });
    }

    #[test]
    fn invalid_group_hashes_length() {
        let proto_msg = SyncMessage {
            payload: Some(Payload::GroupHashes(SyncGroupHashes {
                first_group: 0,
                hashes: vec![0u8; 31], // not multiple of 32
            })),
        };
        let err = Message::try_from(proto_msg).unwrap_err();
        assert!(matches!(err, SyncError::Codec(_)));
    }

    #[test]
    fn invalid_page_hashes_mismatch() {
        let proto_msg = SyncMessage {
            payload: Some(Payload::PageHashes(SyncPageHashes {
                page_nos: vec![1, 2],
                hashes: vec![0u8; 32], // 1 hash for 2 pages
            })),
        };
        let err = Message::try_from(proto_msg).unwrap_err();
        assert!(matches!(err, SyncError::Codec(_)));
    }

    #[test]
    fn empty_payload_rejected() {
        let proto_msg = SyncMessage { payload: None };
        let err = Message::try_from(proto_msg).unwrap_err();
        assert!(matches!(err, SyncError::Protocol(_)));
    }
}
