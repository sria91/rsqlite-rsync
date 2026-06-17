//! Replica-side sync handler.
//!
//! The replica drives the protocol by:
//!
//! 1. Sending its current page hashes (coarse group hashes first).
//! 2. Sending fine-grained per-page hashes for groups the origin flagged.
//! 3. Receiving and applying changed pages sent by the origin.
//! 4. Waiting for [`Message::Done`].

use std::sync::Arc;
use tracing::{debug, info};

use crate::SyncTuning;
use crate::db::Connection;
use crate::error::Result;
use crate::hash::{GROUP_SIZE, HashAlgorithm, PageHash, hash_group_for, hash_page_for};
use crate::protocol::messages::{Message, PROTOCOL_VERSION};
use crate::transport::Transport;

async fn protocol_violation(
    transport: &mut dyn Transport,
    message: impl Into<String>,
) -> crate::error::SyncError {
    let message = message.into();
    let _ = transport
        .send(&Message::Error {
            message: message.clone(),
        })
        .await;
    crate::error::SyncError::Protocol(message)
}

/// Run the replica-side protocol handler.
///
/// `conn` is an open, read-write connection to the replica database.
/// `transport` is the communication channel to the origin endpoint.
///
/// On return the replica database contains a consistent snapshot of the origin
/// as it existed when the origin handler started.
pub async fn run(conn: &Connection, transport: &mut dyn Transport) -> Result<()> {
    let tuning = SyncTuning::from_env();
    run_with_tuning(conn, transport, &tuning).await
}

pub async fn run_with_tuning(
    conn: &Connection,
    transport: &mut dyn Transport,
    tuning: &SyncTuning,
) -> Result<()> {
    let offered_version = PROTOCOL_VERSION;
    let replica_page_size = conn.page_size();
    let replica_page_count = conn.page_count()?;

    // ── Handshake ────────────────────────────────────────────────────────────
    transport
        .send(&Message::Hello {
            version: offered_version,
            page_size: replica_page_size,
            page_count: replica_page_count,
        })
        .await?;

    let ack = transport.recv().await?;
    let (negotiated_version, origin_page_size, origin_page_count) = match ack {
        Message::HelloAck {
            version,
            page_size,
            page_count,
            ..
        } => (version, page_size, page_count),
        Message::Error { message } => return Err(crate::error::SyncError::Protocol(message)),
        other => {
            return Err(
                protocol_violation(transport, format!("expected HelloAck, got {other:?}")).await,
            );
        }
    };

    if negotiated_version == 0 || negotiated_version > offered_version {
        return Err(crate::error::SyncError::Protocol(format!(
            "invalid negotiated protocol version {negotiated_version}"
        )));
    }

    let hash_algorithm =
        HashAlgorithm::from_protocol_version(negotiated_version).ok_or_else(|| {
            crate::error::SyncError::Protocol(format!(
                "unsupported negotiated protocol version {negotiated_version}"
            ))
        })?;

    info!(
        "Origin: protocol v{negotiated_version}, page_size={origin_page_size}, page_count={origin_page_count}"
    );

    // ── Coarse pass ──────────────────────────────────────────────────────────
    // Compute page and group hashes from the replica's current content.
    let replica_bytes = if replica_page_count == 0 {
        Vec::new()
    } else {
        conn.serialize()?
    };
    let replica_page_size_usize = replica_page_size as usize;
    let replica_bytes_parallel = replica_bytes.clone();
    let all_page_hashes: Vec<PageHash> = crate::protocol::compute_with_parallelism(
        tuning,
        replica_page_count,
        move || {
            use rayon::prelude::*;
            (0..replica_page_count as usize)
                .into_par_iter()
                .map(|page_idx| {
                    let offset = page_idx * replica_page_size_usize;
                    hash_page_for(
                        &replica_bytes_parallel[offset..offset + replica_page_size_usize],
                        hash_algorithm,
                    )
                })
                .collect()
        },
        || {
            (0..replica_page_count as usize)
                .map(|page_idx| {
                    let offset = page_idx * replica_page_size_usize;
                    hash_page_for(
                        &replica_bytes[offset..offset + replica_page_size_usize],
                        hash_algorithm,
                    )
                })
                .collect()
        },
    )
    .await;
    let all_page_hashes = Arc::new(all_page_hashes);
    let num_groups = replica_page_count.div_ceil(GROUP_SIZE).max(1);
    let groups_per_chunk = tuning.hash_chunk_groups.max(1) as usize;
    let mut group_hashes: Vec<PageHash> = Vec::with_capacity(num_groups as usize);

    for chunk_start in (0..num_groups as usize).step_by(groups_per_chunk) {
        let chunk_end = (chunk_start + groups_per_chunk).min(num_groups as usize);
        let pages_in_chunk = ((chunk_end - chunk_start) as u32) * GROUP_SIZE;

        let chunk_hashes: Vec<PageHash> = crate::protocol::compute_with_parallelism(
            tuning,
            pages_in_chunk,
            {
                let all_page_hashes_parallel = Arc::clone(&all_page_hashes);
                move || {
                    use rayon::prelude::*;
                    (chunk_start..chunk_end)
                        .into_par_iter()
                        .map(|g| {
                            let first_page = g as u32 * GROUP_SIZE + 1;
                            let last_page = ((g as u32 + 1) * GROUP_SIZE).min(replica_page_count);

                            let start_idx = first_page as usize - 1;
                            let end_idx = last_page as usize;
                            let page_hashes = &all_page_hashes_parallel[start_idx..end_idx];
                            hash_group_for(page_hashes, hash_algorithm)
                        })
                        .collect()
                }
            },
            || {
                (chunk_start..chunk_end)
                    .map(|g| {
                        let first_page = g as u32 * GROUP_SIZE + 1;
                        let last_page = ((g as u32 + 1) * GROUP_SIZE).min(replica_page_count);

                        let start_idx = first_page as usize - 1;
                        let end_idx = last_page as usize;
                        let page_hashes = &all_page_hashes[start_idx..end_idx];
                        hash_group_for(page_hashes, hash_algorithm)
                    })
                    .collect()
            },
        )
        .await;

        group_hashes.extend(chunk_hashes);
    }

    transport
        .send(&Message::GroupHashes {
            first_group: 0,
            hashes: group_hashes,
        })
        .await?;

    let need_fine_msg = transport.recv().await?;
    let need_fine = match need_fine_msg {
        Message::GroupsNeedFine { group_indices } => group_indices,
        other => {
            return Err(protocol_violation(
                transport,
                format!("expected GroupsNeedFine, got {other:?}"),
            )
            .await);
        }
    };

    if need_fine.is_empty() {
        info!("All groups match — nothing to transfer");
        // Still expect a Done from origin.
        let done = transport.recv().await?;
        if !matches!(done, Message::Done) {
            return Err(
                protocol_violation(transport, "expected Done after empty GroupsNeedFine").await,
            );
        }
        return Ok(());
    }

    // ── Fine pass ────────────────────────────────────────────────────────────
    for group_idx in &need_fine {
        let first_page = group_idx * GROUP_SIZE + 1;
        let last_page = ((*group_idx + 1) * GROUP_SIZE).min(replica_page_count);

        let mut page_nos: Vec<u32> = Vec::with_capacity(GROUP_SIZE as usize);
        let mut hashes: Vec<PageHash> = Vec::with_capacity(GROUP_SIZE as usize);

        if first_page <= replica_page_count {
            for p in first_page..=last_page {
                page_nos.push(p);
                hashes.push(all_page_hashes[p as usize - 1]);
            }
        }
        // If the group is entirely beyond the replica's current page count,
        // send an empty PageHashes so origin knows to send all those pages.

        transport
            .send(&Message::PageHashes {
                page_nos: page_nos.into(),
                hashes: hashes.into(),
            })
            .await?;

        // Receive pages from origin and apply them.
        let msg = transport.recv().await?;
        match msg {
            Message::SendPages { pages } => {
                let mut ack_page_nos: Vec<u32> = Vec::with_capacity(pages.len());
                for page in &pages {
                    debug!("Writing page {}", page.page_no);
                    conn.write_page(page.page_no, &page.data)?;
                    ack_page_nos.push(page.page_no);
                }
                transport
                    .send(&Message::PagesAck {
                        page_nos: ack_page_nos,
                    })
                    .await?;
            }
            Message::Done => {
                info!("Origin sent Done early — sync complete");
                return Ok(());
            }
            other => {
                return Err(protocol_violation(
                    transport,
                    format!("expected SendPages or Done, got {other:?}"),
                )
                .await);
            }
        }
    }

    // ── Wait for Done ────────────────────────────────────────────────────────
    let done = transport.recv().await?;
    if !matches!(done, Message::Done) {
        return Err(protocol_violation(transport, "expected Done after fine pass").await);
    }

    info!("Sync complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::path::Path;

    use async_trait::async_trait;
    use libsqlite3_sys as ffi;
    use tempfile::NamedTempFile;

    use super::*;

    struct MockTransport {
        recv_queue: VecDeque<Message>,
        sent: Vec<Message>,
    }

    impl MockTransport {
        fn new(messages: Vec<Message>) -> Self {
            Self {
                recv_queue: messages.into(),
                sent: Vec::new(),
            }
        }
    }

    #[async_trait]
    impl Transport for MockTransport {
        async fn send(&mut self, msg: &Message) -> Result<()> {
            self.sent.push(msg.clone());
            Ok(())
        }

        async fn recv(&mut self) -> Result<Message> {
            self.recv_queue.pop_front().ok_or_else(|| {
                crate::error::SyncError::Protocol("mock transport exhausted input".into())
            })
        }
    }

    fn open_rw(path: &Path) -> Connection {
        Connection::open(path, ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE)
            .expect("open rw")
    }

    #[tokio::test]
    async fn handshake_rejects_zero_negotiated_version() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());

        let mut transport = MockTransport::new(vec![Message::HelloAck {
            version: 0,
            page_size: conn.page_size(),
            page_count: conn.page_count().unwrap_or(0),
        }]);

        let result = run(&conn, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("invalid negotiated protocol version")
        ));
        assert!(matches!(
            transport.sent.first(),
            Some(Message::Hello { version, .. }) if *version == PROTOCOL_VERSION
        ));
    }

    #[tokio::test]
    async fn handshake_rejects_future_negotiated_version() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());

        let mut transport = MockTransport::new(vec![Message::HelloAck {
            version: PROTOCOL_VERSION + 1,
            page_size: conn.page_size(),
            page_count: conn.page_count().unwrap_or(0),
        }]);

        let result = run(&conn, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("invalid negotiated protocol version")
        ));
    }
}
