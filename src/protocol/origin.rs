//! Origin-side sync handler.
//!
//! The origin holds the authoritative copy of the database.  Its job in the
//! protocol is to:
//!
//! 1. Receive group hashes from the replica and identify which groups differ.
//! 2. Receive per-page hashes for differing groups and identify which
//!    individual pages differ.
//! 3. Send the raw bytes of changed pages.
//! 4. Send [`Message::Done`] when finished.

use std::collections::HashSet;
use std::sync::Arc;

use tracing::{debug, info};

use crate::SyncTuning;
use crate::error::Result;
use crate::hash::{GROUP_SIZE, HashAlgorithm, PageHash, hash_group_for, hash_page_for};
use crate::protocol::messages::{Message, PROTOCOL_VERSION, PageData};
use crate::snapshot::Snapshot;
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

fn validate_page_hashes_for_group(
    page_nos: &[u32],
    page_hashes: &[PageHash],
    group_idx: u32,
) -> Result<()> {
    if page_nos.len() != page_hashes.len() {
        return Err(crate::error::SyncError::Protocol(format!(
            "PageHashes has mismatched lengths: page_nos={}, hashes={}",
            page_nos.len(),
            page_hashes.len()
        )));
    }

    let first_page = group_idx * GROUP_SIZE + 1;
    let group_last_page = (group_idx + 1) * GROUP_SIZE;
    let mut seen = HashSet::with_capacity(page_nos.len());
    let mut prev: Option<u32> = None;

    for page_no in page_nos {
        if *page_no == 0 {
            return Err(crate::error::SyncError::Protocol(
                "PageHashes includes invalid page number 0".into(),
            ));
        }
        if *page_no < first_page || *page_no > group_last_page {
            return Err(crate::error::SyncError::Protocol(format!(
                "PageHashes page {page_no} is outside group {group_idx} range [{first_page}, {group_last_page}]"
            )));
        }
        if !seen.insert(*page_no) {
            return Err(crate::error::SyncError::Protocol(format!(
                "PageHashes contains duplicate page number {page_no}"
            )));
        }
        if let Some(prev_page) = prev
            && *page_no <= prev_page
        {
            return Err(crate::error::SyncError::Protocol(format!(
                "PageHashes page numbers must be strictly increasing (got {prev_page} then {page_no})"
            )));
        }
        prev = Some(*page_no);
    }

    for (idx, page_no) in page_nos.iter().enumerate() {
        let expected_page = first_page + idx as u32;
        if *page_no != expected_page {
            return Err(crate::error::SyncError::Protocol(format!(
                "PageHashes page numbers must be a contiguous prefix for group {group_idx} (expected page {expected_page}, got {page_no})"
            )));
        }
    }

    Ok(())
}

fn validate_pages_ack(expected_page_nos: &[u32], ack_page_nos: &[u32]) -> Result<()> {
    let expected: HashSet<u32> = expected_page_nos.iter().copied().collect();
    let actual: HashSet<u32> = ack_page_nos.iter().copied().collect();

    if ack_page_nos.len() != actual.len() {
        return Err(crate::error::SyncError::Protocol(
            "PagesAck contains duplicate page numbers".into(),
        ));
    }

    if expected != actual {
        return Err(crate::error::SyncError::Protocol(format!(
            "PagesAck does not match sent pages: expected {:?}, got {:?}",
            expected, actual
        )));
    }

    Ok(())
}

fn origin_group_hash_for_pages(
    snap_bytes: &[u8],
    snap_page_size: usize,
    page_range: std::ops::RangeInclusive<u32>,
    hash_algorithm: HashAlgorithm,
) -> PageHash {
    let page_hashes: Vec<PageHash> = page_range
        .map(|p| {
            let offset = (p as usize - 1) * snap_page_size;
            hash_page_for(&snap_bytes[offset..offset + snap_page_size], hash_algorithm)
        })
        .collect();
    hash_group_for(&page_hashes, hash_algorithm)
}

fn changed_page_for(
    page_no: u32,
    replica_hash: PageHash,
    snap_bytes: &[u8],
    snap_page_size: usize,
    hash_algorithm: HashAlgorithm,
) -> Option<PageData> {
    let offset = (page_no as usize - 1) * snap_page_size;
    let data_slice = &snap_bytes[offset..offset + snap_page_size];
    let origin_hash = hash_page_for(data_slice, hash_algorithm);

    if origin_hash != replica_hash {
        debug!("Page {page_no} differs — queuing transfer");
        Some(PageData {
            page_no,
            data: data_slice.to_vec(),
        })
    } else {
        None
    }
}

/// Run the origin-side protocol handler.
///
/// `snap` is a consistent read snapshot of the origin database.
/// `transport` is the communication channel to the replica endpoint.
///
/// # Protocol flow
///
/// ```text
/// recv Hello  →  send HelloAck
/// loop {
///   recv GroupHashes  →  compute diff  →  send GroupsNeedFine
///   recv PageHashes   →  compute diff  →  send SendPages
///   recv PagesAck
/// }
/// send Done
/// ```
pub async fn run(snap: &Snapshot<'_>, transport: &mut dyn Transport) -> Result<()> {
    let tuning = SyncTuning::from_env();
    run_with_tuning(snap, transport, &tuning).await
}

pub async fn run_with_tuning(
    snap: &Snapshot<'_>,
    transport: &mut dyn Transport,
    tuning: &SyncTuning,
) -> Result<()> {
    // ── Handshake ────────────────────────────────────────────────────────────
    let hello = transport.recv().await?;
    let (replica_version, replica_page_size, _replica_page_count) = match hello {
        Message::Hello {
            version,
            page_size,
            page_count,
        } => {
            info!(
                "Replica connected: protocol v{version}, page_size={page_size}, \
                 page_count={page_count}"
            );
            (version, page_size, page_count)
        }
        other => {
            return Err(
                protocol_violation(transport, format!("expected Hello, got {other:?}")).await,
            );
        }
    };

    if replica_version == 0 {
        return Err(protocol_violation(
            transport,
            "replica offered unsupported protocol version 0",
        )
        .await);
    }

    let negotiated_version = replica_version.min(PROTOCOL_VERSION);
    let hash_algorithm =
        HashAlgorithm::from_protocol_version(negotiated_version).ok_or_else(|| {
            crate::error::SyncError::Protocol(format!(
                "unsupported negotiated protocol version {negotiated_version}"
            ))
        })?;

    let origin_page_size = snap.page_size();
    if origin_page_size != replica_page_size && replica_page_size != 0 {
        transport
            .send(&Message::Error {
                message: format!(
                    "page size mismatch: origin={origin_page_size}, replica={replica_page_size}"
                ),
            })
            .await?;
        return Err(crate::error::SyncError::PageSizeMismatch {
            origin: origin_page_size,
            replica: replica_page_size,
        });
    }

    let page_count = snap.page_count();
    transport
        .send(&Message::HelloAck {
            version: negotiated_version,
            page_size: origin_page_size,
            page_count,
        })
        .await?;

    // ── Coarse pass ──────────────────────────────────────────────────────────
    let msg = transport.recv().await?;
    let (first_group, replica_group_hashes) = match msg {
        Message::GroupHashes {
            first_group,
            hashes,
        } => (first_group, hashes),
        other => {
            return Err(protocol_violation(
                transport,
                format!("expected GroupHashes, got {other:?}"),
            )
            .await);
        }
    };

    if first_group != 0 {
        return Err(
            protocol_violation(
                transport,
                format!(
                    "unsupported GroupHashes.first_group={first_group}; this protocol version requires first_group=0"
                ),
            )
            .await,
        );
    }

    // Compute group hashes on the origin side.
    // Snapshot bytes are in-memory; hash comparisons are embarrassingly parallel.
    let snap_bytes = Arc::new(snap.all_bytes().to_vec());
    let snap_page_size = snap.page_size() as usize;

    let groups_per_chunk = tuning.hash_chunk_groups.max(1) as usize;
    let mut need_fine: Vec<u32> = Vec::new();

    for chunk_start in (0..replica_group_hashes.len()).step_by(groups_per_chunk) {
        let chunk_end = (chunk_start + groups_per_chunk).min(replica_group_hashes.len());
        let replica_hashes_chunk = &replica_group_hashes[chunk_start..chunk_end];
        let replica_hashes_chunk_parallel = replica_hashes_chunk.to_vec();
        let snap_bytes_parallel = Arc::clone(&snap_bytes);

        let pages_in_chunk = ((chunk_end - chunk_start) as u32) * GROUP_SIZE;

        let chunk_need_fine: Vec<u32> = crate::protocol::compute_with_parallelism(
            tuning,
            pages_in_chunk,
            move || {
                use rayon::prelude::*;
                replica_hashes_chunk_parallel
                    .par_iter()
                    .enumerate()
                    .filter_map(|(i, replica_group_hash)| {
                        let group_idx = first_group + (chunk_start + i) as u32;
                        let first_page = group_idx * GROUP_SIZE + 1; // 1-indexed
                        let last_page = ((group_idx + 1) * GROUP_SIZE).min(page_count);

                        if first_page > page_count {
                            // The replica may have sent hashes for pages that no longer exist.
                            return Some(group_idx);
                        }

                        let origin_group_hash = origin_group_hash_for_pages(
                            snap_bytes_parallel.as_ref(),
                            snap_page_size,
                            first_page..=last_page,
                            hash_algorithm,
                        );

                        if origin_group_hash != *replica_group_hash {
                            debug!("Group {group_idx} differs");
                            Some(group_idx)
                        } else {
                            None
                        }
                    })
                    .collect()
            },
            || {
                replica_hashes_chunk
                    .iter()
                    .enumerate()
                    .filter_map(|(i, replica_group_hash)| {
                        let group_idx = first_group + (chunk_start + i) as u32;
                        let first_page = group_idx * GROUP_SIZE + 1; // 1-indexed
                        let last_page = ((group_idx + 1) * GROUP_SIZE).min(page_count);

                        if first_page > page_count {
                            return Some(group_idx);
                        }

                        let origin_group_hash = origin_group_hash_for_pages(
                            snap_bytes.as_ref(),
                            snap_page_size,
                            first_page..=last_page,
                            hash_algorithm,
                        );

                        if origin_group_hash != *replica_group_hash {
                            debug!("Group {group_idx} differs");
                            Some(group_idx)
                        } else {
                            None
                        }
                    })
                    .collect()
            },
        )
        .await;

        need_fine.extend(chunk_need_fine);
    }

    // Also add groups that exist only on the origin.
    let max_replica_group = first_group + replica_group_hashes.len() as u32;
    let max_origin_group = page_count.div_ceil(GROUP_SIZE);
    for g in max_replica_group..max_origin_group {
        need_fine.push(g);
    }

    transport
        .send(&Message::GroupsNeedFine {
            group_indices: need_fine.clone(),
        })
        .await?;

    // ── Fine pass ────────────────────────────────────────────────────────────
    for group_idx in &need_fine {
        let msg = transport.recv().await?;
        let (replica_page_nos, replica_page_hashes) = match msg {
            Message::PageHashes { page_nos, hashes } => (page_nos, hashes),
            Message::Done => {
                return Err(protocol_violation(
                    transport,
                    "received Done before all requested fine-pass groups were processed",
                )
                .await);
            }
            other => {
                return Err(protocol_violation(
                    transport,
                    format!("expected PageHashes, got {other:?}"),
                )
                .await);
            }
        };

        let first_page = group_idx * GROUP_SIZE + 1;
        let last_page = ((*group_idx + 1) * GROUP_SIZE).min(page_count);

        if let Err(err) =
            validate_page_hashes_for_group(&replica_page_nos, &replica_page_hashes, *group_idx)
        {
            return match err {
                crate::error::SyncError::Protocol(message) => {
                    Err(protocol_violation(transport, message).await)
                }
                other => Err(other),
            };
        }

        // Parallel per-page hash comparison within the group.
        let mut changed_pages: Vec<PageData> = crate::protocol::compute_with_parallelism(
            tuning,
            replica_page_nos.len() as u32,
            {
                let replica_page_nos_parallel = replica_page_nos.clone();
                let replica_page_hashes_parallel = replica_page_hashes.clone();
                let snap_bytes_parallel = Arc::clone(&snap_bytes);
                move || {
                    use rayon::prelude::*;
                    (0..replica_page_nos_parallel.len())
                        .into_par_iter()
                        .filter_map(|i| {
                            let page_no = replica_page_nos_parallel[i];
                            let replica_hash = replica_page_hashes_parallel[i];
                            changed_page_for(
                                page_no,
                                replica_hash,
                                snap_bytes_parallel.as_ref(),
                                snap_page_size,
                                hash_algorithm,
                            )
                        })
                        .collect()
                }
            },
            || {
                (0..replica_page_nos.len())
                    .filter_map(|i| {
                        let page_no = replica_page_nos[i];
                        let replica_hash = replica_page_hashes[i];
                        changed_page_for(
                            page_no,
                            replica_hash,
                            snap_bytes.as_ref(),
                            snap_page_size,
                            hash_algorithm,
                        )
                    })
                    .collect()
            },
        )
        .await;

        // Also send pages that exist only on the origin within this group.
        let next_missing_page = replica_page_nos
            .iter()
            .copied()
            .max()
            .map(|page_no| page_no + 1)
            .unwrap_or(first_page);
        for p in next_missing_page..=last_page {
            let offset = (p as usize - 1) * snap_page_size;
            let data = snap_bytes.as_ref()[offset..offset + snap_page_size].to_vec();
            changed_pages.push(PageData { page_no: p, data });
        }

        let expected_ack_page_nos: Vec<u32> = changed_pages.iter().map(|p| p.page_no).collect();

        transport
            .send(&Message::SendPages {
                pages: changed_pages,
            })
            .await?;

        // Wait for the replica's acknowledgement.
        let ack = transport.recv().await?;
        match ack {
            Message::PagesAck { page_nos } => {
                if let Err(err) = validate_pages_ack(&expected_ack_page_nos, &page_nos) {
                    return match err {
                        crate::error::SyncError::Protocol(message) => {
                            Err(protocol_violation(transport, message).await)
                        }
                        other => Err(other),
                    };
                }
            }
            other => {
                return Err(protocol_violation(
                    transport,
                    format!("expected PagesAck, got {other:?}"),
                )
                .await);
            }
        }
    }

    info!("Sync complete — sending Done");
    transport.send(&Message::Done).await?;
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
    use crate::db::Connection;
    use crate::hash::hash_page;

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

    fn seed_to_page_count(conn: &Connection, target_pages: u32) {
        conn.exec("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
            .unwrap();

        let payload = "x".repeat(3000);
        let mut row_id = 0;
        while conn.page_count().unwrap() < target_pages {
            conn.exec(&format!("INSERT INTO t VALUES ({row_id}, '{payload}')"))
                .unwrap();
            row_id += 1;
        }
    }

    #[tokio::test]
    async fn fine_pass_sends_empty_batch_and_no_duplicate_suffix_pages() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 90);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let group0_pages: Vec<u32> = (1..=64).collect();
        let group0_hashes: Vec<PageHash> = group0_pages
            .iter()
            .map(|page_no| hash_page(&snap.read_page(*page_no).unwrap()))
            .collect();

        let group1_pages: Vec<u32> = (65..=70).collect();
        let group1_hashes: Vec<PageHash> = group1_pages
            .iter()
            .map(|page_no| hash_page(&snap.read_page(*page_no).unwrap()))
            .collect();

        let mut transport = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 70,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[0u8; 32], [1u8; 32]],
            },
            Message::PageHashes {
                page_nos: group0_pages.into(),
                hashes: group0_hashes.into(),
            },
            Message::PagesAck { page_nos: vec![] },
            Message::PageHashes {
                page_nos: group1_pages.into(),
                hashes: group1_hashes.into(),
            },
            Message::PagesAck {
                page_nos: (71..=90).collect(),
            },
        ]);

        run(&snap, &mut transport).await.unwrap();

        assert!(matches!(
            &transport.sent[2],
            Message::SendPages { pages } if pages.is_empty()
        ));

        let sent_page_nos: Vec<u32> = transport
            .sent
            .iter()
            .filter_map(|msg| match msg {
                Message::SendPages { pages } => {
                    Some(pages.iter().map(|page| page.page_no).collect::<Vec<_>>())
                }
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(sent_page_nos, (71..=90).collect::<Vec<_>>());

        snap.commit().unwrap();
    }

    #[tokio::test]
    async fn fine_pass_rejects_mismatched_pagehash_lengths() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 8);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let mut transport = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![1, 2].into(),
                hashes: vec![[0u8; 32]].into(),
            },
        ]);

        let result = run(&snap, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("mismatched lengths")
        ));
    }

    #[tokio::test]
    async fn fine_pass_rejects_page_zero() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 8);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let mut transport = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![0].into(),
                hashes: vec![[0u8; 32]].into(),
            },
        ]);

        let result = run(&snap, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("invalid page number 0")
        ));
    }

    #[tokio::test]
    async fn fine_pass_rejects_out_of_group_and_duplicate_pages() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 8);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let mut out_of_group = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![65].into(),
                hashes: vec![[0u8; 32]].into(),
            },
        ]);

        let out_of_group_result = run(&snap, &mut out_of_group).await;
        assert!(matches!(
            out_of_group_result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("outside group")
        ));

        let mut duplicate = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![1, 1].into(),
                hashes: vec![[0u8; 32], [0u8; 32]].into(),
            },
        ]);

        let duplicate_result = run(&snap, &mut duplicate).await;
        assert!(matches!(
            duplicate_result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("duplicate page number")
        ));

        let mut non_monotonic = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![2, 1].into(),
                hashes: vec![[0u8; 32], [0u8; 32]].into(),
            },
        ]);

        let non_monotonic_result = run(&snap, &mut non_monotonic).await;
        assert!(matches!(
            non_monotonic_result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("strictly increasing")
        ));

        let mut non_contiguous = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![1, 3].into(),
                hashes: vec![[0u8; 32], [0u8; 32]].into(),
            },
        ]);

        let non_contiguous_result = run(&snap, &mut non_contiguous).await;
        assert!(matches!(
            non_contiguous_result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("contiguous prefix")
        ));
    }

    #[tokio::test]
    async fn fine_pass_rejects_mismatched_pages_ack() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 8);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let mut transport = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 0,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![].into(),
                hashes: vec![].into(),
            },
            Message::PagesAck { page_nos: vec![] },
        ]);

        let result = run(&snap, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("PagesAck does not match sent pages")
        ));
    }

    #[tokio::test]
    async fn fine_pass_rejects_duplicate_pages_ack() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 8);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let mut transport = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 0,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::PageHashes {
                page_nos: vec![].into(),
                hashes: vec![].into(),
            },
            Message::PagesAck {
                page_nos: vec![1, 1],
            },
        ]);

        let result = run(&snap, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("duplicate page numbers")
        ));
    }

    #[tokio::test]
    async fn coarse_pass_rejects_non_zero_first_group() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 8);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let mut transport = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 1,
                hashes: vec![[1u8; 32]],
            },
        ]);

        let result = run(&snap, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("first_group=0")
        ));
    }

    #[tokio::test]
    async fn fine_pass_rejects_early_done_before_all_groups() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 8);

        let snap = Snapshot::begin(&conn).unwrap();
        let page_size = snap.page_size();

        let mut transport = MockTransport::new(vec![
            Message::Hello {
                version: PROTOCOL_VERSION,
                page_size,
                page_count: 8,
            },
            Message::GroupHashes {
                first_group: 0,
                hashes: vec![[1u8; 32]],
            },
            Message::Done,
        ]);

        let result = run(&snap, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("before all requested fine-pass groups")
        ));
    }

    #[tokio::test]
    async fn handshake_rejects_zero_protocol_version() {
        let file = NamedTempFile::new().unwrap();
        let conn = open_rw(file.path());
        seed_to_page_count(&conn, 2);

        let snap = Snapshot::begin(&conn).unwrap();
        let mut transport = MockTransport::new(vec![Message::Hello {
            version: 0,
            page_size: snap.page_size(),
            page_count: 2,
        }]);

        let result = run(&snap, &mut transport).await;
        assert!(matches!(
            result,
            Err(crate::error::SyncError::Protocol(message))
                if message.contains("unsupported protocol version 0")
        ));
        assert!(matches!(
            transport.sent.first(),
            Some(Message::Error { message }) if message.contains("unsupported protocol version 0")
        ));
    }
}
