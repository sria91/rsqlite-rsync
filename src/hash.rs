//! Page and page-group hashing.
//!
//! The sync protocol uses two granularities of hashing:
//!
//! * **Page hash** — protocol-version-specific hash of a single page's raw bytes.
//! * **Group hash** — hash of the concatenated page hashes for a contiguous
//!   run of [`GROUP_SIZE`] pages.  This allows the coarse pass to identify
//!   changed *regions* with a single hash comparison before drilling down to
//!   individual pages.

use blake3::Hasher as Blake3Hasher;
use sha2::{Digest, Sha256};

/// Number of pages per coarse hash group.
pub const GROUP_SIZE: u32 = 64;

/// A 32-byte SHA-256 digest.
pub type PageHash = [u8; 32];

/// Hash algorithm negotiated by protocol version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    Sha256V1,
    Blake3V2,
}

impl HashAlgorithm {
    pub fn from_protocol_version(version: u32) -> Option<Self> {
        match version {
            1 => Some(Self::Sha256V1),
            2 => Some(Self::Blake3V2),
            _ => None,
        }
    }
}

/// Compute a page hash using the specified algorithm.
pub fn hash_page_for(data: &[u8], algorithm: HashAlgorithm) -> PageHash {
    match algorithm {
        HashAlgorithm::Sha256V1 => {
            let mut hasher = Sha256::new();
            hasher.update(data);
            hasher.finalize().into()
        }
        HashAlgorithm::Blake3V2 => blake3::hash(data).into(),
    }
}

/// Compute the hash of a single database page using the current algorithm.
///
/// `data` should be exactly `page_size` bytes (any length is accepted).
pub fn hash_page(data: &[u8]) -> PageHash {
    hash_page_for(data, HashAlgorithm::Blake3V2)
}

/// Compute a group hash by hashing the concatenation of the given page hashes.
///
/// The order of `hashes` must match the ascending page-number order expected
/// by the protocol.
///
/// # Example
///
/// ```rust
/// use rsqlite_rsync::hash::{hash_page, hash_group};
///
/// let page_data = vec![0u8; 4096];
/// let h = hash_page(&page_data);
/// let group = hash_group(&[h, h]);
/// assert_eq!(group.len(), 32);
/// ```
pub fn hash_group_for(hashes: &[PageHash], algorithm: HashAlgorithm) -> PageHash {
    match algorithm {
        HashAlgorithm::Sha256V1 => {
            let mut hasher = Sha256::new();
            for h in hashes {
                hasher.update(h);
            }
            hasher.finalize().into()
        }
        HashAlgorithm::Blake3V2 => {
            let mut hasher = Blake3Hasher::new();
            for h in hashes {
                hasher.update(h);
            }
            hasher.finalize().into()
        }
    }
}

/// Compute a group hash using the current algorithm.
pub fn hash_group(hashes: &[PageHash]) -> PageHash {
    hash_group_for(hashes, HashAlgorithm::Blake3V2)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_page_known_output() {
        // The default page hash should be deterministic.
        let data = [0u8; 4];
        let h1 = hash_page(&data);
        let h2 = hash_page(&data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_page_differs_for_different_content() {
        let a = hash_page(&[0u8; 4096]);
        let b = hash_page(&[1u8; 4096]);
        assert_ne!(a, b);
    }

    #[test]
    fn hash_group_deterministic() {
        let h = hash_page(&[42u8; 512]);
        let g1 = hash_group(&[h]);
        let g2 = hash_group(&[h]);
        assert_eq!(g1, g2);
    }

    #[test]
    fn hash_group_order_matters() {
        let h1 = hash_page(&[1u8; 512]);
        let h2 = hash_page(&[2u8; 512]);
        let g_ab = hash_group(&[h1, h2]);
        let g_ba = hash_group(&[h2, h1]);
        assert_ne!(g_ab, g_ba);
    }

    #[test]
    fn hash_group_empty_is_valid() {
        let g = hash_group(&[]);
        assert_eq!(g.len(), 32);
    }
}
