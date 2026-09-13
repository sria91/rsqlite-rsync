//! Minimal `key=value` text format shared by the small on-disk records (HA
//! lease file, freshness ledger file) that are simple enough not to warrant
//! a full serialization format, but still need consistent parsing and
//! consistent "file missing / empty / explicitly `none`" handling.
//!
//! This exists so that behavior — and bugs — in one record format don't
//! silently diverge from the other; both [`crate::ha::parse_lease_record`]
//! and [`crate::ha::parse_freshness_ledger`] are built on top of it.

use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::path::Path;

/// Parse `key=value` lines (blank lines ignored) into an ordered map.
///
/// Each non-blank line must contain exactly one `=`, and its key must be one
/// of `allowed_keys` — anything else is a format error naming the offending
/// line or key. `subject` (e.g. `"lease"`, `"freshness"`) is folded into the
/// error text so callers don't need to re-wrap it.
pub fn parse_kv_lines(
    subject: &str,
    input: &str,
    allowed_keys: &[&str],
) -> Result<BTreeMap<String, String>, String> {
    let mut map = BTreeMap::new();

    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        let Some((key, value)) = line.split_once('=') else {
            return Err(format!(
                "invalid {subject} line (expected key=value): {line}"
            ));
        };

        let key = key.trim();
        if !allowed_keys.contains(&key) {
            return Err(format!("unknown {subject} key: {key}"));
        }

        map.insert(key.to_owned(), value.trim().to_owned());
    }

    Ok(map)
}

/// Look up a required `u64` field by key.
pub fn require_u64(map: &BTreeMap<String, String>, key: &str) -> Result<u64, String> {
    let raw = map.get(key).ok_or_else(|| format!("missing {key}"))?;
    raw.parse::<u64>()
        .map_err(|_| format!("invalid {key}: {raw}"))
}

/// Look up a required, non-empty string field by key.
pub fn require_non_empty(map: &BTreeMap<String, String>, key: &str) -> Result<String, String> {
    let raw = map.get(key).ok_or_else(|| format!("missing {key}"))?;
    if raw.is_empty() {
        return Err(format!("{key} must not be empty"));
    }
    Ok(raw.clone())
}

/// Read a `key=value` file, treating a missing file, empty content, and the
/// literal text `none` as all meaning "no record" — the convention shared by
/// the lease file and the freshness ledger file — rather than an error.
///
/// Returns the trimmed file content when a record is present.
pub fn read_optional_kv_text(path: &Path) -> std::io::Result<Option<String>> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };

    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("none") {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_kv_lines_accepts_known_keys() {
        let map = parse_kv_lines("test", "a=1\nb=2\n", &["a", "b"]).unwrap();
        assert_eq!(map.get("a").map(String::as_str), Some("1"));
        assert_eq!(map.get("b").map(String::as_str), Some("2"));
    }

    #[test]
    fn parse_kv_lines_skips_blank_lines() {
        let map = parse_kv_lines("test", "a=1\n\n  \nb=2\n", &["a", "b"]).unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn parse_kv_lines_rejects_missing_equals() {
        let err = parse_kv_lines("test", "not-a-pair\n", &["a"]).unwrap_err();
        assert!(err.contains("invalid test line"));
    }

    #[test]
    fn parse_kv_lines_rejects_unknown_key() {
        let err = parse_kv_lines("test", "c=1\n", &["a", "b"]).unwrap_err();
        assert!(err.contains("unknown test key: c"));
    }

    #[test]
    fn require_u64_reports_missing_and_invalid() {
        let map = parse_kv_lines("test", "a=xyz\n", &["a"]).unwrap();
        assert!(require_u64(&map, "a").unwrap_err().contains("invalid a"));
        assert!(require_u64(&map, "b").unwrap_err().contains("missing b"));
    }

    #[test]
    fn require_non_empty_rejects_blank_value() {
        let map = parse_kv_lines("test", "a=\n", &["a"]).unwrap();
        assert!(
            require_non_empty(&map, "a")
                .unwrap_err()
                .contains("must not be empty")
        );
    }

    #[test]
    fn read_optional_kv_text_treats_missing_empty_and_none_as_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("record.txt");

        assert_eq!(read_optional_kv_text(&path).unwrap(), None);

        std::fs::write(&path, "\n").unwrap();
        assert_eq!(read_optional_kv_text(&path).unwrap(), None);

        std::fs::write(&path, "none\n").unwrap();
        assert_eq!(read_optional_kv_text(&path).unwrap(), None);

        std::fs::write(&path, "  a=1  \n").unwrap();
        assert_eq!(
            read_optional_kv_text(&path).unwrap(),
            Some("a=1".to_string())
        );
    }
}
