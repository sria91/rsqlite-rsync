#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -n "${RSQLITE_RSYNC_MANIFEST_PATHS:-}" ]]; then
  IFS=',' read -r -a manifests <<<"${RSQLITE_RSYNC_MANIFEST_PATHS}"
else
  manifests=(
    "$repo_root/examples/k8s/ha-deployment.yaml"
    "$repo_root/examples/k8s/ha-deployment-readonly.yaml"
  )
fi

if [[ "${#manifests[@]}" -lt 1 ]]; then
  echo "No manifest paths configured for validation" >&2
  exit 1
fi

# These identities are intentionally shared because the manifests are profile
# variants for the same workload.
if [[ -n "${RSQLITE_RSYNC_ALLOW_CROSS_FILE_DUPLICATES:-}" ]]; then
  IFS=',' read -r -a allow_cross_file_duplicates <<<"${RSQLITE_RSYNC_ALLOW_CROSS_FILE_DUPLICATES}"
else
  allow_cross_file_duplicates=(
    "Lease/sqlite-writer-lease"
    "ServiceAccount/sqlite-ha"
    "StatefulSet/sqlite-ha"
  )
fi

extract_object_ids() {
  local manifest="$1"
  awk '
    /^kind:[[:space:]]*/ {
      kind=$2
    }
    /^metadata:[[:space:]]*$/ {
      in_metadata=1
      next
    }
    /^---[[:space:]]*$/ {
      kind=""
      in_metadata=0
      next
    }
    in_metadata == 1 && /^[[:space:]]*name:[[:space:]]*/ {
      name=$2
      gsub(/"/, "", name)
      if (kind != "") {
        print kind "/" name
      }
      in_metadata=0
    }
  ' "$manifest"
}

is_allowlisted_duplicate() {
  local id="$1"
  local allowed
  for allowed in "${allow_cross_file_duplicates[@]}"; do
    if [[ "$id" == "$allowed" ]]; then
      return 0
    fi
  done
  return 1
}

for manifest in "${manifests[@]}"; do
  if [[ ! -f "$manifest" ]]; then
    echo "Missing required manifest: $manifest" >&2
    exit 1
  fi
done

all_object_ids=()
for manifest in "${manifests[@]}"; do
  object_ids=()
  while IFS= read -r object_id; do
    object_ids+=("$object_id")
  done < <(extract_object_ids "$manifest")

  if [[ "${#object_ids[@]}" -eq 0 ]]; then
    echo "No Kubernetes objects found in manifest: $manifest" >&2
    exit 1
  fi

  duplicate_ids=()
  while IFS= read -r duplicate_id; do
    duplicate_ids+=("$duplicate_id")
  done < <(printf "%s\n" "${object_ids[@]}" | sort | uniq -d)
  if [[ "${#duplicate_ids[@]}" -gt 0 ]]; then
    echo "Duplicate Kubernetes object identities found in a single manifest: $manifest" >&2
    printf '%s\n' "${duplicate_ids[@]}" >&2
    exit 1
  fi

  all_object_ids+=("${object_ids[@]}")
done

cross_file_duplicates=()
while IFS= read -r cross_file_duplicate; do
  cross_file_duplicates+=("$cross_file_duplicate")
done < <(printf "%s\n" "${all_object_ids[@]}" | sort | uniq -d)
if [[ "${#cross_file_duplicates[@]}" -gt 0 ]]; then
  unexpected_duplicates=()
  for object_id in "${cross_file_duplicates[@]}"; do
    if ! is_allowlisted_duplicate "$object_id"; then
      unexpected_duplicates+=("$object_id")
    fi
  done

  if [[ "${#unexpected_duplicates[@]}" -gt 0 ]]; then
    echo "Unexpected duplicate Kubernetes object identities across manifests:" >&2
    printf '%s\n' "${unexpected_duplicates[@]}" >&2
    exit 1
  fi
fi

if [[ "${RSQLITE_RSYNC_DISABLE_KUBECONFORM:-0}" != "1" ]] && command -v kubeconform >/dev/null 2>&1; then
  echo "Validating manifests with kubeconform"
  kubeconform -strict "${manifests[@]}"
  exit 0
fi

if [[ "${RSQLITE_RSYNC_DISABLE_KUBECTL:-0}" != "1" ]] && command -v kubectl >/dev/null 2>&1 && kubectl version --request-timeout=2s >/dev/null 2>&1; then
  echo "kubeconform not found; using kubectl server-side dry-run fallback"
  for manifest in "${manifests[@]}"; do
    echo "Validating $manifest"
    kubectl apply --dry-run=server -f "$manifest" >/dev/null
    echo "OK: $manifest"
  done
  exit 0
fi

echo "Skipping Kubernetes manifest validation: kubeconform not installed and no reachable Kubernetes API server" >&2
exit 0
