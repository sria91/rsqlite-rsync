#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$repo_root/examples/k8s/k3s-ha-stack.yaml"

if [[ ! -f "$template" ]]; then
  echo "Template manifest not found: $template" >&2
  exit 1
fi

: "${RSQLITE_RSYNC_IMAGE:?Set RSQLITE_RSYNC_IMAGE to a pullable image (for example ghcr.io/ORG/rsqlite-rsync:TAG)}"
: "${RSQLITE_RSYNC_REPLICA_SYNC_COMMAND:?Set RSQLITE_RSYNC_REPLICA_SYNC_COMMAND to your replica pull-sync command}"

RSQLITE_RSYNC_NAMESPACE="${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}"
RSQLITE_RSYNC_HOST_DATA_DIR="${RSQLITE_RSYNC_HOST_DATA_DIR:-/var/lib/rsqlite-rsync-ha}"

rendered="$(mktemp)"
trap 'rm -f "$rendered"' EXIT

escape_sed_replacement() {
  printf '%s' "$1" | sed -e 's/[&/]/\\&/g'
}

image_escaped="$(escape_sed_replacement "$RSQLITE_RSYNC_IMAGE")"
sync_cmd_escaped="$(escape_sed_replacement "$RSQLITE_RSYNC_REPLICA_SYNC_COMMAND")"
host_data_dir_escaped="$(escape_sed_replacement "$RSQLITE_RSYNC_HOST_DATA_DIR")"

sed \
  -e "s|__RSQLITE_RSYNC_IMAGE__|$image_escaped|g" \
  -e "s|__RSQLITE_RSYNC_REPLICA_SYNC_COMMAND__|$sync_cmd_escaped|g" \
  -e "s|__RSQLITE_RSYNC_HOST_DATA_DIR__|$host_data_dir_escaped|g" \
  "$template" > "$rendered"

kubectl get namespace "$RSQLITE_RSYNC_NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$RSQLITE_RSYNC_NAMESPACE"
kubectl -n "$RSQLITE_RSYNC_NAMESPACE" apply -f "$rendered"

echo "Applied k3s HA stack"
echo "namespace: $RSQLITE_RSYNC_NAMESPACE"
echo "hostDataDir: $RSQLITE_RSYNC_HOST_DATA_DIR"
echo "image: $RSQLITE_RSYNC_IMAGE"
