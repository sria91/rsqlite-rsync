#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$repo_root/examples/k8s/k3s-ha-stack.yaml"

if [[ ! -f "$template" ]]; then
  echo "Template manifest not found: $template" >&2
  exit 1
fi

: "${RSQLITE_RSYNC_IMAGE:?Set RSQLITE_RSYNC_IMAGE to a pullable image (for example ghcr.io/sria91/rsqlite-rsync:0.5.0 or ghcr.io/sria91/rsqlite-rsync@sha256:<64-hex-digest>)}"
# Defaults to the manifest's own SSH-based sync (default-replica-sync.sh,
# using the sshd sidecar + sqlite-ha-ssh-keys Secret below) if unset. Set
# this to plug in a real transport/auth model instead.
RSQLITE_RSYNC_REPLICA_SYNC_COMMAND="${RSQLITE_RSYNC_REPLICA_SYNC_COMMAND:-sh /scripts/default-replica-sync.sh}"

RSQLITE_RSYNC_NAMESPACE="${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}"
RSQLITE_RSYNC_HOST_DATA_DIR="${RSQLITE_RSYNC_HOST_DATA_DIR:-/var/lib/rsqlite-rsync-ha}"
# The gRPC SQL Gateway refuses to start without a bearer token (see
# `--ha-grpc-auth-token` / README "Security"). Set this to pin a specific
# token (for example, to share it with an out-of-cluster client); otherwise
# one is generated on first apply and reused on every subsequent apply by
# reading it back from the `sqlite-ha-grpc-auth` Secret, so re-running this
# script doesn't rotate the token out from under already-configured clients.
RSQLITE_RSYNC_GRPC_AUTH_TOKEN="${RSQLITE_RSYNC_GRPC_AUTH_TOKEN:-}"

generate_token() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex 32
  else
    head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'
  fi
}

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

if [[ -z "$RSQLITE_RSYNC_GRPC_AUTH_TOKEN" ]]; then
  existing_token="$(kubectl -n "$RSQLITE_RSYNC_NAMESPACE" get secret sqlite-ha-grpc-auth \
    -o go-template='{{if .data.token}}{{.data.token | base64decode}}{{end}}' 2>/dev/null || true)"
  if [[ -n "$existing_token" ]]; then
    RSQLITE_RSYNC_GRPC_AUTH_TOKEN="$existing_token"
  else
    RSQLITE_RSYNC_GRPC_AUTH_TOKEN="$(generate_token)"
  fi
fi

kubectl -n "$RSQLITE_RSYNC_NAMESPACE" create secret generic sqlite-ha-grpc-auth \
  --from-literal=token="$RSQLITE_RSYNC_GRPC_AUTH_TOKEN" \
  --dry-run=client -o yaml | kubectl -n "$RSQLITE_RSYNC_NAMESPACE" apply -f -

# Shared SSH keypair used by default-replica-sync.sh (replica -> writer
# pull) and the sshd sidecar's authorized_keys -- every pod trusts every
# other pod in this ServiceAccount, matching the same reference-manifest
# tradeoff already accepted for the label-updater RBAC grant (see
# docs/k3s-ha-runbook.md). Generated once and reused on every subsequent
# apply (created, never overwritten) so already-synced pods don't get
# locked out by a key rotation underneath them.
if ! kubectl -n "$RSQLITE_RSYNC_NAMESPACE" get secret sqlite-ha-ssh-keys >/dev/null 2>&1; then
  ssh_tmpdir="$(mktemp -d)"
  trap 'rm -f "$rendered"; rm -rf "$ssh_tmpdir"' EXIT
  ssh-keygen -t ed25519 -N "" -f "$ssh_tmpdir/id_ed25519" -C sqlite-ha >/dev/null
  kubectl -n "$RSQLITE_RSYNC_NAMESPACE" create secret generic sqlite-ha-ssh-keys \
    --from-file=id_ed25519="$ssh_tmpdir/id_ed25519" \
    --from-file=authorized_keys="$ssh_tmpdir/id_ed25519.pub"
  rm -rf "$ssh_tmpdir"
fi

kubectl -n "$RSQLITE_RSYNC_NAMESPACE" apply -f "$rendered"

echo "Applied k3s HA stack"
echo "namespace: $RSQLITE_RSYNC_NAMESPACE"
echo "hostDataDir: $RSQLITE_RSYNC_HOST_DATA_DIR"
echo "image: $RSQLITE_RSYNC_IMAGE"
echo "gRPC gateway token: kubectl -n $RSQLITE_RSYNC_NAMESPACE get secret sqlite-ha-grpc-auth -o go-template='{{.data.token | base64decode}}'"
kubectl -n "$RSQLITE_RSYNC_NAMESPACE" rollout restart statefulset/sqlite-ha
kubectl get pods -n "$RSQLITE_RSYNC_NAMESPACE" -w
