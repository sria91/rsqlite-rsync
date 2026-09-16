#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
template="$repo_root/examples/k8s/client-pod.yaml"

if [[ ! -f "$template" ]]; then
  echo "Template manifest not found: $template" >&2
  exit 1
fi

: "${RSQLITE_RSYNC_IMAGE:?Set RSQLITE_RSYNC_IMAGE to a pullable image (for example ghcr.io/sria91/rsqlite-rsync:0.5.0 or ghcr.io/sria91/rsqlite-rsync@sha256:<64-hex-digest>)}"

RSQLITE_RSYNC_NAMESPACE="${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}"
RSQLITE_RSYNC_CLIENT_POD_NAME="${RSQLITE_RSYNC_CLIENT_POD_NAME:-sqlite-ha-client}"
# Security notice: RSQLITE_RSYNC_ENDPOINTS defaults to intra-cluster plaintext HTTP endpoints
# on the assumption of a trusted cluster network (private node network/Service mesh boundary).
# For untrusted network segments or external traffic, terminate TLS using an ingress/mTLS mesh
# (e.g. Istio, Linkerd, Cilium) and configure secure HTTPS endpoints.
RSQLITE_RSYNC_ENDPOINTS="${RSQLITE_RSYNC_ENDPOINTS:-http://sqlite-ha-0.sqlite-ha:50051,http://sqlite-ha-1.sqlite-ha:50051,http://sqlite-ha-2.sqlite-ha:50051}"
RSQLITE_RSYNC_AUTH_SECRET="${RSQLITE_RSYNC_AUTH_SECRET:-sqlite-ha-grpc-auth}"
# The gRPC SQL Gateway requires a bearer token (see `--ha-grpc-auth-token` /
# README "Security"). If RSQLITE_RSYNC_GRPC_AUTH_TOKEN is unset, read the existing
# token from the Secret or generate one if the Secret does not yet exist.
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
pod_name_escaped="$(escape_sed_replacement "$RSQLITE_RSYNC_CLIENT_POD_NAME")"
endpoints_escaped="$(escape_sed_replacement "$RSQLITE_RSYNC_ENDPOINTS")"
auth_secret_escaped="$(escape_sed_replacement "$RSQLITE_RSYNC_AUTH_SECRET")"

sed \
  -e "s|__RSQLITE_RSYNC_IMAGE__|$image_escaped|g" \
  -e "s|__RSQLITE_RSYNC_CLIENT_POD_NAME__|$pod_name_escaped|g" \
  -e "s|__RSQLITE_RSYNC_ENDPOINTS__|$endpoints_escaped|g" \
  -e "s|__RSQLITE_RSYNC_AUTH_SECRET__|$auth_secret_escaped|g" \
  "$template" > "$rendered"

kubectl get namespace "$RSQLITE_RSYNC_NAMESPACE" >/dev/null 2>&1 || kubectl create namespace "$RSQLITE_RSYNC_NAMESPACE"

target_existing_token="$(kubectl -n "$RSQLITE_RSYNC_NAMESPACE" get secret "$RSQLITE_RSYNC_AUTH_SECRET" \
  -o go-template='{{if .data.token}}{{.data.token | base64decode}}{{end}}' 2>/dev/null || true)"

default_gateway_existing_token="$(kubectl -n "$RSQLITE_RSYNC_NAMESPACE" get secret sqlite-ha-grpc-auth \
  -o go-template='{{if .data.token}}{{.data.token | base64decode}}{{end}}' 2>/dev/null || true)"

if [[ -z "$RSQLITE_RSYNC_GRPC_AUTH_TOKEN" ]]; then
  if [[ -n "$target_existing_token" ]]; then
    RSQLITE_RSYNC_GRPC_AUTH_TOKEN="$target_existing_token"
  elif [[ -n "$default_gateway_existing_token" ]]; then
    # Fallback: check if the default gateway secret exists to sync token from it
    RSQLITE_RSYNC_GRPC_AUTH_TOKEN="$default_gateway_existing_token"
  else
    RSQLITE_RSYNC_GRPC_AUTH_TOKEN="$(generate_token)"
  fi
fi

kubectl -n "$RSQLITE_RSYNC_NAMESPACE" create secret generic "$RSQLITE_RSYNC_AUTH_SECRET" \
  --from-literal=token="$RSQLITE_RSYNC_GRPC_AUTH_TOKEN" \
  --dry-run=client -o yaml | kubectl -n "$RSQLITE_RSYNC_NAMESPACE" apply -f -

# If a custom auth secret is specified, also ensure the default sqlite-ha-grpc-auth secret exists and stays in sync
if [[ "$RSQLITE_RSYNC_AUTH_SECRET" != "sqlite-ha-grpc-auth" ]]; then
  kubectl -n "$RSQLITE_RSYNC_NAMESPACE" create secret generic sqlite-ha-grpc-auth \
    --from-literal=token="$RSQLITE_RSYNC_GRPC_AUTH_TOKEN" \
    --dry-run=client -o yaml | kubectl -n "$RSQLITE_RSYNC_NAMESPACE" apply -f -
fi

# If the auth token was explicitly rotated, or if stored tokens were missing or recreated
# (a newly generated or synchronized token was provisioned), restart the StatefulSet so existing
# gateway pods reload the new token into memory.
token_rotated=0
if [[ -z "$target_existing_token" && -z "$default_gateway_existing_token" ]]; then
  token_rotated=1
fi
if [[ -n "$target_existing_token" && -z "$default_gateway_existing_token" ]]; then
  token_rotated=1
fi
if [[ -n "$target_existing_token" && "$target_existing_token" != "$RSQLITE_RSYNC_GRPC_AUTH_TOKEN" ]]; then
  token_rotated=1
fi
if [[ -n "$default_gateway_existing_token" && "$default_gateway_existing_token" != "$RSQLITE_RSYNC_GRPC_AUTH_TOKEN" ]]; then
  token_rotated=1
fi

if [[ "$token_rotated" -eq 1 ]]; then
  for ss in sqlite-ha; do
    if kubectl -n "$RSQLITE_RSYNC_NAMESPACE" get statefulset "$ss" >/dev/null 2>&1; then
      echo "Rotating gateway auth token: restarting statefulset/$ss to load new token..."
      kubectl -n "$RSQLITE_RSYNC_NAMESPACE" rollout restart "statefulset/$ss"
      break
    fi
  done
fi

if kubectl -n "$RSQLITE_RSYNC_NAMESPACE" get pod "$RSQLITE_RSYNC_CLIENT_POD_NAME" >/dev/null 2>&1; then
  echo "Recreating existing client pod $RSQLITE_RSYNC_CLIENT_POD_NAME in namespace $RSQLITE_RSYNC_NAMESPACE..."
  kubectl -n "$RSQLITE_RSYNC_NAMESPACE" delete pod "$RSQLITE_RSYNC_CLIENT_POD_NAME" --wait=true
fi

kubectl -n "$RSQLITE_RSYNC_NAMESPACE" apply -f "$rendered"

echo "Applied client pod"
echo "namespace: $RSQLITE_RSYNC_NAMESPACE"
echo "pod: $RSQLITE_RSYNC_CLIENT_POD_NAME"
echo "image: $RSQLITE_RSYNC_IMAGE"
echo "endpoints: $RSQLITE_RSYNC_ENDPOINTS"
echo "auth secret: $RSQLITE_RSYNC_AUTH_SECRET"
echo "gRPC gateway token: kubectl -n $RSQLITE_RSYNC_NAMESPACE get secret $RSQLITE_RSYNC_AUTH_SECRET -o go-template='{{.data.token | base64decode}}'"
echo ""
echo "Waiting for pod/$RSQLITE_RSYNC_CLIENT_POD_NAME to become Ready..."
wait_status=0
kubectl -n "$RSQLITE_RSYNC_NAMESPACE" wait --for=condition=Ready "pod/$RSQLITE_RSYNC_CLIENT_POD_NAME" --timeout=60s || wait_status=$?

if [[ "$wait_status" -ne 0 ]]; then
  echo "" >&2
  echo "Error: pod/$RSQLITE_RSYNC_CLIENT_POD_NAME failed to become Ready within 60s (exit status $wait_status)." >&2
  echo "Check pod status: kubectl -n $RSQLITE_RSYNC_NAMESPACE describe pod $RSQLITE_RSYNC_CLIENT_POD_NAME" >&2
  echo "Check pod logs:   kubectl -n $RSQLITE_RSYNC_NAMESPACE logs pod/$RSQLITE_RSYNC_CLIENT_POD_NAME" >&2
  exit "$wait_status"
fi

echo ""
echo "Useful commands:"
echo "  kubectl exec -it -n $RSQLITE_RSYNC_NAMESPACE $RSQLITE_RSYNC_CLIENT_POD_NAME -- rsqlite-rsync client status"
echo "  kubectl exec -it -n $RSQLITE_RSYNC_NAMESPACE $RSQLITE_RSYNC_CLIENT_POD_NAME -- rsqlite-rsync sql -d demo.db \"SELECT 1\""
echo "  kubectl exec -it -n $RSQLITE_RSYNC_NAMESPACE $RSQLITE_RSYNC_CLIENT_POD_NAME -- rsqlite-rsync client repl -d demo.db"
