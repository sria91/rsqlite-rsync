# Kubernetes HA Deployment Guide

This guide shows how to run rsqlite-rsync HA mode with Kubernetes probes and lease ownership.

## Goals

- Single active writer at a time.
- Fast demotion when lease is lost.
- Probe behavior that reflects writer-readiness and process liveness.

## Recommended Flags

Use these HA flags in Kubernetes:

- --ha
- --ha-node-id=$(POD_NAME)
- --ha-lease-source=kubernetes
- --ha-kube-namespace=$(POD_NAMESPACE)
- --ha-kube-lease-name=sqlite-writer-lease
- --ha-role-state-file=/var/run/rsqlite-rsync/role_state.txt
- --ha-audit-log-file=/var/log/rsqlite-rsync/ha_audit.log
- --ha-readiness-file=/var/run/rsqlite-rsync/readiness.txt
- --ha-readiness-http-bind=0.0.0.0:8088
- --ha-startup-fence-mode=require-writer
- --ha-tick-interval-ms=250
- --ha-max-freshness-age-secs=10
- --ha-max-future-skew-secs=2

## Probe Endpoints

When --ha-readiness-http-bind is enabled:

- /ready:
  - 200 when node is writer-active.
  - 503 when node is replica, denied, or demoted.
- /live:
  - 200 while process is alive.
- Any other path:
  - 404.

This allows direct httpGet probes without sidecar file checks.

## Startup Fence Behavior

With --ha-startup-fence-mode=require-writer, process startup fails unless initial HA reconciliation can confirm writer state.

Use startupProbe with /ready to keep pod unready until writer eligibility is proven.

## Lease Requirements

The Lease resource must include:

- spec.holderIdentity
- spec.renewTime
- spec.leaseDurationSeconds
- metadata.annotations["rsqlite-rsync.dev/generation"]

If lease parsing fails, HA mode executes fail-safe fallback (replica mode).

## RBAC Requirements

The pod service account needs access to the Lease object in its namespace.

Choose one RBAC profile based on your control-plane model:

- Lease-manager profile:
  - `get`, `list`, `watch` on Lease `sqlite-writer-lease`
  - `update`, `patch`, and `create`
  - Use when this workload is responsible for lease management.
- Read-only profile:
  - `get` on the specific Lease name
  - Use when an external controller performs leader election and lease updates.

Example manifests:

- `examples/k8s/ha-deployment.yaml` (lease-manager profile)
- `examples/k8s/ha-deployment-readonly.yaml` (read-only profile)
- `examples/k8s/k3s-ha-stack.yaml` (k3s-oriented end-to-end stack with lease updater and replica-sync contract)

## Operational Notes

- Keep Lease renewal interval comfortably below leaseDurationSeconds.
- Set max freshness age to your RPO budget.
- Watch ha_audit.log for promotion_denied and demotion entries.
- Prefer a StatefulSet for stable pod identity.

## Example Manifest

Complete deployment examples are available at:

- examples/k8s/ha-deployment.yaml
- examples/k8s/ha-deployment-readonly.yaml
- examples/k8s/k3s-ha-stack.yaml

For a concrete k3s-focused runbook and rollout checklist, see:

- docs/k3s-ha-runbook.md

## Validation

To validate both example manifests locally:

```bash
./scripts/validate-k8s-manifests.sh
```

The script prefers `kubeconform` for cluster-independent schema validation. If `kubeconform` is unavailable, it falls back to `kubectl --dry-run=server` when a Kubernetes API server is reachable.

It also performs structural checks before schema validation:

- required manifest files must exist
- duplicate Kind/name object identities are rejected within each manifest
- unexpected duplicate Kind/name identities across both profile manifests are rejected (with an allowlist for intentionally shared workload objects)

CI installs `kubeconform` and runs the same script on every push and pull request.
