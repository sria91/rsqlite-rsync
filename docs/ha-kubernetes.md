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
  - Writer status, not Pod health — every replica is permanently 503 by
    design. Do not wire this to readinessProbe (see /healthz below); query
    it directly when you need to know which node is the writer.
- /healthz:
  - 200 once the HA reconcile loop has completed at least one tick
    (writer or replica — role-independent).
  - 503 before the first tick completes.
  - Use this as readinessProbe: it passes for replica nodes too, so Pod
    readiness (and StatefulSet rollout progression) isn't gated on which
    single pod happens to be the writer, unlike /ready.
- /live:
  - 200 while process is alive.
- Any other path:
  - 404.

This allows direct httpGet probes without sidecar file checks.

## Startup Fence Behavior

With --ha-startup-fence-mode=require-writer, process startup fails unless initial HA reconciliation can confirm writer state.

Use startupProbe with /live (all example manifests do this) to give the
process time to start before Kubernetes evaluates readiness/liveness at
all; --ha-startup-fence-mode=require-writer is what actually keeps the
process itself from starting (it exits) when writer eligibility can't be
proven on the first tick, independent of any probe.

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
- `examples/k8s/local-dev-file-lease.yaml` (single-node local testing, no Kubernetes Lease election required)
- `examples/k8s/client-pod.yaml` (debug/test client pod for querying any of the above)

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
- examples/k8s/local-dev-file-lease.yaml
- examples/k8s/client-pod.yaml
- scripts/apply-k3s-ha-stack.sh
- scripts/apply-client-pod.sh

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
