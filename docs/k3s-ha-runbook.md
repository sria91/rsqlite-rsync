# k3s HA SQLite Runbook

This runbook provides a concrete k3s deployment pattern for single-writer SQLite with replica synchronization using `rsqlite-rsync` HA mode.

## Architecture

Each pod in a `StatefulSet` runs three containers:

- `rsqlite-rsync` HA controller:
  - decides writer vs replica from Kubernetes Lease
  - exposes readiness/liveness endpoints
  - writes role state and audit logs
- lease-updater sidecar:
  - publishes lease ownership and renewals when local role is writer
  - updates `holderIdentity`, `renewTime`, and generation annotation
- replica-sync sidecar contract:
  - when local role is replica, executes a sync command you provide
  - writes freshness ledger after successful sync

Traffic is routed through a single ClusterIP Service (`sqlite-ha-writer`) that only sends traffic to ready endpoints. In HA mode, only writer-active pods return ready.

## Prerequisites

- k3s cluster with default storage class
- image for `rsqlite-rsync` available to cluster
- RBAC permission for lease updater (`get/list/watch/create/update/patch` on Lease)
- a sync command for replica pods (for example SSH-based sync, or a sidecar that can reach the writer DB path)

## Apply The Stack

1. Apply with required variables:
   - `RSQLITE_RSYNC_IMAGE=ghcr.io/YOUR_ORG/rsqlite-rsync:TAG RSQLITE_RSYNC_REPLICA_SYNC_COMMAND='rsqlite-rsync sqlite-ha-writer:/var/lib/sqlite/app.db /var/lib/sqlite/app.db --ssh-opt StrictHostKeyChecking=no' scripts/apply-k3s-ha-stack.sh`
2. Optional overrides:
   - `RSQLITE_RSYNC_NAMESPACE` (default: `sqlite-ha`)
   - `RSQLITE_RSYNC_STORAGE_CLASS` (default: `local-path`)
3. Watch rollout:
   - `kubectl rollout status statefulset/sqlite-ha`
4. Inspect role transitions:
   - `kubectl logs statefulset/sqlite-ha -c rsqlite-rsync --tail=200`
5. Verify writer service endpoint:
   - `kubectl get endpoints sqlite-ha-writer -o wide`

## Important Operational Notes

- This pattern defaults to `--ha-startup-fence-mode=permissive` so all pods can start and only writer becomes ready.
- If you use `require-writer`, non-writer pods can fail startup by design.
- `RSQLITE_RSYNC_REPLICA_SYNC_COMMAND` is required by [scripts/apply-k3s-ha-stack.sh](scripts/apply-k3s-ha-stack.sh) so you can plug in your real transport and auth model.
- Freshness ledger is written by the sync sidecar to `/var/run/rsqlite-rsync/freshness.txt`, which HA mode consumes for promotion safety.
- Keep lease renew interval shorter than lease duration (the example renews every 2s with 15s duration).

## Failover Validation Checklist

1. Confirm one ready endpoint behind `sqlite-ha-writer`.
2. Delete the current writer pod.
3. Confirm another pod becomes ready.
4. Confirm lease holder and generation update.
5. Confirm replica pods continue sync loop and freshness writes.

## Related Files

- [docs/ha-kubernetes.md](docs/ha-kubernetes.md)
- [examples/k8s/k3s-ha-stack.yaml](examples/k8s/k3s-ha-stack.yaml)
- [scripts/apply-k3s-ha-stack.sh](scripts/apply-k3s-ha-stack.sh)
- [examples/k8s/ha-deployment.yaml](examples/k8s/ha-deployment.yaml)
- [examples/k8s/ha-deployment-readonly.yaml](examples/k8s/ha-deployment-readonly.yaml)
