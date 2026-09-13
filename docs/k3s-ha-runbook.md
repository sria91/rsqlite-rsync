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

There is no Service that routes only to the writer — a readiness-filtered
Service can't safely do that (see [HTTP Probe
Endpoints](ha-kubernetes.md#probe-endpoints)) without also stalling
StatefulSet rollouts. Instead, `sqlite-ha` is a plain headless Service used
for stable per-pod DNS and general access, and clients find the current
writer themselves: the gRPC client follows `NOT_LEADER` redirects
automatically, or discovers it directly via the Kubernetes `Lease`
(`--kube-lease`/`--kube-service`, see README [SQL Gateway and
client](../README.md#sql-gateway-and-client)). Within the cluster,
`role_state.txt` on each pod (`writer:N` or `replica`) is the ground truth
for which pod currently holds the role.

## Prerequisites

- k3s cluster with a writable node filesystem path for hostPath storage (see [Node Storage](#node-storage) below)
- image for `rsqlite-rsync` available to cluster
- RBAC permission for lease updater (`get/list/watch/create/update/patch` on Lease)
- a sync command for replica pods (for example SSH-based sync, or a sidecar that can reach the writer DB path)

## Apply The Stack

1. Apply with required variables:
   - `RSQLITE_RSYNC_IMAGE=ghcr.io/YOUR_ORG/rsqlite-rsync:TAG RSQLITE_RSYNC_REPLICA_SYNC_COMMAND='rsqlite-rsync user@<writer-host>:/var/lib/sqlite/app.db /var/lib/sqlite/app.db --ssh-opt StrictHostKeyChecking=no' scripts/apply-k3s-ha-stack.sh`
     — `<writer-host>` is a placeholder: there's no Service that resolves
     to "whichever pod is currently the writer" (see Architecture above),
     so resolve it in your real sync command (e.g. by reading the Lease
     or `role_state.txt` from within the sync sidecar) before substituting
     it into this variable.
2. Optional overrides:
   - `RSQLITE_RSYNC_NAMESPACE` (default: `sqlite-ha`)
   - `RSQLITE_RSYNC_HOST_DATA_DIR` (default: `/var/lib/rsqlite-rsync-ha`)
3. Watch rollout:
   - `kubectl rollout status statefulset/sqlite-ha`
4. Inspect role transitions:
   - `kubectl logs statefulset/sqlite-ha -c rsqlite-rsync --tail=200`
5. Verify which pod is currently the writer:
   - `kubectl exec sqlite-ha-0 -c rsqlite-rsync -- cat /var/run/rsqlite-rsync/role_state.txt`
     (repeat per pod, or grep the audit log — exactly one pod should report `writer:N`)

## Node Storage

Each pod mounts a `hostPath` volume directly at `RSQLITE_RSYNC_HOST_DATA_DIR` — the same absolute path on whichever node it lands on, with no per-pod subdirectory. Data persists at a known, explicit path on the node, independent of the StatefulSet/pod lifecycle — no PVC or StorageClass is involved. An init container (`data-dir-permissions`) fixes ownership/permissions on that path before the main containers start, since `hostPath` directories are created root-owned by kubelet and the `rsqlite-rsync` container runs as non-root UID `10001`.

Using the same path on every node only works because the pod template also sets a **required** pod anti-affinity rule (`app: sqlite-ha`, `topologyKey: kubernetes.io/hostname`): the scheduler will never place two `sqlite-ha` pods on the same node. Without that rule, two pods sharing a node would both write to the same host directory and silently corrupt each other's SQLite files. This introduces a prerequisite that didn't exist before: **the cluster must have at least as many schedulable nodes as `replicas`** (3 by default), or the extra pod(s) will stay `Pending` instead of doubling up on a node.

**k3d caveat:** a k3d "node" is a Docker container, so a plain `hostPath` lives inside that container's writable layer and is lost if the node container itself is ever recreated (e.g. `k3d cluster delete`). To back it with a real directory on your host machine, recreate the cluster with a bind mount:

```bash
k3d cluster delete ha-test
k3d cluster create ha-test \
  --volume "$HOME/rsqlite-rsync-ha-data:/var/lib/rsqlite-rsync-ha@server:0"
```

This is a **manual, user-run, destructive step** — it wipes the entire existing cluster and all its state, not just this app's data. Only do this deliberately; never as part of a routine redeploy.

**Node-pinning caveat:** in a genuine multi-node cluster, a pod rescheduled to a different node starts with an empty directory there. This is the same limitation the `local-path` PVC provisioner it replaces already had — not a regression.

## Important Operational Notes

- This pattern defaults to `--ha-startup-fence-mode=permissive`, so all pods start and pass Kubernetes readiness (`/healthz`) once their reconcile loop has ticked — writer or replica. `/ready` (writer-only status) still reflects role and is what `role_state.txt`/the audit log/`rsqlite-rsync client status` show.
- If you use `require-writer`, non-writer pods can fail startup by design.
- `RSQLITE_RSYNC_REPLICA_SYNC_COMMAND` is required by [scripts/apply-k3s-ha-stack.sh](scripts/apply-k3s-ha-stack.sh) so you can plug in your real transport and auth model.
- Freshness ledger is written by the sync sidecar to `/var/run/rsqlite-rsync/freshness.txt`, which HA mode consumes for promotion safety.
- Keep lease renew interval shorter than lease duration (the example renews every 2s with 15s duration).

## Failover Validation Checklist

1. Confirm exactly one pod reports `writer:N` in `role_state.txt` (step 5 above).
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
