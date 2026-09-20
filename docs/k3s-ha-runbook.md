# k3s HA SQLite Runbook

This runbook provides a concrete k3s deployment pattern for single-writer SQLite with replica synchronization using `rsqlite-rsync` HA mode.

## Architecture

Each pod in a `StatefulSet` runs five containers:

- `rsqlite-rsync` HA controller:
  - decides writer vs replica from Kubernetes Lease
  - exposes readiness/liveness endpoints
  - writes role state and audit logs
- lease-updater sidecar:
  - publishes lease ownership and renewals when local role is writer
  - updates `holderIdentity`, `renewTime`, and generation annotation
  - when local role is *not* writer, watches for the Lease being missing
    or expired with nobody renewing it, and attempts to claim it —
    **any pod can win this**, not a designated ordinal. Kubernetes'
    optimistic concurrency control (a `kubectl replace` carrying the
    `resourceVersion` last read, or a `kubectl create` when the Lease
    doesn't exist yet) is what actually arbitrates a race between
    simultaneous candidates: at most one candidate's write can land for a
    given vacancy, so the API server — not this sidecar — resolves ties.
    Winning only makes a pod the Lease's `holderIdentity`; the
    `rsqlite-rsync` container still independently decides whether to
    promote based on that pod's own freshness ledger
    (`validate_promotion` in `src/ha.rs`), so a candidate that wins the
    Lease race with stale sync data simply stays a replica and the Lease
    re-expires for another candidate to try
- replica-sync sidecar contract:
  - when local role is replica, executes `RSQLITE_RSYNC_REPLICA_SYNC_COMMAND`
  - writes freshness ledger after successful sync
  - **default command** (`default-replica-sync.sh`, used unless you set
    `RSQLITE_RSYNC_REPLICA_SYNC_COMMAND` on `apply-k3s-ha-stack.sh`):
    resolves the current writer from the Lease, then pulls every `*.db`
    file it finds there via `rsqlite-rsync`'s own SSH transport (against
    the sshd sidecar below), no hardcoded database name. This is what
    makes the reference manifest actually replicate data rather than
    just role state — override the whole command for a real transport/
    auth model in production, same as before
- sshd sidecar:
  - serves the default replica-sync command above: read-only access to
    the data dir over SSH on port 2222, key auth only
  - trust model, appropriate for this reference/test manifest and not
    meant to carry into production as-is: a single SSH keypair
    (`sqlite-ha-ssh-keys` Secret) shared by every pod — any pod can SSH
    into any other — and `StrictHostKeyChecking=no` on the client side,
    since host keys are regenerated fresh on every pod restart and the
    pod behind any given writer hostname changes across failovers
    anyway, so real host-key pinning wouldn't mean anything here
- label-updater sidecar:
  - patches this pod's own `role=writer`/`role=replica` label from its
    local `role_state.txt`

`sqlite-ha-writer` is a ClusterIP Service that selects on that `role=writer`
label, so it routes to whichever pod is currently the writer. **This is a
routing hint, not a safety mechanism** — write safety is enforced
independently at the RPC layer (`HaSharedState::is_writer()` re-validates
the write fence against the lease on every gRPC call), so a stale or wrong
label just means a client's first hop gets a `NOT_LEADER` response with
redirect metadata, which the built-in client already follows automatically.
Zero endpoints are expected and normal during a cold cluster bootstrap
(before any pod has won the initial election) and briefly during failover
— a client relying solely on this Service rather than the mechanisms below
would see connection failures in those windows, same as hitting any
endpointless Service. This Service exists only in this manifest; the
`ha-deployment*.yaml` variants have no lease/label-updating sidecars at
all and rely purely on the mechanisms below.

The portable, always-correct ways to find the writer — used regardless of
whether `sqlite-ha-writer` happens to have a fresh endpoint — remain:
the gRPC client follows `NOT_LEADER` redirects automatically, or discovers
the writer directly via the Kubernetes `Lease` (`--kube-lease`/
`--kube-service`, see README [SQL Gateway and
client](../README.md#sql-gateway-and-client)). `sqlite-ha` (separate from
`sqlite-ha-writer`) is a plain headless Service used for stable per-pod DNS
and general access. Within the cluster, `role_state.txt` on each pod
(`writer:N` or `replica`) is the ground truth for which pod currently
holds the role.

## Prerequisites

- k3s cluster with a writable node filesystem path for hostPath storage (see [Node Storage](#node-storage) below)
- image for `rsqlite-rsync` available to cluster
- RBAC permission for lease updater (`get/list/watch/create/update/patch` on Lease)
- a sync command for replica pods — a working SSH-based default is now
  built into this manifest (see Architecture above), so this is only
  needed if you want to override it with a real transport/auth model

## Apply The Stack

Run these commands from the repository root:

1. Apply with required variables:
   - `RSQLITE_RSYNC_IMAGE=ghcr.io/YOUR_ORG/rsqlite-rsync:TAG scripts/apply-k3s-ha-stack.sh`
2. Optional overrides:
   - `RSQLITE_RSYNC_REPLICA_SYNC_COMMAND` (default: `sh /scripts/default-replica-sync.sh`, which syncs all `*.db` files via the SSH sidecar; override if using a custom sync command/transport)
   - `RSQLITE_RSYNC_NAMESPACE` (default: `sqlite-ha`)
   - `RSQLITE_RSYNC_HOST_DATA_DIR` (default: `/var/lib/rsqlite-rsync-ha`)
   - `RSQLITE_RSYNC_CLIENT_POD_NAME` (default: `sqlite-ha-client`, when applying the client pod)
3. Watch rollout:
   - `kubectl -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" rollout status statefulset/sqlite-ha`
4. Inspect role transitions:
   - `kubectl -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" logs statefulset/sqlite-ha -c rsqlite-rsync --tail=200`
5. Verify which pod is currently the writer:
   - `kubectl -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" exec sqlite-ha-0 -c rsqlite-rsync -- cat /var/run/rsqlite-rsync/role_state.txt`
     (repeat per pod, or grep the audit log — exactly one pod should report `writer:N`)
   - or `kubectl -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" get pods --show-labels` (exactly one `role=writer`) / `kubectl -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" get endpoints sqlite-ha-writer`
     — convenience checks, not the ground truth; see Architecture above
6. Retrieve the gRPC SQL Gateway auth token (the script provisions this automatically — see [Security](../README.md#security) for why it's required):
   - `kubectl -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" get secret sqlite-ha-grpc-auth -o go-template='{{.data.token | base64decode}}'`
   - also printed at the end of `apply-k3s-ha-stack.sh`'s own output
7. Query the cluster: deploy [examples/k8s/client-pod.yaml](../examples/k8s/client-pod.yaml) via [scripts/apply-client-pod.sh](../scripts/apply-client-pod.sh) (`RSQLITE_RSYNC_IMAGE=ghcr.io/YOUR_ORG/rsqlite-rsync:TAG scripts/apply-client-pod.sh`) — it's pre-wired with the `sqlite-ha-writer` service endpoint and auth token, so `kubectl exec -it -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" "${RSQLITE_RSYNC_CLIENT_POD_NAME:-sqlite-ha-client}" -- rsqlite-rsync client status` works with no extra flags.

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

- **Security & Transport Considerations:**
  - The reference HA manifest and the client helper default to intra-cluster plaintext HTTP for gRPC communication, transmitting the `RSQLITE_TOKEN` as cleartext within the cluster network (trusted-network exception).
  - For production environments, multi-tenant clusters, or communication across untrusted network boundaries, this pattern should be enhanced to enforce transport encryption using a TLS/mTLS reverse proxy, Kubernetes ingress with TLS termination, or a service mesh (such as Istio, Linkerd, or Cilium Service Mesh). Configure matching secure endpoints (e.g. `https://...`) when doing so.
- This pattern defaults to `--ha-startup-fence-mode=permissive`, so all pods start and pass Kubernetes readiness (`/healthz`) once their reconcile loop has ticked — writer or replica. `/ready` (writer-only status) still reflects role and is what `role_state.txt`/the audit log/`rsqlite-rsync client status` show.
- If you use `require-writer`, non-writer pods can fail startup by design.
- `RSQLITE_RSYNC_REPLICA_SYNC_COMMAND` defaults to `sh /scripts/default-replica-sync.sh` in [scripts/apply-k3s-ha-stack.sh](../scripts/apply-k3s-ha-stack.sh) when unset; set this variable to override the default for a production-specific transport and authentication model.
- Freshness ledger is written by the sync sidecar to `/var/run/rsqlite-rsync/freshness.txt`, which HA mode consumes for promotion safety.
- Keep lease renew interval shorter than lease duration (the example renews every 2s with 15s duration).

## Failover Validation Checklist

1. Confirm exactly one pod reports `writer:N` in `role_state.txt` (step 5 above).
2. Delete the current writer pod.
3. Confirm some pod's `role_state.txt` reports `writer:N` with a higher `N`
   than before — all pods stay Kubernetes-`Ready` throughout via `/healthz`
   regardless of role, so pod readiness alone doesn't tell you who the new
   writer is; check the role explicitly. Any pod can win this (see
   Architecture above), not just the one that was deleted — but if the
   deleted pod restarts and rejoins before the Lease actually expires
   (`LEASE_DURATION_SECONDS`, 15s by default), it may legitimately resume
   as the still-recorded holder with the *same* generation, since nothing
   else ever got a real opening to claim it. That's expected, not a bug:
   to reliably exercise a genuine cross-pod handoff, the writer needs to
   stay down longer than the lease duration (for example, force-delete it
   repeatedly, or scale the StatefulSet to 0 and back).
4. Confirm lease holder and generation update.
5. Confirm replica pods continue sync loop and freshness writes.
6. Confirm `kubectl -n "${RSQLITE_RSYNC_NAMESPACE:-sqlite-ha}" get endpoints sqlite-ha-writer` converges on the new
   writer's pod IP within roughly one `LABEL_UPDATE_SLEEP_SECONDS` (2s)
   after step 3 — total time-to-new-endpoint is bound by lease-election
   timing, not by this step, which only adds the label-updater's poll
   interval on top.

## Related Files

- [docs/ha-kubernetes.md](ha-kubernetes.md)
- [examples/k8s/k3s-ha-stack.yaml](../examples/k8s/k3s-ha-stack.yaml)
- [scripts/apply-k3s-ha-stack.sh](../scripts/apply-k3s-ha-stack.sh)
- [examples/k8s/client-pod.yaml](../examples/k8s/client-pod.yaml) — debug/test client pod for querying the cluster
- [scripts/apply-client-pod.sh](../scripts/apply-client-pod.sh) — apply helper for client pod
- [examples/k8s/ha-deployment.yaml](../examples/k8s/ha-deployment.yaml)
- [examples/k8s/ha-deployment-readonly.yaml](../examples/k8s/ha-deployment-readonly.yaml)
- [examples/k8s/local-dev-file-lease.yaml](../examples/k8s/local-dev-file-lease.yaml) — single-node local testing without Kubernetes Lease election
