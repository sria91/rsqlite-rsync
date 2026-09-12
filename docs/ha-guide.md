# rsqlite-rsync High Availability (HA) Guide

## Overview

The HA (High Availability) feature enables active-passive SQLite replication with automatic failover. It uses external lease coordination (file-based or Kubernetes) to ensure only one writer is active at any time, preventing split-brain scenarios and data corruption.

### Key Concepts

- **Writer node**: The single node allowed to write to the database, holding the active lease
- **Replica nodes**: Read-only nodes that continuously sync from the writer
- **Lease**: Authoritative record of who holds writer status, with generation tracking
- **Generation**: Monotonic counter tracking writer transitions
- **Freshness ledger**: Metadata about the last successful sync, ensuring safe promotions

## Basic Usage

### Minimal HA Mode Command

```bash
rsqlite-rsync \
  --ha \
  --ha-node-id=node-1 \
  --ha-lease-file=/tmp/lease.txt \
  --ha-role-state-file=/tmp/role.txt \
  --ha-audit-log-file=/tmp/audit.log
```

This starts the HA control loop that:

1. Reads the lease every tick (default: 1 second)
2. Decides whether this node should be writer or replica
3. Executes state transitions (promote/demote/keep)
4. Writes role state and audit entries

## Command-Line Flags

### Required Flags

| Flag | Description |
|------|-------------|
| `--ha` | Enable HA mode |
| `--ha-node-id <ID>` | Unique identifier for this node (e.g., hostname, pod name) |
| `--ha-role-state-file <PATH>` | Where to write current role (`replica` or `writer:N`) |
| `--ha-audit-log-file <PATH>` | Append-only action log for observability |

### Lease Source Configuration

**File-based lease** (default):

```bash
--ha-lease-source=file \
--ha-lease-file=/var/run/lease.txt
```

**Kubernetes lease**:

```bash
--ha-lease-source=kubernetes \
--ha-kube-lease-name=sqlite-writer-lease \
--ha-kube-namespace=default \
--ha-kubectl-path=kubectl  # optional, defaults to "kubectl"
```

Optional Kubernetes flags:

- `--ha-kube-context <CONTEXT>`: Use specific kubectl context
- `--ha-kubeconfig <PATH>`: Path to kubeconfig file

### Safety and Timing Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--ha-tick-interval-ms <MS>` | 1000 | How often to evaluate state (min: 50ms) |
| `--ha-max-freshness-age-secs <SECS>` | 10 | Maximum staleness allowed for promotion |
| `--ha-max-future-skew-secs <SECS>` | 2 | Allowed clock drift for freshness validation |
| `--ha-min-source-generation <N>` | 0 | Minimum generation required for promotion |
| `--ha-continue-on-error` | false | Keep executing actions after one fails |

### Startup Behavior

| Flag | Values | Description |
|------|--------|-------------|
| `--ha-startup-fence-mode` | `permissive` (default), `require-writer` | Whether startup must wait for writer eligibility |

- **permissive**: Process starts regardless of initial state
- **require-writer**: Process exits if it cannot immediately become writer (useful for StatefulSet replicas where only one should run)

### Observability Flags (Optional)

| Flag | Description |
|------|-------------|
| `--ha-freshness-file <PATH>` | Freshness ledger file to read each tick |
| `--ha-readiness-file <PATH>` | File updated with `ready`/`not-ready` status |
| `--ha-readiness-http-bind <ADDR>` | HTTP server for probe endpoints (e.g., `0.0.0.0:8088`) |

## Lease Formats

### File-Based Lease

Create a lease file with key-value format:

```
holder_node_id=node-1
generation=5
renewed_at_secs=1726097280
ttl_secs=15
```

**Key fields:**

- `holder_node_id`: Node that owns the lease
- `generation`: Monotonic counter (increment on each ownership transfer)
- `renewed_at_secs`: Unix timestamp when lease was last renewed
- `ttl_secs`: Lease duration in seconds

**Empty or missing file**: Treated as no lease holder (all nodes stay replica). To release a lease, write `none` or delete the file.

### Kubernetes Lease

The HA controller reads Kubernetes Lease resources via `kubectl get lease -o json`. Required structure:

```yaml
apiVersion: coordination.k8s.io/v1
kind: Lease
metadata:
  name: sqlite-writer-lease
  annotations:
    rsqlite-rsync.dev/generation: "5"
spec:
  holderIdentity: "node-1"
  renewTime: "2026-09-11T23:45:00Z"
  leaseDurationSeconds: 15
```

**RBAC requirements**: The pod needs permissions to `get` the Lease (read-only) or `get/create/update/patch` (if managing the lease itself).

## Freshness Ledger

The freshness ledger tells the controller when the replica last synced and from which source. This prevents unsafe promotions from stale replicas.

### Format

```
source_node_id=node-1
source_generation=5
synced_at_secs=1726097280
```

Write this file after each successful sync from writer to replica.

### Validation Rules

When a node wants to promote to writer, the controller validates:

1. **Recency**: `now - synced_at_secs ≤ max_freshness_age_secs`
2. **No future timestamps**: `synced_at_secs ≤ now + max_future_skew_secs`
3. **Lineage**: `source_generation ≥ min_source_generation`

If any check fails, promotion is denied and logged to the audit log with the specific violation reason.

## State Transitions

### Role State File

The `--ha-role-state-file` contains:

- `replica` — Node is in replica mode
- `writer:N` — Node is writer at generation N

External processes (sync scripts, monitoring) can read this file to know the current role.

### Reconciliation Logic

Each tick, the controller:

1. Reads the lease
2. Reads freshness ledger (if configured)
3. Evaluates promotion/demotion rules
4. Executes actions
5. Writes role state and audit log

**Decision flow:**

```
Is lease held by this node?
├─ Yes → Am I already writer?
│  ├─ Yes → KeepWriter
│  └─ No → PromoteToWriter (if freshness valid)
└─ No → Am I writer?
   ├─ Yes → DemoteToReplica (lost lease)
   └─ No → KeepReplica
```

### Audit Log

Every action is logged to `--ha-audit-log-file`:

```
action=ensure_replica result=ok
action=enable_writer generation=5 result=ok
action=disable_writer reason=lease_missing result=ok
action=promotion_denied violation=stale_freshness result=ok
```

Monitor this file to track:

- When promotions/demotions occur
- Why promotions were denied
- Action execution failures

## HTTP Probe Endpoints

When `--ha-readiness-http-bind` is set (e.g., `0.0.0.0:8088`), the controller exposes:

### `/ready`

- **200 OK** (`ready`) — Node is active writer
- **503 Service Unavailable** (`not-ready`) — Node is replica or demoted

Use as Kubernetes readiness probe so only the writer receives traffic.

### `/live`

- **200 OK** (`live`) — Process is alive

Use as Kubernetes liveness probe.

### Other paths

- **404 Not Found**

## Kubernetes Deployment

### Architecture Pattern

A typical k3s/k8s deployment includes:

1. **StatefulSet** with 2+ replicas
2. **HA controller** (rsqlite-rsync) in each pod
3. **Lease updater sidecar** to publish writer identity to Kubernetes Lease
4. **Replica sync sidecar** to pull from writer when in replica mode
5. **Service** (ClusterIP) routing only to ready (writer) pods

### Example Configuration

```bash
rsqlite-rsync \
  --ha \
  --ha-node-id=$(POD_NAME) \
  --ha-lease-source=kubernetes \
  --ha-kube-namespace=$(POD_NAMESPACE) \
  --ha-kube-lease-name=sqlite-writer-lease \
  --ha-role-state-file=/var/run/rsqlite-rsync/role_state.txt \
  --ha-audit-log-file=/var/log/rsqlite-rsync/ha_audit.log \
  --ha-readiness-http-bind=0.0.0.0:8088 \
  --ha-tick-interval-ms=250 \
  --ha-max-freshness-age-secs=10
```

### Pod Spec Example

```yaml
containers:
- name: rsqlite-rsync
  image: your-registry/rsqlite-rsync:latest
  command: ["/usr/local/bin/rsqlite-rsync"]
  args:
    - --ha
    - --ha-node-id=$(POD_NAME)
    - --ha-lease-source=kubernetes
    - --ha-kube-namespace=$(POD_NAMESPACE)
    - --ha-kube-lease-name=sqlite-writer-lease
    - --ha-role-state-file=/var/run/rsqlite-rsync/role_state.txt
    - --ha-audit-log-file=/var/log/rsqlite-rsync/ha_audit.log
    - --ha-readiness-http-bind=0.0.0.0:8088
    - --ha-freshness-file=/var/run/rsqlite-rsync/freshness.txt
  env:
  - name: POD_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: POD_NAMESPACE
    valueFrom:
      fieldRef:
        fieldPath: metadata.namespace
  readinessProbe:
    httpGet:
      path: /ready
      port: 8088
    periodSeconds: 1
  livenessProbe:
    httpGet:
      path: /live
      port: 8088
    periodSeconds: 5
```

### Sidecars

**Lease updater**:

- Reads role state file every 2 seconds
- When role is `writer:N`, patches the Kubernetes Lease with holder identity and generation
- When not writer, does nothing

**Replica sync**:

- Reads role state file every 5 seconds
- When role is `replica`, executes sync command (e.g., SSH-based rsqlite-rsync pull)
- On success, writes freshness ledger with source node and generation from lease

## Failover Behavior

### Normal Operation

1. One pod holds the lease and is writer
2. Other pods are replicas, continuously syncing
3. Service routes traffic only to writer (via readiness probe)

### When Writer Pod Dies

1. Lease expires (no renewal)
2. Another pod detects lease available
3. That pod checks its freshness ledger
4. If fresh enough, promotes to writer
5. Readiness probe returns 200, receives traffic
6. Starts renewing lease (via sidecar)

### When Network Partitions Writer

1. Writer can't renew lease (no k8s API access)
2. Lease expires from perspective of other pods
3. Writer demotes itself after TTL expires (fail-safe)
4. Another pod promotes
5. Old writer rejoins, sees newer generation, stays replica

### Promotion Denied Cases

Logged in audit log with specific violations:

- `LeaseExpired` — Lease expired before promotion could occur
- `NotLeaseHolder` — Lease held by different node
- `MissingFreshness` — No freshness ledger found
- `StaleFreshness` — Last sync too old
- `FreshnessFromFuture` — Clock skew detected
- `LineageTooOld` — Source generation below minimum

## Operational Best Practices

1. **Lease renewal interval**: Keep it well below `leaseDurationSeconds` (e.g., renew every 2s with 15s lease)
2. **Tick interval**: 250-1000ms balances responsiveness and overhead
3. **Freshness age**: Set based on RPO (Recovery Point Objective) — 10s is aggressive, 60s is more relaxed
4. **Monitor audit log**: Watch for repeated `promotion_denied` entries indicating sync issues
5. **StatefulSet over Deployment**: Stable pod identities help lease management
6. **Test failover**: Regularly delete writer pod and verify promotion
7. **Clock sync**: Use NTP to keep nodes synchronized (freshness validation depends on it)

## Troubleshooting

### No pod becomes writer

**Check:**

- Does lease exist and is held by a pod?
- Do pods have RBAC permission to read lease?
- Check audit logs for `promotion_denied` entries
- Verify freshness ledger exists and is recent

### Multiple writers

**This should never happen** if HA is working correctly. If it does:

- Check that all pods are running HA controller
- Verify lease source is consistent across pods
- Check for clock skew causing freshness validation bypass
- Review audit logs for unexpected `enable_writer` entries

### Promotion denied with StaleFreshness

**Fix:**

- Replica sync is failing or too slow
- Check replica-sync sidecar logs
- Increase `--ha-max-freshness-age-secs` (trades safety for availability)
- Verify network connectivity between replicas and writer

### Frequent demotion/promotion cycling

**Fix:**

- Lease TTL too short relative to renewal interval
- Network instability preventing lease renewal
- Increase `leaseDurationSeconds` and ensure renewal happens 2-3x within TTL

## Example Deployment Manifests

Complete working examples are in the repository:

1. **[examples/k8s/ha-deployment.yaml](../examples/k8s/ha-deployment.yaml)** — Basic deployment with lease manager RBAC
2. **[examples/k8s/ha-deployment-readonly.yaml](../examples/k8s/ha-deployment-readonly.yaml)** — Deployment with read-only RBAC (external lease manager)
3. **[examples/k8s/k3s-ha-stack.yaml](../examples/k8s/k3s-ha-stack.yaml)** — Complete k3s stack with sidecars

### Apply k3s Stack

```bash
RSQLITE_RSYNC_IMAGE=your-registry/rsqlite-rsync:latest \
RSQLITE_RSYNC_REPLICA_SYNC_COMMAND='rsqlite-rsync user@sqlite-ha-writer:/var/lib/sqlite/app.db /var/lib/sqlite/app.db' \
./scripts/apply-k3s-ha-stack.sh
```

Optional environment variables:

- `RSQLITE_RSYNC_NAMESPACE` (default: `sqlite-ha`)
- `RSQLITE_RSYNC_STORAGE_CLASS` (default: `local-path`)

## Summary

The HA feature provides production-ready active-passive SQLite replication with:

✅ **Single-writer guarantee** via external lease coordination  
✅ **Automatic failover** when writer becomes unavailable  
✅ **Safety checks** preventing stale replica promotion  
✅ **Kubernetes-native** with probe endpoints and lease integration  
✅ **Flexible deployment** supporting file-based or Kubernetes leases  
✅ **Comprehensive observability** through role state, audit logs, and HTTP probes

Start with the minimal command for local testing, then move to Kubernetes deployment using the provided manifests and sidecars for production use.

## Related Documentation

- [Kubernetes HA Deployment Guide](ha-kubernetes.md)
- [k3s HA Runbook](k3s-ha-runbook.md)
- [Wire Protocol Documentation](protocol.md)
