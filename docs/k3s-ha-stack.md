### Explanation

This Kubernetes manifest deploys a three-pod SQLite HA StatefulSet. It uses a Kubernetes Lease for writer election, shared files for role and freshness state, sidecars for lease management, replica synchronization, and writer-label updates, plus Services for general pod discovery and current-writer routing.

The ServiceAccount and RBAC resources allow the sidecars to manage the writer Lease and update pod labels. The `lease-updater.sh` sidecar renews the current writer's Lease or attempts to claim an expired Lease using Kubernetes optimistic concurrency. The main `rsqlite-rsync` process independently validates freshness and writer status, so Lease ownership and the `role=writer` label are routing mechanisms rather than the final write-safety authority.

Each replica periodically synchronizes databases from the active writer over authenticated gRPC and records freshness metadata. The headless `sqlite-ha` Service provides stable per-pod DNS for discovery, while `sqlite-ha-writer` routes to the pod currently labeled as the writer. Requests are still fenced at the RPC layer and can receive a `NOT_LEADER` response if routing is stale.

The manifest uses `hostPath` storage for SQLite data, `emptyDir` volumes for runtime state and logs, and probes on ports 8088 and 50051 for liveness, readiness, and gRPC traffic. The image and host data directory values are placeholders that must be substituted before deployment.

### Class diagram

```mermaid
classDiagram
    class StatefulSet_sqlite_ha {
        replicas: 3
        serviceName: sqlite-ha
        podManagementPolicy: Parallel
    }

    class SqliteHaPod {
        app: sqlite-ha
        podName: POD_NAME
        namespace: POD_NAMESPACE
        roleStateFile: role_state.txt
        freshnessFile: freshness.txt
        dataDir: /var/lib/sqlite
    }

    class RsqliteRsync {
        role: writer or replica
        grpcPort: 50051
        probePort: 8088
        validatePromotion()
        isWriter()
        serveGrpc()
    }

    class LeaseUpdater {
        leaseName: sqlite-writer-lease
        leaseDuration: 15s
        tryClaim()
        renewLease()
    }

    class ReplicaSync {
        syncInterval: 5s
        syncDatabases()
        updateFreshness()
    }

    class LabelUpdater {
        desiredRole: writer or replica
        updatePodLabel()
    }

    class KubernetesLease {
        holderIdentity: pod name
        generation: integer
        renewTime: timestamp
        leaseDurationSeconds: integer
    }

    class KubernetesService {
        name: sqlite-ha
        type: Headless
        selector: app=sqlite-ha
        ports: 8088, 50051
    }

    class WriterService {
        name: sqlite-ha-writer
        selector: app=sqlite-ha, role=writer
        ports: 8088, 50051
    }

    class RoleState {
        state: writer:generation or replica
    }

    class FreshnessLedger {
        sourceNodeId: string
        sourceGeneration: integer
        syncedAtSeconds: integer
    }

    class SqliteDataVolume {
        path: /var/lib/sqlite
        storage: hostPath
    }

    class ServiceAccount {
        name: sqlite-ha
    }

    class RbacRole {
        name: sqlite-ha-lease-manager
        permissions: lease and pod get/patch/update
    }

    StatefulSet_sqlite_ha "1" *-- "3" SqliteHaPod : creates
    SqliteHaPod *-- RsqliteRsync : main container
    SqliteHaPod *-- LeaseUpdater : sidecar
    SqliteHaPod *-- ReplicaSync : sidecar
    SqliteHaPod *-- LabelUpdater : sidecar

    RsqliteRsync --> RoleState : writes role
    RsqliteRsync --> FreshnessLedger : validates freshness
    LeaseUpdater --> RoleState : reads
    LeaseUpdater --> KubernetesLease : claims and renews
    ReplicaSync --> RoleState : reads
    ReplicaSync --> KubernetesLease : discovers writer
    ReplicaSync --> FreshnessLedger : writes after sync
    LabelUpdater --> RoleState : reads
    LabelUpdater --> SqliteHaPod : patches role label

    KubernetesService --> SqliteHaPod : routes all pods
    WriterService --> SqliteHaPod : routes role=writer
    RsqliteRsync --> WriterService : serves gRPC
    ReplicaSync --> RsqliteRsync : native gRPC replication

    SqliteHaPod *-- RoleState : shared emptyDir
    SqliteHaPod *-- FreshnessLedger : shared emptyDir
    SqliteHaPod *-- SqliteDataVolume : mounts
    SqliteHaPod --> ServiceAccount : uses
    ServiceAccount --> RbacRole : bound by RoleBinding
    RbacRole --> KubernetesLease : manages
    RbacRole --> SqliteHaPod : patches labels
```
### Sequence diagram

```mermaid
sequenceDiagram
    participant PodA as Candidate Pod A
    participant PodB as Candidate Pod B
    participant Lease as Kubernetes Lease API
    participant State as Local role/freshness state
    participant Writer as Current Writer
    participant Replica as Replica Sync Process
    participant Client as gRPC Client

    loop Every lease renewal interval
        PodA->>State: Read role_state.txt
        alt Pod A is writer
            PodA->>Lease: Patch holder, renewTime, generation
        else Pod A is replica (lease missing)
            PodA->>Lease: Read Lease
            PodA->>Lease: Create Lease
            Lease-->>PodA: Created if first writer
            PodA->>State: Record generation 0 freshness
        else Pod A is replica (lease expired)
            PodA->>Lease: Read Lease
            PodA->>Lease: Replace Lease with resourceVersion
            Lease-->>PodA: Success or 409 conflict
        end
    end

    PodB->>Lease: Compete to create or replace expired Lease
    Lease-->>PodB: Success or conflict, losing candidate remains replica

    loop Replica synchronization interval
        Replica->>State: Read role_state.txt
        alt Replica role
            Replica->>Lease: Read holderIdentity and generation
            Replica->>Writer: Query databases and sync over authenticated gRPC
            Writer-->>Replica: Database list and replication data
            Replica->>State: Write freshness ledger
        else Writer role
            Replica->>Replica: Skip synchronization
        end
    end

    loop Role transition
        PodA->>State: rsqlite-rsync updates role_state.txt
        PodA->>State: label-updater reads role
        PodA->>Lease: Patch own Pod role label
    end

    Client->>Writer: Send gRPC request
    Writer-->>Client: Validate writer fencing on every RPC
    alt Request reaches non-writer or stale route
        Writer-->>Client: NOT_LEADER redirect
        Client->>Lease: Resolve current holder
        Client->>Writer: Retry against current writer
    end
```

### Flow diagram

```mermaid
flowchart TD
    A[Each StatefulSet pod starts] --> B[rsqlite-rsync HA loop reads role_state.txt]
    B --> C{Local role is writer?}

    C -->|Yes| D[Renew Kubernetes Lease
with holderIdentity and generation]
    D --> E[Lease remains authoritative
for writer discovery]

    C -->|No| F[Try to claim Lease]
    F --> G{Lease exists?}
    G -->|No| H[Create Lease with generation 0]
    G -->|Yes| I{Lease expired?}
    I -->|No| J[Remain replica]
    I -->|Yes| K[Replace Lease using resourceVersion]
    K --> L{Kubernetes CAS succeeds?}
    L -->|Yes| M[Pod becomes Lease holder candidate]
    L -->|No| J
    H --> M
    M --> N{Freshness ledger valid?}
    N -->|Yes| O[Promote rsqlite-rsync to writer]
    N -->|No| P[Remain replica; Lease later expires]

    B --> Q[replica-sync loop]
    Q --> R{Local role is writer?}
    R -->|Yes| S[Skip synchronization]
    R -->|No| T[Read Lease holderIdentity]
    T --> U{Active writer exists?}
    U -->|No| V[Wait for next sync interval]
    U -->|Yes| W[Discover writer via pod DNS]
    W --> X[Query database list or use configured list]
    X --> Y[Pull databases over authenticated gRPC]
    Y --> Z[Update freshness ledger]
    Z --> AA[Remove stale local databases if still replica]

    B --> AB[label-updater loop]
    AB --> AC[Read role_state.txt]
    AC --> AD{Role changed?}
    AD -->|Yes| AE[Patch pod role label]
    AD -->|No| AF[Wait and poll again]
    AE --> AG[sqlite-ha-writer Service routes to role=writer]

    E --> AH[Client discovers writer through Lease or writer Service]
    AG --> AH
    AH --> AI[Client sends gRPC request]
    AI --> AJ{RPC write validation passes?}
    AJ -->|Yes| AK[Handle request as writer]
    AJ -->|No| AL[Return NOT_LEADER redirect]
    AL --> AH
```
