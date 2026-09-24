# Architecture & Component Block Diagrams

High-level block diagrams for the `rsqlite-rsync` repository using Mermaid format.

---

## 1. Complete End-to-End System Architecture

Comprehensive system map unifying application clients, connection pooling, gRPC request dispatching, distributed HA lease coordination, and the rsync delta-sync data plane.

```mermaid
flowchart TB
    %% =========================================================
    %% Application & Client Layer
    %% =========================================================
    subgraph ClientLayer["1. Applications & Client Layer"]
        direction TB
        AppAsync["Async Application (Tokio)"]
        AppSync["Sync Application (Threads)"]
        CLIRepl["CLI REPL / sql command"]

        subgraph Pools["Connection Pooling (rsqlite-rsync-pool)"]
            PoolMgr["SqlGatewayManager<br/>(bb8 / deadpool / r2d2)"]
        end

        subgraph ClientCrate["rsqlite-rsync-client"]
            Client["SqlGatewayClient<br/>• Leader Discovery Cache<br/>• Transparent Retry Loop"]
        end

        AppAsync --> PoolMgr
        AppSync --> PoolMgr
        PoolMgr --> Client
        CLIRepl --> Client
    end

    %% =========================================================
    %% Control Plane: Lease Store & Coordination
    %% =========================================================
    subgraph ControlPlane["2. Control Plane (Lease Store & Coordination)"]
        LeaseStore[("Lease Store<br/>• Kubernetes Lease API (Coordination)<br/>• File Lease Record (External Coordination)<br/>• Generation-based Write Fencing")]
    end

    %% =========================================================
    %% Cluster Nodes
    %% =========================================================
    subgraph Cluster["3. Distributed Node Cluster (High Availability)"]
        direction TB

        %% --- Node A (Leader) ---
        subgraph NodeLeader["Node A (Active Leader / Writer)"]
            direction TB
            L_GW["SqlGatewayServer (gRPC :50051)<br/>• Auth Token Validator<br/>• Write Access Gate: ALLOWED"]
            L_Engine["DatabaseEngine & rusqlite<br/>• Snapshot Isolation<br/>• WAL Mode Read/Write"]
            L_DB[("SQLite Database<br/>Primary WAL file")]
            L_HA["HaController (Role: Writer)<br/>• Renews Lease Heartbeat<br/>• Tracks Freshness Ledger"]

            L_GW -->|"Dispatches Query/Execute"| L_Engine
            L_Engine -->|"Reads & Writes"| L_DB
            L_HA -->|"Updates Freshness"| L_Engine
        end

        %% --- Node B (Replica) ---
        subgraph NodeReplica["Node B (Standby Replica)"]
            direction TB
            R_GW["SqlGatewayServer (gRPC :50051)<br/>• Auth Token Validator<br/>• Write Access Gate: REJECTED"]
            R_Engine["DatabaseEngine & rusqlite<br/>• Read-Only Snapshot Engine"]
            R_DB[("SQLite Database<br/>Replicated WAL file")]
            R_HA["HaController (Role: Replica)<br/>• Observes Lease Expiration<br/>• Fencing & Lineage Safety Check"]

            R_GW -->|"Optional Reads"| R_Engine
            R_Engine -->|"Reads Only"| R_DB
            R_HA -->|"Monitors Lag"| R_Engine
        end
    end

    %% =========================================================
    %% Data Plane: Replication Engine & Transports
    %% =========================================================
    subgraph DataPlane["4. Data Plane (Delta-Sync Replication Engine)"]
        direction LR
        SidecarOrigin["Origin Sync Agent<br/>(rsqlite-rsync snapshot::begin)"]
        SidecarReplica["Replica Sync Agent<br/>(rsqlite-rsync page applicator)"]
        
        subgraph TransportLayer["Pluggable Transport Layer"]
            Trans["Transport Trait<br/>• SshTransport (SSH Tunnel)<br/>• StdioTransport (Pipes)<br/>• LocalTransport (Memory)"]
        end

        SidecarReplica -->|"1. Send Page Hash Table"| Trans
        Trans -->|"2. Forward Hashes"| SidecarOrigin
        SidecarOrigin -->|"3. Stream Delta Chunks (BLAKE3/SHA-256)"| Trans
        Trans -->|"4. Deliver Modified Pages"| SidecarReplica
    end

    %% =========================================================
    %% Cross-Subsystem Interactions & Routing
    %% =========================================================
    Client -->|"1. gRPC Read / Write (TLS/mTLS Boundary)"| L_GW
    Client -.->|"2. Stale Write on Replica"| R_GW
    R_GW --x|"3. FAILED_PRECONDITION (Header: x-rsqlite-leader-endpoint)"| Client

    L_HA -.->|"Heartbeat Lease Renewal"| LeaseStore
    LeaseStore -.->|"Observe & Validate Lease Record (Promote)"| R_HA

    L_DB ===|"Consistent Snapshot Source"| SidecarOrigin
    SidecarReplica ===|"Apply Verified Pages & Update Ledger"| R_DB
```

---

## 2. Workspace & Crate Structure

```mermaid
graph TD
    subgraph CargoWorkspace["Cargo Workspace"]
        Proto["crates/rsqlite-rsync-proto<br/>• Protobuf schema (v1)<br/>• Generated Tonic traits & structs"]
        Client["crates/rsqlite-rsync-client<br/>• Async client (tokio/tonic)<br/>• Blocking sync wrapper<br/>• Leader discovery & retry"]
        Pool["crates/rsqlite-rsync-pool<br/>• bb8 manager<br/>• deadpool manager<br/>• r2d2 manager"]
        ServerBin["rsqlite-rsync (Binary / src/)<br/>• CLI daemon & REPL<br/>• Gateway gRPC server<br/>• HA controller<br/>• Delta-sync engine"]
    end

    Client -->|uses stubs| Proto
    Pool -->|manages| Client
    ServerBin -->|implements service| Proto
```

---

## 3. Server Architecture & Request Routing

```mermaid
flowchart TD
    ClientReq([Incoming gRPC Request<br/>Query / Execute / Batch]) --> Server[SqlGatewayServer<br/>Tonic gRPC Service Dispatcher]
    Server --> Auth[Auth Middleware<br/>Bearer Token Validation]
    Auth --> RoleGate{Write Access Gate<br/>SqlGatewayServer::check_write_access}

    RoleGate -->|Write on Replica| ErrorResp["Return gRPC FAILED_PRECONDITION<br/>Header: x-rsqlite-leader-endpoint (optional)"]
    RoleGate -->|Read OR Authorized Leader Write| Engine[DatabaseEngine<br/>Request Dispatcher]
    Engine --> SQLiteConn["rusqlite Connection Engine<br/>• In-memory / WAL file<br/>• Snapshot isolation<br/>• Concurrency control"]

    SQLiteConn --> Disk[("SQLite DB File<br/>db.sqlite + WAL")]
```

---

## 4. rsync-Style Delta Replication Flow

```mermaid
sequenceDiagram
    autonumber
    participant Replica as Replica Node
    participant Transport as Transport Layer (SSH / Stdio / Local)
    participant Origin as Origin (Leader Node)
    participant SQLite as SQLite Database (WAL)

    Replica->>Transport: Initiate Sync Session
    Transport->>Origin: Forward Sync Request
    
    Replica->>Replica: Hash local database pages
    Replica->>Transport: Send Page Hash Table
    Transport->>Origin: Deliver Replica Hashes

    Origin->>SQLite: Snapshot::begin() (Consistent Read)
    Origin->>Origin: Hash Origin pages & compute diff delta
    
    Origin->>Transport: Stream modified pages only (PageData chunks)
    Transport->>Replica: Deliver Delta Pages
    
    Replica->>Replica: Apply pages & verify negotiated hash (BLAKE3 / SHA-256)
    Replica->>Transport: Send Sync ACK & Ledger Update
    Transport->>Origin: Sync Complete
```

---

## 5. High Availability (HA) & Lease Management

```mermaid
flowchart TB
    subgraph LeaseStore["Shared Lease Store"]
        Lease["Kubernetes Lease API (Coordination) OR<br/>File Lease Record (External Coordination)<br/>(Generation-based Fencing)"]
    end

    subgraph NodeA["Node A (Leader)"]
        A_HA["HaController"]
        A_GW["Gateway Server"]
        A_DB[("SQLite Leader")]
        
        A_HA -->|"1. Heartbeat Renew"| Lease
        A_GW -->|"Accepts Writes"| A_DB
        A_HA -->|"Updates"| A_Ledger["FreshnessLedger"]
    end

    subgraph NodeB["Node B (Replica)"]
        B_HA["HaController"]
        B_GW["Gateway Server"]
        B_DB[("SQLite Replica")]
        
        B_HA -->|"2. Watch / Poll"| Lease
        B_GW -->|"Rejects Writes / NOT_LEADER"| B_DB
        B_HA -->|"Monitors Lag"| B_Ledger["FreshnessLedger"]
    end

    A_DB -.->|"External replica-sync sidecar: rsqlite-rsync (outside HA daemon)"| B_DB
    Lease -.->|"3. Lease Store supplies valid lease, B_HA validates & promotes"| B_HA
```

---

## 6. Client Library & Connection Pooling Architecture

```mermaid
graph TD
    subgraph Consumer["Application Code"]
        AsyncApp["Async Application (Tokio)"]
        SyncApp["Sync Application (Threads)"]
    end

    subgraph PoolCrate["rsqlite-rsync-pool"]
        BB8Pool["bb8::Pool"]
        DeadPool["deadpool::managed::Pool"]
        R2D2Pool["r2d2::Pool"]
        Manager["SqlGatewayManager"]
    end

    subgraph ClientCrate["rsqlite-rsync-client"]
        AsyncClient["SqlGatewayClient (Async)"]
        BlockingClient["SqlGatewayClient (Blocking)"]
        Discovery["Discovery & Failover Handler<br/>• Tracks active leader<br/>• Intercepts NOT_LEADER<br/>• Auto-retries next endpoint"]
    end

    subgraph Cluster["rsqlite-rsync Cluster"]
        Leader["gRPC Gateway (Leader Node)"]
        Replica["gRPC Gateway (Replica Node)"]
    end

    AsyncApp --> BB8Pool
    AsyncApp --> DeadPool
    SyncApp --> R2D2Pool
    AsyncApp -->|Direct usage| AsyncClient
    SyncApp -->|Direct usage| BlockingClient

    BB8Pool --> Manager
    DeadPool --> Manager
    R2D2Pool --> Manager
    Manager --> AsyncClient

    BlockingClient --> AsyncClient
    AsyncClient --> Discovery
    Discovery -->|"Writes & Reads"| Leader
```

---

## 7. Modes of Operation

`rsqlite-rsync` operates in four distinct execution modes depending on CLI flags and subcommands:

```mermaid
flowchart TD
    CLI(["rsqlite-rsync CLI Entrypoint"]) --> Switch{"Mode Selector"}

    %% Mode 1: One-Shot Sync
    Switch -->|"rsqlite-rsync <origin> <replica>"| M1["1. One-Shot Sync Mode"]
    subgraph Mode1["Direct Point-to-Point Sync"]
        M1 --> M1_Core["Sync Engine<br/>• Local or SSH Transport<br/>• Single database pair<br/>• Optional --dry-run"]
        M1_Core --> M1_Exit(["Sync completed & exits"])
    end

    %% Mode 2: Batch Sync
    Switch -->|"--batch-manifest <file>"| M2["2. Batch Sync Mode"]
    subgraph Mode2["Multi-Database Batch Sync"]
        M2 --> M2_Pool["Parallel Worker Pool<br/>• --batch-jobs N<br/>• Retry policies with backoff/jitter<br/>• Structured JSON/Text reporting"]
        M2_Pool --> M2_Exit(["All jobs finished & exits"])
    end

    %% Mode 3: HA Mode
    Switch -->|"--ha"| M3["3. High Availability Daemon Mode"]
    subgraph Mode3["Long-Running Cluster Daemon"]
        M3 --> M3_HA["HA Controller Loop<br/>• Lease observation/validation<br/>• Role: Leader vs Replica<br/>• Freshness ledger & health probes"]
        M3 --> M3_GW["Embedded gRPC SQL Gateway<br/>• Optional --ha-grpc-bind<br/>• Bearer token auth<br/>• NOT_LEADER write redirection"]
    end

    %% Mode 4: Client & SQL REPL
    Switch -->|"client / sql subcommand"| M4["4. Client CLI & SQL REPL Mode"]
    subgraph Mode4["Cluster Client & Query Tool"]
        M4 --> M4_Disc["Discovery & Routing<br/>• DNS / K8s Lease / Endpoints<br/>• Interactive SQL REPL<br/>• Scriptable 'sql' command"]
        M4_Disc --> M4_Target[("Target SQLite Cluster")]
    end
```

---

## 8. High Availability (HA) vs. Standalone (SA) Modes

Comparison between running `rsqlite-rsync` in Standalone (SA) local mode vs. a distributed High Availability (HA) cluster.

> **Security Note on Transports:** The gRPC SQL Gateway does not terminate TLS internally. Plaintext HTTP endpoints (such as `http://node-a:50051`) are suitable only within trusted network perimeters or behind TLS/mTLS termination proxies; untrusted networks must terminate TLS in front of the gateway.

```mermaid
flowchart TB
    subgraph SA["Standalone (SA) Mode (Local / Embedded)"]
        direction TB
        SA_User["Local Process / CLI / App"] -->|"Direct FFI / In-Process"| SA_Engine["Local SQLite Engine<br/>(Single Process)"]
        SA_Engine --> SA_DB[("Local SQLite DB File<br/>(Exclusive write lock)")]
        
        SA_Note["• No network overhead<br/>• No lease / coordination dependency<br/>• Single point of failure<br/>• Direct SQLite file locking (single-node)"]
    end

    subgraph HA["High Availability (HA) Mode (Distributed Cluster)"]
        direction TB
        
        subgraph HA_Clients["Clients & Apps"]
            HA_App["Application / Client"]
        end

        subgraph HA_LeaseStore["Distributed Coordination / Lease Store"]
            HA_Lease[("K8s Lease / File Lease Record<br/>(Heartbeat / Generation-based Fencing)")]
        end

        subgraph HA_Leader["Node A (Leader)"]
            L_GW["SqlGatewayServer (gRPC)<br/>• Port 50051<br/>• Bearer Token Auth"]
            L_Ctrl["HaController<br/>• Role: Leader<br/>• Renews Lease"]
            L_DB[("SQLite Database<br/>(WAL Mode - Read/Write)")]
            
            L_Ctrl -.->|"Heartbeat (Renew)"| HA_Lease
            L_GW -->|"Executes Writes & Reads"| L_DB
        end

        subgraph HA_Replica["Node B (Replica / Standby)"]
            R_GW["SqlGatewayServer (gRPC)<br/>• Port 50051<br/>• Bearer Token Auth"]
            R_Ctrl["HaController<br/>• Role: Replica<br/>• Monitors Lease & Lag"]
            R_DB[("SQLite Database<br/>(Replicated Snapshot)")]
            
            HA_Lease -.->|"Supplies Lease Record (Observe & Validate)"| R_Ctrl
            R_GW -->|"Allows Reads Only (if enabled)"| R_DB
        end

        HA_App -->|"1. Reads & Writes"| L_GW
        R_GW --x|"2. Rejects Writes (NOT_LEADER + Optional x-rsqlite-leader-endpoint)"| HA_App

        L_DB ==>|"3. External replica-sync sidecar (rsqlite-rsync)"| R_DB
    end
```

---

## 9. HA Controller State Machine & Reconcile FSM

Node lifecycle, reconciliation decisions, and safety fencing (lease expiration, freshness lag, and lineage checks).

```mermaid
stateDiagram-v2
    [*] --> Replica: Daemon Start

    state Replica {
        [*] --> PollingLease: Monitor Lease Store
        PollingLease --> CheckFreshness: Lease Expired / Missing
        CheckFreshness --> PromotionDenied: Lag > max_freshness_age OR Clock Skew
        PromotionDenied --> PollingLease: Wait for Delta Sync
        CheckFreshness --> AwaitValidLease: Freshness Valid & Lineage Verified
        AwaitValidLease --> PollingLease: Stale / Missing Lease
    }

    Replica --> Writer: Valid Lease Observed (PromoteToWriter + Adopt Lease Generation)
    
    state Writer {
        [*] --> HeartbeatRenew: Authoritative Writer
        HeartbeatRenew --> HeartbeatRenew: Heartbeat OK (KeepWriter)
        HeartbeatRenew --> FenceDetected: WriteFenceViolation
        HeartbeatRenew --> LeaseLost: TTL Expired / Network Partition
    }

    Writer --> Replica: DemoteToReplica (DisableWriter)
    Writer --> [*]: Shutdown / Graceful Stepdown
```

---

## 10. Client Transparent Leader Redirection & Failover Flow

How `rsqlite-rsync-client` and the CLI REPL intercept `NOT_LEADER` (`FAILED_PRECONDITION`) responses to transparently follow `x-rsqlite-leader-endpoint` redirection headers.

```mermaid
sequenceDiagram
    autonumber
    actor App as Application Code
    participant Client as SqlGatewayClient
    participant Replica as Node B (Replica Gateway)
    participant Leader as Node A (Leader Gateway)

    App->>Client: execute("INSERT INTO logs ...")
    Note over Client: Cached Endpoint = Node B
    Client->>Replica: gRPC ExecuteRequest
    Replica-->>Client: gRPC Status: FAILED_PRECONDITION<br/>Header: x-rsqlite-leader-endpoint: http://node-a:50051<br/>(Plaintext for trusted/mTLS networks, TLS for untrusted)
    
    Note over Client: Intercept NOT_LEADER<br/>Update Leader Cache -> Node A
    Client->>Leader: Retry gRPC ExecuteRequest
    Leader-->>Client: gRPC ExecuteResponse (rows_affected = 1)
    Client-->>App: Ok(ExecuteResult { rows_affected: 1 })
```

---

## 11. Pluggable Transport Layer Architecture

Abstraction hierarchy for data-plane sync transfers across in-memory buffers, local files, and secure SSH tunnels.

```mermaid
classDiagram
    class Transport {
        <<interface>>
        +send(msg: Message) Result~()~
        +recv() Result~Message~
        +close() Result~()~
    }

    class LocalTransport {
        -buf: VecDeque~u8~
        +pair() (LocalTransport, LocalTransport)
    }

    class StdioTransport {
        -stdin: Stdin
        -stdout: Stdout
        +from_io(read, write)
    }

    class SshTransport {
        -child: ChildProcess
        -control_path: PathBuf
        -auth_mode: SshAuthMode
        +connect(host, opts)
        +cleanup_control_path()
    }

    Transport <|.. LocalTransport : implements
    Transport <|.. StdioTransport : implements
    Transport <|.. SshTransport : implements
```

---

## 12. Batch Multi-Database Sync Pipeline

Manifest-driven parallel sync execution with thread-pool concurrency, exponential backoff with jitter, and structured run reporting.

```mermaid
flowchart LR
    Manifest["batch_manifest.json<br/>• db1.sqlite -> node2<br/>• db2.sqlite -> node3<br/>• db3.sqlite -> node4"] --> Parser[Manifest Parser]
    
    Parser --> Pool["Worker Pool (--batch-jobs N)"]
    
    subgraph Workers["Concurrent Worker Threads"]
        W1["Worker 1 (Sync db1)"]
        W2["Worker 2 (Sync db2)"]
        WN["Worker N (Sync dbN)"]
    end
    
    Pool --> W1
    Pool --> W2
    Pool --> WN
    
    W1 -.->|"On Error: Retry (Backoff + Jitter)"| W1
    
    W1 --> Aggregator[Results Aggregator]
    W2 --> Aggregator
    WN --> Aggregator
    
    Aggregator --> Report["BatchRunReport<br/>• summary JSON / text<br/>• per-DB sync duration & status<br/>• exit code 0 or partial error"]
```
