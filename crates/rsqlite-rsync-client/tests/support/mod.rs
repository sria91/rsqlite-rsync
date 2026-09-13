//! A scriptable, in-process mock of the `SqlGateway` gRPC service, bound to
//! real `127.0.0.1:0` loopback TCP (no subprocess, no SQLite) so the client
//! crate's retry/discovery/failover logic can be tested deterministically
//! and fast, independent of the real SQLite-backed gateway implementation.

#![allow(dead_code)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_stream::Stream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use rsqlite_rsync_proto::metadata::{
    CODE_NOT_LEADER, HEADER_RSQLITE_CODE, HEADER_RSQLITE_LEADER_ENDPOINT,
};
use rsqlite_rsync_proto::rsqlite::v1::sql_gateway_server::{
    SqlGateway, SqlGatewayServer as TonicSqlGatewayServer,
};
use rsqlite_rsync_proto::rsqlite::v1::{
    BatchRequest, BatchResponse, ClusterStatusRequest, ClusterStatusResponse, ExecuteRequest,
    ExecuteResponse, NodeRole, QueryChunk, QueryRequest, QueryResponse,
};

/// A scripted response for one mock RPC call.
#[derive(Clone, Debug)]
pub enum Behavior {
    /// Succeed with a plausible canned response.
    Ok,
    /// Fail as `FailedPrecondition`/`NOT_LEADER`, optionally redirecting to
    /// `leader_endpoint`.
    NotLeader { leader_endpoint: Option<String> },
    /// Fail as `FailedPrecondition`/`NOT_LEADER` with an empty
    /// leader-endpoint metadata value, to exercise the malformed-header
    /// fallback path (a validly-constructed ASCII metadata value can never
    /// fail `to_str()`, so "empty" is the meaningful malformed case here).
    NotLeaderRaw { header_value: &'static str },
    /// Fail with an arbitrary status code and message.
    Fail(Code, &'static str),
    /// Sleep past the caller's timeout, surfacing as `DeadlineExceeded`.
    Hang(Duration),
}

/// Which RPC a [`Call`] was made against.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Method {
    Execute,
    Query,
    StreamQuery,
    Batch,
    GetClusterStatus,
}

/// A snapshot of one recorded call's request, for round-trip assertions.
#[derive(Clone, Debug)]
pub enum RequestSnapshot {
    Execute(ExecuteRequest),
    Query(QueryRequest),
    StreamQuery(QueryRequest),
    Batch(BatchRequest),
    GetClusterStatus,
}

#[derive(Clone, Debug)]
pub struct Call {
    pub method: Method,
    pub request: RequestSnapshot,
}

struct MockState {
    script: VecDeque<Behavior>,
    default: Behavior,
    calls: Vec<Call>,
    role: NodeRole,
    node_id: String,
}

/// A cloneable handle to a scriptable mock `SqlGateway` implementation.
#[derive(Clone)]
pub struct MockGateway(Arc<Mutex<MockState>>);

impl MockGateway {
    fn new(role: NodeRole) -> Self {
        MockGateway(Arc::new(Mutex::new(MockState {
            script: VecDeque::new(),
            default: Behavior::Ok,
            calls: Vec::new(),
            role,
            node_id: "mock-node".to_string(),
        })))
    }

    /// A mock that reports itself as the writer via `GetClusterStatus`.
    pub fn writer() -> Self {
        Self::new(NodeRole::Writer)
    }

    /// A mock that reports itself as a replica via `GetClusterStatus`.
    pub fn replica() -> Self {
        Self::new(NodeRole::Replica)
    }

    /// Queue behaviors to be consumed one per call, in order. Once
    /// exhausted, [`Self::default_behavior`] is used for further calls.
    pub fn script(self, behaviors: impl IntoIterator<Item = Behavior>) -> Self {
        self.0.lock().unwrap().script.extend(behaviors);
        self
    }

    /// Set the behavior used once the scripted queue is exhausted (default:
    /// [`Behavior::Ok`]).
    pub fn default_behavior(self, behavior: Behavior) -> Self {
        self.0.lock().unwrap().default = behavior;
        self
    }

    pub fn set_node_id(&self, node_id: impl Into<String>) {
        self.0.lock().unwrap().node_id = node_id.into();
    }

    pub fn call_count(&self) -> usize {
        self.0.lock().unwrap().calls.len()
    }

    pub fn calls(&self) -> Vec<Call> {
        self.0.lock().unwrap().calls.clone()
    }

    pub fn last_request(&self) -> Option<RequestSnapshot> {
        self.0
            .lock()
            .unwrap()
            .calls
            .last()
            .map(|c| c.request.clone())
    }

    fn record(&self, method: Method, request: RequestSnapshot) -> Behavior {
        let mut state = self.0.lock().unwrap();
        state.calls.push(Call { method, request });
        state
            .script
            .pop_front()
            .unwrap_or_else(|| state.default.clone())
    }

    async fn apply<T>(
        &self,
        method: Method,
        request: RequestSnapshot,
        ok: impl FnOnce() -> T,
    ) -> Result<Response<T>, Status> {
        match self.record(method, request) {
            Behavior::Ok => Ok(Response::new(ok())),
            Behavior::NotLeader { leader_endpoint } => {
                Err(not_leader_status(leader_endpoint.as_deref()))
            }
            Behavior::NotLeaderRaw { header_value } => Err(not_leader_status(Some(header_value))),
            Behavior::Fail(code, msg) => Err(Status::new(code, msg)),
            Behavior::Hang(duration) => {
                tokio::time::sleep(duration).await;
                Err(Status::deadline_exceeded("mock hang"))
            }
        }
    }
}

fn not_leader_status(leader_endpoint: Option<&str>) -> Status {
    let mut metadata = tonic::metadata::MetadataMap::new();
    if let Ok(v) = CODE_NOT_LEADER.parse() {
        metadata.insert(HEADER_RSQLITE_CODE, v);
    }
    if let Some(value) = leader_endpoint {
        if let Ok(v) = value.parse() {
            metadata.insert(HEADER_RSQLITE_LEADER_ENDPOINT, v);
        }
    }
    Status::with_metadata(
        Code::FailedPrecondition,
        "node is not the active writer",
        metadata,
    )
}

#[tonic::async_trait]
impl SqlGateway for MockGateway {
    async fn execute(
        &self,
        request: Request<ExecuteRequest>,
    ) -> Result<Response<ExecuteResponse>, Status> {
        let req = request.into_inner();
        self.apply(Method::Execute, RequestSnapshot::Execute(req), || {
            ExecuteResponse {
                rows_affected: 1,
                last_insert_rowid: 1,
                execution_time_us: 0,
                generation: 1,
            }
        })
        .await
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        let req = request.into_inner();
        self.apply(Method::Query, RequestSnapshot::Query(req), || {
            QueryResponse {
                columns: vec![],
                rows: vec![],
                total_rows: 0,
                execution_time_us: 0,
                generation: 1,
                is_replica_read: false,
            }
        })
        .await
    }

    type StreamQueryStream =
        Pin<Box<dyn Stream<Item = Result<QueryChunk, Status>> + Send + 'static>>;

    async fn stream_query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::StreamQueryStream>, Status> {
        let req = request.into_inner();
        self.apply(
            Method::StreamQuery,
            RequestSnapshot::StreamQuery(req),
            || {
                let chunk = QueryChunk {
                    columns: vec![],
                    rows: vec![],
                    is_last: true,
                    total_rows: 0,
                    execution_time_us: 0,
                };
                Box::pin(tokio_stream::iter(vec![Ok(chunk)])) as Self::StreamQueryStream
            },
        )
        .await
    }

    async fn batch(
        &self,
        request: Request<BatchRequest>,
    ) -> Result<Response<BatchResponse>, Status> {
        let req = request.into_inner();
        self.apply(Method::Batch, RequestSnapshot::Batch(req), || {
            BatchResponse {
                results: vec![],
                total_execution_time_us: 0,
                generation: 1,
                committed: true,
            }
        })
        .await
    }

    async fn get_cluster_status(
        &self,
        _request: Request<ClusterStatusRequest>,
    ) -> Result<Response<ClusterStatusResponse>, Status> {
        let (role, node_id) = {
            let state = self.0.lock().unwrap();
            (state.role, state.node_id.clone())
        };
        self.apply(
            Method::GetClusterStatus,
            RequestSnapshot::GetClusterStatus,
            || ClusterStatusResponse {
                node_id,
                role: role as i32,
                local_generation: 1,
                lease: None,
                current_leader_id: String::new(),
                current_leader_endpoint: String::new(),
                databases: vec![],
                uptime_secs: 0,
                version: "mock".to_string(),
            },
        )
        .await
    }
}

/// A running in-process mock gRPC server. Dropping it shuts the server down.
pub struct MockServer {
    addr: SocketAddr,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockServer {
    /// Bind to `127.0.0.1:0` and start serving `gateway` in the background.
    pub async fn start(gateway: MockGateway) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let svc = TonicSqlGatewayServer::new(gateway);

        tokio::spawn(async move {
            let _ = Server::builder()
                .add_service(svc)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = rx.await;
                })
                .await;
        });

        MockServer {
            addr,
            shutdown: Some(tx),
        }
    }

    /// The `http://127.0.0.1:<port>` endpoint this mock server is listening on.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
    }
}

/// A `http://127.0.0.1:<port>` endpoint guaranteed to refuse connections
/// (bound briefly, then dropped).
pub fn closed_endpoint() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    format!("http://{addr}")
}
