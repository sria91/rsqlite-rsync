//! Server-side gRPC Replication Service implementation.
//!
//! [`ReplicationServer`] implements the [`ReplicationService`] gRPC trait,
//! providing streaming SQLite delta synchronization over HTTP/2.

use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use libsqlite3_sys as ffi;
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::SyncTuning;
use crate::db::Connection;
use crate::gateway::auth::AuthConfig;
use crate::gateway::engine::DatabaseEngine;
use crate::ha::HaSharedState;
use crate::proto::rsqlite::v1::replication_service_server::ReplicationService;
use crate::proto::rsqlite::v1::sync_init::ClientRole;
use crate::proto::rsqlite::v1::{SyncMessage, sync_message};
use crate::protocol::messages::Message;
use crate::protocol::{origin, replica};
use crate::snapshot::Snapshot;
use crate::transport::Transport;
use crate::transport::grpc::{DEFAULT_GRPC_CHANNEL_CAPACITY, GrpcTransport};

/// gRPC Replication Service server handler.
#[derive(Clone)]
pub struct ReplicationServer {
    engine: DatabaseEngine,
    auth: AuthConfig,
    tuning: SyncTuning,
    ha_state: Option<Arc<RwLock<HaSharedState>>>,
}

impl ReplicationServer {
    /// Create a new `ReplicationServer`.
    pub fn new(
        engine: DatabaseEngine,
        auth: AuthConfig,
        tuning: SyncTuning,
        ha_state: Option<Arc<RwLock<HaSharedState>>>,
    ) -> Self {
        Self {
            engine,
            auth,
            tuning,
            ha_state,
        }
    }
}

#[tonic::async_trait]
impl ReplicationService for ReplicationServer {
    type SyncStream =
        Pin<Box<dyn Stream<Item = std::result::Result<SyncMessage, Status>> + Send + 'static>>;

    async fn sync(
        &self,
        request: Request<Streaming<SyncMessage>>,
    ) -> std::result::Result<Response<Self::SyncStream>, Status> {
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();

        // The first message in the stream must be SyncInit
        let first_msg = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("failed to receive initial sync message: {e}")))?
            .ok_or_else(|| {
                Status::invalid_argument("client closed stream before sending SyncInit")
            })?;

        let init = match first_msg.payload {
            Some(sync_message::Payload::Init(init)) => init,
            _ => {
                return Err(Status::invalid_argument(
                    "first message on replication stream must be SyncInit",
                ));
            }
        };

        // Authenticate request using headers or explicit SyncInit token
        self.auth
            .check_request_or_token(&metadata, Some(&init.auth_token))?;

        // Validate target database path
        let db_path = self.engine.resolve_db_path(&init.database).map_err(|e| {
            Status::invalid_argument(format!("invalid database name '{}': {e}", init.database))
        })?;

        let client_role = match ClientRole::try_from(init.client_role) {
            Ok(ClientRole::Replica) => ClientRole::Replica,
            Ok(ClientRole::Origin) => ClientRole::Origin,
            _ => {
                return Err(Status::invalid_argument(
                    "client_role must be CLIENT_ROLE_REPLICA or CLIENT_ROLE_ORIGIN",
                ));
            }
        };

        if let (ClientRole::Origin, Some(ha_state)) = (client_role, &self.ha_state) {
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let state = ha_state.read().unwrap_or_else(|p| p.into_inner());
            if state.is_writer(now_secs) {
                return Err(Status::failed_precondition(
                    "cannot push replication to an active writer node",
                ));
            }
        }

        let (outgoing_tx, outgoing_rx) =
            mpsc::channel::<SyncMessage>(DEFAULT_GRPC_CHANNEL_CAPACITY);
        let mut transport = GrpcTransport::new(outgoing_tx, inbound);
        let tuning = self.tuning.clone();
        let db_name = init.database.clone();

        match client_role {
            ClientRole::Replica => {
                // Client is Replica -> Server is Origin (Pull Sync)
                tokio::spawn(async move {
                    let origin_conn = match Connection::open(&db_path, ffi::SQLITE_OPEN_READONLY) {
                        Ok(conn) => conn,
                        Err(e) => {
                            tracing::error!(database = %db_name, error = %e, "failed to open origin database");
                            let _ = transport
                                .send(&Message::Error {
                                    message: e.to_string(),
                                })
                                .await;
                            let _ = transport.close().await;
                            return;
                        }
                    };

                    let snap = match Snapshot::begin(&origin_conn) {
                        Ok(snap) => snap,
                        Err(e) => {
                            tracing::error!(database = %db_name, error = %e, "failed to begin snapshot on origin database");
                            let _ = transport
                                .send(&Message::Error {
                                    message: e.to_string(),
                                })
                                .await;
                            let _ = transport.close().await;
                            return;
                        }
                    };

                    let run_res = origin::run_with_tuning(&snap, &mut transport, &tuning).await;
                    if let Err(ref e) = run_res {
                        tracing::error!(database = %db_name, error = %e, "server origin sync failed");
                        let _ = transport
                            .send(&Message::Error {
                                message: e.to_string(),
                            })
                            .await;
                    }
                    let _ = snap.commit();
                    let _ = transport.close().await;
                });
            }
            ClientRole::Origin => {
                // Client is Origin -> Server is Replica (Push Sync)
                tokio::spawn(async move {
                    let replica_conn = match Connection::open(
                        &db_path,
                        ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                    ) {
                        Ok(conn) => conn,
                        Err(e) => {
                            tracing::error!(database = %db_name, error = %e, "failed to open/create replica database");
                            let _ = transport
                                .send(&Message::Error {
                                    message: e.to_string(),
                                })
                                .await;
                            let _ = transport.close().await;
                            return;
                        }
                    };

                    let run_res =
                        replica::run_with_tuning(&replica_conn, &mut transport, &tuning).await;
                    if let Err(ref e) = run_res {
                        tracing::error!(database = %db_name, error = %e, "server replica sync failed");
                        let _ = transport
                            .send(&Message::Error {
                                message: e.to_string(),
                            })
                            .await;
                    }
                    let _ = transport.close().await;
                });
            }
            _ => unreachable!(),
        }

        let stream = ReceiverStream::new(outgoing_rx).map(Ok);
        Ok(Response::new(Box::pin(stream) as Self::SyncStream))
    }
}
