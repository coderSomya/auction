use axum::{
    extract::{Path, ws::WebSocket, WebSocketUpgrade},
    response::IntoResponse,
};
use dashmap::DashMap;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender, UnboundedReceiver};
use tokio::time::interval;
use tracing::{info, warn};
use uuid::Uuid;

/// The envelope for messages sent over the ws (client <-> server).
/// `msg_type` is an arbitrary string you can use for routing on client side.
/// `payload` is free-form JSON and can be deserialized into your typed event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsEnvelope {
    msg_type: String,
    payload: Value,
}

/// Internal message commands for the Hub
enum HubCommand {
    Join {
        room_id: String,
        user_id: Uuid,
        tx: UnboundedSender<WsEnvelope>, // outgoing to this client
    },
    Leave {
        room_id: String,
        user_id: Uuid,
    },
    SendToRoom {
        room_id: String,
        from: Uuid,
        envelope: WsEnvelope,
    },
    SendToUser {
        room_id: String,
        to: Uuid,
        envelope: WsEnvelope,
    },
}

/// A concurrency-safe hub for rooms -> user -> outgoing channel.
#[derive(Clone)]
pub struct Hub {
    /// room_id -> (user_id -> sender)
    rooms: Arc<DashMap<String, DashMap<Uuid, UnboundedSender<WsEnvelope>>>>,
    cmd_tx: UnboundedSender<HubCommand>,
}

impl Hub {
    fn new() -> Self {
        let rooms = Arc::new(DashMap::new());
        let (cmd_tx, mut cmd_rx) = unbounded_channel::<HubCommand>();
        let rooms_clone = rooms.clone();

        // Spawn the hub event loop
        tokio::spawn(async move {
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    HubCommand::Join { room_id, user_id, tx } => {
                        let user_map = rooms_clone
                            .entry(room_id.clone())
                            .or_insert_with(|| DashMap::new());
                        user_map.insert(user_id, tx);
                        info!(room = %room_id, user = %user_id, "joined room");
                    }
                    HubCommand::Leave { room_id, user_id } => {
                        if let Some(user_map) = rooms_clone.get(&room_id) {
                            user_map.remove(&user_id);
                            info!(room = %room_id, user = %user_id, "left room");
                            // remove room if empty
                            if user_map.is_empty() {
                                rooms_clone.remove(&room_id);
                                info!(room = %room_id, "removed empty room");
                            }
                        }
                    }
                    HubCommand::SendToRoom { room_id, from: _from, envelope } => {
                        if let Some(user_map) = rooms_clone.get(&room_id) {
                            for entry in user_map.iter() {
                                // best-effort send (if receiver dropped, remove user)
                                if let Err(_e) = entry.value().send(envelope.clone()) {
                                    // receiver likely dropped
                                    // can't remove from inside iteration safely; mark later (simple approach: ignore)
                                    warn!(room = %room_id, user = ?entry.key(), "failed to send to user");
                                }
                            }
                        }
                    }
                    HubCommand::SendToUser { room_id, to, envelope } => {
                        if let Some(user_map) = rooms_clone.get(&room_id) {
                            if let Some(tx) = user_map.get(&to) {
                                if let Err(_e) = tx.send(envelope) {
                                    warn!(room = %room_id, user = %to, "failed to send to user");
                                }
                            }
                        }
                    }
                }
            }
            info!("Hub command channel closed");
        });

        Self { rooms, cmd_tx }
    }

    /// Join a room (client provides its outgoing sender)
    fn join(&self, room_id: impl Into<String>, user_id: Uuid, tx: UnboundedSender<WsEnvelope>) {
        let room_id = room_id.into();
        let _ = self.cmd_tx.send(HubCommand::Join { room_id, user_id, tx });
    }

    /// Leave a room
    fn leave(&self, room_id: impl Into<String>, user_id: Uuid) {
        let room_id = room_id.into();
        let _ = self.cmd_tx.send(HubCommand::Leave { room_id, user_id });
    }

    /// Broadcast to everyone in a room
    fn send_to_room(&self, room_id: impl Into<String>, from: Uuid, envelope: WsEnvelope) {
        let room_id = room_id.into();
        let _ = self.cmd_tx.send(HubCommand::SendToRoom { room_id, from, envelope });
    }

    /// Send to a single user in a room
    fn send_to_user(&self, room_id: impl Into<String>, to: Uuid, envelope: WsEnvelope) {
        let room_id = room_id.into();
        let _ = self.cmd_tx.send(HubCommand::SendToUser { room_id, to, envelope });
    }

    /// Helper: list user ids in a room
    fn list_users(&self, room_id: &str) -> Vec<Uuid> {
        if let Some(user_map) = self.rooms.get(room_id) {
            user_map.iter().map(|e| *e.key()).collect()
        } else {
            vec![]
        }
    }
}

/// Client -> server command shape (typed)
#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", content = "data")]
enum ClientCmd {
    /// broadcast to room
    Broadcast { msg_type: String, payload: Value },
    /// send to specific user (UUID string)
    Direct { to_user: String, msg_type: String, payload: Value },
    /// ping heartbeat (can be empty)
    Ping,
    /// custom app-level command
    App { msg_type: String, payload: Value },
}

/// Helper to convert typed payloads into envelope
pub fn make_envelope(msg_type: impl Into<String>, payload: Value) -> WsEnvelope {
    WsEnvelope {
        msg_type: msg_type.into(),
        payload,
    }
}

/// WebSocket handler entry
pub async fn ws_handler(
    ws: WebSocketUpgrade,
    Path((room_id, user_id_str)): Path<(String, String)>,
    // we inject the hub via axum State normally; for simplicity, we use a global singleton here
    // but in a real app, pass Arc<Hub> via `axum::extract::Extension(Arc<Hub>)`.
    // We'll take it from a global for this example.
) -> impl IntoResponse {
    // validate/parse user id
    let user_id = match Uuid::parse_str(&user_id_str) {
        Ok(u) => u,
        Err(_) => {
            return (axum::http::StatusCode::BAD_REQUEST, "invalid user id").into_response();
        }
    };

    // capture room id as-is, and proceed to upgrade
    ws.on_upgrade(move |socket| handle_socket(socket, room_id, user_id))
}

// For convenience in this example we'll store the hub in a static.
// In production, store the hub in axum::Extension(Arc<Hub>) and extract it.
use once_cell::sync::Lazy;
pub static HUB: Lazy<Arc<Hub>> = Lazy::new(|| Arc::new(Hub::new()));

/// Get a reference to the global WebSocket hub
pub fn get_hub() -> Arc<Hub> {
    HUB.clone()
}

async fn handle_socket(socket: WebSocket, room_id: String, user_id: Uuid) {
    info!(room = %room_id, user = %user_id, "socket connected");

    // outgoing channel: hub -> this client
    let (tx, mut rx): (UnboundedSender<WsEnvelope>, UnboundedReceiver<WsEnvelope>) = unbounded_channel();

    // register in hub
    HUB.join(room_id.clone(), user_id, tx.clone());

    // Spawn a task that receives from hub (rx) and writes to WebSocket
    let (mut ws_sender, mut ws_receiver) = socket.split();

    // task: forward hub -> websocket
    let forward_outgoing = tokio::spawn(async move {
        while let Some(envelope) = rx.recv().await {
            // convert envelope to text
            match serde_json::to_string(&envelope) {
                Ok(txt) => {
                    if ws_sender.send(axum::extract::ws::Message::Text(txt)).await.is_err() {
                        // client disconnected
                        break;
                    }
                }
                Err(e) => {
                    warn!(error = %e, "failed to serialize envelope");
                }
            }
        }
        info!(user = %user_id, "outgoing forward task ended");
    });

    // task: read websocket -> handle commands
    let hub_clone = HUB.clone();
    let room_for_reader = room_id.clone();
    let read_incoming = tokio::spawn(async move {
        // We'll keep a heartbeat timer to detect dead clients (optional)
        let mut ping_interval = interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                biased;

                _ = ping_interval.tick() => {
                    // Send an application-level ping to client to keep connection alive
                    // (axum/tungstenite also supports ping/pong low-level if needed).
                    let ping_envelope = make_envelope("server_ping", serde_json::json!({}));
                    if tx.send(ping_envelope).is_err() {
                        break;
                    }
                }

                msg = ws_receiver.next() => {
                    match msg {
                        Some(Ok(axum::extract::ws::Message::Text(txt))) => {
                            // parse client command
                            match serde_json::from_str::<ClientCmd>(&txt) {
                                Ok(cmd) => {
                                    handle_client_cmd(cmd, &hub_clone, &room_for_reader, user_id).await;
                                }
                                Err(e) => {
                                    // send error back
                                    let err = make_envelope("error", serde_json::json!({ "message": format!("invalid command: {}", e) }));
                                    let _ = tx.send(err);
                                }
                            }
                        }
                        Some(Ok(axum::extract::ws::Message::Binary(_bin))) => {
                            // ignore binary in this example
                        }
                        Some(Ok(axum::extract::ws::Message::Ping(_))) => {
                            // optionally respond; axum handles this automatically
                        }
                        Some(Ok(axum::extract::ws::Message::Pong(_))) => {}
                        Some(Ok(axum::extract::ws::Message::Close(_))) => {
                            break;
                        }
                        Some(Err(e)) => {
                            warn!(error = %e, "websocket error");
                            break;
                        }
                        None => {
                            // socket closed
                            break;
                        }
                    }
                }
            } // select
        } // loop
        info!(user = %user_id, "incoming reader task ended");
    });

    // await both tasks
    let _ = tokio::join!(forward_outgoing, read_incoming);

    // cleanup
    HUB.leave(room_id, user_id);
    info!(user = %user_id, "connection handler done");
}

/// Handle typed client command and route through hub
async fn handle_client_cmd(cmd: ClientCmd, hub: &Arc<Hub>, room_id: &str, from: Uuid) {
    match cmd {
        ClientCmd::Broadcast { msg_type, payload } => {
            hub.send_to_room(room_id.to_owned(), from, make_envelope(msg_type, payload));
        }
        ClientCmd::Direct { to_user, msg_type, payload } => {
            if let Ok(to_uuid) = Uuid::parse_str(&to_user) {
                hub.send_to_user(room_id.to_owned(), to_uuid, make_envelope(msg_type, payload));
            } else {
                warn!(user=%from, "invalid target uuid for direct message");
            }
        }
        ClientCmd::Ping => {
            let env = make_envelope("pong", serde_json::json!({}));
            hub.send_to_user(room_id.to_owned(), from, env);
        }
        ClientCmd::App { msg_type, payload } => {
            // application-level messages - you can route these however you like.
            // By default, we'll broadcast the app message to the room.
            hub.send_to_room(room_id.to_owned(), from, make_envelope(msg_type, payload));
        }
    }
}

/// Helper to send a domain event into a room from server-side code.
/// You can serialize any `T: Serialize` into payload.
pub fn broadcast_to_room<T: Serialize>(room_id: &str, msg_type: &str, event: &T) {
    let payload = serde_json::to_value(event).unwrap_or_else(|_| serde_json::json!({}));
    HUB.send_to_room(room_id.to_string(), Uuid::new_v4(), make_envelope(msg_type.to_string(), payload));
}

/*

usage:

tracing_subscriber::fmt::init();

// build routes
let app = Router::new()
    .route("/ws/:room_id/:user_id", get(ws_handler));

let addr = SocketAddr::from(([127, 0, 0, 1], 6969));
info!("listening on {}", addr);
axum::Server::bind(&addr)
    .serve(app.into_make_service())
    .await
    .unwrap();

*/
