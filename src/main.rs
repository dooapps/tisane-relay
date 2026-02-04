use std::net::SocketAddr;
use std::time::Duration;
use std::sync::Arc;

use axum::{
    extract::{State, Query}, 
    routing::{get, post}, 
    Json, Router, response::IntoResponse, 
    http::{StatusCode, HeaderMap}
};
use clap::{Parser, Subcommand};
use infusion::infusion::sign;
use infusion::infusion::cid::cid_blake3;
use ed25519_dalek::VerifyingKey;
use hex;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{info, error, warn};
use uuid::Uuid;

use tisane_relay::db;
use tisane_relay::utils::compute_payload_hash;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the relay server
    Serve {
        /// Port to bind to (or use PORT env var)
        #[arg(long, env = "PORT", default_value_t = 8080)]
        port: u16,

        /// Postgres database URL (or use DATABASE_URL env var)
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Unique ID for this relay (if not provided, one is generated randomly)
        #[arg(long, env = "RELAY_ID")]
        relay_id: Option<Uuid>,
    },
    /// Add a new peer
    AddPeer {
        /// Peer URL (e.g., http://peer-relay:8080)
        #[arg(long)]
        url: String,
        /// Shared secret for authentication
        #[arg(long)]
        secret: String,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// List all peers
    ListPeers {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Remove a peer
    RemovePeer {
        /// Peer ID to remove
        #[arg(long)]
        peer_id: Uuid,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    relay_id: Uuid,
}

#[derive(Deserialize)]
struct PullQuery {
    since: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct PullResp {
    events: Vec<db::Event>,
    next_cursor: i64,
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status":"ok"})))
}

// Reusable logic to validate and insert events
async fn validate_and_insert(pool: &PgPool, mut events: Vec<db::EventInput>) -> Result<Vec<i64>, (StatusCode, String)> {
    for ev in &mut events {
        // 1. Calculate payload_hash via Infusion (canonical hash)
        ev.payload_hash = compute_payload_hash(&ev.payload_json);

        // 2. Validate signature using Infusion
        let pubkey_bytes = hex::decode(&ev.author_pubkey)
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid author_pubkey hex".to_string()))?;
        let sig_bytes = hex::decode(&ev.signature)
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid signature hex".to_string()))?;

        let vk = VerifyingKey::from_bytes(&pubkey_bytes.try_into().unwrap_or([0u8; 32]))
            .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid public key".to_string()))?;

        let sig_array: [u8; 64] = sig_bytes.try_into()
            .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid signature length".to_string()))?;

        let payload_bytes = if let Some(p) = ev.payload_json.as_ref() {
            p.to_string().into_bytes()
        } else {
            vec![]
        };

        if let Err(_) = sign::verify(&vk, &payload_bytes, &sig_array) {
           return Err((StatusCode::UNAUTHORIZED, "invalid signature".to_string()));
        }
    }

    match db::insert_events(pool, &events).await {
        Ok(inserted) => Ok(inserted),
        Err(e) => {
            error!("insert error: {}", e);
            Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    }
}

async fn push_handler(State(state): State<AppState>, Json(events): Json<Vec<db::EventInput>>) -> impl IntoResponse {
    const MAX_BATCH_SIZE: usize = 100;
    if events.len() > MAX_BATCH_SIZE {
        return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "batch size exceeds limit (100)"}))).into_response();
    }
    // Validation Logic for Value Protocol
    for ev in &events {
        if let Some(etype) = &ev.event_type {
            match etype.as_str() {
                "read.completed" | "derivative.created" | "citation.created" | "value.snapshot" => {
                    // Strong validation for Value Protocol events
                    if let Some(payload) = &ev.payload_json {
                        let has_content_id = payload.get("content_id")
                            .and_then(|v| v.as_str())
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);
                        
                        if !has_content_id {
                             return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                                 "error": format!("missing or empty content_id for event type '{}'", etype)
                             }))).into_response();
                        }

                        // Additional validation for value.snapshot
                        if etype == "value.snapshot" {
                            let has_window = payload.get("window_start").is_some() && payload.get("window_end").is_some();
                            if !has_window {
                                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                                    "error": "missing window_start or window_end for value.snapshot"
                                }))).into_response();
                            }
                        }
                    } else {
                         return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                             "error": format!("missing payload for event type '{}'", etype)
                         }))).into_response();
                    }
                },
                _ => {
                    // Legacy/Other events: Accepted without strict schema validation (as per requirements)
                }
            }
        }
    }

    match validate_and_insert(&state.pool, events).await {
        Ok(inserted) => (StatusCode::OK, Json(serde_json::json!({"inserted": inserted.len()}))).into_response(),
        Err((code, msg)) => (code, Json(serde_json::json!({"error": msg}))).into_response(),
    }
}

async fn pull_handler(State(state): State<AppState>, Query(q): Query<PullQuery>) -> impl IntoResponse {
    let since = q.since.unwrap_or(0);
    let limit = q.limit.unwrap_or(100);
    match db::fetch_events_since(&state.pool, since, limit).await {
        Ok((events, next_cursor)) => (StatusCode::OK, Json(PullResp{ events, next_cursor })).into_response(),
        Err(e) => {
            error!("pull error: {}", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response()
        }
    }
}

// ----- REPLICATION HANDLERS -----

async fn replicate_handler(
    State(state): State<AppState>, 
    headers: HeaderMap, 
    Json(events): Json<Vec<db::EventInput>>
) -> impl IntoResponse {
    // 1. Peer Auth
    let token = match headers.get("X-Peer-Token") {
        Some(v) => v.to_str().unwrap_or(""),
        None => return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "missing X-Peer-Token"}))).into_response(),
    };

    let peer = match db::validate_peer_token(&state.pool, token).await {
        Ok(Some(p)) => p,
        Ok(None) => return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({"error": "invalid peer token"}))).into_response(),
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    };

    // 2. Loop Prevention
    if let Some(relay_id_val) = headers.get("X-Relay-Id") {
        if let Ok(rid) = relay_id_val.to_str() {
            if rid == state.relay_id.to_string() {
                 warn!("Loop detected from peer {}", peer.peer_id);
                 return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "loop detected: my own relay id"}))).into_response();
            }
        }
    }

    if let Some(hop_val) = headers.get("X-Hop") {
        if let Ok(hop_str) = hop_val.to_str() {
            if let Ok(hops) = hop_str.parse::<i32>() {
                if hops > 3 {
                    return (StatusCode::BAD_REQUEST, Json(serde_json::json!({"error": "max hops exceeded"}))).into_response();
                }
            }
        }
    }

    // 3. Process Events
    match validate_and_insert(&state.pool, events).await {
        Ok(inserted) => (StatusCode::OK, Json(serde_json::json!({"inserted": inserted.len()}))).into_response(),
        Err((code, msg)) => (code, Json(serde_json::json!({"error": msg}))).into_response(),
    }
}

async fn peers_handler(State(state): State<AppState>) -> impl IntoResponse {
    match db::fetch_healthy_peers(&state.pool).await {
        Ok(peers) => (StatusCode::OK, Json(peers)).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()}))).into_response(),
    }
}

// ----- BACKGROUND WORKER -----

async fn replication_worker(state: AppState) {
    info!("Helper: Replication worker started with Relay ID: {}", state.relay_id);
    let client = reqwest::Client::new();

    loop {
        tokio::time::sleep(Duration::from_secs(5)).await;

        let peers = match db::fetch_healthy_peers(&state.pool).await {
            Ok(p) => p,
            Err(e) => {
                error!("Worker failed to fetch peers: {}", e);
                continue;
            }
        };

        for peer in peers {
            // Fetch batch to send
            let events_to_send = match db::fetch_replication_batch(&state.pool, peer.last_cursor_time, peer.last_cursor_id, 50).await {
                Ok(evs) => evs,
                Err(e) => {
                    error!("Failed to fetch replication batch for {}: {}", peer.peer_id, e);
                    continue;
                }
            };

            if events_to_send.is_empty() {
                continue;
            }

            // Convert DB events back to EventInput for transport (simplification for MVP)
            // Ideally we transfer specific replication DTOs
            let payload: Vec<db::EventInput> = events_to_send.iter().map(|e| db::EventInput {
                event_id: e.event_id,
                author_pubkey: e.author_pubkey.clone(),
                signature: e.signature.clone(),
                payload_hash: e.payload_hash.clone(),
                device_id: e.device_id.clone(),
                author_id: e.author_id.clone(),
                content_id: e.content_id.clone(),
                event_type: e.event_type.clone(),
                payload_json: e.payload_json.clone(),
                occurred_at: e.occurred_at,
                lamport: e.lamport,
            }).collect();

            // Send via POST
            let res = client.post(format!("{}/relay/replicate", peer.url))
                .header("X-Peer-Token", &peer.shared_secret)
                .header("X-Relay-Id", state.relay_id.to_string())
                .header("X-Hop", "1")
                .json(&payload)
                .send()
                .await;

            match res {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let last = events_to_send.last().unwrap();
                        // Update cursor
                        if let Err(e) = db::update_peer_cursor(
                            &state.pool, 
                            peer.peer_id, 
                            last.occurred_at.unwrap_or(chrono::Utc::now()), 
                            last.event_id
                        ).await {
                            error!("Failed to update cursor for peer {}: {}", peer.peer_id, e);
                        } else {
                            info!("Replicated {} events to peer {}", events_to_send.len(), peer.peer_id);
                        }
                    } else {
                        warn!("Replication failed for peer {}: Status {}", peer.peer_id, resp.status());
                    }
                },
                Err(e) => {
                    warn!("Replication request failed for peer {}: {}", peer.peer_id, e);
                }
            }
        }
    }
}

async fn serve_command(port: u16, database_url: String, relay_id_opt: Option<Uuid>) -> anyhow::Result<()> {
    // Use provided ID or generate random one
    let relay_id = relay_id_opt.unwrap_or_else(Uuid::new_v4);

    info!("connecting to database: {}", database_url);
    let pool = PgPool::connect(&database_url).await?;

    info!("running migrations");
    db::run_migrations(&pool).await?;

    let state = AppState { 
        pool,
        relay_id
    };

    // Spawn replication worker
    let worker_state = state.clone();
    tokio::spawn(async move {
        replication_worker(worker_state).await;
    });

    let app = Router::new()
        .route("/health", get(health))
        .route("/relay/push", post(push_handler))
        .route("/relay/pull", get(pull_handler))
        .route("/relay/replicate", post(replicate_handler))
        .route("/relay/peers", get(peers_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("server (ID: {}) listening on {}", relay_id, addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}

async fn add_peer_command(url: String, secret: String, database_url: String) -> anyhow::Result<()> {
    let pool = PgPool::connect(&database_url).await?;
    let id = db::add_peer(&pool, url.clone(), secret).await?;
    println!("Added peer {} with ID {}", url, id);
    Ok(())
}

async fn list_peers_command(database_url: String) -> anyhow::Result<()> {
    let pool = PgPool::connect(&database_url).await?;
    let peers = db::fetch_all_peers(&pool).await?;
    println!("{:<36} | {:<30} | {:<10}", "ID", "URL", "Health");
    println!("{}", "-".repeat(80));
    for p in peers {
        println!("{} | {:<30} | {}", p.peer_id, p.url, p.health);
    }
    Ok(())
}

async fn remove_peer_command(peer_id: Uuid, database_url: String) -> anyhow::Result<()> {
    let pool = PgPool::connect(&database_url).await?;
    if db::remove_peer(&pool, peer_id).await? {
        println!("Removed peer {}", peer_id);
    } else {
        println!("Peer {} not found", peer_id);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    
    match args.command {
        Commands::Serve { port, database_url, relay_id } => {
            serve_command(port, database_url, relay_id).await?;
        },
        Commands::AddPeer { url, secret, database_url } => {
            add_peer_command(url, secret, database_url).await?;
        },
        Commands::ListPeers { database_url } => {
            list_peers_command(database_url).await?;
        },
        Commands::RemovePeer { peer_id, database_url } => {
            remove_peer_command(peer_id, database_url).await?;
        }
    }

    Ok(())
}
