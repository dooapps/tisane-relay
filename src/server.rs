use std::net::SocketAddr;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use ed25519_dalek::VerifyingKey;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    AppState, db,
    distillery_bridge::{distribute_handler, rank_handler},
    distillery_runtime::{
        distribute_from_events_handler, feed_from_events_handler, rank_from_events_handler,
    },
    utils::compute_payload_hash,
};

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

fn distillery_app<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/health", get(health))
        .route("/distillery/distribute", post(distribute_handler))
        .route("/distillery/rank", post(rank_handler))
}

fn relay_app(state: AppState) -> Router {
    distillery_app()
        .route(
            "/distillery/feed-from-events",
            post(feed_from_events_handler),
        )
        .route(
            "/distillery/distribute-from-events",
            post(distribute_from_events_handler),
        )
        .route(
            "/distillery/rank-from-events",
            post(rank_from_events_handler),
        )
        .route("/relay/push", post(push_handler))
        .route("/relay/pull", get(pull_handler))
        .route("/relay/replicate", post(replicate_handler))
        .route("/relay/peers", get(peers_handler))
        .with_state(state)
}

async fn validate_and_insert(
    pool: &PgPool,
    mut events: Vec<db::EventInput>,
) -> Result<Vec<i64>, (StatusCode, String)> {
    for ev in &mut events {
        ev.payload_hash = compute_payload_hash(&ev.payload_json);

        let pubkey_bytes = hex::decode(&ev.author_pubkey).map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "invalid author_pubkey hex".to_string(),
            )
        })?;
        let sig_bytes = hex::decode(&ev.signature)
            .map_err(|_| (StatusCode::BAD_REQUEST, "invalid signature hex".to_string()))?;

        let vk = VerifyingKey::from_bytes(&pubkey_bytes.try_into().unwrap_or([0u8; 32]))
            .map_err(|_| (StatusCode::UNAUTHORIZED, "invalid public key".to_string()))?;

        let sig_array: [u8; 64] = sig_bytes.try_into().map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                "invalid signature length".to_string(),
            )
        })?;

        let payload_bytes = if let Some(p) = ev.payload_json.as_ref() {
            p.to_string().into_bytes()
        } else {
            vec![]
        };

        if infusion::infusion::sign::verify(&vk, &payload_bytes, &sig_array).is_err() {
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

async fn push_handler(
    State(state): State<AppState>,
    Json(events): Json<Vec<db::EventInput>>,
) -> impl IntoResponse {
    const MAX_BATCH_SIZE: usize = 100;
    if events.len() > MAX_BATCH_SIZE {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "batch size exceeds limit (100)"})),
        )
            .into_response();
    }

    for ev in &events {
        if let Some(etype) = &ev.event_type {
            match etype.as_str() {
                "read.completed" | "derivative.created" | "citation.created" | "value.snapshot" => {
                    if let Some(payload) = &ev.payload_json {
                        let has_content_id = payload
                            .get("content_id")
                            .and_then(|v| v.as_str())
                            .map(|s| !s.is_empty())
                            .unwrap_or(false);

                        if !has_content_id {
                            return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                                 "error": format!("missing or empty content_id for event type '{}'", etype)
                             }))).into_response();
                        }

                        if etype == "value.snapshot" {
                            let has_window = payload.get("window_start").is_some()
                                && payload.get("window_end").is_some();
                            if !has_window {
                                return (StatusCode::BAD_REQUEST, Json(serde_json::json!({
                                    "error": "missing window_start or window_end for value.snapshot"
                                }))).into_response();
                            }
                        }
                    } else {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(serde_json::json!({
                                "error": format!("missing payload for event type '{}'", etype)
                            })),
                        )
                            .into_response();
                    }
                }
                _ => {}
            }
        }
    }

    match validate_and_insert(&state.pool, events).await {
        Ok(inserted) => (
            StatusCode::OK,
            Json(serde_json::json!({"inserted": inserted.len()})),
        )
            .into_response(),
        Err((code, msg)) => (code, Json(serde_json::json!({"error": msg}))).into_response(),
    }
}

async fn pull_handler(
    State(state): State<AppState>,
    Query(q): Query<PullQuery>,
) -> impl IntoResponse {
    let since = q.since.unwrap_or(0);
    let limit = q.limit.unwrap_or(100);
    match db::fetch_events_since(&state.pool, since, limit).await {
        Ok((events, next_cursor)) => (
            StatusCode::OK,
            Json(PullResp {
                events,
                next_cursor,
            }),
        )
            .into_response(),
        Err(e) => {
            error!("pull error: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response()
        }
    }
}

async fn replicate_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(events): Json<Vec<db::EventInput>>,
) -> impl IntoResponse {
    let token = match headers.get("X-Peer-Token") {
        Some(v) => v.to_str().unwrap_or(""),
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "missing X-Peer-Token"})),
            )
                .into_response();
        }
    };

    let peer = match db::validate_peer_token(&state.pool, token).await {
        Ok(Some(p)) => p,
        Ok(None) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "invalid peer token"})),
            )
                .into_response();
        }
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    if let Some(relay_id_val) = headers.get("X-Relay-Id") {
        if let Ok(rid) = relay_id_val.to_str() {
            if rid == state.relay_id.to_string() {
                warn!("Loop detected from peer {}", peer.peer_id);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "loop detected: my own relay id"})),
                )
                    .into_response();
            }
        }
    }

    if let Some(hop_val) = headers.get("X-Hop") {
        if let Ok(hop_str) = hop_val.to_str() {
            if let Ok(hops) = hop_str.parse::<i32>() {
                if hops > 3 {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"error": "max hops exceeded"})),
                    )
                        .into_response();
                }
            }
        }
    }

    match validate_and_insert(&state.pool, events).await {
        Ok(inserted) => (
            StatusCode::OK,
            Json(serde_json::json!({"inserted": inserted.len()})),
        )
            .into_response(),
        Err((code, msg)) => (code, Json(serde_json::json!({"error": msg}))).into_response(),
    }
}

async fn peers_handler(State(state): State<AppState>) -> impl IntoResponse {
    match db::fetch_healthy_peers(&state.pool).await {
        Ok(peers) => (StatusCode::OK, Json(peers)).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn replication_worker(state: AppState) {
    info!(
        "Helper: Replication worker started with Relay ID: {}",
        state.relay_id
    );
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
            let events_to_send = match db::fetch_replication_batch(
                &state.pool,
                peer.last_cursor_time,
                peer.last_cursor_id,
                50,
            )
            .await
            {
                Ok(evs) => evs,
                Err(e) => {
                    error!(
                        "Failed to fetch replication batch for {}: {}",
                        peer.peer_id, e
                    );
                    continue;
                }
            };

            if events_to_send.is_empty() {
                continue;
            }

            let payload: Vec<db::EventInput> = events_to_send
                .iter()
                .map(|e| db::EventInput {
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
                })
                .collect();

            let res = client
                .post(format!("{}/relay/replicate", peer.url))
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
                        if let Err(e) = db::update_peer_cursor(
                            &state.pool,
                            peer.peer_id,
                            last.occurred_at.unwrap_or(chrono::Utc::now()),
                            last.event_id,
                        )
                        .await
                        {
                            error!("Failed to update cursor for peer {}: {}", peer.peer_id, e);
                        } else {
                            info!(
                                "Replicated {} events to peer {}",
                                events_to_send.len(),
                                peer.peer_id
                            );
                        }
                    } else {
                        warn!(
                            "Replication failed for peer {}: Status {}",
                            peer.peer_id,
                            resp.status()
                        );
                    }
                }
                Err(e) => {
                    warn!(
                        "Replication request failed for peer {}: {}",
                        peer.peer_id, e
                    );
                }
            }
        }
    }
}

pub async fn serve_command(
    port: u16,
    database_url: String,
    relay_id_opt: Option<Uuid>,
) -> anyhow::Result<()> {
    let relay_id = relay_id_opt.unwrap_or_else(Uuid::new_v4);

    info!("connecting to database: {}", database_url);
    let pool = sqlx::PgPool::connect(&database_url).await?;

    info!("running migrations");
    db::run_migrations(&pool).await?;

    let state = AppState { pool, relay_id };
    let worker_state = state.clone();
    tokio::spawn(async move {
        replication_worker(worker_state).await;
    });

    let app = relay_app(state);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("server (ID: {}) listening on {}", relay_id, addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}

pub async fn serve_distillery_command(port: u16) -> anyhow::Result<()> {
    let app = distillery_app();
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("distillery-only server listening on {}", addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}
