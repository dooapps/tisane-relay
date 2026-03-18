use std::net::SocketAddr;
use std::time::Duration;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    AppState,
    auth::{DistilleryAccessConfig, require_distillery_access},
    database::{PostgresPoolConfig, connect_pool},
    db,
    distillery_bridge::{
        attention_handler, discover_handler, distribute_authors_handler, distribute_handler,
        rank_authors_handler, rank_handler,
    },
    distillery_runtime::{
        attention_from_events_handler, discover_authors_from_events_handler,
        discover_from_events_handler, distribute_authors_from_events_handler,
        distribute_from_events_handler, feed_from_events_handler, rank_authors_from_events_handler,
        rank_from_events_handler,
    },
    ingestion::IngestionError,
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

fn direct_distillery_routes<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/distillery/attention", post(attention_handler))
        .route("/distillery/discover", post(discover_handler))
        .route(
            "/distillery/distribute-authors",
            post(distribute_authors_handler),
        )
        .route("/distillery/distribute", post(distribute_handler))
        .route("/distillery/rank-authors", post(rank_authors_handler))
        .route("/distillery/rank", post(rank_handler))
}

fn runtime_distillery_routes() -> Router<AppState> {
    Router::new()
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
        .route(
            "/distillery/rank-authors-from-events",
            post(rank_authors_from_events_handler),
        )
        .route(
            "/distillery/distribute-authors-from-events",
            post(distribute_authors_from_events_handler),
        )
        .route(
            "/distillery/discover-authors-from-events",
            post(discover_authors_from_events_handler),
        )
        .route(
            "/distillery/attention-from-events",
            post(attention_from_events_handler),
        )
        .route(
            "/distillery/discover-from-events",
            post(discover_from_events_handler),
        )
}

fn protected_distillery_routes<S>(config: DistilleryAccessConfig) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    direct_distillery_routes().route_layer(middleware::from_fn_with_state(
        config,
        require_distillery_access,
    ))
}

fn protected_runtime_distillery_routes(config: DistilleryAccessConfig) -> Router<AppState> {
    runtime_distillery_routes().route_layer(middleware::from_fn_with_state(
        config,
        require_distillery_access,
    ))
}

fn relay_app(state: AppState, distillery_access: DistilleryAccessConfig) -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(protected_distillery_routes(distillery_access.clone()))
        .merge(protected_runtime_distillery_routes(distillery_access))
        .route("/relay/push", post(push_handler))
        .route("/relay/pull", get(pull_handler))
        .route("/relay/replicate", post(replicate_handler))
        .route("/relay/peers", get(peers_handler))
        .with_state(state)
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

    match state.ingestion_service.validate_and_insert(events).await {
        Ok(inserted) => (
            StatusCode::OK,
            Json(serde_json::json!({"inserted": inserted.len()})),
        )
            .into_response(),
        Err(error) => ingestion_error_response(error),
    }
}

async fn pull_handler(
    State(state): State<AppState>,
    Query(q): Query<PullQuery>,
) -> impl IntoResponse {
    let since = q.since.unwrap_or(0);
    let limit = q.limit.unwrap_or(100);
    match state.sync_service.pull_since(since, limit).await {
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

    let peer = match state.sync_service.authorize_peer(token).await {
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

    match state.ingestion_service.validate_and_insert(events).await {
        Ok(inserted) => (
            StatusCode::OK,
            Json(serde_json::json!({"inserted": inserted.len()})),
        )
            .into_response(),
        Err(error) => ingestion_error_response(error),
    }
}

fn ingestion_error_response(error: IngestionError) -> axum::response::Response {
    match error {
        IngestionError::BadRequest(message) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": message})),
        )
            .into_response(),
        IngestionError::Unauthorized(message) => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": message})),
        )
            .into_response(),
        IngestionError::Internal(message) => {
            error!("insert error: {}", message);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": message})),
            )
                .into_response()
        }
    }
}

async fn peers_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.sync_service.healthy_peers().await {
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

        let peers = match state.sync_service.healthy_peers().await {
            Ok(p) => p,
            Err(e) => {
                error!("Worker failed to fetch peers: {}", e);
                continue;
            }
        };

        for peer in peers {
            let events_to_send = match state
                .sync_service
                .replication_batch(peer.last_cursor_time, peer.last_cursor_id, 50)
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
                        if let Err(e) = state
                            .sync_service
                            .acknowledge_peer(
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
    pool_config: PostgresPoolConfig,
    distillery_access: DistilleryAccessConfig,
) -> anyhow::Result<()> {
    let relay_id = relay_id_opt.unwrap_or_else(Uuid::new_v4);

    info!("connecting to database: {}", database_url);
    let pool = connect_pool(&database_url, pool_config).await?;

    info!("running migrations");
    db::run_migrations(&pool).await?;

    let state = AppState::new(pool, relay_id);
    let worker_state = state.clone();
    tokio::spawn(async move {
        replication_worker(worker_state).await;
    });

    let app = relay_app(state, distillery_access);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("server (ID: {}) listening on {}", relay_id, addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}

pub async fn serve_distillery_command(
    port: u16,
    distillery_access: DistilleryAccessConfig,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/health", get(health))
        .merge(protected_distillery_routes(distillery_access));
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!("distillery-only server listening on {}", addr);

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use axum::{
        body::{Body, to_bytes},
        http::Request,
    };
    use chrono::{DateTime, Utc};
    use tower::util::ServiceExt;

    use crate::{
        storage::{CandidateSignalStore, EventBatchStore, RelayStore},
        db::{AggregatedAuthor, AggregatedCandidate, CandidateAggregationQuery, Event, EventInput, Peer},
    };

    use super::*;

    #[derive(Clone, Default)]
    struct TestStorage;

    #[async_trait::async_trait]
    impl CandidateSignalStore for TestStorage {
        async fn aggregate_candidate_signals(
            &self,
            _query: &CandidateAggregationQuery,
        ) -> Result<Vec<AggregatedCandidate>> {
            Ok(Vec::new())
        }

        async fn aggregate_author_signals(
            &self,
            _query: &CandidateAggregationQuery,
        ) -> Result<Vec<AggregatedAuthor>> {
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl EventBatchStore for TestStorage {
        async fn insert_events(&self, _events: &[EventInput]) -> Result<Vec<i64>> {
            Ok(Vec::new())
        }
    }

    #[async_trait::async_trait]
    impl RelayStore for TestStorage {
        async fn fetch_events_since(&self, _since: i64, _limit: i64) -> Result<(Vec<Event>, i64)> {
            Ok((Vec::new(), 0))
        }

        async fn fetch_healthy_peers(&self) -> Result<Vec<Peer>> {
            Ok(Vec::new())
        }

        async fn validate_peer_token(&self, _token: &str) -> Result<Option<Peer>> {
            Ok(None)
        }

        async fn update_peer_cursor(
            &self,
            _peer_id: Uuid,
            _last_time: DateTime<Utc>,
            _last_id: Uuid,
        ) -> Result<()> {
            Ok(())
        }

        async fn fetch_replication_batch(
            &self,
            _last_time: DateTime<Utc>,
            _last_id: Uuid,
            _limit: i64,
        ) -> Result<Vec<Event>> {
            Ok(Vec::new())
        }
    }

    fn rank_request_body() -> String {
        serde_json::json!({
            "surface": "discover",
            "account_id": "acct-1",
            "candidates": [
                {
                    "candidate_id": "content-a",
                    "read_completed": 1,
                    "citation_created": 0,
                    "derivative_created": 0,
                    "value_snapshot": 0.0
                }
            ]
        })
        .to_string()
    }

    fn test_state() -> AppState {
        AppState::from_storage(Uuid::nil(), Arc::new(TestStorage))
    }

    #[tokio::test]
    async fn distillery_routes_are_public_by_default() {
        let app = Router::new()
            .route("/health", get(health))
            .merge(protected_distillery_routes(DistilleryAccessConfig::default()));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/distillery/rank")
                    .header("content-type", "application/json")
                    .body(Body::from(rank_request_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn distillery_routes_require_token_when_configured() {
        let app = Router::new()
            .route("/health", get(health))
            .merge(protected_distillery_routes(DistilleryAccessConfig::new(Some(
                "secret-token".to_string(),
            ))));

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/distillery/rank")
                    .header("content-type", "application/json")
                    .body(Body::from(rank_request_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            payload["error"],
            serde_json::Value::String("missing or invalid distillery token".to_string())
        );
    }

    #[tokio::test]
    async fn runtime_distillery_routes_accept_bearer_token() {
        let app = relay_app(
            test_state(),
            DistilleryAccessConfig::new(Some("secret-token".to_string())),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/distillery/rank-from-events")
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer secret-token")
                    .body(Body::from(
                        serde_json::json!({
                            "surface": "discover",
                            "limit": 10
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }
}
