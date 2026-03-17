use std::env;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
    routing::post,
};
use chrono::Utc;
use sqlx::PgPool;
use uuid::Uuid;

use ed25519_dalek::SigningKey;
use infusion::infusion::cid::cid_blake3;
use infusion::infusion::sign;
use rand::{RngCore, thread_rng};
use tisane_relay::AppState;
use tisane_relay::database::{PostgresPoolConfig, connect_pool};
use tisane_relay::db::{self, CandidateAggregationQuery, EventInput};
use tisane_relay::distillery_bridge::{
    DistributionResponse, RankingResponse, distribute_handler, rank_handler,
};
use tisane_relay::distillery_runtime::{
    EventDistributionRequest, EventRankingRequest, FeedFromEventsRequest,
    distribute_from_events_handler, feed_from_events_handler, rank_from_events_handler,
};
use tisane_relay::utils::compute_payload_hash;
use tower::util::ServiceExt;

fn get_database_url() -> Option<String> {
    env::var("DATABASE_URL").ok()
}

async fn connect_test_pool(database_url: &str) -> anyhow::Result<PgPool> {
    Ok(connect_pool(database_url, PostgresPoolConfig::for_admin()).await?)
}

fn generate_signing_key() -> SigningKey {
    let mut seed = [0u8; 32];
    thread_rng().fill_bytes(&mut seed);
    SigningKey::from_bytes(&seed)
}

#[tokio::test]
async fn test_push_then_pull() -> anyhow::Result<()> {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test_push_then_pull because DATABASE_URL is not set.");
        return Ok(());
    };
    let pool = connect_test_pool(&database_url).await?;

    db::run_migrations(&pool).await?;

    // Ensure starting clean
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    // Generate Infusion keypair
    let signing_key = generate_signing_key();
    let verifying_key = signing_key.verifying_key();

    let author_pubkey = hex::encode(verifying_key.to_bytes());
    let payload_json = Some(serde_json::json!({"k":"v"}));

    // Sign payload
    let payload_bytes = payload_json.as_ref().unwrap().to_string().into_bytes();
    let signature_bytes = sign::sign(&signing_key, &payload_bytes);
    let signature = hex::encode(signature_bytes);

    let payload_hash = compute_payload_hash(&payload_json);

    let ev1 = EventInput {
        event_id: Uuid::new_v4(),
        author_pubkey,
        signature,
        payload_hash,
        device_id: Some("dev-a".into()),
        author_id: Some("author-a".into()),
        content_id: Some("content-a".into()),
        event_type: Some("type-a".into()),
        payload_json,
        occurred_at: Some(Utc::now()),
        lamport: Some(1),
    };

    let inserted = db::insert_events(&pool, &[ev1.clone()]).await?;
    assert_eq!(inserted.len(), 1, "one event should be inserted");

    let (events, next_cursor) = db::fetch_events_since(&pool, 0, 100).await?;
    assert!(events.len() >= 1);
    assert!(next_cursor >= 1);

    Ok(())
}

#[tokio::test]
async fn test_dedup() -> anyhow::Result<()> {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test_dedup because DATABASE_URL is not set.");
        return Ok(());
    };
    let pool = connect_test_pool(&database_url).await?;

    db::run_migrations(&pool).await?;
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    let signing_key = generate_signing_key();
    let author_pubkey = hex::encode(signing_key.verifying_key().to_bytes());
    let payload_json = Some(serde_json::json!({"x":1}));
    let payload_bytes = payload_json.as_ref().unwrap().to_string().into_bytes();
    let signature = hex::encode(sign::sign(&signing_key, &payload_bytes));
    let payload_hash = compute_payload_hash(&payload_json);

    let ev = EventInput {
        event_id: Uuid::new_v4(),
        author_pubkey,
        signature,
        payload_hash,
        device_id: Some("dev-d".into()),
        author_id: Some("author-d".into()),
        content_id: Some("content-d".into()),
        event_type: Some("type-d".into()),
        payload_json,
        occurred_at: Some(Utc::now()),
        lamport: Some(5),
    };

    let first = db::insert_events(&pool, &[ev.clone()]).await?;
    assert_eq!(first.len(), 1);

    let second = db::insert_events(&pool, &[ev.clone()]).await?;
    assert_eq!(second.len(), 0, "duplicate insert should be ignored");

    let (events, _) = db::fetch_events_since(&pool, 0, 100).await?;
    let count = events.iter().filter(|e| e.event_id == ev.event_id).count();
    assert_eq!(count, 1, "there should be a single persisted event");

    Ok(())
}
#[tokio::test]
async fn test_hash_consistency() {
    let payload = serde_json::json!({"hello": "world"});
    let hash = compute_payload_hash(&Some(payload.clone()));

    // Manual computation for comparison
    let bytes = payload.to_string().into_bytes();
    let expected_hash = hex::encode(cid_blake3(&bytes));

    assert_eq!(hash, expected_hash, "Hash must be stable and consistent");
}

#[tokio::test]
async fn test_distillery_rank_endpoint() {
    let app = Router::new().route("/distillery/rank", post(rank_handler));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/distillery/rank")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "surface": "discover",
                        "account_id": "acct-1",
                        "candidates": [
                            {
                                "candidate_id": "quiet",
                                "read_completed": 3,
                                "citation_created": 0,
                                "derivative_created": 0,
                                "value_snapshot": 0.0
                            },
                            {
                                "candidate_id": "strong",
                                "read_completed": 1,
                                "citation_created": 1,
                                "derivative_created": 1,
                                "value_snapshot": 2.0
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: RankingResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.items[0].candidate_id, "strong");
    assert!(payload.items[0].final_score > payload.items[1].final_score);
}

#[tokio::test]
async fn test_distillery_distribute_endpoint() {
    let app = Router::new().route("/distillery/distribute", post(distribute_handler));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/distillery/distribute")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({
                        "surface": "home",
                        "account_id": "acct-1",
                        "slot_count": 2,
                        "max_per_author": 1,
                        "max_per_channel": 2,
                        "candidates": [
                            {
                                "candidate_id": "author-a-strong",
                                "author_id": "author-a",
                                "channel": "essays",
                                "read_completed": 1,
                                "citation_created": 1,
                                "derivative_created": 1,
                                "value_snapshot": 1.0
                            },
                            {
                                "candidate_id": "author-a-second",
                                "author_id": "author-a",
                                "channel": "essays",
                                "read_completed": 2,
                                "citation_created": 1,
                                "derivative_created": 0,
                                "value_snapshot": 0.0
                            },
                            {
                                "candidate_id": "author-b-alt",
                                "author_id": "author-b",
                                "channel": "briefs",
                                "read_completed": 1,
                                "citation_created": 0,
                                "derivative_created": 0,
                                "value_snapshot": 0.0
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: DistributionResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.slots.len(), 2);
    assert_eq!(payload.slots[0].item.candidate_id, "author-a-strong");
    assert_eq!(payload.slots[1].item.candidate_id, "author-b-alt");
}

#[tokio::test]
async fn test_aggregate_candidate_signals() -> anyhow::Result<()> {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test_aggregate_candidate_signals because DATABASE_URL is not set.");
        return Ok(());
    };
    let pool = connect_test_pool(&database_url).await?;

    db::run_migrations(&pool).await?;
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    let signing_key = generate_signing_key();
    let author_pubkey = hex::encode(signing_key.verifying_key().to_bytes());

    let events = vec![
        build_event(
            &author_pubkey,
            &signing_key,
            "content-a",
            "author-a",
            "read.completed",
            serde_json::json!({
                "content_id": "content-a",
                "channel": "essays",
                "surface": "home",
                "account_id": "acct-1"
            }),
        ),
        build_event(
            &author_pubkey,
            &signing_key,
            "content-a",
            "author-a",
            "citation.created",
            serde_json::json!({
                "content_id": "content-a",
                "channel": "essays",
                "surface": "home",
                "account_id": "acct-1"
            }),
        ),
        build_event(
            &author_pubkey,
            &signing_key,
            "content-a",
            "author-a",
            "value.snapshot",
            serde_json::json!({
                "content_id": "content-a",
                "channel": "essays",
                "surface": "home",
                "account_id": "acct-1",
                "score": 7.5,
                "window_start": "2026-03-01T00:00:00Z",
                "window_end": "2026-03-02T00:00:00Z"
            }),
        ),
        build_event(
            &author_pubkey,
            &signing_key,
            "content-b",
            "author-b",
            "read.completed",
            serde_json::json!({
                "content_id": "content-b",
                "channel": "briefs",
                "surface": "home",
                "account_id": "acct-1"
            }),
        ),
    ];

    db::insert_events(&pool, &events).await?;

    let aggregated = db::aggregate_candidate_signals(
        &pool,
        &CandidateAggregationQuery {
            since: None,
            surface: Some("home".to_string()),
            account_id: Some("acct-1".to_string()),
            channel: None,
            limit: 50,
        },
    )
    .await?;

    assert_eq!(aggregated.len(), 2);
    let content_a = aggregated
        .iter()
        .find(|candidate| candidate.candidate_id == "content-a")
        .unwrap();
    assert_eq!(content_a.read_completed, 1);
    assert_eq!(content_a.citation_created, 1);
    assert_eq!(content_a.derivative_created, 0);
    assert_eq!(content_a.value_snapshot, 7.5);

    Ok(())
}

#[tokio::test]
async fn test_rank_from_events_endpoint() -> anyhow::Result<()> {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test_rank_from_events_endpoint because DATABASE_URL is not set.");
        return Ok(());
    };
    let pool = connect_test_pool(&database_url).await?;

    db::run_migrations(&pool).await?;
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    seed_value_protocol_events(&pool).await?;

    let app = Router::new()
        .route(
            "/distillery/rank-from-events",
            post(rank_from_events_handler),
        )
        .with_state(AppState::new(pool, Uuid::new_v4()));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/distillery/rank-from-events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&EventRankingRequest {
                        surface: Some("home".to_string()),
                        account_id: Some("acct-1".to_string()),
                        channel: None,
                        since_hours: None,
                        limit: 50,
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: RankingResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.items[0].candidate_id, "content-a");
    assert!(payload.items[0].final_score > payload.items[1].final_score);

    Ok(())
}

#[tokio::test]
async fn test_distribute_from_events_endpoint() -> anyhow::Result<()> {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test_distribute_from_events_endpoint because DATABASE_URL is not set.");
        return Ok(());
    };
    let pool = connect_test_pool(&database_url).await?;

    db::run_migrations(&pool).await?;
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    seed_value_protocol_events(&pool).await?;

    let app = Router::new()
        .route(
            "/distillery/distribute-from-events",
            post(distribute_from_events_handler),
        )
        .with_state(AppState::new(pool, Uuid::new_v4()));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/distillery/distribute-from-events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&EventDistributionRequest {
                        surface: Some("home".to_string()),
                        account_id: Some("acct-1".to_string()),
                        channel: None,
                        slot_count: 2,
                        max_per_author: 1,
                        max_per_channel: 2,
                        since_hours: None,
                        limit: 50,
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: DistributionResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.slots.len(), 2);
    assert_eq!(payload.slots[0].item.candidate_id, "content-a");
    assert_eq!(payload.slots[1].item.candidate_id, "content-b");

    Ok(())
}

#[tokio::test]
async fn test_feed_from_events_endpoint() -> anyhow::Result<()> {
    let Some(database_url) = get_database_url() else {
        eprintln!("Skipping test_feed_from_events_endpoint because DATABASE_URL is not set.");
        return Ok(());
    };
    let pool = connect_test_pool(&database_url).await?;

    db::run_migrations(&pool).await?;
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    seed_value_protocol_events(&pool).await?;

    let app = Router::new()
        .route(
            "/distillery/feed-from-events",
            post(feed_from_events_handler),
        )
        .with_state(AppState::new(pool, Uuid::new_v4()));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/distillery/feed-from-events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&FeedFromEventsRequest {
                        surface: Some("home".to_string()),
                        account_id: Some("acct-1".to_string()),
                        channel: None,
                        slot_count: 2,
                        max_per_author: 1,
                        max_per_channel: 2,
                        since_hours: None,
                        limit: 50,
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: DistributionResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.slots.len(), 2);
    assert_eq!(payload.slots[0].item.candidate_id, "content-a");
    assert_eq!(payload.slots[1].item.candidate_id, "content-b");

    Ok(())
}

#[tokio::test]
async fn test_rank_from_events_filters_by_channel() -> anyhow::Result<()> {
    let Some(database_url) = get_database_url() else {
        eprintln!(
            "Skipping test_rank_from_events_filters_by_channel because DATABASE_URL is not set."
        );
        return Ok(());
    };
    let pool = connect_test_pool(&database_url).await?;

    db::run_migrations(&pool).await?;
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    seed_value_protocol_events(&pool).await?;

    let app = Router::new()
        .route(
            "/distillery/rank-from-events",
            post(rank_from_events_handler),
        )
        .with_state(AppState::new(pool, Uuid::new_v4()));

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/distillery/rank-from-events")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_string(&EventRankingRequest {
                        surface: Some("desk".to_string()),
                        account_id: Some("acct-2".to_string()),
                        channel: Some("video".to_string()),
                        since_hours: None,
                        limit: 50,
                    })
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let payload: RankingResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.items.len(), 1);
    assert_eq!(payload.items[0].candidate_id, "content-c");

    Ok(())
}

fn build_event(
    author_pubkey: &str,
    signing_key: &SigningKey,
    content_id: &str,
    author_id: &str,
    event_type: &str,
    payload: serde_json::Value,
) -> EventInput {
    let payload_json = Some(payload);
    let payload_bytes = payload_json.as_ref().unwrap().to_string().into_bytes();
    let signature = hex::encode(sign::sign(signing_key, &payload_bytes));
    let payload_hash = compute_payload_hash(&payload_json);

    EventInput {
        event_id: Uuid::new_v4(),
        author_pubkey: author_pubkey.to_string(),
        signature,
        payload_hash,
        device_id: Some("dev-seed".into()),
        author_id: Some(author_id.to_string()),
        content_id: Some(content_id.to_string()),
        event_type: Some(event_type.to_string()),
        payload_json,
        occurred_at: Some(Utc::now()),
        lamport: Some(1),
    }
}

async fn seed_value_protocol_events(pool: &PgPool) -> anyhow::Result<()> {
    let signing_key = generate_signing_key();
    let author_pubkey = hex::encode(signing_key.verifying_key().to_bytes());

    let events = vec![
        build_event(
            &author_pubkey,
            &signing_key,
            "content-a",
            "author-a",
            "read.completed",
            serde_json::json!({
                "content_id": "content-a",
                "channel": "essays",
                "surface": "home",
                "account_id": "acct-1"
            }),
        ),
        build_event(
            &author_pubkey,
            &signing_key,
            "content-a",
            "author-a",
            "citation.created",
            serde_json::json!({
                "content_id": "content-a",
                "channel": "essays",
                "surface": "home",
                "account_id": "acct-1"
            }),
        ),
        build_event(
            &author_pubkey,
            &signing_key,
            "content-a",
            "author-a",
            "value.snapshot",
            serde_json::json!({
                "content_id": "content-a",
                "channel": "essays",
                "surface": "home",
                "account_id": "acct-1",
                "score": 7.5,
                "window_start": "2026-03-01T00:00:00Z",
                "window_end": "2026-03-02T00:00:00Z"
            }),
        ),
        build_event(
            &author_pubkey,
            &signing_key,
            "content-b",
            "author-b",
            "read.completed",
            serde_json::json!({
                "content_id": "content-b",
                "channel": "briefs",
                "surface": "home",
                "account_id": "acct-1"
            }),
        ),
        build_event(
            &author_pubkey,
            &signing_key,
            "content-c",
            "author-c",
            "read.completed",
            serde_json::json!({
                "content_id": "content-c",
                "channel": "video",
                "surface": "desk",
                "account_id": "acct-2"
            }),
        ),
    ];

    db::insert_events(pool, &events).await?;
    Ok(())
}
