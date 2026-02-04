use std::env;

use sqlx::PgPool;
use uuid::Uuid;
use chrono::Utc;

use tisane_relay::db::{self, EventInput};
use tisane_relay::utils::compute_payload_hash;
use infusion::infusion::sign;
use infusion::infusion::cid::cid_blake3;
use ed25519_dalek::{SigningKey, VerifyingKey};
use rand::thread_rng;

// Helper: require DATABASE_URL to run tests
fn get_database_url() -> String {
    env::var("DATABASE_URL").expect("DATABASE_URL must be set to run integration tests")
}

#[tokio::test]
async fn test_push_then_pull() -> anyhow::Result<()> {
    let database_url = get_database_url();
    let pool = PgPool::connect(&database_url).await?;

    db::run_migrations(&pool).await?;

    // Ensure starting clean
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    // Generate Infusion keypair
    let mut rng = thread_rng();
    let signing_key = SigningKey::generate(&mut rng);
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
    let database_url = get_database_url();
    let pool = PgPool::connect(&database_url).await?;

    db::run_migrations(&pool).await?;
    sqlx::query("TRUNCATE TABLE events").execute(&pool).await?;

    let mut rng = thread_rng();
    let signing_key = SigningKey::generate(&mut rng);
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
