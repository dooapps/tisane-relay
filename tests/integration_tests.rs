use std::env;

use sqlx::PgPool;
use uuid::Uuid;
use chrono::Utc;

use tisane_relay::db::{self, EventInput};

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

    let ev1 = EventInput {
        event_id: Uuid::new_v4(),
        device_id: Some("dev-a".into()),
        author_id: Some("author-a".into()),
        content_id: Some("content-a".into()),
        event_type: Some("type-a".into()),
        payload_json: Some(serde_json::json!({"k":"v"})),
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

    let ev = EventInput {
        event_id: Uuid::new_v4(),
        device_id: Some("dev-d".into()),
        author_id: Some("author-d".into()),
        content_id: Some("content-d".into()),
        event_type: Some("type-d".into()),
        payload_json: Some(serde_json::json!({"x":1})),
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
