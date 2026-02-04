use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Embeds migrations from ./migrations
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

#[derive(Debug, Deserialize, Clone)]
pub struct EventInput {
    #[serde(with = "uuid::serde")]
    pub event_id: Uuid,
    pub device_id: Option<String>,
    pub author_id: Option<String>,
    pub content_id: Option<String>,
    pub event_type: Option<String>,
    pub payload_json: Option<serde_json::Value>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub lamport: Option<i64>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Event {
    #[serde(with = "uuid::serde")]
    pub event_id: Uuid,
    pub server_seq: i64,
    pub device_id: Option<String>,
    pub author_id: Option<String>,
    pub content_id: Option<String>,
    pub event_type: Option<String>,
    pub payload_json: Option<serde_json::Value>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub lamport: Option<i64>,
}

pub async fn insert_events(pool: &PgPool, events: &[EventInput]) -> Result<Vec<i64>, sqlx::Error> {
    let mut inserted = Vec::new();

    for ev in events {
        let row = sqlx::query("INSERT INTO events (event_id, device_id, author_id, content_id, event_type, payload_json, occurred_at, lamport) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (event_id) DO NOTHING RETURNING server_seq")
            .bind(ev.event_id)
            .bind(&ev.device_id)
            .bind(&ev.author_id)
            .bind(&ev.content_id)
            .bind(&ev.event_type)
            .bind(&ev.payload_json)
            .bind(&ev.occurred_at)
            .bind(&ev.lamport)
            .fetch_optional(pool)
            .await?;

        if let Some(r) = row {
            let seq: i64 = r.get("server_seq");
            inserted.push(seq);
        }
    }

    Ok(inserted)
}

pub async fn fetch_events_since(pool: &PgPool, since: i64, limit: i64) -> Result<(Vec<Event>, i64), sqlx::Error> {
    let rows = sqlx::query("SELECT event_id, server_seq, device_id, author_id, content_id, event_type, payload_json, occurred_at, lamport FROM events WHERE server_seq > $1 ORDER BY server_seq ASC LIMIT $2")
        .bind(since)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let event = Event {
            event_id: row.get::<Uuid, _>("event_id"),
            server_seq: row.get::<i64, _>("server_seq"),
            device_id: row.get::<Option<String>, _>("device_id"),
            author_id: row.get::<Option<String>, _>("author_id"),
            content_id: row.get::<Option<String>, _>("content_id"),
            event_type: row.get::<Option<String>, _>("event_type"),
            payload_json: row.get::<Option<serde_json::Value>, _>("payload_json"),
            occurred_at: row.get::<Option<DateTime<Utc>>, _>("occurred_at"),
            lamport: row.get::<Option<i64>, _>("lamport"),
        };
        events.push(event);
    }

    let next_cursor = events.last().map(|e| e.server_seq).unwrap_or(since);
    Ok((events, next_cursor))
}
