use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // Embeds migrations from ./migrations
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct EventInput {
    pub event_id: Uuid,
    pub author_pubkey: String,
    pub signature: String,
    pub payload_hash: String,
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
    pub event_id: Uuid,
    pub server_seq: i64,
    pub author_pubkey: String,
    pub signature: String,
    pub payload_hash: String,
    pub device_id: Option<String>,
    pub author_id: Option<String>,
    pub content_id: Option<String>,
    pub event_type: Option<String>,
    pub payload_json: Option<serde_json::Value>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub lamport: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, sqlx::FromRow)]
pub struct Peer {
    pub peer_id: Uuid,
    pub url: String,
    pub shared_secret: String,
    pub last_cursor_time: DateTime<Utc>,
    pub last_cursor_id: Uuid,
    pub health: String,
}

pub async fn insert_events(pool: &PgPool, events: &[EventInput]) -> Result<Vec<i64>, sqlx::Error> {
    let mut inserted = Vec::new();

    for ev in events {
        let row = sqlx::query("INSERT INTO events (event_id, author_pubkey, signature, payload_hash, device_id, author_id, content_id, event_type, payload_json, occurred_at, lamport) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) ON CONFLICT (event_id) DO NOTHING RETURNING server_seq")
            .bind(ev.event_id)
            .bind(&ev.author_pubkey)
            .bind(&ev.signature)
            .bind(&ev.payload_hash)
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
    let rows = sqlx::query("SELECT event_id, server_seq, author_pubkey, signature, payload_hash, device_id, author_id, content_id, event_type, payload_json, occurred_at, lamport FROM events WHERE server_seq > $1 ORDER BY server_seq ASC LIMIT $2")
        .bind(since)
        .bind(limit)
        .fetch_all(pool)
        .await?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        let event = Event {
            event_id: row.get::<Uuid, _>("event_id"),
            server_seq: row.get::<i64, _>("server_seq"),
            author_pubkey: row.get::<String, _>("author_pubkey"),
            signature: row.get::<String, _>("signature"),
            payload_hash: row.get::<String, _>("payload_hash"),
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

// ----- PEER & REPLICATION QUERIES -----

// Fetch all healthy peers
pub async fn fetch_healthy_peers(pool: &PgPool) -> Result<Vec<Peer>, sqlx::Error> {
    sqlx::query_as::<_, Peer>("SELECT peer_id, url, shared_secret, last_cursor_time, last_cursor_id, health FROM peers WHERE health = 'healthy' OR health = 'unknown'")
        .fetch_all(pool)
        .await
}

// Fetch all peers (for admin listing)
pub async fn fetch_all_peers(pool: &PgPool) -> Result<Vec<Peer>, sqlx::Error> {
    sqlx::query_as::<_, Peer>("SELECT peer_id, url, shared_secret, last_cursor_time, last_cursor_id, health FROM peers")
        .fetch_all(pool)
        .await
}

// Add a new peer
pub async fn add_peer(pool: &PgPool, url: String, shared_secret: String) -> Result<Uuid, sqlx::Error> {
    let peer_id = Uuid::new_v4();
    // Default to UNIX epoch to avoid 'infinity' parsing issues in chrono
    let default_time = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z").unwrap().with_timezone(&Utc);
    
    sqlx::query("INSERT INTO peers (peer_id, url, shared_secret, last_cursor_time) VALUES ($1, $2, $3, $4)")
        .bind(peer_id)
        .bind(url)
        .bind(shared_secret)
        .bind(default_time)
        .execute(pool)
        .await?;
    Ok(peer_id)
}

// Remove a peer
pub async fn remove_peer(pool: &PgPool, peer_id: Uuid) -> Result<bool, sqlx::Error> {
    let result = sqlx::query("DELETE FROM peers WHERE peer_id = $1")
        .bind(peer_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

// Validate a peer token (returns the Peer if found and authorized)
pub async fn validate_peer_token(pool: &PgPool, token: &str) -> Result<Option<Peer>, sqlx::Error> {
    sqlx::query_as::<_, Peer>("SELECT peer_id, url, shared_secret, last_cursor_time, last_cursor_id, health FROM peers WHERE shared_secret = $1")
        .bind(token)
        .fetch_optional(pool)
        .await
}

// Update cursor for a peer
pub async fn update_peer_cursor(pool: &PgPool, peer_id: Uuid, last_time: DateTime<Utc>, last_id: Uuid) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE peers SET last_cursor_time = $1, last_cursor_id = $2, updated_at = NOW() WHERE peer_id = $3")
        .bind(last_time)
        .bind(last_id)
        .bind(peer_id)
        .execute(pool)
        .await?;
    Ok(())
}

// Fetch events for replication (since time,id)
pub async fn fetch_replication_batch(pool: &PgPool, last_time: DateTime<Utc>, last_id: Uuid, limit: i64) -> Result<Vec<Event>, sqlx::Error> {
    // Composite cursor Logic:
    // (occurred_at, event_id) > (last_time, last_id)
    // equiv to: occurred_at > last_time OR (occurred_at = last_time AND event_id > last_id)
    
    let rows = sqlx::query("SELECT event_id, server_seq, author_pubkey, signature, payload_hash, device_id, author_id, content_id, event_type, payload_json, occurred_at, lamport FROM events WHERE (occurred_at > $1) OR (occurred_at = $1 AND event_id > $2) ORDER BY occurred_at ASC, event_id ASC LIMIT $3")
        .bind(last_time)
        .bind(last_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;

        let mut events = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            let event = Event {
                event_id: row.get::<Uuid, _>("event_id"),
                server_seq: row.get::<i64, _>("server_seq"),
                author_pubkey: row.get::<String, _>("author_pubkey"),
                signature: row.get::<String, _>("signature"),
                payload_hash: row.get::<String, _>("payload_hash"),
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

    Ok(events)
}
