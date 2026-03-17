use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Postgres, QueryBuilder, Row};
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AggregatedCandidate {
    pub candidate_id: String,
    pub author_id: Option<String>,
    pub channel: Option<String>,
    pub read_completed: i64,
    pub citation_created: i64,
    pub derivative_created: i64,
    pub value_snapshot: f64,
}

#[derive(Debug, Clone)]
pub struct CandidateAggregationQuery {
    pub since: Option<DateTime<Utc>>,
    pub surface: Option<String>,
    pub account_id: Option<String>,
    pub channel: Option<String>,
    pub limit: i64,
}

pub async fn insert_events(pool: &PgPool, events: &[EventInput]) -> Result<Vec<i64>, sqlx::Error> {
    if events.is_empty() {
        return Ok(Vec::new());
    }

    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        "INSERT INTO events (event_id, author_pubkey, signature, payload_hash, device_id, author_id, content_id, event_type, payload_json, occurred_at, lamport) ",
    );

    builder.push_values(events, |mut row, event| {
        row.push_bind(event.event_id)
            .push_bind(&event.author_pubkey)
            .push_bind(&event.signature)
            .push_bind(&event.payload_hash)
            .push_bind(&event.device_id)
            .push_bind(&event.author_id)
            .push_bind(&event.content_id)
            .push_bind(&event.event_type)
            .push_bind(&event.payload_json)
            .push_bind(&event.occurred_at)
            .push_bind(&event.lamport);
    });

    builder.push(" ON CONFLICT (event_id) DO NOTHING RETURNING server_seq");

    let rows = builder.build().fetch_all(pool).await?;
    let mut inserted = Vec::with_capacity(rows.len());
    for row in rows {
        inserted.push(row.get::<i64, _>("server_seq"));
    }

    Ok(inserted)
}

pub async fn fetch_events_since(
    pool: &PgPool,
    since: i64,
    limit: i64,
) -> Result<(Vec<Event>, i64), sqlx::Error> {
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
    sqlx::query_as::<_, Peer>(
        "SELECT peer_id, url, shared_secret, last_cursor_time, last_cursor_id, health FROM peers",
    )
    .fetch_all(pool)
    .await
}

// Add a new peer
pub async fn add_peer(
    pool: &PgPool,
    url: String,
    shared_secret: String,
) -> Result<Uuid, sqlx::Error> {
    let peer_id = Uuid::new_v4();
    // Default to UNIX epoch to avoid 'infinity' parsing issues in chrono
    let default_time = DateTime::parse_from_rfc3339("1970-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    sqlx::query(
        "INSERT INTO peers (peer_id, url, shared_secret, last_cursor_time) VALUES ($1, $2, $3, $4)",
    )
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
pub async fn update_peer_cursor(
    pool: &PgPool,
    peer_id: Uuid,
    last_time: DateTime<Utc>,
    last_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE peers SET last_cursor_time = $1, last_cursor_id = $2, updated_at = NOW() WHERE peer_id = $3")
        .bind(last_time)
        .bind(last_id)
        .bind(peer_id)
        .execute(pool)
        .await?;
    Ok(())
}

// Fetch events for replication (since time,id)
pub async fn fetch_replication_batch(
    pool: &PgPool,
    last_time: DateTime<Utc>,
    last_id: Uuid,
    limit: i64,
) -> Result<Vec<Event>, sqlx::Error> {
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

pub async fn aggregate_candidate_signals(
    pool: &PgPool,
    query: &CandidateAggregationQuery,
) -> Result<Vec<AggregatedCandidate>, sqlx::Error> {
    let mut builder: QueryBuilder<'_, Postgres> = QueryBuilder::new(
        r#"
        SELECT
            candidate_id_resolved AS candidate_id,
            MAX(author_id) AS author_id,
            MAX(channel_scope) AS channel,
            COALESCE(SUM(CASE WHEN event_type = 'read.completed' THEN 1 ELSE 0 END), 0) AS read_completed,
            COALESCE(SUM(CASE WHEN event_type = 'citation.created' THEN 1 ELSE 0 END), 0) AS citation_created,
            COALESCE(SUM(CASE WHEN event_type = 'derivative.created' THEN 1 ELSE 0 END), 0) AS derivative_created,
            COALESCE(MAX(snapshot_score), 0.0) AS value_snapshot
        FROM events
        WHERE candidate_id_resolved IS NOT NULL
          AND event_type IN ('read.completed', 'citation.created', 'derivative.created', 'value.snapshot')
        "#,
    );

    if let Some(since) = query.since {
        builder.push(" AND occurred_at >= ");
        builder.push_bind(since);
    }

    if let Some(surface) = query.surface.as_deref() {
        builder.push(" AND surface_scope = ");
        builder.push_bind(surface);
    }

    if let Some(account_id) = query.account_id.as_deref() {
        builder.push(" AND account_scope = ");
        builder.push_bind(account_id);
    }

    if let Some(channel) = query.channel.as_deref() {
        builder.push(" AND channel_scope = ");
        builder.push_bind(channel);
    }

    builder.push(
        r#"
        GROUP BY candidate_id_resolved
        ORDER BY MAX(occurred_at) DESC, candidate_id ASC
        LIMIT 
        "#,
    );
    builder.push_bind(query.limit);

    let rows = builder.build().fetch_all(pool).await?;

    let mut candidates = Vec::with_capacity(rows.len());
    for row in rows {
        candidates.push(AggregatedCandidate {
            candidate_id: row.get::<String, _>("candidate_id"),
            author_id: row.get::<Option<String>, _>("author_id"),
            channel: row.get::<Option<String>, _>("channel"),
            read_completed: row.get::<i64, _>("read_completed"),
            citation_created: row.get::<i64, _>("citation_created"),
            derivative_created: row.get::<i64, _>("derivative_created"),
            value_snapshot: row.get::<f64, _>("value_snapshot"),
        });
    }

    Ok(candidates)
}
