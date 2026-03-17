use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::{
    self, AggregatedAuthor, AggregatedCandidate, CandidateAggregationQuery, Event, EventInput, Peer,
};

#[async_trait]
pub trait CandidateSignalStore: Send + Sync {
    async fn aggregate_candidate_signals(
        &self,
        query: &CandidateAggregationQuery,
    ) -> Result<Vec<AggregatedCandidate>>;

    async fn aggregate_author_signals(
        &self,
        query: &CandidateAggregationQuery,
    ) -> Result<Vec<AggregatedAuthor>>;
}

#[async_trait]
pub trait EventBatchStore: Send + Sync {
    async fn insert_events(&self, events: &[EventInput]) -> Result<Vec<i64>>;
}

#[async_trait]
pub trait RelayStore: Send + Sync {
    async fn fetch_events_since(&self, since: i64, limit: i64) -> Result<(Vec<Event>, i64)>;
    async fn fetch_healthy_peers(&self) -> Result<Vec<Peer>>;
    async fn validate_peer_token(&self, token: &str) -> Result<Option<Peer>>;
    async fn update_peer_cursor(
        &self,
        peer_id: Uuid,
        last_time: DateTime<Utc>,
        last_id: Uuid,
    ) -> Result<()>;
    async fn fetch_replication_batch(
        &self,
        last_time: DateTime<Utc>,
        last_id: Uuid,
        limit: i64,
    ) -> Result<Vec<Event>>;
}

pub trait RelayStorage: CandidateSignalStore + EventBatchStore + RelayStore {}

impl<T> RelayStorage for T where T: CandidateSignalStore + EventBatchStore + RelayStore {}

#[derive(Clone)]
pub struct PostgresRelayStorage {
    pool: PgPool,
}

impl PostgresRelayStorage {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CandidateSignalStore for PostgresRelayStorage {
    async fn aggregate_candidate_signals(
        &self,
        query: &CandidateAggregationQuery,
    ) -> Result<Vec<AggregatedCandidate>> {
        db::aggregate_candidate_signals(&self.pool, query)
            .await
            .map_err(Into::into)
    }

    async fn aggregate_author_signals(
        &self,
        query: &CandidateAggregationQuery,
    ) -> Result<Vec<AggregatedAuthor>> {
        db::aggregate_author_signals(&self.pool, query)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl EventBatchStore for PostgresRelayStorage {
    async fn insert_events(&self, events: &[EventInput]) -> Result<Vec<i64>> {
        db::insert_events(&self.pool, events)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl RelayStore for PostgresRelayStorage {
    async fn fetch_events_since(&self, since: i64, limit: i64) -> Result<(Vec<Event>, i64)> {
        db::fetch_events_since(&self.pool, since, limit)
            .await
            .map_err(Into::into)
    }

    async fn fetch_healthy_peers(&self) -> Result<Vec<Peer>> {
        db::fetch_healthy_peers(&self.pool)
            .await
            .map_err(Into::into)
    }

    async fn validate_peer_token(&self, token: &str) -> Result<Option<Peer>> {
        db::validate_peer_token(&self.pool, token)
            .await
            .map_err(Into::into)
    }

    async fn update_peer_cursor(
        &self,
        peer_id: Uuid,
        last_time: DateTime<Utc>,
        last_id: Uuid,
    ) -> Result<()> {
        db::update_peer_cursor(&self.pool, peer_id, last_time, last_id)
            .await
            .map_err(Into::into)
    }

    async fn fetch_replication_batch(
        &self,
        last_time: DateTime<Utc>,
        last_id: Uuid,
        limit: i64,
    ) -> Result<Vec<Event>> {
        db::fetch_replication_batch(&self.pool, last_time, last_id, limit)
            .await
            .map_err(Into::into)
    }
}
