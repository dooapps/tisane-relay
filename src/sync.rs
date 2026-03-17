use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::db::{self, Event, Peer};

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

#[derive(Clone)]
pub struct SqlRelayStore {
    pool: PgPool,
}

impl SqlRelayStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl RelayStore for SqlRelayStore {
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

#[derive(Clone)]
pub struct RelaySyncService {
    store: Arc<dyn RelayStore>,
}

impl RelaySyncService {
    pub fn new(store: Arc<dyn RelayStore>) -> Self {
        Self { store }
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self::new(Arc::new(SqlRelayStore::new(pool)))
    }

    pub async fn pull_since(&self, since: i64, limit: i64) -> Result<(Vec<Event>, i64)> {
        self.store.fetch_events_since(since, limit).await
    }

    pub async fn healthy_peers(&self) -> Result<Vec<Peer>> {
        self.store.fetch_healthy_peers().await
    }

    pub async fn authorize_peer(&self, token: &str) -> Result<Option<Peer>> {
        self.store.validate_peer_token(token).await
    }

    pub async fn replication_batch(
        &self,
        last_time: DateTime<Utc>,
        last_id: Uuid,
        limit: i64,
    ) -> Result<Vec<Event>> {
        self.store
            .fetch_replication_batch(last_time, last_id, limit)
            .await
    }

    pub async fn acknowledge_peer(
        &self,
        peer_id: Uuid,
        last_time: DateTime<Utc>,
        last_id: Uuid,
    ) -> Result<()> {
        self.store
            .update_peer_cursor(peer_id, last_time, last_id)
            .await
    }
}
