use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    db::{Event, Peer},
    storage::RelayStore,
};

#[derive(Clone)]
pub struct RelaySyncService {
    store: Arc<dyn RelayStore>,
}

impl RelaySyncService {
    pub fn new<T>(store: Arc<T>) -> Self
    where
        T: RelayStore + 'static,
    {
        Self { store }
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
