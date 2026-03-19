use std::sync::Arc;

use application::DistilleryFeedService;
use ingestion::EventIngestionService;
use sqlx::PgPool;
use storage::{PostgresRelayStorage, RelayStorage};
use sync::RelaySyncService;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub relay_id: Uuid,
    pub distillery_service: Arc<DistilleryFeedService>,
    pub ingestion_service: Arc<EventIngestionService>,
    pub sync_service: Arc<RelaySyncService>,
}

impl AppState {
    pub fn new(pool: PgPool, relay_id: Uuid) -> Self {
        Self::from_storage(relay_id, Arc::new(PostgresRelayStorage::new(pool)))
    }

    pub fn from_storage<T>(relay_id: Uuid, storage: Arc<T>) -> Self
    where
        T: RelayStorage + 'static,
    {
        let distillery_service = Arc::new(DistilleryFeedService::new(storage.clone()));
        let ingestion_service = Arc::new(EventIngestionService::new(storage.clone()));
        let sync_service = Arc::new(RelaySyncService::new(storage));
        Self {
            relay_id,
            distillery_service,
            ingestion_service,
            sync_service,
        }
    }
}

pub mod api;
pub mod application;
pub mod auth;
pub mod cors;
pub mod database;
pub mod db;
pub mod distillery_bridge;
pub mod distillery_runtime;
pub mod ingestion;
pub mod observability;
pub mod rate_limit;
pub mod server;
pub mod storage;
pub mod sync;
pub mod utils;
