use std::sync::Arc;

use application::DistilleryFeedService;
use ingestion::EventIngestionService;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub relay_id: Uuid,
    pub distillery_service: Arc<DistilleryFeedService>,
    pub ingestion_service: Arc<EventIngestionService>,
}

impl AppState {
    pub fn new(pool: PgPool, relay_id: Uuid) -> Self {
        let distillery_service = Arc::new(DistilleryFeedService::from_pool(pool.clone()));
        let ingestion_service = Arc::new(EventIngestionService::from_pool(pool.clone()));
        Self {
            pool,
            relay_id,
            distillery_service,
            ingestion_service,
        }
    }
}

pub mod application;
pub mod db;
pub mod distillery_bridge;
pub mod distillery_runtime;
pub mod ingestion;
pub mod server;
pub mod utils;
