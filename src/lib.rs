use sqlx::PgPool;
use uuid::Uuid;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub relay_id: Uuid,
}

pub mod db;
pub mod distillery_bridge;
pub mod distillery_runtime;
pub mod server;
pub mod utils;
