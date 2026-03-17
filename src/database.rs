use std::time::Duration;

use sqlx::{PgPool, postgres::PgPoolOptions};

#[derive(Debug, Clone, Copy)]
pub struct PostgresPoolConfig {
    pub max_connections: u32,
    pub min_connections: u32,
    pub acquire_timeout_secs: u64,
    pub idle_timeout_secs: u64,
    pub max_lifetime_secs: u64,
}

impl Default for PostgresPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 10,
            min_connections: 1,
            acquire_timeout_secs: 5,
            idle_timeout_secs: 300,
            max_lifetime_secs: 1800,
        }
    }
}

impl PostgresPoolConfig {
    pub fn for_admin() -> Self {
        Self {
            max_connections: 1,
            min_connections: 0,
            acquire_timeout_secs: 5,
            idle_timeout_secs: 30,
            max_lifetime_secs: 300,
        }
    }
}

pub async fn connect_pool(
    database_url: &str,
    config: PostgresPoolConfig,
) -> Result<PgPool, sqlx::Error> {
    let max_connections = config.max_connections.max(1);
    let min_connections = config.min_connections.min(max_connections);

    PgPoolOptions::new()
        .max_connections(max_connections)
        .min_connections(min_connections)
        .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs))
        .idle_timeout(Some(Duration::from_secs(config.idle_timeout_secs)))
        .max_lifetime(Some(Duration::from_secs(config.max_lifetime_secs)))
        .connect(database_url)
        .await
}
