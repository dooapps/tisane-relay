use clap::{Parser, Subcommand};
use uuid::Uuid;

use tisane_relay::auth::DistilleryAccessConfig;
use tisane_relay::cors::DistilleryCorsConfig;
use tisane_relay::database::{PostgresPoolConfig, connect_pool};
use tisane_relay::db;
use tisane_relay::rate_limit::DistilleryRateLimitConfig;
use tisane_relay::server::{serve_command, serve_distillery_command};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start the relay server
    Serve {
        /// Port to bind to (or use PORT env var)
        #[arg(long, env = "PORT", default_value_t = 8080)]
        port: u16,

        /// Postgres database URL (or use DATABASE_URL env var)
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,

        /// Unique ID for this relay (if not provided, one is generated randomly)
        #[arg(long, env = "RELAY_ID")]
        relay_id: Option<Uuid>,

        /// Maximum Postgres connections held by the relay server
        #[arg(long, env = "DB_MAX_CONNECTIONS", default_value_t = 10)]
        db_max_connections: u32,

        /// Minimum Postgres connections kept warm by the relay server
        #[arg(long, env = "DB_MIN_CONNECTIONS", default_value_t = 1)]
        db_min_connections: u32,

        /// Seconds to wait when acquiring a Postgres connection
        #[arg(long, env = "DB_ACQUIRE_TIMEOUT_SECS", default_value_t = 5)]
        db_acquire_timeout_secs: u64,

        /// Seconds an idle Postgres connection may stay in the pool
        #[arg(long, env = "DB_IDLE_TIMEOUT_SECS", default_value_t = 300)]
        db_idle_timeout_secs: u64,

        /// Seconds before recycling a Postgres connection
        #[arg(long, env = "DB_MAX_LIFETIME_SECS", default_value_t = 1800)]
        db_max_lifetime_secs: u64,

        /// Optional API key required for distillery endpoints
        #[arg(long, env = "DISTILLERY_API_KEY")]
        distillery_api_key: Option<String>,

        /// Optional per-window request limit for distillery endpoints
        #[arg(long, env = "DISTILLERY_RATE_LIMIT_MAX_REQUESTS")]
        distillery_rate_limit_max_requests: Option<u32>,

        /// Window size in seconds for the distillery rate limit
        #[arg(long, env = "DISTILLERY_RATE_LIMIT_WINDOW_SECS", default_value_t = 60)]
        distillery_rate_limit_window_secs: u64,

        /// Comma-separated origins allowed to call distillery endpoints from the web
        #[arg(long, env = "DISTILLERY_ALLOWED_ORIGINS")]
        distillery_allowed_origins: Option<String>,
    },
    /// Start only Distillery endpoints for local algorithm development
    ServeDistillery {
        /// Port to bind to (or use PORT env var)
        #[arg(long, env = "PORT", default_value_t = 8080)]
        port: u16,

        /// Optional API key required for distillery endpoints
        #[arg(long, env = "DISTILLERY_API_KEY")]
        distillery_api_key: Option<String>,

        /// Optional per-window request limit for distillery endpoints
        #[arg(long, env = "DISTILLERY_RATE_LIMIT_MAX_REQUESTS")]
        distillery_rate_limit_max_requests: Option<u32>,

        /// Window size in seconds for the distillery rate limit
        #[arg(long, env = "DISTILLERY_RATE_LIMIT_WINDOW_SECS", default_value_t = 60)]
        distillery_rate_limit_window_secs: u64,

        /// Comma-separated origins allowed to call distillery endpoints from the web
        #[arg(long, env = "DISTILLERY_ALLOWED_ORIGINS")]
        distillery_allowed_origins: Option<String>,
    },
    /// Add a new peer
    AddPeer {
        /// Peer URL (e.g., http://peer-relay:8080)
        #[arg(long)]
        url: String,
        /// Shared secret for authentication
        #[arg(long)]
        secret: String,
        /// Authorized owner unit refs for selective delivery. Repeat the flag to add more scopes.
        #[arg(long = "owner-unit-ref")]
        owner_unit_refs: Vec<String>,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// List all peers
    ListPeers {
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
    /// Remove a peer
    RemovePeer {
        /// Peer ID to remove
        #[arg(long)]
        peer_id: Uuid,
        #[arg(long, env = "DATABASE_URL")]
        database_url: String,
    },
}

async fn add_peer_command(
    url: String,
    secret: String,
    owner_unit_refs: Vec<String>,
    database_url: String,
) -> anyhow::Result<()> {
    let pool = connect_pool(&database_url, PostgresPoolConfig::for_admin()).await?;
    let id = db::add_peer(&pool, url.clone(), secret, owner_unit_refs.clone()).await?;
    println!(
        "Added peer {} with ID {} and owner scopes {:?}",
        url, id, owner_unit_refs
    );
    Ok(())
}

async fn list_peers_command(database_url: String) -> anyhow::Result<()> {
    let pool = connect_pool(&database_url, PostgresPoolConfig::for_admin()).await?;
    let peers = db::fetch_all_peers(&pool).await?;
    println!(
        "{:<36} | {:<30} | {:<10} | {}",
        "ID", "URL", "Health", "Owner scopes"
    );
    println!("{}", "-".repeat(80));
    for p in peers {
        println!(
            "{} | {:<30} | {:<10} | {}",
            p.peer_id,
            p.url,
            p.health,
            p.owner_unit_refs.join(",")
        );
    }
    Ok(())
}

async fn remove_peer_command(peer_id: Uuid, database_url: String) -> anyhow::Result<()> {
    let pool = connect_pool(&database_url, PostgresPoolConfig::for_admin()).await?;
    if db::remove_peer(&pool, peer_id).await? {
        println!("Removed peer {}", peer_id);
    } else {
        println!("Peer {} not found", peer_id);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    match args.command {
        Commands::Serve {
            port,
            database_url,
            relay_id,
            db_max_connections,
            db_min_connections,
            db_acquire_timeout_secs,
            db_idle_timeout_secs,
            db_max_lifetime_secs,
            distillery_api_key,
            distillery_rate_limit_max_requests,
            distillery_rate_limit_window_secs,
            distillery_allowed_origins,
        } => {
            serve_command(
                port,
                database_url,
                relay_id,
                PostgresPoolConfig {
                    max_connections: db_max_connections,
                    min_connections: db_min_connections,
                    acquire_timeout_secs: db_acquire_timeout_secs,
                    idle_timeout_secs: db_idle_timeout_secs,
                    max_lifetime_secs: db_max_lifetime_secs,
                },
                DistilleryAccessConfig::new(distillery_api_key),
                DistilleryRateLimitConfig::new(
                    distillery_rate_limit_max_requests,
                    distillery_rate_limit_window_secs,
                ),
                DistilleryCorsConfig::from_csv(distillery_allowed_origins),
            )
            .await?;
        }
        Commands::ServeDistillery {
            port,
            distillery_api_key,
            distillery_rate_limit_max_requests,
            distillery_rate_limit_window_secs,
            distillery_allowed_origins,
        } => {
            serve_distillery_command(
                port,
                DistilleryAccessConfig::new(distillery_api_key),
                DistilleryRateLimitConfig::new(
                    distillery_rate_limit_max_requests,
                    distillery_rate_limit_window_secs,
                ),
                DistilleryCorsConfig::from_csv(distillery_allowed_origins),
            )
            .await?;
        }
        Commands::AddPeer {
            url,
            secret,
            owner_unit_refs,
            database_url,
        } => {
            add_peer_command(url, secret, owner_unit_refs, database_url).await?;
        }
        Commands::ListPeers { database_url } => {
            list_peers_command(database_url).await?;
        }
        Commands::RemovePeer {
            peer_id,
            database_url,
        } => {
            remove_peer_command(peer_id, database_url).await?;
        }
    }

    Ok(())
}
