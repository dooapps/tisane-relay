use clap::{Parser, Subcommand};
use sqlx::PgPool;
use uuid::Uuid;

use tisane_relay::db;
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
    },
    /// Start only Distillery endpoints for local algorithm development
    ServeDistillery {
        /// Port to bind to (or use PORT env var)
        #[arg(long, env = "PORT", default_value_t = 8080)]
        port: u16,
    },
    /// Add a new peer
    AddPeer {
        /// Peer URL (e.g., http://peer-relay:8080)
        #[arg(long)]
        url: String,
        /// Shared secret for authentication
        #[arg(long)]
        secret: String,
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

async fn add_peer_command(url: String, secret: String, database_url: String) -> anyhow::Result<()> {
    let pool = PgPool::connect(&database_url).await?;
    let id = db::add_peer(&pool, url.clone(), secret).await?;
    println!("Added peer {} with ID {}", url, id);
    Ok(())
}

async fn list_peers_command(database_url: String) -> anyhow::Result<()> {
    let pool = PgPool::connect(&database_url).await?;
    let peers = db::fetch_all_peers(&pool).await?;
    println!("{:<36} | {:<30} | {:<10}", "ID", "URL", "Health");
    println!("{}", "-".repeat(80));
    for p in peers {
        println!("{} | {:<30} | {}", p.peer_id, p.url, p.health);
    }
    Ok(())
}

async fn remove_peer_command(peer_id: Uuid, database_url: String) -> anyhow::Result<()> {
    let pool = PgPool::connect(&database_url).await?;
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
        } => {
            serve_command(port, database_url, relay_id).await?;
        }
        Commands::ServeDistillery { port } => {
            serve_distillery_command(port).await?;
        }
        Commands::AddPeer {
            url,
            secret,
            database_url,
        } => {
            add_peer_command(url, secret, database_url).await?;
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
