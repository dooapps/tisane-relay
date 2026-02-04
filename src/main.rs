use std::net::SocketAddr;

use axum::{extract::State, routing::{get, post}, Json, Router, response::IntoResponse, http::StatusCode, extract::Query};
use clap::Parser;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{info, error};

use crate::db; // use DB helpers from lib

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to bind to (or use PORT env var)
    #[arg(long, env = "PORT", default_value_t = 8080)]
    port: u16,

    /// Postgres database URL (or use DATABASE_URL env var)
    #[arg(long, env = "DATABASE_URL")]
    database_url: String,
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[derive(Deserialize)]
struct PushReq(pub Vec<db::EventInput>);

#[derive(Deserialize)]
struct PullQuery {
    since: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct PullResp {
    events: Vec<db::Event>,
    next_cursor: i64,
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status":"ok"})))
}

async fn push_handler(State(state): State<AppState>, Json(events): Json<Vec<db::EventInput>>) -> impl IntoResponse {
    match db::insert_events(&state.pool, &events).await {
        Ok(inserted) => (StatusCode::OK, Json(serde_json::json!({"inserted": inserted.len()}))),
        Err(e) => {
            error!("push error: %", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))
        }
    }
}

async fn pull_handler(State(state): State<AppState>, Query(q): Query<PullQuery>) -> impl IntoResponse {
    let since = q.since.unwrap_or(0);
    let limit = q.limit.unwrap_or(100);
    match db::fetch_events_since(&state.pool, since, limit).await {
        Ok((events, next_cursor)) => (StatusCode::OK, Json(PullResp{ events, next_cursor })),
        Err(e) => {
            error!("pull error: %", e);
            (StatusCode::INTERNAL_SERVER_ERROR, Json(serde_json::json!({"error": e.to_string()})))
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    info!("connecting to database: {}", args.database_url);
    let pool = PgPool::connect(&args.database_url).await?;

    info!("running migrations");
    db::run_migrations(&pool).await?;

    let state = AppState { pool };

    let app = Router::new()
        .route("/health", get(health))
        .route("/relay/push", post(push_handler))
        .route("/relay/pull", get(pull_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], args.port));
    info!("server listening on {}", addr);

    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .map_err(|e| anyhow::anyhow!(e))?;

    Ok(())
}
