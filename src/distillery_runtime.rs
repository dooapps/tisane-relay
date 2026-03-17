use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    AppState, db,
    distillery_bridge::{CandidateSignals, DistributionRequest, RankingRequest, distribute, rank},
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventRankingRequest {
    pub surface: Option<String>,
    pub account_id: Option<String>,
    pub channel: Option<String>,
    pub since_hours: Option<i64>,
    #[serde(default = "default_event_limit")]
    pub limit: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventDistributionRequest {
    pub surface: Option<String>,
    pub account_id: Option<String>,
    pub channel: Option<String>,
    #[serde(default = "default_slot_count")]
    pub slot_count: usize,
    #[serde(default = "default_occurrence_limit")]
    pub max_per_author: usize,
    #[serde(default = "default_occurrence_limit")]
    pub max_per_channel: usize,
    pub since_hours: Option<i64>,
    #[serde(default = "default_event_limit")]
    pub limit: i64,
}

pub type FeedFromEventsRequest = EventDistributionRequest;

pub async fn rank_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<EventRankingRequest>,
) -> impl IntoResponse {
    let query = build_candidate_query(
        request.since_hours,
        request.surface.clone(),
        request.account_id.clone(),
        request.channel.clone(),
        request.limit,
    );
    match db::aggregate_candidate_signals(&state.pool, &query).await {
        Ok(candidates) => {
            let response = rank(RankingRequest {
                surface: request.surface,
                account_id: request.account_id,
                candidates: candidates.into_iter().map(map_candidate).collect(),
            });
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn distribute_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<EventDistributionRequest>,
) -> impl IntoResponse {
    let query = build_candidate_query(
        request.since_hours,
        request.surface.clone(),
        request.account_id.clone(),
        request.channel.clone(),
        request.limit,
    );
    match db::aggregate_candidate_signals(&state.pool, &query).await {
        Ok(candidates) => {
            let response = distribute(DistributionRequest {
                surface: request.surface,
                account_id: request.account_id,
                slot_count: request.slot_count,
                max_per_author: request.max_per_author,
                max_per_channel: request.max_per_channel,
                candidates: candidates.into_iter().map(map_candidate).collect(),
            });
            (StatusCode::OK, Json(response)).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn feed_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<FeedFromEventsRequest>,
) -> impl IntoResponse {
    distribute_from_events_handler(State(state), Json(request)).await
}

fn map_candidate(record: db::AggregatedCandidate) -> CandidateSignals {
    CandidateSignals {
        candidate_id: record.candidate_id,
        author_id: record.author_id,
        channel: record.channel,
        read_completed: saturating_u32(record.read_completed),
        citation_created: saturating_u32(record.citation_created),
        derivative_created: saturating_u32(record.derivative_created),
        value_snapshot: record.value_snapshot.max(0.0),
    }
}

fn saturating_u32(value: i64) -> u32 {
    if value <= 0 {
        0
    } else {
        u32::try_from(value).unwrap_or(u32::MAX)
    }
}

fn normalize_since(since_hours: Option<i64>) -> Option<DateTime<Utc>> {
    since_hours
        .filter(|hours| *hours > 0)
        .map(|hours| Utc::now() - Duration::hours(hours))
}

fn build_candidate_query(
    since_hours: Option<i64>,
    surface: Option<String>,
    account_id: Option<String>,
    channel: Option<String>,
    limit: i64,
) -> db::CandidateAggregationQuery {
    db::CandidateAggregationQuery {
        since: normalize_since(since_hours),
        surface,
        account_id,
        channel,
        limit,
    }
}

fn default_event_limit() -> i64 {
    500
}

fn default_slot_count() -> usize {
    10
}

fn default_occurrence_limit() -> usize {
    2
}
