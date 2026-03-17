use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};

use crate::{
    AppState,
    application::{DistilleryEventQuery, DistributionPolicy},
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct EventAttentionDistributionRequest {
    pub surface: Option<String>,
    pub account_id: Option<String>,
    pub channel: Option<String>,
    #[serde(default = "default_slot_count")]
    pub slot_count: usize,
    #[serde(default)]
    pub min_content_slots: usize,
    #[serde(default)]
    pub min_author_slots: usize,
    #[serde(default = "default_occurrence_limit")]
    pub max_per_author: usize,
    #[serde(default = "default_occurrence_limit")]
    pub max_per_channel: usize,
    pub since_hours: Option<i64>,
    #[serde(default = "default_event_limit")]
    pub limit: i64,
}

pub type FeedFromEventsRequest = EventDistributionRequest;
pub type EventAuthorRankingRequest = EventRankingRequest;
pub type EventAuthorDistributionRequest = EventDistributionRequest;

pub async fn rank_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<EventRankingRequest>,
) -> impl IntoResponse {
    match state
        .distillery_service
        .rank_from_events(map_ranking_query(&request))
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
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
    match state
        .distillery_service
        .distribute_from_events(map_distribution_query(&request), map_policy(&request))
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn rank_authors_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<EventAuthorRankingRequest>,
) -> impl IntoResponse {
    match state
        .distillery_service
        .rank_authors_from_events(map_ranking_query(&request))
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn distribute_authors_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<EventAuthorDistributionRequest>,
) -> impl IntoResponse {
    match state
        .distillery_service
        .distribute_authors_from_events(map_distribution_query(&request), map_policy(&request))
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": error.to_string() })),
        )
            .into_response(),
    }
}

pub async fn attention_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<EventAttentionDistributionRequest>,
) -> impl IntoResponse {
    match state
        .distillery_service
        .attention_from_events(
            map_attention_query(&request),
            map_attention_policy(&request),
        )
        .await
    {
        Ok(response) => (StatusCode::OK, Json(response)).into_response(),
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

pub async fn discover_authors_from_events_handler(
    State(state): State<AppState>,
    Json(request): Json<EventAuthorDistributionRequest>,
) -> impl IntoResponse {
    distribute_authors_from_events_handler(State(state), Json(request)).await
}

fn map_ranking_query(request: &EventRankingRequest) -> DistilleryEventQuery {
    DistilleryEventQuery {
        surface: request.surface.clone(),
        account_id: request.account_id.clone(),
        channel: request.channel.clone(),
        since_hours: request.since_hours,
        limit: request.limit,
    }
}

fn map_policy(request: &EventDistributionRequest) -> DistributionPolicy {
    DistributionPolicy {
        slot_count: request.slot_count,
        min_content_slots: 0,
        min_author_slots: 0,
        max_per_author: request.max_per_author,
        max_per_channel: request.max_per_channel,
    }
}

fn map_attention_policy(request: &EventAttentionDistributionRequest) -> DistributionPolicy {
    DistributionPolicy {
        slot_count: request.slot_count,
        min_content_slots: request.min_content_slots,
        min_author_slots: request.min_author_slots,
        max_per_author: request.max_per_author,
        max_per_channel: request.max_per_channel,
    }
}

fn map_distribution_query(request: &EventDistributionRequest) -> DistilleryEventQuery {
    DistilleryEventQuery {
        surface: request.surface.clone(),
        account_id: request.account_id.clone(),
        channel: request.channel.clone(),
        since_hours: request.since_hours,
        limit: request.limit,
    }
}

fn map_attention_query(request: &EventAttentionDistributionRequest) -> DistilleryEventQuery {
    DistilleryEventQuery {
        surface: request.surface.clone(),
        account_id: request.account_id.clone(),
        channel: request.channel.clone(),
        since_hours: request.since_hours,
        limit: request.limit,
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
