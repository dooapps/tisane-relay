use axum::{Json, http::StatusCode, response::IntoResponse};

pub use distillery::{
    AttentionDistributionRequest, AttentionDistributionResponse, AttentionItem, AttentionMixPolicy,
    AuthorDistributionRequest, AuthorDistributionResponse, AuthorRankingRequest,
    AuthorRankingResponse, AuthorSignals, CandidateSignals, DiscoveryRequest, DiscoveryResponse,
    DistributionRequest, DistributionResponse, RankedAuthor, RankedCandidate, RankingRequest,
    RankingResponse, discover, distribute, distribute_attention, distribute_authors, rank,
    rank_authors,
};

pub async fn rank_handler(Json(request): Json<RankingRequest>) -> impl IntoResponse {
    let response = rank(request);
    (StatusCode::OK, Json(response))
}

pub async fn rank_authors_handler(Json(request): Json<AuthorRankingRequest>) -> impl IntoResponse {
    let response = rank_authors(request);
    (StatusCode::OK, Json(response))
}

pub async fn distribute_authors_handler(
    Json(request): Json<AuthorDistributionRequest>,
) -> impl IntoResponse {
    let response = distribute_authors(request);
    (StatusCode::OK, Json(response))
}

pub async fn distribute_handler(Json(request): Json<DistributionRequest>) -> impl IntoResponse {
    let response = distribute(request);
    (StatusCode::OK, Json(response))
}

pub async fn attention_handler(
    Json(request): Json<AttentionDistributionRequest>,
) -> impl IntoResponse {
    let response = distribute_attention(request);
    (StatusCode::OK, Json(response))
}

pub async fn discover_handler(Json(request): Json<DiscoveryRequest>) -> impl IntoResponse {
    let response = discover(request);
    (StatusCode::OK, Json(response))
}
