use axum::{Json, http::StatusCode, response::IntoResponse};

pub use distillery::{
    CandidateSignals, DistributionRequest, DistributionResponse, RankedCandidate, RankingRequest,
    RankingResponse, distribute, rank,
};

pub async fn rank_handler(Json(request): Json<RankingRequest>) -> impl IntoResponse {
    let response = rank(request);
    (StatusCode::OK, Json(response))
}

pub async fn distribute_handler(Json(request): Json<DistributionRequest>) -> impl IntoResponse {
    let response = distribute(request);
    (StatusCode::OK, Json(response))
}
