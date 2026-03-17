use axum::{Json, http::StatusCode, response::IntoResponse};

pub use distillery::{
    AuthorRankingRequest, AuthorRankingResponse, AuthorSignals, CandidateSignals,
    DistributionRequest, DistributionResponse, RankedAuthor, RankedCandidate, RankingRequest,
    RankingResponse, distribute, rank, rank_authors,
};

pub async fn rank_handler(Json(request): Json<RankingRequest>) -> impl IntoResponse {
    let response = rank(request);
    (StatusCode::OK, Json(response))
}

pub async fn rank_authors_handler(Json(request): Json<AuthorRankingRequest>) -> impl IntoResponse {
    let response = rank_authors(request);
    (StatusCode::OK, Json(response))
}

pub async fn distribute_handler(Json(request): Json<DistributionRequest>) -> impl IntoResponse {
    let response = distribute(request);
    (StatusCode::OK, Json(response))
}
