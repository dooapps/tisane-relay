use axum::{Json, http::StatusCode, response::IntoResponse};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorPayload {
    pub error: ApiErrorBody,
}

#[derive(Debug, Clone, Serialize)]
pub struct ApiErrorBody {
    pub code: &'static str,
    pub message: String,
}

pub fn error_response(
    status: StatusCode,
    code: &'static str,
    message: impl Into<String>,
) -> axum::response::Response {
    (
        status,
        Json(ApiErrorPayload {
            error: ApiErrorBody {
                code,
                message: message.into(),
            },
        }),
    )
        .into_response()
}
