use std::time::Instant;

use axum::{
    body::Body,
    http::{HeaderName, HeaderValue, Request},
    middleware::Next,
    response::Response,
};
use tracing::{info, warn};
use uuid::Uuid;

const REQUEST_ID_HEADER: HeaderName = HeaderName::from_static("x-request-id");

pub async fn observe_http_request(mut request: Request<Body>, next: Next) -> Response {
    let request_id = request
        .headers()
        .get(&REQUEST_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let method = request.method().clone();
    let path = request.uri().path().to_string();

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        request.headers_mut().insert(REQUEST_ID_HEADER.clone(), value);
    }

    let started_at = Instant::now();
    let mut response = next.run(request).await;
    let latency_ms = started_at.elapsed().as_millis();
    let status = response.status();

    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(REQUEST_ID_HEADER.clone(), value);
    }

    if status.is_server_error() {
        warn!(
            request_id = %request_id,
            method = %method,
            path = %path,
            status = status.as_u16(),
            latency_ms,
            "http request failed"
        );
    } else {
        info!(
            request_id = %request_id,
            method = %method,
            path = %path,
            status = status.as_u16(),
            latency_ms,
            "http request completed"
        );
    }

    response
}
