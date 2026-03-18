use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, Request, header::AUTHORIZATION},
    middleware::Next,
    response::Response,
};

use crate::api::error_response;

#[derive(Debug, Clone, Default)]
pub struct DistilleryAccessConfig {
    api_key: Option<String>,
}

impl DistilleryAccessConfig {
    pub fn new(api_key: Option<String>) -> Self {
        Self {
            api_key: api_key.filter(|value| !value.trim().is_empty()),
        }
    }

    pub fn is_public(&self) -> bool {
        self.api_key.is_none()
    }

    fn matches(&self, token: Option<&str>) -> bool {
        match (&self.api_key, token) {
            (None, _) => true,
            (Some(expected), Some(actual)) => expected == actual,
            (Some(_), None) => false,
        }
    }
}

pub async fn require_distillery_access(
    State(config): State<DistilleryAccessConfig>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if config.is_public() || config.matches(extract_distillery_token(request.headers())) {
        return next.run(request).await;
    }

    error_response(
        axum::http::StatusCode::UNAUTHORIZED,
        "distillery_auth_invalid",
        "missing or invalid distillery token",
    )
}

fn extract_distillery_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("X-Distillery-Key")
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            headers
                .get(AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(parse_bearer_token)
        })
}

fn parse_bearer_token(value: &str) -> Option<&str> {
    value.strip_prefix("Bearer ").map(str::trim).filter(|token| !token.is_empty())
}

#[cfg(test)]
mod tests {
    use super::parse_bearer_token;

    #[test]
    fn parses_bearer_token() {
        assert_eq!(parse_bearer_token("Bearer secret-token"), Some("secret-token"));
        assert_eq!(parse_bearer_token("Bearer   secret-token  "), Some("secret-token"));
        assert_eq!(parse_bearer_token("Basic secret-token"), None);
        assert_eq!(parse_bearer_token("Bearer "), None);
    }
}
