use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderValue, Request},
    middleware::Next,
    response::Response,
};

use crate::api::error_response;

#[derive(Debug, Clone, Default)]
pub struct DistilleryRateLimitConfig {
    pub max_requests: Option<u32>,
    pub window_seconds: u64,
}

impl DistilleryRateLimitConfig {
    pub fn new(max_requests: Option<u32>, window_seconds: u64) -> Self {
        Self {
            max_requests: max_requests.filter(|value| *value > 0),
            window_seconds: window_seconds.max(1),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.max_requests.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct DistilleryRateLimiter {
    config: DistilleryRateLimitConfig,
    state: Arc<Mutex<HashMap<String, RateLimitEntry>>>,
}

#[derive(Debug, Clone)]
struct RateLimitEntry {
    window_started_at: Instant,
    request_count: u32,
}

impl DistilleryRateLimiter {
    pub fn new(config: DistilleryRateLimitConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.is_enabled()
    }

    fn evaluate(&self, key: &str) -> RateLimitDecision {
        let Some(max_requests) = self.config.max_requests else {
            return RateLimitDecision::Allowed;
        };

        let now = Instant::now();
        let window = Duration::from_secs(self.config.window_seconds);
        let mut state = self.state.lock().expect("rate limit mutex poisoned");
        let entry = state
            .entry(key.to_string())
            .or_insert_with(|| RateLimitEntry {
                window_started_at: now,
                request_count: 0,
            });

        if now.duration_since(entry.window_started_at) >= window {
            entry.window_started_at = now;
            entry.request_count = 0;
        }

        if entry.request_count >= max_requests {
            let elapsed = now.duration_since(entry.window_started_at);
            let retry_after = self
                .config
                .window_seconds
                .saturating_sub(elapsed.as_secs())
                .max(1);
            return RateLimitDecision::Limited { retry_after };
        }

        entry.request_count += 1;
        RateLimitDecision::Allowed
    }
}

pub async fn enforce_distillery_rate_limit(
    State(limiter): State<DistilleryRateLimiter>,
    request: Request<Body>,
    next: Next,
) -> Response {
    if !limiter.is_enabled() {
        return next.run(request).await;
    }

    match limiter.evaluate(&request_identity(request.headers())) {
        RateLimitDecision::Allowed => next.run(request).await,
        RateLimitDecision::Limited { retry_after } => {
            let mut response = error_response(
                axum::http::StatusCode::TOO_MANY_REQUESTS,
                "distillery_rate_limited",
                "distillery request rate limit exceeded",
            );
            if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                response.headers_mut().insert("retry-after", value);
            }
            response
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RateLimitDecision {
    Allowed,
    Limited { retry_after: u64 },
}

fn request_identity(headers: &HeaderMap) -> String {
    distillery_key(headers)
        .or_else(|| forwarded_for(headers))
        .unwrap_or_else(|| "anonymous".to_string())
}

fn distillery_key(headers: &HeaderMap) -> Option<String> {
    headers
        .get("X-Distillery-Key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .filter(|value| !value.trim().is_empty())
                .map(ToOwned::to_owned)
        })
}

fn forwarded_for(headers: &HeaderMap) -> Option<String> {
    headers
        .get("X-Forwarded-For")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use axum::http::{HeaderMap, HeaderValue};

    use super::{
        DistilleryRateLimitConfig, DistilleryRateLimiter, RateLimitDecision, request_identity,
    };

    #[test]
    fn identity_prefers_distillery_key_over_forwarded_for() {
        let mut headers = HeaderMap::new();
        headers.insert("X-Distillery-Key", HeaderValue::from_static("secret"));
        headers.insert("X-Forwarded-For", HeaderValue::from_static("10.0.0.1"));

        assert_eq!(request_identity(&headers), "secret");
    }

    #[test]
    fn limiter_blocks_after_configured_capacity() {
        let limiter = DistilleryRateLimiter::new(DistilleryRateLimitConfig::new(Some(2), 60));

        assert_eq!(limiter.evaluate("client-a"), RateLimitDecision::Allowed);
        assert_eq!(limiter.evaluate("client-a"), RateLimitDecision::Allowed);
        assert!(matches!(
            limiter.evaluate("client-a"),
            RateLimitDecision::Limited {
                retry_after: 1..=60
            }
        ));
    }
}
