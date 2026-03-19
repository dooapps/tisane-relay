use axum::http::{
    HeaderName, HeaderValue, Method,
    header::{AUTHORIZATION, CONTENT_TYPE},
};
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};

#[derive(Debug, Clone, Default)]
pub struct DistilleryCorsConfig {
    allowed_origins: Vec<String>,
}

impl DistilleryCorsConfig {
    pub fn new(allowed_origins: Vec<String>) -> Self {
        Self {
            allowed_origins: allowed_origins
                .into_iter()
                .map(|origin| origin.trim().to_string())
                .filter(|origin| !origin.is_empty())
                .collect(),
        }
    }

    pub fn from_csv(value: Option<String>) -> Self {
        match value {
            Some(origins) => Self::new(
                origins
                    .split(',')
                    .map(ToOwned::to_owned)
                    .collect::<Vec<_>>(),
            ),
            None => Self::default(),
        }
    }

    pub fn layer(&self) -> Option<CorsLayer> {
        let allow_origin = if self.allowed_origins.is_empty() {
            return None;
        } else if self.allowed_origins.iter().any(|origin| origin == "*") {
            AllowOrigin::any()
        } else {
            let origins = self
                .allowed_origins
                .iter()
                .filter_map(|origin| HeaderValue::from_str(origin).ok())
                .collect::<Vec<_>>();

            if origins.is_empty() {
                return None;
            }

            AllowOrigin::list(origins)
        };

        Some(
            CorsLayer::new()
                .allow_origin(allow_origin)
                .allow_methods(AllowMethods::list([
                    Method::GET,
                    Method::POST,
                    Method::OPTIONS,
                ]))
                .allow_headers(AllowHeaders::list([
                    CONTENT_TYPE,
                    AUTHORIZATION,
                    HeaderName::from_static("x-distillery-key"),
                    HeaderName::from_static("x-peer-token"),
                    HeaderName::from_static("x-request-id"),
                ])),
        )
    }
}
