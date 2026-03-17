use std::{error::Error, fmt, sync::Arc};

use anyhow::Result;
use async_trait::async_trait;
use ed25519_dalek::VerifyingKey;
use sqlx::PgPool;

use crate::{db, utils::compute_payload_hash};

#[derive(Debug)]
pub enum IngestionError {
    BadRequest(String),
    Unauthorized(String),
    Internal(String),
}

impl fmt::Display for IngestionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadRequest(message) | Self::Unauthorized(message) | Self::Internal(message) => {
                write!(f, "{message}")
            }
        }
    }
}

impl Error for IngestionError {}

#[async_trait]
pub trait EventBatchStore: Send + Sync {
    async fn insert_events(&self, events: &[db::EventInput]) -> Result<Vec<i64>>;
}

#[derive(Clone)]
pub struct RelayEventBatchStore {
    pool: PgPool,
}

impl RelayEventBatchStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl EventBatchStore for RelayEventBatchStore {
    async fn insert_events(&self, events: &[db::EventInput]) -> Result<Vec<i64>> {
        db::insert_events(&self.pool, events)
            .await
            .map_err(Into::into)
    }
}

#[derive(Clone)]
pub struct EventIngestionService {
    event_store: Arc<dyn EventBatchStore>,
}

impl EventIngestionService {
    pub fn new(event_store: Arc<dyn EventBatchStore>) -> Self {
        Self { event_store }
    }

    pub fn from_pool(pool: PgPool) -> Self {
        Self::new(Arc::new(RelayEventBatchStore::new(pool)))
    }

    pub async fn validate_and_insert(
        &self,
        mut events: Vec<db::EventInput>,
    ) -> Result<Vec<i64>, IngestionError> {
        for event in &mut events {
            validate_value_protocol_event(event)?;
            sign_and_validate_event(event)?;
        }

        self.event_store
            .insert_events(&events)
            .await
            .map_err(|error| IngestionError::Internal(error.to_string()))
    }
}

fn validate_value_protocol_event(event: &db::EventInput) -> Result<(), IngestionError> {
    let Some(event_type) = event.event_type.as_deref() else {
        return Ok(());
    };

    match event_type {
        "read.completed" | "derivative.created" | "citation.created" | "value.snapshot" => {
            let Some(payload) = &event.payload_json else {
                return Err(IngestionError::BadRequest(format!(
                    "missing payload for event type '{}'",
                    event_type
                )));
            };

            let has_content_id = payload
                .get("content_id")
                .and_then(|value| value.as_str())
                .map(|value| !value.is_empty())
                .unwrap_or(false);

            if !has_content_id {
                return Err(IngestionError::BadRequest(format!(
                    "missing or empty content_id for event type '{}'",
                    event_type
                )));
            }

            if event_type == "value.snapshot" {
                let has_window =
                    payload.get("window_start").is_some() && payload.get("window_end").is_some();
                if !has_window {
                    return Err(IngestionError::BadRequest(
                        "missing window_start or window_end for value.snapshot".to_string(),
                    ));
                }
            }
        }
        _ => {}
    }

    Ok(())
}

fn sign_and_validate_event(event: &mut db::EventInput) -> Result<(), IngestionError> {
    event.payload_hash = compute_payload_hash(&event.payload_json);

    let pubkey_bytes = hex::decode(&event.author_pubkey)
        .map_err(|_| IngestionError::BadRequest("invalid author_pubkey hex".to_string()))?;
    let sig_bytes = hex::decode(&event.signature)
        .map_err(|_| IngestionError::BadRequest("invalid signature hex".to_string()))?;

    let verifying_key = VerifyingKey::from_bytes(&pubkey_bytes.try_into().unwrap_or([0u8; 32]))
        .map_err(|_| IngestionError::Unauthorized("invalid public key".to_string()))?;

    let signature: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| IngestionError::Unauthorized("invalid signature length".to_string()))?;

    let payload_bytes = event
        .payload_json
        .as_ref()
        .map(|payload| payload.to_string().into_bytes())
        .unwrap_or_default();

    if infusion::infusion::sign::verify(&verifying_key, &payload_bytes, &signature).is_err() {
        return Err(IngestionError::Unauthorized(
            "invalid signature".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use ed25519_dalek::SigningKey;
    use rand::{RngCore, thread_rng};
    use uuid::Uuid;

    use super::*;

    struct FakeEventBatchStore;

    #[async_trait]
    impl EventBatchStore for FakeEventBatchStore {
        async fn insert_events(&self, events: &[db::EventInput]) -> Result<Vec<i64>> {
            Ok((1..=events.len() as i64).collect())
        }
    }

    #[tokio::test]
    async fn rejects_value_protocol_events_without_content_id() {
        let service = EventIngestionService::new(Arc::new(FakeEventBatchStore));
        let event = signed_event(
            "read.completed",
            serde_json::json!({
                "channel": "essays"
            }),
        );

        let error = service
            .validate_and_insert(vec![event])
            .await
            .expect_err("missing content_id should fail");

        assert!(matches!(error, IngestionError::BadRequest(_)));
    }

    fn signed_event(event_type: &str, payload: serde_json::Value) -> db::EventInput {
        let mut seed = [0u8; 32];
        thread_rng().fill_bytes(&mut seed);
        let signing_key = SigningKey::from_bytes(&seed);
        let author_pubkey = hex::encode(signing_key.verifying_key().to_bytes());
        let payload_json = Some(payload);
        let signature = hex::encode(infusion::infusion::sign::sign(
            &signing_key,
            &payload_json
                .as_ref()
                .expect("payload should exist")
                .to_string()
                .into_bytes(),
        ));

        db::EventInput {
            event_id: Uuid::new_v4(),
            author_pubkey,
            signature,
            payload_hash: String::new(),
            device_id: Some("test-device".to_string()),
            author_id: Some("author-a".to_string()),
            content_id: Some("content-a".to_string()),
            event_type: Some(event_type.to_string()),
            payload_json,
            occurred_at: Some(Utc::now()),
            lamport: Some(1),
        }
    }
}
