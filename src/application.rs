use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

use crate::{
    db::{AggregatedCandidate, CandidateAggregationQuery},
    distillery_bridge::{
        CandidateSignals, DistributionRequest, DistributionResponse, RankingRequest,
        RankingResponse, distribute, rank,
    },
    storage::CandidateSignalStore,
};

#[derive(Debug, Clone)]
pub struct DistilleryEventQuery {
    pub surface: Option<String>,
    pub account_id: Option<String>,
    pub channel: Option<String>,
    pub since_hours: Option<i64>,
    pub limit: i64,
}

#[derive(Debug, Clone)]
pub struct DistributionPolicy {
    pub slot_count: usize,
    pub max_per_author: usize,
    pub max_per_channel: usize,
}

#[derive(Clone)]
pub struct DistilleryFeedService {
    signal_store: Arc<dyn CandidateSignalStore>,
}

impl DistilleryFeedService {
    pub fn new<T>(signal_store: Arc<T>) -> Self
    where
        T: CandidateSignalStore + 'static,
    {
        Self { signal_store }
    }

    pub async fn rank_from_events(&self, query: DistilleryEventQuery) -> Result<RankingResponse> {
        let candidates = self.load_candidates(&query).await?;
        Ok(rank(RankingRequest {
            surface: query.surface,
            account_id: query.account_id,
            candidates,
        }))
    }

    pub async fn distribute_from_events(
        &self,
        query: DistilleryEventQuery,
        policy: DistributionPolicy,
    ) -> Result<DistributionResponse> {
        let candidates = self.load_candidates(&query).await?;
        Ok(distribute(DistributionRequest {
            surface: query.surface,
            account_id: query.account_id,
            slot_count: policy.slot_count,
            max_per_author: policy.max_per_author,
            max_per_channel: policy.max_per_channel,
            candidates,
        }))
    }

    async fn load_candidates(&self, query: &DistilleryEventQuery) -> Result<Vec<CandidateSignals>> {
        let candidates = self
            .signal_store
            .aggregate_candidate_signals(&build_candidate_query(query))
            .await?;
        Ok(candidates.into_iter().map(map_candidate).collect())
    }
}

fn build_candidate_query(query: &DistilleryEventQuery) -> CandidateAggregationQuery {
    CandidateAggregationQuery {
        since: normalize_since(query.since_hours),
        surface: query.surface.clone(),
        account_id: query.account_id.clone(),
        channel: query.channel.clone(),
        limit: query.limit,
    }
}

fn map_candidate(record: AggregatedCandidate) -> CandidateSignals {
    CandidateSignals {
        candidate_id: record.candidate_id,
        author_id: record.author_id,
        channel: record.channel,
        freshness_hours: Some(freshness_hours(record.last_signal_at)),
        read_completed: saturating_u32(record.read_completed),
        citation_created: saturating_u32(record.citation_created),
        derivative_created: saturating_u32(record.derivative_created),
        value_snapshot: record.value_snapshot.max(0.0),
    }
}

fn saturating_u32(value: i64) -> u32 {
    if value <= 0 {
        0
    } else {
        u32::try_from(value).unwrap_or(u32::MAX)
    }
}

fn normalize_since(since_hours: Option<i64>) -> Option<DateTime<Utc>> {
    since_hours
        .filter(|hours| *hours > 0)
        .map(|hours| Utc::now() - Duration::hours(hours))
}

fn freshness_hours(last_signal_at: DateTime<Utc>) -> f64 {
    let elapsed = Utc::now().signed_duration_since(last_signal_at);
    elapsed.num_seconds().max(0) as f64 / 3600.0
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeCandidateSignalStore {
        candidates: Vec<AggregatedCandidate>,
    }

    #[async_trait::async_trait]
    impl CandidateSignalStore for FakeCandidateSignalStore {
        async fn aggregate_candidate_signals(
            &self,
            _query: &CandidateAggregationQuery,
        ) -> Result<Vec<AggregatedCandidate>> {
            Ok(self.candidates.clone())
        }
    }

    #[tokio::test]
    async fn ranks_candidates_through_application_service() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "quiet".to_string(),
                    author_id: Some("author-a".to_string()),
                    channel: Some("essays".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(6),
                    read_completed: 3,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
                AggregatedCandidate {
                    candidate_id: "strong".to_string(),
                    author_id: Some("author-b".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 1,
                    citation_created: 1,
                    derivative_created: 1,
                    value_snapshot: 2.0,
                },
            ],
        }));

        let response = service
            .rank_from_events(DistilleryEventQuery {
                surface: Some("discover".to_string()),
                account_id: Some("acct-1".to_string()),
                channel: None,
                since_hours: None,
                limit: 50,
            })
            .await
            .expect("service should rank candidates");

        assert_eq!(response.items[0].candidate_id, "strong");
    }

    #[tokio::test]
    async fn maps_recency_from_candidate_aggregate() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "stale".to_string(),
                    author_id: Some("author-a".to_string()),
                    channel: Some("essays".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(72),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
                AggregatedCandidate {
                    candidate_id: "fresh".to_string(),
                    author_id: Some("author-b".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
            ],
        }));

        let response = service
            .rank_from_events(DistilleryEventQuery {
                surface: Some("home".to_string()),
                account_id: Some("acct-1".to_string()),
                channel: None,
                since_hours: None,
                limit: 50,
            })
            .await
            .expect("service should rank candidates");

        assert_eq!(response.items[0].candidate_id, "fresh");
        assert!(response.items[0].recency_score > response.items[1].recency_score);
    }
}
