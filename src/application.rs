use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

use crate::{
    db::{AggregatedAuthor, AggregatedCandidate, CandidateAggregationQuery},
    distillery_bridge::{
        AttentionDistributionRequest, AttentionDistributionResponse, AttentionMixPolicy,
        AuthorDistributionRequest, AuthorDistributionResponse, AuthorRankingRequest,
        AuthorRankingResponse, AuthorSignals, CandidateSignals, DiscoveryRequest,
        DiscoveryResponse, DistilleryConfig, DistributionRequest, DistributionResponse,
        RankingRequest, RankingResponse, RecentAttentionContext, RecentAttentionSignal, discover,
        distribute, distribute_attention, distribute_authors, rank, rank_authors,
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
    pub min_content_slots: usize,
    pub min_author_slots: usize,
    pub attention_mix_policy: AttentionMixPolicy,
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

    pub async fn rank_authors_from_events(
        &self,
        query: DistilleryEventQuery,
    ) -> Result<AuthorRankingResponse> {
        let authors = self.load_authors(&query).await?;
        Ok(rank_authors(AuthorRankingRequest {
            surface: query.surface,
            account_id: query.account_id,
            authors,
        }))
    }

    pub async fn distribute_authors_from_events(
        &self,
        query: DistilleryEventQuery,
        policy: DistributionPolicy,
    ) -> Result<AuthorDistributionResponse> {
        let authors = self.load_authors(&query).await?;
        Ok(distribute_authors(AuthorDistributionRequest {
            surface: query.surface,
            account_id: query.account_id,
            slot_count: policy.slot_count,
            max_per_channel: policy.max_per_channel,
            authors,
        }))
    }

    pub async fn attention_from_events(
        &self,
        query: DistilleryEventQuery,
        policy: DistributionPolicy,
    ) -> Result<AttentionDistributionResponse> {
        let candidates = self.load_candidates(&query).await?;
        let authors = self.load_authors(&query).await?;
        Ok(distribute_attention(AttentionDistributionRequest {
            surface: query.surface,
            account_id: query.account_id,
            slot_count: policy.slot_count,
            min_content_slots: policy.min_content_slots,
            min_author_slots: policy.min_author_slots,
            mix_policy: policy.attention_mix_policy,
            max_per_author: policy.max_per_author,
            max_per_channel: policy.max_per_channel,
            candidates,
            authors,
        }))
    }

    pub async fn discover_from_events(
        &self,
        query: DistilleryEventQuery,
        slot_count: Option<usize>,
        excluded_candidate_ids: Vec<String>,
        excluded_author_ids: Vec<String>,
        recent_attention: RecentAttentionContext,
    ) -> Result<DiscoveryResponse> {
        let recent_attention = self
            .derive_recent_attention(&query, recent_attention)
            .await?;
        let candidates = self.load_candidates(&query).await?;
        let authors = self.load_authors(&query).await?;
        Ok(discover(DiscoveryRequest {
            surface: query.surface,
            account_id: query.account_id,
            slot_count,
            excluded_candidate_ids,
            excluded_author_ids,
            recent_attention,
            candidates,
            authors,
        }))
    }

    async fn derive_recent_attention(
        &self,
        query: &DistilleryEventQuery,
        supplied: RecentAttentionContext,
    ) -> Result<RecentAttentionContext> {
        let policy = DistilleryConfig::default().discovery_policy(query.surface.as_deref());

        let derived_authors = if let Some(window_hours) = policy.author_saturation_window_hours {
            let authors = self
                .signal_store
                .aggregate_author_signals(&build_candidate_query_for_window(query, window_hours))
                .await?;
            authors
                .into_iter()
                .map(|author| {
                    let recent_impressions = recent_author_impressions(&author);
                    RecentAttentionSignal {
                        id: author.author_id,
                        recent_impressions,
                        last_served_hours: Some(freshness_hours(author.last_signal_at)),
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        let derived_channels = if let Some(window_hours) = policy.channel_saturation_window_hours {
            let candidates = self
                .signal_store
                .aggregate_candidate_signals(&build_candidate_query_for_window(query, window_hours))
                .await?;
            collapse_recent_channels(candidates)
        } else {
            Vec::new()
        };

        Ok(merge_recent_attention(
            RecentAttentionContext {
                candidates: Vec::new(),
                authors: derived_authors,
                channels: derived_channels,
            },
            supplied,
        ))
    }

    async fn load_candidates(&self, query: &DistilleryEventQuery) -> Result<Vec<CandidateSignals>> {
        let candidates = self
            .signal_store
            .aggregate_candidate_signals(&build_candidate_query(query))
            .await?;
        Ok(candidates.into_iter().map(map_candidate).collect())
    }

    async fn load_authors(&self, query: &DistilleryEventQuery) -> Result<Vec<AuthorSignals>> {
        let authors = self
            .signal_store
            .aggregate_author_signals(&build_candidate_query(query))
            .await?;
        Ok(authors.into_iter().map(map_author).collect())
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

fn build_candidate_query_for_window(
    query: &DistilleryEventQuery,
    window_hours: f64,
) -> CandidateAggregationQuery {
    CandidateAggregationQuery {
        since: merge_since(
            normalize_since(query.since_hours),
            normalize_window_since(Some(window_hours)),
        ),
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

fn map_author(record: AggregatedAuthor) -> AuthorSignals {
    AuthorSignals {
        author_id: record.author_id,
        primary_channel: record.primary_channel,
        freshness_hours: Some(freshness_hours(record.last_signal_at)),
        unique_content_count: saturating_u32(record.unique_content_count),
        read_completed: saturating_u32(record.read_completed),
        citation_created: saturating_u32(record.citation_created),
        derivative_created: saturating_u32(record.derivative_created),
        value_snapshot: record.avg_value_snapshot.max(0.0),
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

fn normalize_window_since(window_hours: Option<f64>) -> Option<DateTime<Utc>> {
    window_hours.filter(|hours| *hours > 0.0).map(|hours| {
        let seconds = (hours * 3600.0).round() as i64;
        Utc::now() - Duration::seconds(seconds)
    })
}

fn merge_since(left: Option<DateTime<Utc>>, right: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(left), None) => Some(left),
        (None, Some(right)) => Some(right),
        (None, None) => None,
    }
}

fn freshness_hours(last_signal_at: DateTime<Utc>) -> f64 {
    let elapsed = Utc::now().signed_duration_since(last_signal_at);
    elapsed.num_seconds().max(0) as f64 / 3600.0
}

fn collapse_recent_channels(candidates: Vec<AggregatedCandidate>) -> Vec<RecentAttentionSignal> {
    let mut channels: HashMap<String, RecentAttentionSignal> = HashMap::new();

    for candidate in candidates {
        let Some(channel) = candidate.channel else {
            continue;
        };

        let entry = channels
            .entry(channel.clone())
            .or_insert_with(|| RecentAttentionSignal {
                id: channel,
                recent_impressions: 0,
                last_served_hours: Some(freshness_hours(candidate.last_signal_at)),
            });

        entry.recent_impressions = entry.recent_impressions.saturating_add(1);
        let freshness = freshness_hours(candidate.last_signal_at);
        entry.last_served_hours = Some(
            entry
                .last_served_hours
                .map_or(freshness, |current| current.min(freshness)),
        );
    }

    let mut channels = channels.into_values().collect::<Vec<_>>();
    channels.sort_by(|left, right| left.id.cmp(&right.id));
    channels
}

fn recent_author_impressions(author: &AggregatedAuthor) -> u32 {
    saturating_u32(
        (author.read_completed + author.citation_created + author.derivative_created)
            .max(author.unique_content_count)
            .max(1),
    )
}

fn merge_recent_attention(
    derived: RecentAttentionContext,
    supplied: RecentAttentionContext,
) -> RecentAttentionContext {
    RecentAttentionContext {
        candidates: merge_recent_signals(derived.candidates, supplied.candidates),
        authors: merge_recent_signals(derived.authors, supplied.authors),
        channels: merge_recent_signals(derived.channels, supplied.channels),
    }
}

fn merge_recent_signals(
    base: Vec<RecentAttentionSignal>,
    overlay: Vec<RecentAttentionSignal>,
) -> Vec<RecentAttentionSignal> {
    let mut merged = HashMap::<String, RecentAttentionSignal>::new();

    for entry in base.into_iter().chain(overlay) {
        merged
            .entry(entry.id.clone())
            .and_modify(|current| {
                current.recent_impressions = current
                    .recent_impressions
                    .saturating_add(entry.recent_impressions);
                current.last_served_hours =
                    match (current.last_served_hours, entry.last_served_hours) {
                        (Some(left), Some(right)) => Some(left.min(right)),
                        (Some(left), None) => Some(left),
                        (None, Some(right)) => Some(right),
                        (None, None) => None,
                    };
            })
            .or_insert(entry);
    }

    let mut values = merged.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| left.id.cmp(&right.id));
    values
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::distillery_bridge::AttentionItem;

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

        async fn aggregate_author_signals(
            &self,
            _query: &CandidateAggregationQuery,
        ) -> Result<Vec<AggregatedAuthor>> {
            let mut authors: HashMap<String, AggregatedAuthor> = HashMap::new();

            for candidate in &self.candidates {
                let Some(author_id) = candidate.author_id.clone() else {
                    continue;
                };

                let entry = authors
                    .entry(author_id.clone())
                    .or_insert_with(|| AggregatedAuthor {
                        author_id: author_id.clone(),
                        primary_channel: candidate.channel.clone(),
                        last_signal_at: candidate.last_signal_at,
                        unique_content_count: 0,
                        read_completed: 0,
                        citation_created: 0,
                        derivative_created: 0,
                        avg_value_snapshot: 0.0,
                    });

                entry.primary_channel = entry
                    .primary_channel
                    .clone()
                    .or_else(|| candidate.channel.clone());
                entry.last_signal_at = entry.last_signal_at.max(candidate.last_signal_at);
                entry.unique_content_count += 1;
                entry.read_completed += candidate.read_completed;
                entry.citation_created += candidate.citation_created;
                entry.derivative_created += candidate.derivative_created;
                entry.avg_value_snapshot += candidate.value_snapshot;
            }

            let mut authors: Vec<AggregatedAuthor> = authors
                .into_values()
                .map(|mut author| {
                    if author.unique_content_count > 0 {
                        author.avg_value_snapshot /= author.unique_content_count as f64;
                    }
                    author
                })
                .collect();
            authors.sort_by(|left, right| left.author_id.cmp(&right.author_id));

            Ok(authors)
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

    #[tokio::test]
    async fn ranks_authors_through_application_service() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "content-a".to_string(),
                    author_id: Some("author-a".to_string()),
                    channel: Some("essays".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(6),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
                AggregatedCandidate {
                    candidate_id: "content-b".to_string(),
                    author_id: Some("author-b".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 2,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.5,
                },
                AggregatedCandidate {
                    candidate_id: "content-c".to_string(),
                    author_id: Some("author-b".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(2),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
            ],
        }));

        let response = service
            .rank_authors_from_events(DistilleryEventQuery {
                surface: Some("discover".to_string()),
                account_id: Some("acct-1".to_string()),
                channel: None,
                since_hours: None,
                limit: 50,
            })
            .await
            .expect("service should rank authors");

        assert_eq!(response.items[0].author_id, "author-b");
        assert!(response.items[0].coverage_score > response.items[1].coverage_score);
    }

    #[tokio::test]
    async fn distributes_authors_through_application_service() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "content-a".to_string(),
                    author_id: Some("author-a".to_string()),
                    channel: Some("essays".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 2,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.5,
                },
                AggregatedCandidate {
                    candidate_id: "content-b".to_string(),
                    author_id: Some("author-b".to_string()),
                    channel: Some("essays".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(2),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
                AggregatedCandidate {
                    candidate_id: "content-c".to_string(),
                    author_id: Some("author-c".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(4),
                    read_completed: 1,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
            ],
        }));

        let response = service
            .distribute_authors_from_events(
                DistilleryEventQuery {
                    surface: Some("discover".to_string()),
                    account_id: Some("acct-1".to_string()),
                    channel: None,
                    since_hours: None,
                    limit: 50,
                },
                DistributionPolicy {
                    slot_count: 2,
                    min_content_slots: 0,
                    min_author_slots: 0,
                    attention_mix_policy: AttentionMixPolicy::default(),
                    max_per_author: 1,
                    max_per_channel: 1,
                },
            )
            .await
            .expect("service should distribute authors");

        assert_eq!(response.slots.len(), 2);
        assert_eq!(response.slots[0].item.author_id, "author-a");
        assert_eq!(response.slots[1].item.author_id, "author-c");
    }

    #[tokio::test]
    async fn distributes_mixed_attention_through_application_service() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "content-a".to_string(),
                    author_id: Some("author-a".to_string()),
                    channel: Some("essays".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 2,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.5,
                },
                AggregatedCandidate {
                    candidate_id: "content-b".to_string(),
                    author_id: Some("author-b".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(2),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
            ],
        }));

        let response = service
            .attention_from_events(
                DistilleryEventQuery {
                    surface: Some("home".to_string()),
                    account_id: Some("acct-1".to_string()),
                    channel: None,
                    since_hours: None,
                    limit: 50,
                },
                DistributionPolicy {
                    slot_count: 2,
                    min_content_slots: 0,
                    min_author_slots: 0,
                    attention_mix_policy: AttentionMixPolicy {
                        target_author_share: 0.5,
                        fairness_weight: 5.0,
                        max_consecutive_same_kind: 0,
                    },
                    max_per_author: 2,
                    max_per_channel: 2,
                },
            )
            .await
            .expect("service should distribute mixed attention");

        assert_eq!(response.slots.len(), 2);
        assert!(
            response
                .slots
                .iter()
                .any(|slot| matches!(slot.item, AttentionItem::Candidate(_)))
        );
        assert!(
            response
                .slots
                .iter()
                .any(|slot| matches!(slot.item, AttentionItem::Author(_)))
        );
    }

    #[tokio::test]
    async fn discovers_surface_mix_through_application_service() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "content-a".to_string(),
                    author_id: Some("author-a".to_string()),
                    channel: Some("essays".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 2,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.5,
                },
                AggregatedCandidate {
                    candidate_id: "content-b".to_string(),
                    author_id: Some("author-b".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(2),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
            ],
        }));

        let response = service
            .discover_from_events(
                DistilleryEventQuery {
                    surface: Some("discover".to_string()),
                    account_id: Some("acct-1".to_string()),
                    channel: None,
                    since_hours: None,
                    limit: 50,
                },
                Some(2),
                Vec::new(),
                Vec::new(),
                RecentAttentionContext::default(),
            )
            .await
            .expect("service should discover feed");

        assert_eq!(response.slots.len(), 2);
        assert!(
            response
                .slots
                .iter()
                .any(|slot| matches!(slot.item, AttentionItem::Author(_)))
        );
    }

    #[tokio::test]
    async fn discover_from_events_excludes_recently_served_ids() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "content-served".to_string(),
                    author_id: Some("author-served".to_string()),
                    channel: Some("analysis".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 3,
                    citation_created: 1,
                    derivative_created: 1,
                    value_snapshot: 0.5,
                },
                AggregatedCandidate {
                    candidate_id: "content-open".to_string(),
                    author_id: Some("author-open".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(2),
                    read_completed: 1,
                    citation_created: 0,
                    derivative_created: 0,
                    value_snapshot: 0.0,
                },
            ],
        }));

        let response = service
            .discover_from_events(
                DistilleryEventQuery {
                    surface: Some("discover".to_string()),
                    account_id: Some("acct-1".to_string()),
                    channel: None,
                    since_hours: None,
                    limit: 50,
                },
                Some(3),
                vec!["content-served".to_string()],
                vec!["author-served".to_string()],
                RecentAttentionContext::default(),
            )
            .await
            .expect("service should discover filtered feed");

        assert!(!response.slots.iter().any(|slot| matches!(
            &slot.item,
            AttentionItem::Candidate(item) if item.candidate_id == "content-served"
        )));
        assert!(!response.slots.iter().any(|slot| matches!(
            &slot.item,
            AttentionItem::Author(item) if item.author_id == "author-served"
        )));
    }

    #[tokio::test]
    async fn discover_from_events_derives_author_and_channel_saturation() {
        let service = DistilleryFeedService::new(Arc::new(FakeCandidateSignalStore {
            candidates: vec![
                AggregatedCandidate {
                    candidate_id: "recent-analysis".to_string(),
                    author_id: Some("author-recent".to_string()),
                    channel: Some("analysis".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(1),
                    read_completed: 2,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.5,
                },
                AggregatedCandidate {
                    candidate_id: "recent-analysis-second".to_string(),
                    author_id: Some("author-recent".to_string()),
                    channel: Some("analysis".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(2),
                    read_completed: 2,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.5,
                },
                AggregatedCandidate {
                    candidate_id: "older-brief".to_string(),
                    author_id: Some("author-open".to_string()),
                    channel: Some("briefs".to_string()),
                    last_signal_at: Utc::now() - Duration::hours(18),
                    read_completed: 2,
                    citation_created: 1,
                    derivative_created: 0,
                    value_snapshot: 0.5,
                },
            ],
        }));

        let recent_attention = service
            .derive_recent_attention(
                &DistilleryEventQuery {
                    surface: Some("discover".to_string()),
                    account_id: Some("acct-1".to_string()),
                    channel: None,
                    since_hours: None,
                    limit: 50,
                },
                RecentAttentionContext::default(),
            )
            .await
            .expect("service should derive saturation context from recent ledger");

        assert!(recent_attention.candidates.is_empty());
        assert!(
            recent_attention
                .authors
                .iter()
                .any(|signal| { signal.id == "author-recent" && signal.recent_impressions >= 2 })
        );
        assert!(
            recent_attention
                .channels
                .iter()
                .any(|signal| { signal.id == "analysis" && signal.recent_impressions >= 2 })
        );
    }
}
