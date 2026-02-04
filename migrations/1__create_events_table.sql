-- Migration: create events table

CREATE TABLE IF NOT EXISTS events (
    event_id UUID PRIMARY KEY,
    server_seq BIGSERIAL NOT NULL UNIQUE,
    device_id TEXT,
    author_id TEXT,
    content_id TEXT,
    event_type TEXT,
    payload_json JSONB,
    occurred_at TIMESTAMPTZ,
    lamport BIGINT
);

CREATE INDEX IF NOT EXISTS events_server_seq_idx ON events (server_seq);
