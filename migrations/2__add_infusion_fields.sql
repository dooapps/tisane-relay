-- Migration: add infusion fields for identity and signature validation

ALTER TABLE events
    ADD COLUMN author_pubkey TEXT NOT NULL DEFAULT '',
    ADD COLUMN signature TEXT NOT NULL DEFAULT '',
    ADD COLUMN payload_hash TEXT NOT NULL DEFAULT '';

-- Remove defaults after adding columns to ensure future inserts require them
ALTER TABLE events 
    ALTER COLUMN author_pubkey DROP DEFAULT,
    ALTER COLUMN signature DROP DEFAULT,
    ALTER COLUMN payload_hash DROP DEFAULT;
