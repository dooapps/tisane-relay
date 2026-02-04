# Tisane Relay Self-Hosting Guide

This guide explains how to deploy a "Community Relay" for the Tisane Federation. By running your own node, you contribute to the network's resilience and censorship resistance.

## Prerequisites
- **Docker & Docker Compose**
- **Git**
- A public URL (HTTPS) if you intend to federate with others.
- **Google Cloud Run** or any VPS/Container capability (optional, for public deployment).

## Quickstart (Local)

1. **Clone and Setup**
   ```bash
   git clone <repo_url>
   cd tisane-relay
   cp .env.example .env
   ```
   *Edit `.env` to set your PostgreSQL credentials.*

2. **Run with Docker**
   ```bash
   docker-compose -f docker-compose.prod.yml up -d
   ```
   The relay will be available at `http://localhost:8080`.

3. **Verify**
   ```bash
   curl http://localhost:8080/health
   # {"status":"ok"}
   ```

## Public Deployment (Google Cloud)

Since our reference architecture uses Google Cloud:

1. **Database**: Provision a Cloud SQL (PostgreSQL 15) instance.
2. **Container**:
   ```bash
   gcloud builds submit --tag gcr.io/PROJECT/tisane-relay
   gcloud run deploy tisane-relay --image gcr.io/PROJECT/tisane-relay \
     --set-env-vars="DATABASE_URL=postgres://user:pass@host:5432/db,RUST_LOG=info" \
     --allow-unauthenticated
   ```
3. **Domain**: Map a custom domain to your Cloud Run service for a stable identity.

## Admin CLI & Peering

You can manage your relay using the built-in CLI commands locally or via `docker exec`.

### 1. Add a Peer
To allow another relay to replicate *to* or *from* you, you must exchange **URLs** and a **Shared Secret**.

```bash
# Add a peer to your whitelist
docker-compose -f docker-compose.prod.yml exec tisane-relay \
  /app/tisane-relay add-peer \
  --url https://their-relay.com \
  --secret shared-secret-123
```

### 2. List Peers
View all configured peers and their health status.
```bash
docker-compose -f docker-compose.prod.yml exec tisane-relay \
  /app/tisane-relay list-peers
```

### 3. Remove Peer
```bash
docker-compose -f docker-compose.prod.yml exec tisane-relay \
  /app/tisane-relay remove-peer --peer-id <UUID>
```

## Federation Protocol
- **Push**: Clients push events to `/relay/push`.
- **Replication**: Relays sync via `/relay/replicate`. Authentication uses the `X-Peer-Token` header (the shared secret).
- **Loop Protection**: Headers `X-Relay-Id` and `X-Hop` prevent cycles.
