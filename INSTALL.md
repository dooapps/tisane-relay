# Tisane Relay Installation & Operations

## Deployment Modes

### 1. Community Relay (Recommended)
**Goal**: Run a public node that federation with other relays.
- **Access**: Public (HTTPS).
- **Security**: Caddy reverse proxy (Rate Limiting, Header protection).
- **Auth**: Federation via `X-Peer-Token`.

### 2. Private Relay
**Goal**: Internal relay for your own apps/devices only.
- **Access**: Private (VPN or Authenticated Cloud Proxy).
- **Security**: Cloud IAM or specific VPN controls.
- **Auth**: None (typically relying on valid signatures and network isolation).

---

## Prerequisites
- Docker & Docker Compose
- Git
- Google Cloud Project (for Artifact Registry deployment)

---

## 1. Community Relay Setup

### Step 1: Configuration
```bash
git clone <repo_url>
cd tisane-relay
cp .env.example .env
uuidgen # Copy output to RELAY_ID in .env
```

**Edit `.env`**:
- Set `DOMAIN_NAME` (e.g., `relay.yourdomain.com`).
- Set `ACME_EMAIL` (for SSL).
- Set `RELAY_ID` (CRITICAL: Keep this constant to maintain peering trust).

### Step 2: Run with Anti-Abuse Protection
We use Caddy to handle SSL and Rate Limiting.
```bash
docker-compose -f docker-compose.prod.yml up -d
```

**Protections Enabled**:
- **HTTPS**: Auto-provisioned by Let's Encrypt.
- **Rate Limit**: 10 req/s per IP.
- **Body Limit**: Max 5MB per request.
- **Batch Limit**: Max 100 events per Push (Enforced by App).

---

## 2. Google Cloud Deployment (Artifact Registry)

For production Google Cloud Run deployment:

### Build & Push
```bash
# Set your variables
PROJECT_ID=your-project-id
REGION=us-central1
REPO=tisane-repo
IMAGE=tisane-relay

# Authenticate
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# Build & Push
docker build -t ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE}:latest .
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE}:latest
```

### Deploy (Cloud Run)
```bash
gcloud run deploy tisane-relay \
  --image ${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO}/${IMAGE}:latest \
  --region ${REGION} \
  --set-env-vars="DATABASE_URL=...,RELAY_ID=...,RUST_LOG=info" \
  --allow-unauthenticated # For Community Relay (Public)
  # OR remove --allow-unauthenticated for Private Relay (IAM required)
```

---

## 3. Operations

### Backups
Dump the Postgres database using our helper script:
```bash
./scripts/backup.sh
# Output: backup_tisane_relay_20260204_153000.sql
```

### Restore
**WARNING**: Overwrites existing data.
```bash
./scripts/restore.sh backup_file.sql
```

### Peer Management (CLI)
Manage your federation whitelist:
```bash
# Add Peer
docker-compose -f docker-compose.prod.yml exec tisane-relay \
  /app/tisane-relay add-peer --url https://peer.com --secret their-secret

# List Peers
docker-compose -f docker-compose.prod.yml exec tisane-relay \
  /app/tisane-relay list-peers
```
