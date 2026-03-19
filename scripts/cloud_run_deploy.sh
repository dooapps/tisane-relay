#!/usr/bin/env bash
set -euo pipefail

SERVICE_NAME=${SERVICE_NAME:-tisane-relay}
TAG=${1:-1}
REGION=${REGION:-us-central1}
IMAGE="us-central1-docker.pkg.dev/ledit-1f375/tisane-repo/tisane-relay:${TAG}"

if [ -z "${DATABASE_URL:-}" ]; then
  echo "Please set DATABASE_URL to the production Postgres connection string (example: cloudsql or external DB)" >&2
  exit 1
fi

# Deploy to Cloud Run (private, requires IAM to call)
gcloud run deploy "${SERVICE_NAME}" \
  --image "${IMAGE}" \
  --region "${REGION}" \
  --platform managed \
  --no-allow-unauthenticated \
  --set-env-vars "DATABASE_URL=${DATABASE_URL},PORT=8080"

echo "Cloud Run deploy completed: ${SERVICE_NAME} -> ${IMAGE}" 
