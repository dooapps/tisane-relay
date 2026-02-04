#!/usr/bin/env bash
set -euo pipefail

TAG=${1:-1}
IMAGE_NAME="us-central1-docker.pkg.dev/ledit-1f375/tisane-repo/tisane-relay:${TAG}"

# Build local image
docker build -t tisane-relay:${TAG} .

# Configure gcloud docker auth (requires gcloud installed and logged in)
gcloud auth configure-docker us-central1-docker.pkg.dev

# Tag and push
docker tag tisane-relay:${TAG} ${IMAGE_NAME}
docker push ${IMAGE_NAME}

echo "Pushed ${IMAGE_NAME}" 
