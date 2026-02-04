#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="ledit-1f375"
API_NAME="ledit-api"
GATEWAY_NAME="ledit-gateway"
SERVICE_NAME="tisane-relay"
OPENAPI_FILE="infra/openapi.yaml"

# Discover gateway full name and location using JSON parsing (robust)
GW_FULL_NAME="$(gcloud api-gateway gateways list \
  --project "$PROJECT_ID" \
  --format=json 2>/dev/null | python3 -c '
import sys, json
try:
    data = json.load(sys.stdin)
except Exception:
    sys.exit(0)
GW = sys.argv[1]
name = None
for g in data:
    if g.get("displayName") == GW:
        name = g.get("name") or g.get("displayName")
        break
if not name:
    for g in data:
        n = g.get("name") or g.get("displayName")
        if n and n.endswith("/gateways/" + GW):
            name = n
            break
if not name:
    for g in data:
        short = g.get("name") or g.get("displayName")
        if short == GW:
            name = short
            break
if name:
    print(name)
' "$GATEWAY_NAME")"

if [[ -z "${GW_FULL_NAME}" ]]; then
  echo "ERROR: Gateway ${GATEWAY_NAME} not found"
  exit 1
fi

# Try to parse location from full resource name; if not present, assume 'global'
if [[ "$GW_FULL_NAME" =~ /locations/([^/]+)/gateways/ ]]; then
  GW_LOCATION="${BASH_REMATCH[1]}"
else
  echo "Warning: could not parse gateway location from: $GW_FULL_NAME. Assuming 'global'."
  GW_LOCATION="global"
fi

echo "Gateway location: $GW_LOCATION"

# Discover Cloud Run region by service (fixed in your case, but keeping safe)
RUN_REGION="us-central1"

RUN_URL="$(gcloud run services describe "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$RUN_REGION" \
  --format='value(status.url)')"

if [[ -z "$RUN_URL" ]]; then
  echo "ERROR: Cloud Run URL not found for $SERVICE_NAME"
  exit 1
fi

echo "Cloud Run URL: $RUN_URL"

python3 - "$OPENAPI_FILE" "$RUN_URL" <<'PY'
import re, sys
path=sys.argv[1]
run_url=sys.argv[2]
txt=open(path,"r",encoding="utf-8").read()

pattern = r'(x-google-backend:\s*\n(?:[ \t].*\n)*?[ \t]*address:\s*)(\S+)'
if re.search(pattern, txt):
    txt = re.sub(pattern, r'\1'+run_url, txt)
else:
    pattern2 = r'(x-google-backend:\s*\{[^}]*address:\s*)([^,\s}]+)'
    if re.search(pattern2, txt):
        txt = re.sub(pattern2, r'\1'+run_url, txt)
    else:
        raise SystemExit("ERROR: Could not find x-google-backend.address in OpenAPI")

open(path,"w",encoding="utf-8").write(txt)
print("Updated backend address in OpenAPI.")
PY

CONFIG_ID="ledit-config-$(date +%Y%m%d-%H%M%S)"

echo "Creating api-config: $CONFIG_ID"
gcloud api-gateway api-configs create "$CONFIG_ID" \
  --project "$PROJECT_ID" \
  --api "$API_NAME" \
  --openapi-spec "$OPENAPI_FILE" \
  --backend-auth-service-account "tisane-gateway-invoker@${PROJECT_ID}.iam.gserviceaccount.com"

echo "Updating gateway..."
gcloud api-gateway gateways update "$GATEWAY_NAME" \
  --project "$PROJECT_ID" \
  --location "$GW_LOCATION" \
  --api "$API_NAME" \
  --api-config "$CONFIG_ID"

gcloud api-gateway gateways describe "$GATEWAY_NAME" \
  --project "$PROJECT_ID" \
  --location "$GW_LOCATION" \
  --format="yaml(state,defaultHostname)"
