#!/usr/bin/env bash
# Pull recent Railway deployment logs via GraphQL.
# Usage: ./scripts/railway_logs.sh [limit]
# Requires RAILWAY_API_TOKEN in env.

set -euo pipefail

: "${RAILWAY_API_TOKEN:?RAILWAY_API_TOKEN not set — export it (account-level token)}"

PROJECT_ID="264ac4f6-3554-4801-9d75-b177c1eadbf2"
ENV_ID="7852d18e-bc72-4bce-94f3-267ca3b24617"
SERVICE_ID="7d9018a8-8b71-41a0-90f6-e633fa8cdb1c"
LIMIT="${1:-200}"

# Find the most recent SUCCESS deployment for the service
DEPLOY_ID=$(curl -sS https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $RAILWAY_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"query { deployments(first: 5, input: {projectId: \\\"$PROJECT_ID\\\", environmentId: \\\"$ENV_ID\\\", serviceId: \\\"$SERVICE_ID\\\"}) { edges { node { id status } } } }\"}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin)['data']['deployments']['edges']; print(next(e['node']['id'] for e in d if e['node']['status']=='SUCCESS'))")

curl -sS https://backboard.railway.com/graphql/v2 \
  -H "Authorization: Bearer $RAILWAY_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"query { deploymentLogs(deploymentId: \\\"$DEPLOY_ID\\\", limit: $LIMIT) { message timestamp } }\"}" \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)['data']['deploymentLogs']
# logs come in arbitrary order; sort by timestamp ascending for readability
d.sort(key=lambda r: r['timestamp'])
for r in d:
    print(r['message'])
"
