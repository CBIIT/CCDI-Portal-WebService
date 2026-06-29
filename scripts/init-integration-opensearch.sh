#!/bin/sh
# Create CCDI integration-test indices on OpenSearch (mappings from
# src/test/resources/opensearch/mappings), then bulk-load minimal documents from
# src/test/resources/opensearch/fixtures/integration_minimal_bulk.ndjson via
# scripts/seed-integration-opensearch-fixtures.sh.
#
# Environment:
#   OPENSEARCH_URL  Base URL (default http://localhost:9200)
#   MAPPINGS_DIR    Directory of <index>.json bodies

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
BASE="${OPENSEARCH_URL:-http://localhost:9200}"
MAPPINGS_DIR="${MAPPINGS_DIR:-$ROOT/src/test/resources/opensearch/mappings}"

i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS "${BASE}/_cluster/health" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
if ! curl -fsS "${BASE}/_cluster/health" >/dev/null 2>&1; then
  echo "Timeout waiting for OpenSearch at ${BASE}" >&2
  exit 1
fi

nfiles=$(find "$MAPPINGS_DIR" -maxdepth 1 -name '*.json' | wc -l | tr -d ' ')
if [ "$nfiles" -eq 0 ]; then
  echo "No mapping JSON files under ${MAPPINGS_DIR}" >&2
  exit 1
fi

for f in $(find "$MAPPINGS_DIR" -maxdepth 1 -name '*.json' | sort); do
  idx=$(basename "$f" .json)
  curl -fsS -X DELETE "${BASE}/${idx}" >/dev/null 2>&1 || true
  curl -fsS -X PUT "${BASE}/${idx}" \
    -H 'Content-Type: application/json' \
    --data-binary "@${f}" >/dev/null
  echo "Created index: ${idx}"
done

echo "OpenSearch integration indices ready at ${BASE} (${nfiles} indices)"

sh "${ROOT}/scripts/seed-integration-opensearch-fixtures.sh"
