#!/bin/sh
# Bulk-index minimal CCDI documents for integration tests (one row per model index).
# Expects indices to exist (run init-integration-opensearch.sh first).
#
# Environment:
#   OPENSEARCH_URL  Base URL (default http://localhost:9200)
#   FIXTURES_DIR    Directory containing integration_minimal_bulk.ndjson

set -eu

ROOT="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
BASE="${OPENSEARCH_URL:-http://localhost:9200}"
FIXTURES_DIR="${FIXTURES_DIR:-$ROOT/src/test/resources/opensearch/fixtures}"
BULK_FILE="${BULK_FILE:-$FIXTURES_DIR/integration_minimal_bulk.ndjson}"

if ! curl -fsS "${BASE}/_cluster/health" >/dev/null 2>&1; then
  echo "OpenSearch not reachable at ${BASE}" >&2
  exit 1
fi

if [ ! -f "$BULK_FILE" ]; then
  echo "Bulk fixture not found: ${BULK_FILE}" >&2
  exit 1
fi

echo "Bulk indexing fixtures from ${BULK_FILE} ..."
resp=$(curl -sS -X POST "${BASE}/_bulk" \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary "@${BULK_FILE}")

# Fail if any item errored (mapping rejections, etc.)
if command -v python3 >/dev/null 2>&1; then
  printf '%s' "$resp" | python3 -c 'import json,sys; r=json.load(sys.stdin); e=r.get("errors"); assert e is False, json.dumps(r, indent=2)[:12000]'
else
  case "$resp" in
    *'"errors":false'*) ;;
    *)
      echo "Bulk indexing reported errors (install python3 for details):" >&2
      echo "$resp" >&2
      exit 1
      ;;
  esac
fi

echo "OpenSearch integration fixtures indexed at ${BASE}"
