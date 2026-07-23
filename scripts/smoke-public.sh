#!/usr/bin/env bash
# Optional public-path smoke: requires MESH_PUBLIC_URL and MESH_TOKEN / MESH_API_KEYS
# already pointing at a TLS-terminated Hub. Skips cleanly when unset.
set -euo pipefail
if [[ -z "${MESH_PUBLIC_URL:-}" ]]; then
  echo "smoke-public: MESH_PUBLIC_URL unset — skip (exit 0)"
  exit 0
fi
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MESH="${ROOT}/bin/mesh"
if [[ ! -x "$MESH" ]]; then
  unset ALL_PROXY HTTP_PROXY HTTPS_PROXY all_proxy http_proxy https_proxy
  mkdir -p "${ROOT}/bin"
  go build -o "$MESH" "${ROOT}/cmd/mesh"
fi
TOKEN="${MESH_TOKEN:-ka}"
AGENT_ID="${MESH_SMOKE_AGENT:-public-echo-$$}"
"$MESH" agent --hub "$MESH_PUBLIC_URL" --token "$TOKEN" --agent-id "$AGENT_ID" --caps echo &
APID=$!
trap 'kill $APID 2>/dev/null || true' EXIT
sleep 2
OUT=$("$MESH" call --hub "$MESH_PUBLIC_URL" --token "$TOKEN" --to "$AGENT_ID" --payload '{"hello":"public"}' --ttl-ms 8000)
echo "$OUT" | grep -q public
echo "smoke-public PASS: $OUT"
