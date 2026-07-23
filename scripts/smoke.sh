#!/usr/bin/env bash
# scripts/smoke.sh — cross-process DoD:
#   meshd (loopback) → mesh agent (echo) → mesh call → assert echo payload
# Exit 0 only when the full hub+agent+call path works end-to-end.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

unset ALL_PROXY HTTP_PROXY HTTPS_PROXY all_proxy http_proxy https_proxy
export PATH="${PATH}:/usr/local/go/bin:${HOME}/go/bin"

BIN_DIR="${TMPDIR:-/tmp}/agentmesh-smoke-$$"
mkdir -p "$BIN_DIR"
MESHD="$BIN_DIR/meshd"
MESH="$BIN_DIR/mesh"
LOG_DIR="$BIN_DIR/logs"
mkdir -p "$LOG_DIR"

cleanup() {
  local code=$?
  if [[ -n "${AGENT_PID:-}" ]] && kill -0 "$AGENT_PID" 2>/dev/null; then
    kill "$AGENT_PID" 2>/dev/null || true
    wait "$AGENT_PID" 2>/dev/null || true
  fi
  if [[ -n "${HUB_PID:-}" ]] && kill -0 "$HUB_PID" 2>/dev/null; then
    kill "$HUB_PID" 2>/dev/null || true
    wait "$HUB_PID" 2>/dev/null || true
  fi
  rm -rf "$BIN_DIR"
  exit "$code"
}
trap cleanup EXIT INT TERM

echo "==> building meshd + mesh"
go build -o "$MESHD" ./cmd/meshd
go build -o "$MESH"  ./cmd/mesh

# Pick a free loopback port.
PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
HUB_URL="http://127.0.0.1:${PORT}"
TOKEN="ka"
AGENT_ID="smoke-echo"
PAYLOAD='{"hello":"smoke"}'

export MESH_HOST="127.0.0.1"
export MESH_PORT="$PORT"
export MESH_API_KEYS="a:${TOKEN}:smoke:default"
# loopback → TLS not required

echo "==> starting meshd on ${HUB_URL}"
"$MESHD" serve >"$LOG_DIR/meshd.log" 2>&1 &
HUB_PID=$!

# Wait for /health
deadline=$((SECONDS + 10))
until curl -sf "${HUB_URL}/health" >/dev/null 2>&1; do
  if ! kill -0 "$HUB_PID" 2>/dev/null; then
    echo "meshd died during startup:" >&2
    cat "$LOG_DIR/meshd.log" >&2 || true
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo "meshd did not become healthy in time" >&2
    cat "$LOG_DIR/meshd.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
echo "    meshd healthy"

echo "==> starting mesh agent ${AGENT_ID}"
"$MESH" agent \
  --hub "$HUB_URL" \
  --token "$TOKEN" \
  --agent-id "$AGENT_ID" \
  --caps echo \
  >"$LOG_DIR/agent.log" 2>&1 &
AGENT_PID=$!

# Wait until agent appears in /v1/agents
deadline=$((SECONDS + 10))
until curl -sf -H "Authorization: Bearer ${TOKEN}" "${HUB_URL}/v1/agents" \
    | grep -q "$AGENT_ID"; do
  if ! kill -0 "$AGENT_PID" 2>/dev/null; then
    echo "agent died during startup:" >&2
    cat "$LOG_DIR/agent.log" >&2 || true
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo "agent did not register in time" >&2
    cat "$LOG_DIR/agent.log" >&2 || true
    exit 1
  fi
  sleep 0.1
done
echo "    agent registered"

echo "==> mesh call → ${AGENT_ID}"
OUT="$("$MESH" call \
  --hub "$HUB_URL" \
  --token "$TOKEN" \
  --to "$AGENT_ID" \
  --payload "$PAYLOAD" \
  --ttl-ms 5000)"
echo "    result: $OUT"

# Assert the echo payload is present (FormatResult wraps as JSON with "payload").
if ! echo "$OUT" | grep -q 'smoke'; then
  echo "FAIL: expected echo payload containing 'smoke', got: $OUT" >&2
  exit 1
fi
if ! echo "$OUT" | grep -q "$AGENT_ID"; then
  echo "FAIL: expected from=$AGENT_ID in result, got: $OUT" >&2
  exit 1
fi
# Structured: payload should round-trip the JSON object keys.
if ! echo "$OUT" | grep -q 'hello'; then
  echo "FAIL: expected payload key 'hello' in result, got: $OUT" >&2
  exit 1
fi

echo "==> smoke PASS"
