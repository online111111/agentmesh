#!/usr/bin/env bash
# Cross-compile meshd + mesh for the v1 release matrix.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
unset ALL_PROXY HTTP_PROXY HTTPS_PROXY all_proxy http_proxy https_proxy
export PATH="${PATH}:/usr/local/go/bin:${HOME}/go/bin"
OUT="${ROOT}/dist"
mkdir -p "$OUT"

targets=(
  "linux/amd64"
  "linux/arm64"
  "darwin/amd64"
  "darwin/arm64"
  "windows/amd64"
  "windows/arm64"
)

for t in "${targets[@]}"; do
  GOOS="${t%/*}"
  GOARCH="${t#*/}"
  ext=""
  [[ "$GOOS" == "windows" ]] && ext=".exe"
  for cmd in meshd mesh; do
    out="${OUT}/${cmd}-${GOOS}-${GOARCH}${ext}"
    echo "==> $out"
    CGO_ENABLED=0 GOOS="$GOOS" GOARCH="$GOARCH" \
      go build -trimpath -ldflags="-s -w" -o "$out" "./cmd/${cmd}"
  done
done
echo "cross-compile OK → ${OUT}"
ls -la "$OUT"
