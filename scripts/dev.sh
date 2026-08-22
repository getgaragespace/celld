#!/usr/bin/env bash
# Local celld development with a file:// fleet bucket (no cloud or Azurite).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
FLEET_DIR="${CELLD_FLEET_DIR:-$ROOT/.celld-fleet}"
BIN="$ROOT/target/lab/celld"

cargo build --profile lab -p celld

mkdir -p "$FLEET_DIR"
export CELLD_BUCKET="file://$FLEET_DIR"
export CELLD_WATCH="${CELLD_WATCH:-$(mktemp -d /tmp/celld-watch.XXXXXX)}"

echo "Fleet bucket: $CELLD_BUCKET"
echo "Local state:  $CELLD_WATCH"
echo
echo "Start celld:"
echo "  $BIN --bucket \"$CELLD_BUCKET\""
echo
echo "Deploy the counter example (requires esbuild on PATH):"
echo "  $BIN deploy \"$ROOT/examples/counter\" --bucket \"$CELLD_BUCKET\""
echo
echo "Then curl http://127.0.0.1:8080/"
