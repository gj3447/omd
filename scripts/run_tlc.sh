#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC_DIR="$ROOT/spec"
TOOLS_DIR="${TLA_TOOLS_DIR:-$ROOT/.tla-tools}"
TLA2TOOLS_JAR="${TLA2TOOLS_JAR:-$TOOLS_DIR/tla2tools.jar}"
TLA2TOOLS_URL="${TLA2TOOLS_URL:-https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar}"

if ! command -v java >/dev/null 2>&1; then
  echo "java is required to run TLC" >&2
  exit 127
fi

if [ ! -f "$TLA2TOOLS_JAR" ]; then
  mkdir -p "$TOOLS_DIR"
  curl -fsSL "$TLA2TOOLS_URL" -o "$TLA2TOOLS_JAR"
fi

for spec in omd_lease omd_connect omd_leader; do
  echo "== TLC $spec =="
  (
    cd "$SPEC_DIR"
    java -XX:+UseParallelGC -jar "$TLA2TOOLS_JAR" "${spec}.tla"
  )
done
