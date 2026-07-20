#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SPEC_DIR="$ROOT/spec"
CONFIG_DIR="${TLA_CONFIG_DIR:-$SPEC_DIR/deep}"
TOOLS_DIR="${TLA_TOOLS_DIR:-$ROOT/.tla-tools}"
TLA2TOOLS_JAR="${TLA2TOOLS_JAR:-$TOOLS_DIR/tla2tools.jar}"
TLA2TOOLS_URL="${TLA2TOOLS_URL:-https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar}"
TLA2TOOLS_SHA256="${TLA2TOOLS_SHA256:-936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88}"
TLA_JAVA_OPTS="${TLA_JAVA_OPTS:--Xmx2g -XX:+UseParallelGC}"
SPECS=("$@")

if [ "${#SPECS[@]}" -eq 0 ]; then
  SPECS=(omd_lease omd_connect omd_leader omd_admission)
fi

if ! command -v java >/dev/null 2>&1; then
  echo "java is required to run TLC" >&2
  exit 127
fi

if [ ! -f "$TLA2TOOLS_JAR" ]; then
  mkdir -p "$TOOLS_DIR"
  curl -fsSL "$TLA2TOOLS_URL" -o "$TLA2TOOLS_JAR"
fi

if command -v sha256sum >/dev/null 2>&1; then
  actual_sha256="$(sha256sum "$TLA2TOOLS_JAR" | awk '{print $1}')"
else
  actual_sha256="$(shasum -a 256 "$TLA2TOOLS_JAR" | awk '{print $1}')"
fi
if [ "$actual_sha256" != "$TLA2TOOLS_SHA256" ]; then
  echo "tla2tools.jar SHA-256 mismatch: got $actual_sha256" >&2
  exit 2
fi

for spec in "${SPECS[@]}"; do
  cfg="$CONFIG_DIR/${spec}.cfg"
  if [ ! -f "$cfg" ]; then
    echo "missing deep config: $cfg" >&2
    exit 2
  fi
  echo "== TLC deep $spec =="
  (
    cd "$SPEC_DIR"
    java $TLA_JAVA_OPTS -jar "$TLA2TOOLS_JAR" -config "$cfg" "${spec}.tla"
  )
done
