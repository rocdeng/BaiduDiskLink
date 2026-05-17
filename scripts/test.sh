#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOCACHE="${GOCACHE:-/private/tmp/baidudisklink-go-cache}"
export GOMODCACHE="${GOMODCACHE:-/private/tmp/baidudisklink-gomodcache}"

cd "$ROOT_DIR"
exec /usr/local/go/bin/go test ./...
