#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOCACHE="${GOCACHE:-/private/tmp/baidudisklink-go-cache}"
export GOMODCACHE="${GOMODCACHE:-/private/tmp/baidudisklink-gomodcache}"

cd "$ROOT_DIR"

/usr/local/go/bin/go build -o /tmp/baidudisklink ./cmd/baidudisklink

run_and_expect_failure() {
	local expected="$1"
	shift

	set +e
	output="$(
		env -i \
			PATH="$PATH" \
			HOME="$HOME" \
			"$@" 2>&1
	)"
	status=$?
	set -e

	if [ "$status" -eq 0 ]; then
		echo "expected startup failure for missing configuration" >&2
		exit 1
	fi

	printf '%s\n' "$output" | grep -q "$expected"
}

run_and_expect_failure "mount path is required" /tmp/baidudisklink
run_and_expect_failure "token path is required" \
	BAIDUDISKLINK_MOUNT_PATH="/mnt/baidu" /tmp/baidudisklink
run_and_expect_failure "metadata db path is required" \
	BAIDUDISKLINK_MOUNT_PATH="/mnt/baidu" \
	BAIDUDISKLINK_TOKEN_PATH="/data/token.json" \
	/tmp/baidudisklink
run_and_expect_failure "client id is required" \
	BAIDUDISKLINK_MOUNT_PATH="/mnt/baidu" \
	BAIDUDISKLINK_TOKEN_PATH="/data/token.json" \
	BAIDUDISKLINK_META_DB_PATH="/data/meta.db" \
	/tmp/baidudisklink
run_and_expect_failure "redirect uri is required" \
	BAIDUDISKLINK_MOUNT_PATH="/mnt/baidu" \
	BAIDUDISKLINK_TOKEN_PATH="/data/token.json" \
	BAIDUDISKLINK_META_DB_PATH="/data/meta.db" \
	BAIDUDISKLINK_CLIENT_ID="client" \
	/tmp/baidudisklink
