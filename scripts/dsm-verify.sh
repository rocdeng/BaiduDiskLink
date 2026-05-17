#!/usr/bin/env bash
set -euo pipefail

CONTAINER="${BAIDUDISKLINK_CONTAINER:-baidudisklink}"
MOUNT_PATH="${BAIDUDISKLINK_MOUNT_PATH:-/mnt/baidu}"
TOKEN_PATH="${BAIDUDISKLINK_TOKEN_PATH:-/data/token.json}"
META_DB_PATH="${BAIDUDISKLINK_META_DB_PATH:-/data/meta.db}"
READ_TIMEOUT="${BAIDUDISKLINK_VERIFY_READ_TIMEOUT:-20s}"

echo "== DSM BaiduDiskLink verification =="
echo "container: $CONTAINER"
echo "mount path: $MOUNT_PATH"
echo "read timeout: $READ_TIMEOUT"
echo "host prerequisites: docker"
echo "container prerequisites: /dev/fuse, find, dd, timeout"
echo "expected results: mounted path exists, contains files, and first file is readable"

passed=0
failed=0
failed_checks=()

summary() {
	echo "DSM verification summary: $passed passed, $failed failed"
	if [ "${#failed_checks[@]}" -gt 0 ]; then
		printf 'DSM verification failed checks: %s\n' "${failed_checks[*]}"
	fi
}

trap summary EXIT

check() {
	local name="$1"
	shift
	printf 'checking: %s... ' "$name"
	if "$@"; then
		echo "ok"
		passed=$((passed + 1))
		return 0
	fi
	echo "failed" >&2
	failed=$((failed + 1))
	failed_checks+=("$name")
	return 1
}

check "docker command is available" sh -c "command -v docker >/dev/null"
check "container is running" sh -c "docker ps --filter 'name=$CONTAINER' --format '{{.Names}} {{.Status}}' | grep -q '$CONTAINER'"
check "fuse device is visible" docker exec "$CONTAINER" test -e /dev/fuse
check "mount directory exists" docker exec "$CONTAINER" test -d "$MOUNT_PATH"
check "token file exists" docker exec "$CONTAINER" test -s "$TOKEN_PATH"
check "metadata db exists" docker exec "$CONTAINER" test -s "$META_DB_PATH"
check "mount table contains mount path" docker exec "$CONTAINER" sh -lc "mount | grep -q '$MOUNT_PATH'"
check "find command is available" docker exec "$CONTAINER" sh -lc "command -v find >/dev/null"
check "dd command is available" docker exec "$CONTAINER" sh -lc "command -v dd >/dev/null"
check "timeout command is available" docker exec "$CONTAINER" sh -lc "command -v timeout >/dev/null"

echo "mounted entries:"
docker exec "$CONTAINER" sh -lc "find '$MOUNT_PATH' -maxdepth 2 -mindepth 1 | head -n 20"
check "mounted path contains at least one file" docker exec "$CONTAINER" sh -lc "test -n \"\$(find '$MOUNT_PATH' -type f | head -n 1)\""
check "can read one byte from first mounted file" docker exec "$CONTAINER" sh -lc "first_file=\$(find '$MOUNT_PATH' -type f | head -n 1); timeout '$READ_TIMEOUT' dd if=\"\$first_file\" of=/dev/null bs=1 count=1 status=none"

if [ "$failed" -gt 0 ]; then
	exit 1
fi
echo "DSM verification probes completed."
