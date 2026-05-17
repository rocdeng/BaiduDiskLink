#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export GOCACHE="${GOCACHE:-/private/tmp/baidudisklink-go-cache}"
export GOMODCACHE="${GOMODCACHE:-/private/tmp/baidudisklink-gomodcache}"

cd "$ROOT_DIR"

/usr/local/go/bin/go test ./...
/usr/local/go/bin/go build -o /tmp/baidudisklink ./cmd/baidudisklink
scripts/smoke.sh
bash -n scripts/test.sh scripts/smoke.sh scripts/verify.sh scripts/dsm-verify.sh

grep -q "BAIDUDISKLINK_OAUTH_LISTEN_ADDR" README.md
grep -q "make dsm-verify" README.md
grep -q "make check" README.md
grep -q "日常改动优先跑" README.md
grep -q "BAIDUDISKLINK_CLIENT_ID" .env.example
grep -q "BAIDUDISKLINK_CLIENT_SECRET" .env.example
grep -q "BAIDUDISKLINK_REDIRECT_URI" .env.example
grep -q "BAIDUDISKLINK_OAUTH_LISTEN_ADDR" .env.example
test -x scripts/dsm-verify.sh
grep -q "TestEnvExampleCoversRequiredDeploymentVariables" scripts/env_test.go
grep -q "TestMakeTargetsExpandAsExpected" scripts/make_test.go
grep -q "TestMakefileDeclaresPhonyCheckTarget" scripts/make_test.go
grep -q "dsm-verify:" Makefile
grep -q "check: test verify" Makefile
grep -q ".PHONY: test verify check dsm-verify build docker-build docker-up" Makefile
grep -q "/dev/fuse" docker-compose.yml
grep -q "0.0.0.0:8765" docker-compose.yml
grep -q "privileged: true" docker-compose.yml
grep -q "propagation: rshared" docker-compose.yml
grep -q "BAIDUDISKLINK_VERIFY_READ_TIMEOUT" scripts/dsm-verify.sh
grep -q "mounted path contains at least one file" scripts/dsm-verify.sh
grep -q "docker command is available" scripts/dsm-verify.sh
grep -q "find command is available" scripts/dsm-verify.sh
grep -q "dd command is available" scripts/dsm-verify.sh
grep -q "timeout command is available" scripts/dsm-verify.sh
grep -q "DSM verification summary" scripts/dsm-verify.sh
grep -q "DSM verification failed checks" scripts/dsm-verify.sh
grep -q "trap summary EXIT" scripts/dsm-verify.sh
grep -q "host prerequisites: docker" scripts/dsm-verify.sh
grep -q "container prerequisites: /dev/fuse, find, dd, timeout" scripts/dsm-verify.sh
grep -q "expected results: mounted path exists, contains files, and first file is readable" scripts/dsm-verify.sh
test -x scripts/dsm-verify.sh
make_dsm="$(make -n dsm-verify)"
printf '%s\n' "$make_dsm" | grep -q 'scripts/dsm-verify.sh'
make_check="$(make -n check)"
printf '%s\n' "$make_check" | grep -q 'go test ./...'
printf '%s\n' "$make_check" | grep -q 'scripts/verify.sh'
