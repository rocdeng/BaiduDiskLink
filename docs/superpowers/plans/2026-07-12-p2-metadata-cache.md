# P2 Metadata and Cache Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make metadata snapshot writes atomic and reduce SQLite and multi-file cache lookup overhead.

**Architecture:** Store batch operations share one transaction-scoped prepared upsert helper. Directory replacement deletes and inserts within the same transaction. Remote read cache keeps existing byte-budget LRU semantics while adding an FSID bucket index.

**Tech Stack:** Go 1.24, `database/sql`, modernc SQLite, existing synchronized remote cache.

## Global Constraints

- No schema migration, dependency, environment variable, or user-facing behavior change.
- Preserve 64 MiB cache budget and immutable cached slices.
- Keep remote directory listing as the authoritative direct-child snapshot.
- Every production change follows a failing focused test.

---

### Task 1: Atomic directory replacement

**Files:** `internal/store/store.go`, `internal/store/store_test.go`

- [ ] Add a failing test that forces an insert constraint failure after an existing child is present and asserts the old child remains.
- [ ] Run `go test ./internal/store` and observe the old child is lost.
- [ ] Move delete and per-entry prepared upserts into one `sql.Tx`; defer rollback and commit only after every entry succeeds.
- [ ] Add empty-snapshot coverage and rerun `go test ./internal/store` to green.

### Task 2: Transactional batch upserts

**Files:** `internal/store/store.go`, `internal/store/store_test.go`

- [ ] Add failing rollback tests for `UpsertEntries` and parent-preservation tests for `UpsertFromRemote`.
- [ ] Run `go test ./internal/store` and verify partial writes expose the current behavior.
- [ ] Extract one internal transaction helper that prepares the canonical upsert once and executes all entries.
- [ ] Route `UpsertEntries` and `UpsertFromRemote` through it; rerun store tests.

### Task 3: FSID-bucketed read-cache lookup

**Files:** `internal/remote/remote.go`, `internal/remote/remote_test.go`

- [ ] Add failing tests that inspect bucket membership after insert, replacement, LRU eviction, and clear.
- [ ] Run `go test ./internal/remote` and observe missing index state.
- [ ] Add `cacheByFSID map[string]map[cacheKey]struct{}` to `Reader`; initialize and clear it with cache state.
- [ ] Make reads inspect only the requested FSID bucket and keep LRU promotion unchanged.
- [ ] Update replacement and eviction to remove stale bucket keys and empty buckets; rerun remote tests.

### Task 4: Verification

- [ ] Run `gofmt` on touched Go files.
- [ ] Run `/usr/local/go/bin/go test -race ./internal/store ./internal/remote ./internal/fs`.
- [ ] Run `/usr/local/go/bin/go vet ./internal/store ./internal/remote ./internal/fs`.
- [ ] Run `make check`.
- [ ] Record live-only DSM validation requirements without claiming measured performance gains.
