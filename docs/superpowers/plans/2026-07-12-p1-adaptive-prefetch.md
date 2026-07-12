# P1 Adaptive Read-Ahead Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add handle-local adaptive 4–32 MiB windows and one cancellable next-window prefetch to improve FUSE startup, seek recovery, and sustained playback throughput.

**Architecture:** `entryFileHandle` owns the access-pattern state machine and prefetch lifecycle. `remote.Reader` exposes an explicit `Prefetch` operation that reuses P0 cache, inflight coalescing, cancellation, HTTP validation, and resource limits. Published windows remain immutable and no second prediction system is added to playback.

**Tech Stack:** Go 1.24, `context`, `sync`, go-fuse v2, existing P0 remote cache and Range client.

## Global Constraints

- Use the fixed window ladder 4 MiB, 8 MiB, 16 MiB, 32 MiB.
- Trigger next-window prefetch only after at least 50% of the current actual window is consumed.
- Keep at most one prefetch goroutine per FUSE file handle.
- Cancel prefetch on seek, replacement, and `Release`; wait for exit during `Release`.
- Do not add environment variables, dependencies, disk caching, playback prediction, or global access-pattern inference.
- Preserve P0 defaults: download concurrency 1, chunk size 4 MiB, read-cache budget 64 MiB.
- Prefetch errors must not fail an already successful foreground FUSE read.

---

### Task 1: Explicit remote prefetch with inflight reuse

**Files:**
- Modify: `internal/remote/remote.go`
- Test: `internal/remote/remote_test.go`

**Interfaces:**
- Produces: `func (r *Reader) Prefetch(ctx context.Context, fsid string, offset, length int64) error`.
- Consumes: existing `ReadExactRange`, read cache, inflight map, P0 context-aware Baidu reads.

- [ ] **Step 1: Add failing prefetch behavior tests**

Add tests proving an already cached range causes no new client read, a successful prefetch populates cache, concurrent foreground read coalesces with the same prefetch window, cancellation releases a blocked prefetch, and failures do not leave an inflight entry.

Core test shape:

```go
func TestPrefetchPopulatesCache(t *testing.T) {
    client := &stubClient{}
    r := NewReader(client)
    if err := r.Prefetch(context.Background(), "1", 0, 4); err != nil {
        t.Fatal(err)
    }
    before := client.readCount()
    if _, err := r.ReadExactRange(context.Background(), "1", 0, 4); err != nil {
        t.Fatal(err)
    }
    if client.readCount() != before {
        t.Fatal("foreground read missed prefetched cache")
    }
}
```

- [ ] **Step 2: Run remote tests and verify RED**

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test ./internal/remote
```

Expected: FAIL because `Reader.Prefetch` does not exist.

- [ ] **Step 3: Implement `Reader.Prefetch`**

Validate reader, FSID, offset, length, and context. Return immediately on a full cache hit. Reuse exact-window inflight ownership: an owner downloads once using the existing exact Range path and publishes the immutable result; a waiter observes `inflight.done` or its own context cancellation. Ensure every owner path calls `finishInflight`, including cancellation and HTTP errors.

- [ ] **Step 4: Run remote tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

---

### Task 2: Pure adaptive-window state transitions

**Files:**
- Modify: `internal/fs/filesystem.go`
- Test: `internal/fs/filesystem_test.go`

**Interfaces:**
- Produces: fixed constants `minSequentialWindow = 4<<20`, `maxSequentialWindow = 32<<20`.
- Produces: handle helpers that classify a request and return fetch offset/length without network calls.
- Consumes: entry size, requested offset/length, previous returned range, and current window bounds.

- [ ] **Step 1: Add failing table tests for window transitions**

Cover first read at 4 MiB, sequential progression 4→8→16→32 MiB, no growth on repeated same-range reads, maximum clamp, large-request coverage, file-tail truncation, forward/backward/hole seek reset, and post-seek regrowth.

Representative table row:

```go
{name: "stable sequential reaches max", offsets: []int64{0, 4<<20, 12<<20, 28<<20}, wantWindows: []int64{4<<20, 8<<20, 16<<20, 32<<20}}
```

- [ ] **Step 2: Run filesystem tests and verify RED**

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test ./internal/fs
```

Expected: FAIL because the handle still uses a fixed 16 MiB window.

- [ ] **Step 3: Implement the pure state machine**

Replace fixed `windowSize` behavior with a ladder index, `lastReadStart`, `lastReadEnd`, and `lastProgressEnd`. Classify seek before network I/O. A seek uses `min(max(length*2, 1<<20), 4<<20)` and resets the ladder. A new sequential boundary advances one ladder step; repeated reads that do not advance `lastProgressEnd` do not grow. Always clamp fetch length to cover the request and file remainder.

- [ ] **Step 4: Run filesystem tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

---

### Task 3: Handle-local asynchronous next-window prefetch

**Files:**
- Modify: `internal/fs/filesystem.go`
- Test: `internal/fs/filesystem_test.go`

**Interfaces:**
- Consumes: `Reader.Prefetch(context.Context, string, int64, int64) error` from Task 1.
- Produces: one prefetch task per `entryFileHandle` with cancel/done identity.
- Produces: `Release(context.Context) syscall.Errno` on the file handle.

- [ ] **Step 1: Add failing trigger and lifecycle tests**

Use a controllable Baidu client to assert: consumption below 50% starts no task; crossing 50% starts exactly one adjacent request; repeated reads do not duplicate it; file tail starts no zero-length request; seek cancels a blocked request; `Release` cancels and waits; prefetch failure leaves foreground data successful.

The cancellation test must block the fake client until `ctx.Done()` and use a bounded test timeout so leaked goroutines fail deterministically.

- [ ] **Step 2: Run filesystem tests and verify RED**

Run the Task 2 command. Expected: FAIL because no prefetch lifecycle or `Release` exists.

- [ ] **Step 3: Add prefetch lifecycle state**

Extend `entryFileHandle` with `prefetchCancel`, `prefetchDone`, `prefetchOff`, `prefetchLen`, monotonic `prefetchID`, and `closed`. Add a helper that registers a task under the mutex, then launches the network call after unlocking. Completion reacquires the mutex and clears state only if IDs match.

- [ ] **Step 4: Trigger prefetch after successful foreground reads**

After selecting/returning foreground data, compute actual consumed position relative to the current window. At 50%, schedule `[windowOff+len(window), adaptiveWindowSize]`, clamped to file size. Skip cached, duplicate, zero-length, or closed work. Do not inherit the single FUSE request context; create a handle-lifetime child context.

- [ ] **Step 5: Cancel on seek and implement `Release`**

Before a seek fetch, detach and cancel the prior task outside the mutex. `Release` marks the handle closed, extracts cancel/done under lock, unlocks, calls cancel, then waits for done. It returns success and is idempotent.

- [ ] **Step 6: Run filesystem tests and verify GREEN**

Run the Task 2 command. Expected: PASS.

---

### Task 4: Trace visibility and regression coverage

**Files:**
- Modify: `internal/fs/filesystem.go`
- Test: `internal/fs/filesystem_test.go`
- Test: `internal/app/playback_test.go`

**Interfaces:**
- Consumes: existing `traceReads` switch.
- Produces: foreground strategy labels that distinguish adaptive window, seek exact, handle cache, and prefetched cache hits.

- [ ] **Step 1: Add failing strategy tests**

Assert sequential foreground reads report adaptive-window strategy, seeks report seek-exact, and a completed next-window prefetch is consumed without another client read. Retain playback Range and HEAD behavior tests to prove no second playback predictor was introduced.

- [ ] **Step 2: Run affected tests and verify RED**

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test ./internal/fs ./internal/app
```

Expected: FAIL on new strategy expectations before labels are wired.

- [ ] **Step 3: Wire stable strategy labels**

Use `adaptive-window`, `seek-exact`, `handle-cache`, and `prefetched-cache`. Log non-cancellation prefetch failures only when `Filesystem.traceReads` is enabled. Do not change normal logging volume.

- [ ] **Step 4: Run affected tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

---

### Task 5: Race and project verification

**Files:**
- Modify only if verification exposes a P1 regression.

**Interfaces:**
- Consumes all prior outputs.
- Produces verified P1 behavior without live-environment claims.

- [ ] **Step 1: Run race-enabled affected tests**

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test -race ./internal/remote ./internal/fs ./internal/app
```

Expected: PASS with no race report or leaked-test timeout.

- [ ] **Step 2: Run full project checks**

```bash
make check
```

Expected: all Go tests, build, smoke, script syntax, and deployment assertions pass.

- [ ] **Step 3: Record live validation requirements**

Report that DSM validation still requires the same file and sample settings for `bench` and `bench-fuse`, plus Emby startup/seek tests, Range request counts, peak RSS, and goroutine/connection observation after repeated seeks.
