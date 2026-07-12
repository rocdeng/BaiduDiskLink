# P0 Read Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce large-buffer copies, cap read cache and in-flight memory at 64 MiB, configure bounded HTTP transports, and propagate cancellation through concurrent ranged downloads.

**Architecture:** `remote.Reader` owns each mutable download window until completion, then publishes it as immutable cached data. `baidu.Client` fills caller-provided slices using context-aware bounded HTTP reads. A reader-wide semaphore and weighted byte budget constrain all downloads across files; an LRU enforces the 64 MiB cache budget.

**Tech Stack:** Go 1.24, `net/http`, `container/list`, `context`, `sync`, go-fuse v2, standard Go tests.

## Global Constraints

- Preserve all environment variable names and defaults.
- Keep default download concurrency at 1 and default chunk size at 4 MiB.
- Use a 64 MiB hard limit for global read cache and aggregate in-flight window bytes.
- Do not add dependencies, disk content caching, adaptive windows, or background prefetch.
- Make a clean internal API cutover; no deprecated adapters or duplicate read methods.
- Keep FUSE, playback, bench, OAuth, deletion, and directory refresh user-facing behavior unchanged.

---

### Task 1: Context-aware bounded Baidu range reads

**Files:**
- Modify: `internal/baidu/types.go`
- Modify: `internal/baidu/client.go`
- Modify: `internal/baidu/adapter.go`
- Test: `internal/baidu/client_test.go`

**Interfaces:**
- Produces: `Client.ReadRange(context.Context, string, int64, []byte) (int, error)`.
- Produces: internal `parseContentRange(string) (start, end int64, err error)` validation helper.
- Consumes: existing dlink cache and injected `*http.Client`.

- [ ] **Step 1: Add failing range-contract tests**

Add tests that call the new destination-buffer API and assert: a valid `206` fills the supplied buffer; `200` is rejected; mismatched `Content-Range` is rejected; a body larger than `dst` is rejected; cancellation reaches the transport. Update existing test fakes only enough for the tests to compile.

Representative contract:

```go
func TestAPIClientReadRangeFillsDestination(t *testing.T) {
    dst := make([]byte, 4)
    n, err := client.ReadRange(context.Background(), "42", 10, dst)
    if err != nil || n != 4 || string(dst) != "data" {
        t.Fatalf("n=%d data=%q err=%v", n, dst, err)
    }
}
```

- [ ] **Step 2: Run the Baidu package tests and verify RED**

Run:

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test ./internal/baidu
```

Expected: FAIL because the old `ReadRange(fsid, offset, length) ([]byte, error)` API does not satisfy the new calls/contract.

- [ ] **Step 3: Implement the API cutover and strict bounded read**

Change the interface to:

```go
ReadRange(ctx context.Context, fsid string, offset int64, dst []byte) (int, error)
```

Build the request with `http.NewRequestWithContext`, require `206`, validate `Content-Range`, and read through a `len(dst)+1` limit. Copy at most `len(dst)` bytes into `dst`; return an explicit oversized-response error if the extra probe byte exists. Accept short reads only when the response range ends at the declared total file size; otherwise return `io.ErrUnexpectedEOF`.

- [ ] **Step 4: Run Baidu tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

---

### Task 2: Reader-wide cancellation and download resource limits

**Files:**
- Modify: `internal/remote/remote.go`
- Test: `internal/remote/remote_test.go`
- Modify compile-only client fakes in: `internal/app/bench_test.go`, `internal/fs/filesystem_test.go`

**Interfaces:**
- Consumes: `baidu.Client.ReadRange(context.Context, string, int64, []byte) (int, error)`.
- Produces: `Reader.ReadRange(context.Context, string, int64) ([]byte, error)`.
- Produces: `Reader.ReadExactRange(context.Context, string, int64) ([]byte, error)`.
- Produces: reader-wide download semaphore and 64 MiB in-flight byte limiter.

- [ ] **Step 1: Add failing cancellation and global-limit tests**

Extend the remote stub so reads fill `dst`, track active requests, and optionally block until `ctx.Done()`. Add tests proving:

```go
func TestReadConcurrentCancelsOtherChunksAfterFirstError(t *testing.T)
func TestConcurrentReadsShareDownloadLimit(t *testing.T)
func TestCanceledReadDoesNotRefreshAuth(t *testing.T)
```

The first test uses one failing chunk and one blocking chunk and requires the blocking request to observe cancellation. The second starts reads for separate FSIDs and asserts observed peak active requests never exceeds configured concurrency. The third cancels the parent context and asserts refresh count remains zero.

- [ ] **Step 2: Run remote tests and verify RED**

Run:

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test ./internal/remote
```

Expected: FAIL because context is not accepted or propagated and workers use per-call concurrency only.

- [ ] **Step 3: Implement context propagation and shared limits**

Add context to every remote read method. Replace per-result `[]byte` values with workers writing directly into disjoint slices of the final window. Add a reader-wide buffered semaphore sized by `SetDownloadOptions`.

Implement a condition-variable byte limiter with this contract:

```go
type byteLimiter struct {
    mu    sync.Mutex
    used  int64
    limit int64
    wake  chan struct{}
}
func (l *byteLimiter) acquire(ctx context.Context, n int64) error
func (l *byteLimiter) release(n int64)
```

Requests at or below 64 MiB wait until `used+n <= limit`. A request above 64 MiB waits for `used == 0`, then runs exclusively and accounts its full size. Waiting observes `ctx.Done()`.

For concurrent chunks, derive a child context, cancel on the first non-cancellation error, stop dispatching new jobs, and preserve the first business error. Do not refresh authentication for `context.Canceled` or `context.DeadlineExceeded`.

- [ ] **Step 4: Run remote tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

---

### Task 3: Byte-budgeted immutable LRU and zero-copy handle reuse

**Files:**
- Modify: `internal/remote/remote.go`
- Modify: `internal/fs/filesystem.go`
- Test: `internal/remote/remote_test.go`
- Test: `internal/fs/filesystem_test.go`

**Interfaces:**
- Produces: immutable read windows stored in a 64 MiB LRU.
- Produces: cache-hit slices whose backing storage stays alive through the returned slice or file-handle reference.
- Consumes: context-aware remote methods from Task 2.

- [ ] **Step 1: Add failing byte-budget and backing-storage tests**

Add tests for: eviction by total bytes; LRU promotion on hit; a single window over 64 MiB not cached; clearing resets used bytes. Add a filesystem test that compares backing storage addresses for the remote result and handle window, proving the handle no longer duplicates the complete window.

Representative assertion:

```go
if &handle.window[0] != &source[0] {
    t.Fatal("file handle copied immutable read window")
}
```

- [ ] **Step 2: Run remote and filesystem tests and verify RED**

Run:

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test ./internal/remote ./internal/fs
```

Expected: FAIL because cache eviction is count-based and handle caching copies the full window.

- [ ] **Step 3: Implement the 64 MiB LRU**

Use `container/list` plus a key-to-element map. Each element stores one immutable `cachedRead`. Track `cacheBytes int64`. Promote on hit, subtract replaced/evicted entries, skip insertion when `len(data) > 64<<20`, and reset list/map/bytes when clearing.

Do not copy a freshly allocated completed download when publishing it to cache. Cached data becomes immutable after publication. Return sub-slices rather than `append` copies on cache hits.

- [ ] **Step 4: Remove file-handle full-window duplication**

Pass FUSE request context to remote reads. Store the returned immutable window directly:

```go
h.windowOff = fetchOff
h.window = data
```

Return valid sub-slices directly. Preserve the existing handle mutex so one handle cannot replace its window while another read is slicing it.

- [ ] **Step 5: Run remote and filesystem tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

---

### Task 4: Dedicated HTTP transports and application wiring

**Files:**
- Modify: `internal/baidu/client.go`
- Modify: `internal/app/app.go`
- Modify: `internal/app/playback.go`
- Modify: `internal/app/bench.go`
- Test: `internal/baidu/client_test.go`
- Test: `internal/app/app_test.go`
- Test: `internal/app/playback_test.go`
- Test: `internal/app/bench_test.go`

**Interfaces:**
- Produces: `baidu.NewMetadataHTTPClient() *http.Client`.
- Produces: `baidu.NewDownloadHTTPClient(concurrency int) *http.Client` or an equivalent constructor used by `APIClient`.
- Consumes: context-aware remote read methods.

- [ ] **Step 1: Add failing Transport configuration tests**

Assert the download client uses no whole-request timeout, disables compression, has dial/TLS/header/idle timeouts from the design, and sets per-host limits to at least configured concurrency. Assert metadata client retains a 30-second total timeout.

- [ ] **Step 2: Run Baidu and app tests and verify RED**

Run:

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test ./internal/baidu ./internal/app
```

Expected: FAIL because dedicated constructors and request-context propagation do not exist.

- [ ] **Step 3: Implement dedicated clients and wire configuration**

Create explicit `http.Transport` values with the approved timeout and connection settings. Preserve injected clients in tests. Ensure the production API client uses the configured download concurrency for connection limits while OAuth/metadata operations retain bounded total timeouts.

Update playback to pass `r.Context()` into every remote read. Use `context.Background()` only for CLI bench operations that have no caller context. Update all remaining callsites and fakes in one clean cutover.

- [ ] **Step 4: Run Baidu and app tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

---

### Task 5: Regression and completion verification

**Files:**
- Modify only if focused verification exposes a P0 regression.

**Interfaces:**
- Consumes all prior task outputs.
- Produces a verified build with no internal compatibility shim.

- [ ] **Step 1: Run all affected tests with race detection**

```bash
GOCACHE=/private/tmp/baidudisklink-go-cache GOMODCACHE=/private/tmp/baidudisklink-gomodcache /usr/local/go/bin/go test -race ./internal/baidu ./internal/remote ./internal/fs ./internal/app
```

Expected: PASS with no race report.

- [ ] **Step 2: Run project completion checks**

```bash
make check
```

Expected: all Go tests, build, smoke, script syntax, and deployment/config assertions pass.

- [ ] **Step 3: Record live-only validation gap**

Report that real Baidu/DSM/Emby throughput and RSS still require the approved `bench` versus `bench-fuse` comparison on the target DSM. Do not claim live performance improvement from unit tests alone.
