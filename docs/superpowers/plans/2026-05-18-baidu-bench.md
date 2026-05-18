# Baidu dlink Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a local benchmark command that downloads a chosen file through the existing Baidu official API + `dlink` path and reports real throughput.

**Architecture:** Extend the existing CLI entrypoint with a `bench` subcommand. The benchmark will reuse the current auth token in `data/token.json`, resolve the requested remote path through the same Baidu client used by the mount flow, fetch the `dlink` with the same request headers and redirect handling, then read a bounded byte range and measure elapsed time. Keep the benchmark in a small internal package so the main mount path stays untouched.

**Tech Stack:** Go, existing `internal/auth`, `internal/baidu`, `internal/config`, standard library `flag`/`time`/`fmt`.

---

### Task 1: Add a `bench` CLI entrypoint

**Files:**
- Modify: `cmd/baidudisklink/main.go`
- Create: `internal/bench/bench.go`
- Create: `internal/bench/bench_test.go`

- [ ] **Step 1: Write the failing test**

```go
func TestBenchmarkReadsRemotePath(t *testing.T) {
	// construct a bench runner with a stub client that returns a known dlink
	// and a fixed byte slice, then assert the reported bytes and speed are non-zero
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/bench -run TestBenchmarkReadsRemotePath -v`
Expected: FAIL because the package and runner are not implemented yet.

- [ ] **Step 3: Write minimal implementation**

```go
package bench

type Result struct {
	Path      string
	Bytes     int64
	Elapsed   time.Duration
	Throughput float64
}

type Runner struct {
	cfg    config.Config
	client baidu.Client
}

func New(cfg config.Config, client baidu.Client) *Runner { return &Runner{cfg: cfg, client: client} }

func (r *Runner) Run(ctx context.Context, remotePath string, sampleSize int64) (Result, error) {
	// load token-backed client behavior via the existing Baidu client,
	// resolve the remote path's metadata with dlink enabled, read sampleSize bytes,
	// and calculate throughput from elapsed wall time
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/bench -run TestBenchmarkReadsRemotePath -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/baidudisklink/main.go internal/bench/bench.go internal/bench/bench_test.go
git commit -m "feat: add baidu dlink benchmark command"
```

### Task 2: Wire the benchmark into the existing auth/token flow

**Files:**
- Modify: `cmd/baidudisklink/main.go`
- Modify: `internal/app/app.go`
- Modify: `internal/config/config.go`

- [ ] **Step 1: Write the failing test**

```go
func TestBenchUsesStoredTokenAndRemoteClient(t *testing.T) {
	// verify the benchmark path loads token.json, builds the existing Baidu client,
	// and does not start the FUSE mount server
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/app ./cmd/baidudisklink -run TestBenchUsesStoredTokenAndRemoteClient -v`
Expected: FAIL because the CLI still only supports the mount flow.

- [ ] **Step 3: Write minimal implementation**

```go
if len(os.Args) > 1 && os.Args[1] == "bench" {
	remotePath := flag.String("path", "/Videos/test.zip", "remote path to benchmark")
	// load config, construct app, bind remote client from data/token.json,
	// run benchmark, print summary, exit without mounting
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add cmd/baidudisklink/main.go internal/app/app.go internal/config/config.go
git commit -m "feat: wire benchmark command into auth flow"
```

### Task 3: Document the benchmark usage

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Write the failing test**

```bash
grep -q 'bench --path /Videos/test.zip' README.md
```

- [ ] **Step 2: Run test to verify it fails**

Run: `grep -q 'bench --path /Videos/test.zip' README.md`
Expected: no match before the docs are added.

- [ ] **Step 3: Write minimal implementation**

```md
## 性能测速

容器启动并完成授权后，可以直接执行：

```bash
docker-compose run --rm baidudisklink bench --path /Videos/test.zip
```

该命令会复用 `data/token.json`，按和挂载一致的官方 API + dlink 方式读取文件，并输出平均速度。
```

- [ ] **Step 4: Run test to verify it passes**

Run: `grep -q 'bench --path /Videos/test.zip' README.md`
Expected: match.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs: add benchmark usage"
```

### Task 4: Verify the full flow locally

**Files:**
- No file changes

- [ ] **Step 1: Run the full test suite**

Run: `go test ./...`
Expected: PASS.

- [ ] **Step 2: Run the new benchmark command**

Run: `docker-compose run --rm baidudisklink bench --path /Videos/test.zip`
Expected: print elapsed time, bytes read, and throughput.

- [ ] **Step 3: Commit if any last-minute fixes were needed**

```bash
git add <any touched files>
git commit -m "test: verify baidu benchmark flow"
```

## Self-Review

- Spec coverage: benchmark command, token reuse, official dlink flow, docs.
- Placeholder scan: no TBDs or vague implementation notes remain.
- Type consistency: the plan refers only to `config.Config`, `baidu.Client`, and a new `bench.Runner`.
