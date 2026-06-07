# Performance Audit

Date: 2026-06-07

## Scope

This audit covers downloader startup, cached-run exit, post-download tail work,
S3 sync planning, worker utilization, manifest generation, and dependency
choices. Measurements used the local archive at `data`, which currently points
to an archive with about 566k files.

## Findings

### Downloader startup and cached no-op exit

Measured command:

```bash
just databento --start 2023-07-17 --end 2023-07-17 --schemas definition status statistics mbo --workers 8 --symbols ES.FUT
```

Current result: about 0.38s, ending immediately after:

```text
All partitions already cached, nothing to do.
```

Previous behavior included an archive-wide coverage manifest rewrite after the
message. On the measured archive, manifest local collection alone took about
44s and made the process look hung after success output.

Fixes:

- Avoid real Databento client construction until missing work exists.
- Avoid stream and validation imports on the cached fast path.
- Do not rewrite the coverage manifest for cached no-op downloader runs.
- Make downloader coverage manifests request-scoped instead of archive-wide.

### Post-download finish tail

Downloader runs that place files now write a request-scoped coverage manifest.
Measured single-day, four-schema manifest write: about 0.005s. This replaces the
previous archive-wide scan that took about 44s on the local archive.

Tradeoff: downloader manifests now describe the requested run scope. Archive-wide
local plus S3 reconciliation remains a sync responsibility.

### S3 sync planning

Local inventory remains the largest measured local hotspot.

Measured `walk_local(data)`:

- Previous `Path.rglob` plus per-file sidecar probes: about 26.3s.
- Current `os.scandir` traversal plus sidecar attachment from discovered keys:
  about 18.1s to 18.6s.

The remaining cost is dominated by statting and materializing about 566k local
files. A full S3 push plan must know local keys and sizes unless a persistent
local inventory is introduced.

No-op sync runs now return after the no-op plan instead of performing a second
remote inventory and manifest refresh.

### Downloader worker utilization

The downloader previously had two independent concurrency controls:

- user-facing `--workers`
- a hidden cap of two large streams

For MBO files this could show only a few active workers even with hundreds of
queued files. The hidden large-stream cap has been removed. High-volume schemas
are still capped by the config validator at 8 workers unless explicitly
overridden.

The remaining admission limiter is the cost-cap guard. If the next partition
would exceed the configured in-flight planning cap, the scheduler intentionally
waits instead of starting more work.

## Rust Rewrite Assessment

A Rust rewrite is not justified by the measured bottlenecks.

Reasons:

- The dominant user-visible hangs were Python control-flow problems: eager
  imports, unnecessary manifest writes, and hidden scheduler throttling.
- Download throughput is network and vendor-SDK bound. Rewriting the wrapper in
  Rust does not make Databento or S3 send bytes faster.
- DBN strict validation already uses Databento DBN and NumPy primitives for the
  expensive record path. Python mostly orchestrates file and SDK calls.
- The remaining large local hotspot is filesystem traversal over about 566k
  files. Rust may reduce per-entry overhead, but it cannot remove the need to
  read directory entries and stat files. The better structural fix would be a
  persistent local inventory or ledger-backed sync planner, not a language
  rewrite.

Rust or PyO3 may be worth reconsidering only if a future benchmark shows a
CPU-bound pure-Python loop dominating runtime after network, filesystem, and SDK
work are excluded. Current evidence does not show that.

## Library Assessment

### Databento SDK

Keep the official Databento SDK for historical downloads. It owns Databento
request semantics, DBN streaming, auth behavior, and vendor errors. Replacing it
with a generic HTTP client would force this repo to reimplement vendor protocol
details and retry/error classification with no measured evidence that HTTP
client overhead is the bottleneck.

Potential future investigation: Databento exposes async historical range APIs.
An async downloader could reduce thread overhead, but only after proving the SDK
async path preserves file streaming, timeout, warning, and retry semantics needed
here.

### boto3 and botocore

Keep boto3 for S3. It owns AWS credentials, pagination, managed transfers,
retry modes, and metadata behavior. The prior flakiness came from nested boto3
transfer pools, not from boto3 itself. The current wrapper disables per-object
nested transfer threads so `--workers` maps to real repo-level transfer
concurrency.

Alternatives such as `aiobotocore` or direct HTTP signing would add operational
surface area. They are not justified until S3 API latency, not local inventory,
is the measured bottleneck.

### Generic HTTP libraries

HTTPX and aiohttp both provide connection pooling and async clients. They are
not drop-in replacements for the Databento SDK or boto3. They are relevant only
if this repo deliberately replaces vendor SDKs with protocol-level clients,
which would be a correctness and maintenance tradeoff rather than a simple
performance optimization.

## HTTP Robustness Audit

### Databento historical client

Observed:

- Historical clients are thread-local, avoiding shared mutable SDK client state
  across downloader workers.
- Request, metadata, and stream calls use separate retry budgets.
- HTTP 408, 429, and 5xx responses are retryable.
- Numeric and HTTP-date `Retry-After` values are honored with a cap.
- Retryable `BentoError`, DNS, socket, and timeout failures are classified as
  retryable.
- Streaming uses a larger timeout than small metadata requests.
- Partial stream retries are suppressed once the temp file exceeds the large
  partial threshold, preventing expensive restart loops on large DBN streams.

Assessment: keep the Databento SDK. The repo has the right control points for
vendor errors and retry pacing without reimplementing Databento protocol
semantics. A generic HTTP client would reduce correctness and not address the
measured startup issues.

Remaining limit: Databento stream retries restart the request from the
beginning. That is vendor-SDK behavior from this repo's perspective and is why
large partial retries are deliberately capped.

### S3 client

Observed before this pass:

- Botocore adaptive retries were enabled.
- HTTP 408, 429, and 5xx `ClientError` failures were retryable.
- 401 and 403 failures were fatal config errors.
- Core transport exceptions were retryable.
- Managed transfer nested thread pools were disabled so repo-level `--workers`
  controls transfer concurrency.

Gaps fixed:

- S3 clients now set explicit connect and read timeouts.
- S3 clients now enable TCP keepalive.
- S3 clients now use a larger connection pool, and sync sizes it to at least
  the configured worker count.
- Partial credentials and credential-provider failures are fatal config errors.
- Managed transfer wrapper failures and generic botocore failures are routed
  through the repo retry taxonomy instead of escaping as raw SDK exceptions.

Source authority:

- Botocore `Config` documents `connect_timeout`, `read_timeout`, and
  `max_pool_connections`.
- Boto3 S3 transfer docs document managed transfer behavior and
  `TransferConfig`.

## Remaining Performance Work

The next major improvement, if needed, is a persistent local inventory for sync
planning. It would let `just s3 push` avoid a full 566k-file scan on every run by
using the downloader ledger and targeted filesystem validation. That is a larger
contract change than this pass and needs its own design because stale inventory
can cause missed uploads.
