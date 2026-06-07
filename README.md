# Databento Stream Downloader

[![CI](https://github.com/joshkeldam/databento-stream-downloader/actions/workflows/ci.yml/badge.svg)](https://github.com/joshkeldam/databento-stream-downloader/actions/workflows/ci.yml)
![Python 3.14](https://img.shields.io/badge/python-3.14-blue)
![MIT License](https://img.shields.io/badge/license-MIT-green)

`databento-stream-downloader` builds and maintains a canonical local archive of
Databento historical DBN data. It downloads one UTC-day partition at a time,
stores raw `.dbn.zst` files without converting them, records an operational
ledger, writes coverage manifests, and can mirror the archive to or from S3.

The package is intentionally narrow:

- Download raw Databento historical streams into a deterministic filesystem
  layout.
- Estimate missing-work cost before execution and require an explicit planning
  cap for billable runs.
- Treat semantic no-data days as real coverage by writing metadata-valid,
  zero-record DBN files.
- Keep validation, sidecars, fsync, and strict record checks opt-in so cached
  and no-op runs start quickly.
- Sync the archive to S3 with idempotent planning and explicit delete
  confirmation.

It does not implement trading strategy logic, backtests, feature engineering,
DBN-to-Parquet conversion, or Databento account billing reconciliation.

## Installation

This repository is currently a clone-and-run package. It has package metadata
and console scripts, but there is no PyPI publishing workflow or signed release
artifact yet.

Requirements:

- Python `3.14`
- `uv`
- A Databento API key for download and dry-run cost estimation
- AWS credentials only when using S3 sync

Set up the locked development environment:

```bash
git clone https://github.com/joshkeldam/databento-stream-downloader.git
cd databento-stream-downloader
uv sync --locked --all-groups
```

The installed console scripts are:

```text
databento-stream-downloader
databento-stream-sync
```

The repository `justfile` provides short aliases:

```bash
just databento --help
just s3 --help
```

Equivalent direct commands:

```bash
uv run --locked databento-stream-downloader --help
uv run --locked databento-stream-sync --help
```

## Quick Start

Create a `.env` file in the repository root, or export the same variables in
your shell. The CLI loads `.env` from the current working directory before
reading environment variables.

```text
DATABENTO_API_KEY=your_key_here
DATABENTO_MAX_COST_CENTS=0
DATABENTO_ALLOW_FREE_ONLY=true
```

Run a free metadata-schema download for the default CME futures universe:

```bash
just databento \
  --start 2026-04-01 \
  --end 2026-04-01 \
  --schemas definition status statistics \
  --yes
```

Run the same request as a dry run:

```bash
just databento \
  --start 2026-04-01 \
  --end 2026-04-01 \
  --schemas definition status statistics \
  --dry-run
```

Download an explicit billable MBO range:

```bash
just databento \
  --symbol ES.FUT \
  --schemas mbo definition status statistics \
  --start 2026-04-01 \
  --end 2026-04-24 \
  --max-cost-cents 25000 \
  --yes
```

Validate an existing cached scope without a Databento API key or cost request:

```bash
just databento \
  --symbol ES.FUT \
  --schemas mbo \
  --start 2026-04-01 \
  --end 2026-04-24 \
  --validate-only
```

## Archive Layout

The default archive root is `./data`. Canonical DBN files are stored at:

```text
data/raw/glbx-mdp3/{symbol}/{schema}/{YYYY-MM-DD}.dbn.zst
```

For example:

```text
data/raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst
```

Every canonical file is intended to represent:

```text
dataset = GLBX.MDP3
stype_in = parent
symbols = [{symbol}]
schema = {schema}
start/end = exact UTC day bounds
```

A missing canonical file means missing coverage. A valid zero-record DBN file
means Databento returned semantic no data for that symbol, schema, and UTC day.
Databento HTTP 422 responses are only treated as semantic no-data when the
response text matches known no-records messages. Unsupported schemas, invalid
symbols, malformed parameters, and other validation failures are fatal API
errors.

The downloader treats request dates as UTC calendar days, not CME session days.
Default-universe requests are clipped by the first UTC data day recorded in
`src/databento_stream_downloader/universe.toml`, so broad historical runs do not
create pre-data no-data files for newer products.

## Symbols and Schemas

If `--symbols` is omitted, the downloader uses the default CME futures universe
from `src/databento_stream_downloader/universe.toml`.

If `--schemas` is omitted, the downloader uses the free metadata schemas:

```text
definition statistics status
```

Supported schemas:

```text
definition
statistics
status
mbo
mbp-10
```

`mbo` and `mbp-10` are high-volume book schemas. They are opt-in because they
can be expensive over broad ranges and because failed stream retries restart
from byte zero.

`--symbol` and `--symbols` are aliases. Pass one occurrence with one or more
values:

```bash
just databento --symbols ES.FUT NQ.FUT --start 2026-04-01 --end 2026-04-01
```

Symbols are normalized to uppercase and must be Databento parent futures
symbols such as `ES.FUT`.

## Cost and Execution Safety

Execute runs require either `--max-cost-cents` or
`DATABENTO_MAX_COST_CENTS`. This requirement applies even when Databento
estimates `$0.00`, so unattended commands cannot accidentally start billable
work without an explicit planning cap.

`--max-cost-cents 0` is accepted only with `--allow-free-only` or
`DATABENTO_ALLOW_FREE_ONLY=true`. That combination means every estimated bucket
and the aggregate run estimate must be `$0.00`.

Important billing caveats:

- The cap is a preflight planning cap on Databento estimates, not a hard billing
  cap.
- Databento billing records are the source of truth.
- Failed or retried streams can repeat billable work without increasing landed
  completed-partition cost.
- Concurrent workers can put several billable partitions in flight at once.
- The runner refuses execution when the largest possible in-flight planning
  window exceeds the planning cap, unless `--allow-burst-exposure` is passed.

The estimator makes one account-aware Databento `get_cost` request per missing
`(symbol, schema)` span, with metadata concurrency bounded internally. If a span
exhausts retry attempts, the estimator recursively splits only that failing span
before giving up on a single UTC day.

Interactive execute runs ask for confirmation before download or sync mutation.
Non-interactive runs must pass `--yes`; otherwise the command exits before
making download requests or S3 changes.

## Downloader Options

Common target and planning options:

| Option | Meaning |
|---|---|
| `--symbols`, `--symbol` | Parent futures symbols to download. Defaults to the bundled CME futures universe. |
| `--schemas` | Databento schemas to download. Defaults to `definition statistics status`. |
| `--start` | Inclusive UTC start date in `YYYY-MM-DD` form. Required. |
| `--end` | Inclusive UTC end date in `YYYY-MM-DD` form. Required. |
| `--data-dir` | Local archive root. Defaults to `./data`. |
| `--dry-run` | Estimate and print planned work without downloading billable data. |
| `--yes`, `-y` | Proceed without an interactive confirmation prompt. |

Cost and concurrency options:

| Option | Meaning |
|---|---|
| `--workers` | Concurrent download workers. Default `4`, hard max `100`. |
| `--allow-high-volume-workers` | Allow `mbo` or `mbp-10` above 8 workers after accepting retry and billing exposure. |
| `--max-cost-cents` | Required execute-run Databento estimated planning cap in cents. |
| `--allow-free-only` | Allows `--max-cost-cents 0` as an explicit free-only cap. |
| `--max-cost-cents-per-bucket` | Optional cap for any one symbol/schema estimate bucket. |
| `--allow-burst-exposure` | Use admission throttling instead of refusing when the largest possible in-flight window exceeds the cap. |
| `--request-timeout-seconds` | Best-effort Databento SDK request timeout. Defaults to `600`. |

Integrity, validation, and operations options:

| Option | Meaning |
|---|---|
| `--validate-cached` | Revalidate already-cached files in scope before download work. |
| `--validate-only` | Validate cached files only, with no Databento API key, cost, or download request. |
| `--validate-on-write` | Validate DBN metadata after each write. |
| `--deep-validate` | Drain zstd frames to EOF during validation. |
| `--strict-validate` | Decode DBN records and run timestamp, symbology, and monotonicity checks. |
| `--write-sidecars` | Write SHA256 sidecars for placed files. |
| `--fsync-writes` | fsync placed files, sidecars, and parent directories. |
| `--paranoid` | Enable sidecars, write-time validation, and fsync together. |
| `--ledger-rotate-mb` | Rotate `download-ledger.jsonl` when it already exceeds this size. Defaults to `50`. |
| `--suspicious-no-data-weekdays` | Fail all-no-data spans after this many expected weekdays. Defaults to `5`. |
| `--quiet` | Suppress human progress output where possible. |
| `--verbose` | Emit INFO-level operational logs. |
| `--log-format` | `pretty` or `json`. |
| `--log-file` | Append structured logs to a file instead of stderr. |
| `--show-retries` | Print retry totals at the end of real-client runs. |
| `--version` | Print the installed package version. |

## Performance and Integrity Modes

The default execute path is optimized for fast startup and high throughput:

- Existing canonical files in the requested scope are treated as cached.
- Cached files are not re-read on startup unless validation is requested.
- New downloads stream to temporary files and are atomically renamed into place.
- Per-file SHA256 sidecars, write-time DBN validation, and fsync are off by
  default.
- Cached no-op downloader runs do not rewrite the coverage manifest.

Opt into stronger integrity checks when the run needs them:

| Flag | What it adds | Cost |
|---|---|---|
| `--validate-on-write` | DBN metadata validation after each download | Reads enough DBN metadata to validate the file |
| `--write-sidecars` | `{file}.sha256` next to every placed file | Full file read plus sidecar write |
| `--fsync-writes` | fsync placed file, sidecar, and parent directory | Extra syscalls, often slow on macOS |
| `--validate-cached` | Revalidate cached files in the requested scope | Reads cached files before download work |
| `--validate-only` | Validate cached files and make no Databento requests | Local validation only |
| `--deep-validate` | Drain each zstd frame to EOF | Full decompression |
| `--strict-validate` | DBNStore record-level checks | Full DBN record decode |
| `--paranoid` | Enables `--write-sidecars`, `--validate-on-write`, and `--fsync-writes` | Strictest write posture |

Strict validation checks record timestamp bounds for the half-open UTC day
`[start, end)`, symbology coverage, and per-instrument `ts_recv` monotonicity
where the schema exposes the required fields. It treats `instrument_id == 0` as
a system or sentinel record for those checks. This is intentionally expensive on
large MBO days.

What default mode gives up:

- No bit-rot detection unless sidecars or validation are enabled.
- No write-time DBN metadata validation unless requested.
- No explicit crash-durability fsync after atomic rename unless requested.

These tradeoffs are recoverable for this archive model: rerunning the same
scope fills missing files and can revalidate cached files.

## S3 Sync

`databento-stream-sync` mirrors the local archive to S3 or restores it from S3.

```bash
just s3 push --bucket my-bucket --workers 50 --yes
just s3 pull --bucket my-bucket --workers 50 --yes
just s3 push --bucket my-bucket --dry-run
just s3 pull --bucket my-bucket --delete
```

S3 keys mirror the archive layout with the leading `data/` stripped:

```text
data/raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst
s3://{bucket}/{prefix}/raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst
```

Configuration can be supplied by flags or environment variables:

| Flag | Environment variable |
|---|---|
| `--bucket` | `DATABENTO_S3_BUCKET` |
| `--prefix` | `DATABENTO_S3_PREFIX` |
| `--region` | `DATABENTO_S3_REGION` |

AWS credentials use the standard boto3 provider chain, including environment
variables, shared credentials files, SSO or profile configuration, and instance
or role credentials where available.

Sync planning modes:

| Mode | Behavior |
|---|---|
| `size` | Treat matching relative keys and byte sizes as equal |
| `sidecar` | Use local `.sha256` sidecars where available |
| `head-metadata` | Use S3 `Metadata.sha256` for stronger comparison |

`--verify-sha256` cross-checks local sidecars against S3 object metadata.
`--delete` removes destination-only files and requires typed confirmation with
the word `delete`. Transfer-only runs use the normal `Proceed? [y/N]`
confirmation. `--fsync-writes` is pull-only and applies the downloader's fsync
posture to restored local files.

The S3 client uses botocore adaptive retries, explicit socket timeouts, TCP
keepalive, and a connection pool sized to at least the configured sync worker
count. Managed boto3 transfer threads are disabled per object so repo-level
`--workers` controls actual transfer concurrency.

S3 sync options:

| Option | Meaning |
|---|---|
| `push` | Upload local archive files to S3. |
| `pull` | Download S3 archive files to local storage. |
| `--data-dir` | Local archive root. Defaults to `./data`. |
| `--bucket` | S3 bucket name. Defaults to `DATABENTO_S3_BUCKET`. |
| `--prefix` | S3 key prefix. Defaults to `DATABENTO_S3_PREFIX`. |
| `--region` | AWS region. Defaults to `DATABENTO_S3_REGION` or the boto3 default. |
| `--workers` | Concurrent transfer workers. Default `4`, hard max `100`. |
| `--delete` | Remove destination-only files after typed confirmation. |
| `--verify-sha256` | Cross-check local sidecars against S3 object metadata. |
| `--planning-mode` | `size`, `sidecar`, or `head-metadata`. |
| `--fsync-writes` | Pull only: fsync placed files and parent directories. |
| `--dry-run` | Render the plan and exit without transfer or delete work. |
| `--yes`, `-y` | Proceed without an interactive confirmation prompt. |
| `--quiet` | Suppress the Live UI. Logs and failures still print. |
| `--verbose` | Emit INFO-level operational logs. |
| `--log-format` | `pretty` or `json`. |
| `--log-file` | Append structured logs to a file instead of stderr. |
| `--version` | Print the installed package version. |

## Manifests and Ledger

The downloader writes an operational ledger to:

```text
data/download-ledger.jsonl
```

Ledger records include `ledger_schema_version`. Consumers should ignore unknown
fields. Breaking ledger changes increment the schema version, and JSON schemas
are stored under `schemas/`. The active ledger rotates before append when it
already exceeds `--ledger-rotate-mb` MiB, defaulting to 50 MiB. Ledger records
include local `host` and `user` fields for incident correlation, so do not
publish raw ledgers unchanged.

Coverage manifests are written to:

```text
data/databento-coverage-manifest.json
```

Downloader runs write the manifest for the requested symbol, schema, and date
scope after files are placed or validated. They do not scan the full archive for
cached no-op runs.

S3 sync runs write a manifest from local and remote inventories. Successful push
runs also upload it to:

```text
s3://{bucket}/{prefix}/databento-coverage-manifest.json
```

The manifest includes:

- generator and schema version
- request or sync context
- local file totals
- S3 file totals when remote inventory is available
- expected partitions by symbol and schema
- missing local dates
- missing S3 dates
- local files not present in S3
- S3 files not present locally

## Failure Handling and Retries

Databento request, metadata, and stream operations use separate retry budgets.
The downloader retries transient HTTP 408, HTTP 429, HTTP 5xx, retryable
Databento SDK errors, DNS failures, socket failures, and timeouts. Numeric and
HTTP-date `Retry-After` values are honored with a cap.

Streaming downloads are not resumable. If a stream fails mid-file, the partition
can only restart from byte zero. Small or early stream failures are retried, but
once a failed stream has already written a large temporary file, in-process
retries for that partition are suppressed to avoid repeated multi-GiB restarts.

Ctrl+C and SIGTERM cancel pending work and exit with documented codes. An
already-issued Databento `timeseries.get_range(path=...)` call can only stop
when the SDK or socket call returns.

Temporary files older than five minutes are removed at run start. The sweep is
scoped to the requested symbol and schema directories. Normal failure paths
clean their own temporary files.

## Local Filesystem Expectations

Use a local filesystem for `--data-dir`. The runner uses local lock and atomic
rename semantics. It rejects known network filesystems from Linux mount tables,
macOS mount output, and Windows remote-drive detection when possible. If
detection is unavailable, it prints a warning before continuing.

Symlinked data directories are accepted for local volume mounts. Locking and
durability apply to the resolved local filesystem target.

The preflight size check uses Databento's billable-size estimate as a disk-space
heuristic. Actual compressed DBN size can differ by schema and day.

## Logging

Default logging is human-readable. Use:

```bash
--verbose
--log-format json
--log-file path/to/events.jsonl
--quiet
--show-retries
```

When JSON logs are written to stderr, human progress output is routed to stdout
so the JSON stream remains parseable. Failed partition lines still go to stderr
so cron logs retain actionable failure context.

## Exit Codes

| Code | Meaning |
|---:|---|
| `0` | Success |
| `1` | Retry exhaustion or fatal API failure before streaming completes |
| `2` | Usage, config, or safety refusal |
| `3` | Partial download failure |
| `4` | Unexpected internal error |
| `5` | Validation failure |
| `130` | Interrupted by Ctrl+C |
| `143` | Graceful shutdown signal |

## Python API

The CLI is the primary compatibility target. A small typed import surface is
available for embedding and tests:

```python
from datetime import date
from pathlib import Path

from rich.console import Console

from databento_stream_downloader import (
    DownloadConfig,
    RunMode,
    configure_logging,
    run_download_with_client,
)

configure_logging(log_format="pretty", log_file=None, verbose=False)

config = DownloadConfig(
    data_dir=Path("data"),
    symbols=("ES.FUT",),
    schemas=("definition",),
    start=date(2026, 4, 1),
    end=date(2026, 4, 1),
    mode=RunMode.DRY_RUN,
    max_cost_cents=0,
    allow_free_only=True,
)

run_download_with_client(config, fake_client, Console())
```

Exported names include configuration types, error types, logging setup, default
symbol loading, DBN metadata validation, canonical path construction, and
download runner entry points. Private modules under `_runner` and `_sync` are
implementation details.

## Development

Run the full local gate:

```bash
just check
```

Important subcommands:

```bash
just lint
just typecheck
just test
just test-cov
just audit
just build
```

GitHub Actions runs the gate on Linux, macOS, and Windows. Coverage is enforced
with a 92% global floor plus per-file floors for critical modules. The package
build is verified, dependency auditing runs from the locked runtime
requirements, and CI emits a CycloneDX JSON dependency SBOM artifact.

Dependency versions are exact in `pyproject.toml` and locked in `uv.lock` by
design. Dependency refreshes should update both files together.

## Security and Data Handling

Do not commit `.env`, API keys, AWS credentials, ledgers, or downloaded market
data. The default `./data` directory is git-ignored.

Ledger files can contain local host and user metadata. Treat them as operational
artifacts, not public release assets.

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Non-Goals

- No strategy, signal, backtest, or portfolio logic.
- No DBN-to-Parquet or database loading pipeline.
- No exchange calendar inference beyond configured first UTC data days.
- No hard Databento billing cap or refund-aware billing reconciliation.
- No resumable partial DBN stream downloads.

## License

MIT
