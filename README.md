# Databento Stream Downloader

[![CI](https://github.com/joshkeldam/databento-stream-downloader/actions/workflows/ci.yml/badge.svg)](https://github.com/joshkeldam/databento-stream-downloader/actions/workflows/ci.yml)
![Python 3.13-3.14](https://img.shields.io/badge/python-3.13--3.14-blue)
![MIT License](https://img.shields.io/badge/license-MIT-green)

Standalone Databento historical stream downloader for canonical raw market data
archives.

## Status

This is a focused raw-data downloader optimized for throughput by default.
Execute runs require an explicit estimated-cost planning cap and no-data days
are still materialized as zero-record DBN files. Per-file SHA256 sidecars,
post-write DBN metadata validation, fsync, and cached-file DBN preflight are
opt-in via `--write-sidecars`, `--validate-on-write`, `--fsync-writes`, and
`--validate-cached`; the `--paranoid` preset enables all of them at once. See
the **Performance modes** section below.

## Canonical Layout

```text
raw/glbx-mdp3/{symbol}/{schema}/{YYYY-MM-DD}.dbn.zst
```

Every canonical file must satisfy:

```text
dataset = GLBX.MDP3
stype_in = parent
symbols = [{symbol}]
schema = {schema}
start/end = exact UTC day bounds
```

Semantic no-data days are represented by a zero-record, metadata-valid DBN file.
A missing file means missing coverage. Databento HTTP 422 responses are only
treated as semantic no-data when the response text matches known no-records
messages; unsupported schemas, invalid symbols, malformed parameters, and other
gateway validation failures are fatal API errors.

Validation defaults to metadata-first. The DBN metadata validator checks
magic/version, metadata shape, and parent symbol semantics. SHA256 sidecar
consistency is checked when cached files are validated through
`--validate-cached` or `--validate-only`; the startup repair preflight only
ensures sidecars exist and are syntactically/hash consistent enough to avoid
signing unknown bytes. `--deep-validate` additionally drains the full zstd
frame. `--strict-validate` performs a DBNStore record pass: it decodes records
in chunks, verifies `ts_recv` is inside the requested half-open UTC day
`[start, end)` when available and falls back to `ts_event` for schemas without
`ts_recv`, checks
per-instrument `ts_recv` monotonicity where the schema exposes `instrument_id`
and `ts_recv`, and verifies observed instrument IDs are present in the DBN
symbology mappings.
It still does not infer CME trading calendars or reject zero-record semantic
no-data files. Strict validation is intentionally opt-in and expensive on large
MBO days. It treats `instrument_id == 0` as a system/sentinel record and
excludes those records from symbology coverage and per-instrument monotonicity
checks; Databento's public DBN docs do not currently state this as a permanent
reservation, so this assumption should be re-confirmed before applying strict
validation to new datasets or schemas.

## Usage

Clone the repository, install the locked environment with `uv`, and run the CLI:

```bash
uv sync --locked --all-groups
```

```bash
export DATABENTO_API_KEY=...
export DATABENTO_MAX_COST_CENTS=0
export DATABENTO_ALLOW_FREE_ONLY=true

just databento \
  --schemas definition status statistics \
  --start 2015-01-01 \
  --end 2026-03-31
```

Or create a local `.env` file in the repository root:

```text
DATABENTO_API_KEY=your_key_here
DATABENTO_MAX_COST_CENTS=0
DATABENTO_ALLOW_FREE_ONLY=true
```

The CLI loads `.env` from the current working directory before reading
environment variables.

The default worker count is `4`. Increase `--workers` only after observing
Databento retry/rate-limit logs for your account; the hard CLI maximum is `100`.
MBO downloads are capped at `8` workers unless you pass
`--allow-high-mbo-workers`, because each failed/retried MBO stream restarts from
byte zero and can materially increase billable attempted bytes.
The cost-estimation phase follows the Helix downloader model: one account-aware
`get_cost` request per missing `(symbol, schema)` span, bounded to at most 40
concurrent metadata requests. If a span exhausts retry attempts, the estimator
recursively splits only that failing span into smaller `get_cost` requests
before giving up on a single UTC day. This preserves Databento account discounts
and free-schema pricing. Use `--show-retries` to print the total retry sleeps
attempted by the real Databento client at the end of the run.

## Performance modes

The default execute path follows a "trust file existence + atomic move"
posture: the runner streams to a temporary file, atomically renames into the
canonical path, and trusts the OS for durability. Cached files are not
re-read on startup. Validation and integrity checks are opt-in:

| Flag | What it adds | Per-file overhead |
|---|---|---|
| `--validate-on-write` | DBN metadata header check after each download | One zstd-decompress + header decode |
| `--write-sidecars` | `{file}.sha256` written next to every placed file | One full file read + sidecar write |
| `--fsync-writes` | `fsync` on data file, sidecar, and parent directory | Several syscalls; can be very slow on macOS |
| `--validate-cached` | DBN metadata header check on every cached file at startup | Reads every cached file's header before any download starts |
| `--validate-only` | Only validate cached files; no Databento requests | Same as `--validate-cached` |
| `--deep-validate` | Drain full zstd frame to EOF (implies `--validate-on-write`) | Full file decompression |
| `--strict-validate` | DBNStore record-level checks (implies `--validate-on-write`) | Full file decode |
| `--paranoid` | Preset that enables `--write-sidecars`, `--validate-on-write`, and `--fsync-writes` | All of the above |

What you give up in default mode: bit-rot detection (no per-file SHA256), per-write
header validation (corruption surfaces only at the next `--validate-cached` run
or at consumer read time), and crash durability after rename (atomic replace
still applies, so no half-files are visible — but recently written files may be
lost on power loss). All four are recoverable: re-run the download. The run is
incremental.

## Syncing to S3

`databento-stream-sync` mirrors the local archive to (or from) an S3 bucket
with the same Live UI as the downloader. Two subcommands cover the
directions:

```bash
just s3 push --workers 50          # local → s3
just s3 pull --workers 50          # s3   → local
just s3 push --dry-run             # plan, no transfer
just s3 pull --delete              # remove local files not in s3
```

S3 keys mirror the archive layout with the leading `data/` stripped:
`data/raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst` ↔
`s3://{bucket}/raw/glbx-mdp3/ES.FUT/mbo/2026-04-01.dbn.zst`. An optional
`DATABENTO_S3_PREFIX` is prepended after stripping `data/`.

Configuration is taken from `--bucket / --prefix / --region` flags or
`DATABENTO_S3_BUCKET / DATABENTO_S3_PREFIX / DATABENTO_S3_REGION` env vars.
AWS credentials follow the standard boto3 chain (env vars,
`~/.aws/credentials`, instance profile).

Difference detection is size-based and idempotent: a re-run uploads or
downloads nothing. `--verify-sha256` additionally cross-checks the local
`.sha256` sidecar against the `Metadata.sha256` set during the original
push. `--delete` is opt-in and requires the user to type `delete` (not
just `y`) at the confirmation prompt; the standard `Proceed? [y/N]`
applies to transfer-only runs. `--fsync-writes` is pull-only and brings
back the same fsync posture available to the downloader.

The default cap of `--workers` is `4` and the hard maximum is `100`,
matching the downloader. Throughput is byte-based in the Live UI, so the
ETA reflects bytes-remaining and not just file-count remaining.

When `--symbols` is omitted, the downloader uses the default CME futures
universe in `src/databento_stream_downloader/universe.toml`. That file also
contains first UTC data days for newer products. Requested ranges are clipped
per symbol before cost estimation and streaming so broad historical default
runs do not materialize pre-data no-data coverage. These are UTC archive
coverage dates, not local exchange launch dates; CME Sunday-evening CT launches
typically start on the following UTC day.

When `--schemas` is omitted, the downloader uses the free metadata schemas:
`definition`, `status`, and `statistics`. MBO is intentionally opt-in because it
can be expensive over broad ranges.

Explicit symbols override the default universe:

```bash
just databento \
  --symbol ES.FUT \
  --schemas mbo definition status statistics \
  --start 2026-04-01 \
  --end 2026-04-24 \
  --max-cost-cents 25000 \
  --yes
```

`--symbol` and `--symbols` are aliases for the same option. Pass one occurrence
with one or more values; if both aliases are repeated, argparse keeps the last
occurrence.

By default, files are written under `./data`, matching:

```text
data/raw/glbx-mdp3/{symbol}/{schema}/{YYYY-MM-DD}.dbn.zst
```

The downloader estimates cost and size once for each missing symbol/schema span.
Execute runs require either `--max-cost-cents` or
`DATABENTO_MAX_COST_CENTS`, even when Databento estimates `$0.00`. This is a
preflight planning cap on Databento's estimate, not a hard billing cap;
Databento's billing records are the source of truth. A planning cap of `0` is
accepted only with `--allow-free-only`; that combination means every estimated
bucket and the run aggregate must be `$0.00`. The run exits before any download
request if the estimate exceeds the planning cap. During streaming, the runner
also tracks landed planned cost for completed partitions as a secondary guard.
Failed and retried stream attempts can repeat billable work without increasing
the landed completed-partition total, so actual billing can exceed
`--max-cost-cents`. Concurrent workers can also have multiple billable
partitions in flight. Before confirmation, the runner refuses when the largest
currently possible in-flight set exceeds the planning cap. Pass
`--allow-burst-exposure` only when you intentionally want admission throttling
instead of that refusal. Use `--max-cost-cents-per-bucket` to set a planning cap
for any single symbol/schema bucket. When a global planning cap is configured,
the runner also warns if one bucket exceeds 25% of the global planning cap so an
accidentally expensive line item is visible before execution.

Interactive runs ask for confirmation before downloading. Non-interactive runs
must pass `--yes`; otherwise the command exits before making download requests.
Use `--quiet` for cron-style runs where human progress output should be
suppressed. Failed partition lines still go to stderr so cron logs retain
actionable failure context; no-data partition rows are suppressed and summarized
by the final counts.

Downloads are written through a temporary file, metadata-validated as DBN, then
placed at the canonical path. A `{file}.sha256` sidecar is written after
placement. At run start, cached files in the requested scope that are missing
sidecars, have malformed sidecars, or have sidecars whose digest does not match
the canonical file are detected. Missing or malformed sidecars are repaired only
after the canonical DBN metadata validates. Digest mismatches are treated as
validation issues and are not rewritten, because rewriting would sign bytes that
may be corrupt. Repairing a missing or malformed sidecar computes SHA256 over
the canonical DBN file once. Each completed run appends a durable JSON record to
`data/download-ledger.jsonl`; ledger records include `ledger_schema_version`.
Consumers should ignore unknown fields, and breaking ledger changes will
increment the schema version. Ledger schemas are stored under `schemas/`, and
`scripts/validate_ledger.py` validates JSONL ledgers against the matching schema
version. Version 4 records include `exit_code`, `interrupted`, retry counts,
stream retry/attempt estimates, terminal outcome counts, and directory-fsync
skip counts for post-run reconciliation. Ledger records
include local `host` and `user` fields for incident correlation; treat ledgers
as operational metadata and do not publish them unchanged. The active ledger
rotates before append on the next ledger write when it already exceeds
`--ledger-rotate-mb` MiB, defaulting to 50 MiB; a single large run is not rotated
mid-run. Use
`--deep-validate` to fully drain each zstd frame during validation. The public
validation helper caps default decompression at 32 GiB; runner-managed
validation uses the larger of 64 MiB or twice Databento's billable-size estimate
for the partition. Use `--strict-validate` for record-level structural checks.
Existing canonical files are treated as cached after sidecar preflight; use
`--validate-cached` to verify cached file hashes and DBN metadata during a run.
Cache discovery is filesystem-authoritative: only canonical files present under
`data/raw/...` are treated as cached.

Use `--validate-only` to scrub cached files in the requested scope without a
Databento API key, cost estimate, or download request. This is the intended
periodic integrity check for existing archives.

## Embedded Use

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

Warnings and errors are emitted to structured logs by default. Use `--verbose`
for INFO-level operational events. Use `--log-format=json` for machine-readable
JSON logs, and `--log-file path/to/log.jsonl` to persist them. When JSON logs
are written to stderr, human progress output is routed to stdout so the JSON
stream remains parseable.

The default `./data` directory is git-ignored. For large archives, point
`--data-dir` at a dedicated local volume or mounted object-store cache.
The local filesystem backend assumes local-disk locking semantics; do not point
`--data-dir` at NFS or another shared network filesystem without an external
lock. The runner rejects known network filesystems from Linux mount tables,
macOS mount output, and Windows remote-drive detection when possible; if
detection is unavailable, it prints a warning before continuing. Symlinked data
directories are accepted for local volume mounts; locking and durability apply
to the resolved local filesystem target.

The preflight size check uses Databento's billable-size estimate as a
conservative disk-space heuristic. Actual compressed DBN-on-disk size can differ
by schema and day.

Cost estimates are summed as decimal dollar values after Databento returns each
missing symbol/schema span estimate. Per-row table display is rounded to cents,
but the run-level planning cap is checked after aggregating all Decimal dollar
estimates and rounding once. The in-flight planning guard is based on the
planned per-partition estimate allocation; it is not a live billing feed and
does not include failed or retried stream attempts that never become canonical
files. Admission control submits new workers only when committed plus
outstanding planned cost stays within the cap, unless `--allow-burst-exposure`
was used. In-flight landed planned cost is accumulated from
the symbol/schema bucket estimate assigned to completed partitions. Semantic
no-data partitions still receive planned estimated cost and bytes, and sparse
missing days are allocated evenly within each symbol/schema estimate bucket.

On POSIX filesystems, placement uses the standard fsync-file, rename,
fsync-directory pattern as far as the Databento SDK path-writing API allows. The
SDK owns the original writer file descriptor, so the downloader performs a
best-effort fsync after the SDK closes the temporary file, then renames and
fsyncs the directory. On Windows, Python does not expose equivalent directory
fsync semantics through `os.open`, so crash durability after rename is weaker.

Shutdown is cooperative. Ctrl+C and SIGTERM cancel pending work and exit with a
documented code, but an already-issued Databento stream in a worker thread can
only stop when the SDK/socket call returns. The CLI applies
`--request-timeout-seconds` as a best-effort timeout to the current Databento SDK
metadata and timeseries HTTP APIs. The SDK still does not expose a hard
cancellation handle for an already-running `timeseries.get_range(path=...)`, so
large MBO files may take time to unwind after a signal.

Streaming downloads are not resumable. If the SDK or network fails mid-file, the
partition can only restart from byte zero. Small/early stream failures are
retried, but once a failed stream has already written a large temporary file the
runner suppresses further in-process retries for that partition to avoid
repeated multi-GiB restarts. Actual Databento billing can still exceed
`--max-cost-cents` on failed/retried streams because the planning cap is based on
completed partitions, not attempted bytes or a refund-aware billing feed.
Metadata estimation and stream retries use a larger MBO-oriented backoff budget
than ordinary API requests: six attempts with exponential sleeps capped at 60
seconds when retrying remains safe. Retry logs include the operation name and
mark stream retries as restarting from byte zero. Ledger v4 records the run exit
code, total retry counts, retry counts by operation, stream retry count,
estimated stream attempt count, and terminal outcomes for post-run
reconciliation.

Temporary files older than five minutes are removed at run start and logged as
warnings. The sweep is scoped to the requested symbol/schema directories rather
than the whole archive tree. Normal failure paths clean their own temporary
files; the sweep is a watchdog for SIGKILL or process crashes, not part of the
happy-path cleanup contract.

No-data files do not distinguish CME holidays from valid trading days with no
records. Request bounds are UTC calendar days, not CME session days. To catch
bad parent symbols or pre-data date ranges, the runner fails loudly if every
partition for one symbol/schema returns no data across at least five expected
weekdays on or after that symbol's configured first UTC data day.
Use `--suspicious-no-data-weekdays` to tune that threshold for short runs.

DBN header versions newer than the versions known to this package are not
rejected solely by header number. The downloader logs one warning for the
unknown version and lets the installed Databento DBN SDK attempt metadata decode;
if the SDK cannot decode the file, validation fails.

## Exit Codes

- `0` success.
- `1` retry exhaustion or fatal API failure before streaming completes.
- `2` usage/config/safety refusal, including missing planning cap.
- `3` partial download failure.
- `4` unexpected internal error.
- `5` validation failure.
- `130` interrupted by Ctrl+C.
- `143` graceful shutdown signal.

## Quality Gates

```bash
just check
```

GitHub Actions runs these checks on Linux, macOS, and Windows. Coverage is
enforced in CI with a 92% global floor plus explicit per-file floors for the CLI
and Databento client adapter. Pre-commit hooks and dependency auditing run in
CI, and the package build is verified as part of the quality gate. CI also emits
a CycloneDX JSON dependency SBOM artifact generated from the locked runtime
requirements.

This repository is currently clone-and-run software, not a published release
channel. There is no PyPI publishing workflow, signed wheel, or provenance
attestation yet. If release artifacts are added later, they should use PyPI
Trusted Publishing, Sigstore signing, and GitHub provenance attestations.

Dependency versions are exact in `pyproject.toml` by design. This repository is
intended to be cloned and run with the committed `uv.lock`, not imported as a
library dependency with flexible transitive constraints. Dependabot is scheduled
monthly rather than weekly so dependency refreshes are deliberate maintenance
events that update both `pyproject.toml` and `uv.lock` together.

The Python import surface is intentionally small: configuration, error types,
logging setup, default-symbol loading, and `run_download` entry points are
exported for embedding. CLI behavior remains the primary compatibility target.

## Roadmap

1. Add an S3 storage backend.
2. Add deterministic dry-run plan artifacts with `--from-plan` replay in the
   next minor release.
3. Add container packaging.
4. Add scheduled daily capture examples for common cloud runtimes.
5. Add optional Databento-key integration smoke tests for SDK compatibility.
6. Add optional OpenTelemetry export for run, request, and partition metrics.

## Non-Goals

- No strategy, backtest, or feature logic.
- No conversion away from raw DBN in this package.

## License

MIT
