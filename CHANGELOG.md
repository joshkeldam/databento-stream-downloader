# Changelog

All notable changes to this project are documented here.

The format follows Keep a Changelog, and this project uses semantic versioning
for repository tags even though it is intended to be cloned and run locally.

## [Unreleased]

### Added

- Ledger v4 records now include `exit_code`, `interrupted`, stream retry counts,
  estimated stream attempts, and terminal outcomes for post-run forensics.
- New `databento-stream-sync` console script (with `push` and `pull`
  subcommands) mirrors the local archive to/from S3 with the same Rich Live
  UI as the downloader. Reads bucket/prefix/region from `DATABENTO_S3_BUCKET`,
  `DATABENTO_S3_PREFIX`, `DATABENTO_S3_REGION` (or matching CLI flags) and
  uses the standard boto3 credential chain. Diff detection is size-based;
  `--verify-sha256` cross-checks local `.sha256` sidecars against
  `Metadata.sha256` written during prior pushes. `--delete` is opt-in and
  requires typed `delete` confirmation. `--fsync-writes` (pull only) and
  `--dry-run` mirror the downloader. Wired up as `just s3 push|pull`.

### Changed

- Runner internals are split into lifecycle, filesystem, cost, streaming,
  validation, ledger, work-discovery, formatting, and shared-type modules while
  preserving the public `databento_stream_downloader.runner` import path.
- Existing-archive discovery now classifies each symbol/schema directory in a
  single pass instead of warning and globbing in two separate scans.
- Execute runs now warn before streaming when concurrent workers can have
  non-zero planned cost in flight before the secondary planning guard can react.
- Network-filesystem preflight detection now covers macOS mount output and
  Windows remote drives, and detection-unavailable cases emit a visible warning.
- Failed partition rows are now routed consistently to stderr through the error
  console in both quiet and non-quiet runs.
- Dependabot now runs monthly for Python and GitHub Actions dependencies to
  match the repository's exact-pin, clone-and-run dependency policy.
- The temporary pip audit suppression now documents its advisory rationale and
  review trigger in both local and CI quality gates.
- Test coverage gates now require 92% global coverage plus explicit per-file
  floors for `cli.py` and `databento_client.py`.
- Property tests now cover cost allocation invariants, cost-range gap handling,
  universe semantic hash ordering, and SHA256 sidecar round trips.
- Cached archive discovery now accepts only strict `YYYY-MM-DD.dbn.zst`
  canonical filenames and warns on DBN-looking noncanonical leftovers.
- Removed the unexported `validate_dbn_file` alias; embedded callers should use
  the public `validate_dbn_metadata` validator.
- The hard `--workers` cap is raised from 50 to 100 for accounts whose Databento
  rate limits permit higher concurrency. The `> 8` soft warning still fires for
  any value above 8 so accidental high-concurrency runs remain visible.
- Default streaming posture flipped to a fast path that trusts file existence
  and atomic rename. SHA256 sidecars, post-write DBN metadata validation,
  file/directory fsync, and the cached-file DBN preflight read are now opt-in
  via `--write-sidecars`, `--validate-on-write`, `--fsync-writes`, and
  `--validate-cached`. The new `--paranoid` preset enables all three integrity
  flags at once. `--deep-validate` and `--strict-validate` continue to imply
  `--validate-on-write`. End-of-run validation under `--validate-cached` /
  `--validate-only` is unchanged.
- Cost-estimation phase concurrency is internally capped at 40 workers (matching
  the empirical Databento metadata-API 429 ceiling) regardless of `--workers`,
  so high streaming concurrency no longer rate-limits the planning phase.
- The Databento SDK warning suppression that previously serialized every API
  call on a process-global lock is replaced with a module-import-time
  `warnings.filterwarnings()` install. Worker threads now actually run the SDK
  in parallel — high `--workers` values translate into proportionate speedups
  instead of being silently bottlenecked at one in-flight request.
- The streaming progress display is now a Rich Live panel that shows the
  overall progress bar, throughput, ETA, the count of currently active
  workers, the symbols/schemas/days each one is streaming, and a running tally
  of placed/no_data/failed partitions.
- The pre-confirmation plan is rendered as a panel that visually matches the
  streaming UI: a tight header-rule data table sorted by cost, an explicit
  Total row, and a labelled summary block with bucket count, planning cap,
  remaining headroom (with percent), and archive path. The previously
  duplicated `Archive:` / `Estimated:` lines outside the table are removed —
  they are now consolidated inside the panel.
- Transient transport failures — `socket.gaierror` and `requests`-wrapped
  `ConnectionError` instances with `errno=None` (DNS hiccups, urllib3
  `NameResolutionError`, "max retries exceeded" wrappers) — are now
  classified as retryable. Previously they fell through `errno`-set logic and
  raised `FatalConfigError`, aborting the entire run on a single partition's
  network blip. After exhausted retries the partition still fails the way any
  other retryable failure does, so the run can complete and the missing
  partitions are re-fetched on the next incremental run.

## [0.2.0] - 2026-04-26

### Added

- Strict DBN validation mode with DBNStore record decoding, timestamp-bound
  checks, per-instrument `ts_recv` monotonicity checks, and symbology coverage
  checks.
- Deep zstd validation with a decompressed byte cap.
- Required execute-mode estimated-cost planning caps, including `$0.00`
  estimates only when `--allow-free-only` is set, with secondary in-flight
  planned-cost tracking.
- Optional `--max-cost-cents-per-bucket` guard and large-bucket warnings when a
  single symbol/schema estimate exceeds 25% of the run-level planning cap.
- Structured retry logging, `Retry-After` handling, and retry classification for
  selected network-layer errors.
- CLI `--show-retries` summary for real-client retry counts.
- CLI `--validate-only` archive scrub mode for cached files without Databento
  cost or download requests.
- Configurable `--suspicious-no-data-weekdays` coverage guard threshold.
- Best-effort Databento SDK request timeout configuration via
  `--request-timeout-seconds`.
- SHA256 sidecar creation, cached-sidecar repair, and cached-sidecar validation.
- Exclusive local run locking with stale temporary-file watchdog cleanup.
- Durable JSONL run ledger with `ledger_schema_version`, package version, host,
  user, run mode, validation flags, planned estimates, landed estimates,
  universe hash, and symbol hash.
- Ledger v1/v2 JSON Schemas and a standard-library `scripts/validate_ledger.py`
  validator.
- Version/changelog consistency check in the local and CI quality gates.
- First UTC data-day metadata for newer default-universe products so broad
  historical requests are clipped before cost estimation and streaming.
- CLI `--quiet`, `--log-format`, `--log-file`, `--version`, `--deep-validate`,
  `--strict-validate`, and `--validate-cached` operational flags.
- CI dependency auditing, pre-commit execution, package build verification, and
  offline wheel smoke validation against a real non-empty DBN fixture.
- Public repository hygiene files: `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md`,
  `CODE_OF_CONDUCT.md`, issue template, pull request template, and CODEOWNERS.

### Changed

- MBO is opt-in by default; free metadata schemas remain the default.
- Cost estimates are rounded after per-symbol/schema decimal aggregation instead
  of per-range rounding, and the run planning cap now rounds once after
  aggregating all estimate buckets.
- Package and changelog versions now both report `0.2.0`.
- Cost estimation and cached validation now honor the configured worker count
  instead of silently capping at 16 workers.
- In-flight planning-guard accounting now accumulates per-partition estimated
  cost from symbol/schema buckets instead of byte-prorating across heterogeneous
  schemas.
- Canonical path containment now uses resolved paths instead of lexical
  `absolute()` paths.
- Default worker count reduced to `4`; max workers remain capped at `50`.
- Safety and configuration refusals are routed to stderr even under `--quiet`.
- HTTP 422 responses are only materialized as semantic no-data when the response
  text matches known no-records messages; invalid request 422s are fatal API
  errors.
- Per-stream `warnings.catch_warnings()` suppression was removed so concurrent
  worker threads do not race on Python's process-global warning filters.
- Fatal errors are split into API/account failures and local configuration or
  safety failures for clearer exit-code handling.
- DBN header versions newer than the known set are handed to the installed SDK
  for decode instead of being rejected solely by header number.
- The public deep-validation decompression cap defaults to 16 GiB instead of a
  multi-terabyte ceiling.
- Strict validation now uses vectorized numpy checks for record chunks.
- Runtime placement follows temp-file validation, best-effort file fsync,
  atomic replace, directory fsync, and SHA256 sidecar placement.
- Stale temporary-file cleanup is scoped to requested symbol/schema directories.
- Run lock files now store only an opaque run identifier; host/user details are
  retained in the append-only ledger, and `run_started` logs include PID for
  operator correlation.
- Missing API-key failures now print a compact stderr error instead of argparse
  usage output.
- Universe hashes now use parsed symbol and first-data UTC-day data rather than
  raw TOML text, so comment-only universe edits do not churn ledger identifiers.
- Python warnings, including Databento SDK warnings, are routed through logging
  instead of being suppressed in worker threads.
- The active JSONL ledger rotates before append when it exceeds the configured
  size threshold, with a collision-resistant rotated filename.
- Directory fsync failures now log at warning level and are recorded in the
  ledger.
- Unexpected streaming exceptions now surface as fatal internal errors instead
  of being folded into ordinary per-partition failures.
- Databento retry counters are synchronized across worker threads and persisted
  in ledger v2 records.
- Parent-symbol validation is tightened to one-to-five alphanumeric characters
  before `.FUT`.
- The package targets Python `>=3.13,<3.15` and is tested on Python 3.13 and
  3.14 in CI.

## [0.1.0] - 2026-04-25

### Added

- Initial standalone Databento stream downloader.
- Canonical DBN layout under `raw/glbx-mdp3/{symbol}/{schema}/{YYYY-MM-DD}.dbn.zst`.
- Estimated-cost planning caps, no-data materialization, metadata validation,
  SHA256 sidecars, and JSONL run ledger.
