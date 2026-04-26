# Changelog

All notable changes to this project are documented here.

The format follows Keep a Changelog, and this project uses semantic versioning
for repository tags even though it is intended to be cloned and run locally.

## [Unreleased]

### Added

- Nothing yet.

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
- Parent-symbol validation is tightened to one-to-four alphanumeric characters
  before `.FUT`.
- The package targets Python `>=3.13,<3.15` and is tested on Python 3.13 and
  3.14 in CI.

## [0.1.0] - 2026-04-25

### Added

- Initial standalone Databento stream downloader.
- Canonical DBN layout under `raw/glbx-mdp3/{symbol}/{schema}/{YYYY-MM-DD}.dbn.zst`.
- Estimated-cost planning caps, no-data materialization, metadata validation,
  SHA256 sidecars, and JSONL run ledger.
