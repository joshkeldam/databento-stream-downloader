# Contributing

## Development

Install `uv` before running the local hooks or quality gate. `mise install` is
optional, but it will install the Python and `uv` versions used by the
maintainer.

```bash
uv sync --locked --all-groups
just check
```

The pre-commit hooks are intentionally local hooks that run through `uv`, so
contributors need `uv` on `PATH` before `pre-commit run --all-files` can work.

Run `just check` before opening a pull request. Changes that affect download
semantics should include tests for cost accounting, validation, and failure
behavior.

Dependency pins in `pyproject.toml` are exact on purpose. This is a clone-and-run
CLI with a committed `uv.lock`; dependency update PRs should change both files
and pass the full quality gate. Automated dependency PRs run monthly, not
weekly, to keep dependency refreshes reviewable instead of creating routine
version churn.

## Test Expectations

Use unit tests for pure helpers, contract tests with fake clients for runner
behavior, property tests for allocation/parsing invariants, and integration
tests only when they can be skipped cleanly without credentials. Archive
integrity changes should include failure-path tests, not only happy-path output
assertions.

## Data Safety

Do not commit `.env`, API keys, downloaded DBN files, or generated ledgers.
The `data/` directory is intentionally ignored. Ledger records include local
host and user fields for operational correlation; redact or keep ledgers private
before sharing logs, archives, or bug reports outside your organization.
