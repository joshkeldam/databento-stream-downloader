databento *args:
  uv run databento-stream-downloader {{args}}

lock:
  uv lock --check

sync:
  uv sync --locked --all-groups

lint:
  uv run --locked ruff check .

typecheck:
  uv run --locked basedpyright src tests

version-check:
  uv run --locked python scripts/check_version.py

test:
  uv run --locked pytest

test-properties:
  uv run --locked pytest tests/test_properties.py

test-cov:
  uv run --locked pytest --cov=databento_stream_downloader --cov-report=term-missing --cov-fail-under=86

audit:
  uv export --format requirements-txt --locked --no-hashes --no-dev --output-file .audit-requirements.txt
  uv run --locked pip-audit -r .audit-requirements.txt --no-deps --disable-pip --skip-editable --ignore-vuln GHSA-58qw-9mgm-455v

pre-commit:
  UV_CACHE_DIR=/tmp/databento-stream-downloader-uv-cache PRE_COMMIT_HOME=/tmp/databento-stream-downloader-pre-commit uv run --locked pre-commit run --all-files

build:
  uv build

clean:
  rm -rf dist .coverage .hypothesis .pytest_cache .ruff_cache .basedpyright .smoke-venv .audit-requirements.txt

check: clean lock sync lint typecheck version-check test-cov audit pre-commit build
