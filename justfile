databento *args:
  uv run databento-stream-downloader {{args}}

s3 *args:
  uv run databento-stream-sync {{args}}

lock:
  uv lock --check

sync:
  uv sync --locked --all-groups

lint:
  uv run --locked ruff check .

typecheck:
  uv run --locked basedpyright src tests scripts

version-check:
  uv run --locked python scripts/check_version.py

test:
  uv run --locked pytest

test-properties:
  uv run --locked pytest tests/test_properties.py

test-cov:
  uv run --locked pytest --cov=databento_stream_downloader --cov-report=term-missing --cov-report=json:.coverage.json --cov-fail-under=92
  uv run --locked python scripts/check_coverage_floors.py .coverage.json

audit:
  uv export --format requirements-txt --locked --no-hashes --no-dev --output-file .audit-requirements.txt
  uv run --locked pip-audit -r .audit-requirements.txt --no-deps --disable-pip --skip-editable

pre-commit:
  UV_CACHE_DIR=/tmp/databento-stream-downloader-uv-cache PRE_COMMIT_HOME=/tmp/databento-stream-downloader-pre-commit uv run --locked pre-commit run --all-files

build:
  uv build

clean:
  rm -rf dist .coverage .coverage.json .hypothesis .pytest_cache .ruff_cache .basedpyright .smoke-venv .audit-requirements.txt

check: clean lock sync lint typecheck version-check test-cov audit pre-commit build
