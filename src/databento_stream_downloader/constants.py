"""Project-wide constants for the supported Databento dataset."""

from __future__ import annotations

from pathlib import Path

DATASET = "GLBX.MDP3"
DATASET_PATH_SEGMENT = DATASET.lower().replace(".", "-")
RAW_PREFIX = Path("raw") / DATASET_PATH_SEGMENT
