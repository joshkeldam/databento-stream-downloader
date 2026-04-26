"""Shared validation primitives."""

from __future__ import annotations

import re

PARENT_SYMBOL_RE = re.compile(r"^[A-Z0-9]{1,8}\.FUT$")
