"""Shared helpers for the data and SQL generators."""

from __future__ import annotations

import random
from pathlib import Path


def project_root() -> Path:
    """Walk up from this file to the directory containing pyproject.toml."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").is_file():
            return parent
    raise RuntimeError("Could not locate project root (no pyproject.toml found)")


def seeded_rng(seed: int, *parts: object) -> random.Random:
    """Deterministic RNG for one logical unit of the dataset."""
    return random.Random("|".join([str(seed), *map(str, parts)]))
