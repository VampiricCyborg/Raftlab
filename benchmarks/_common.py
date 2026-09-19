"""Shared bits for the benchmark scripts: percentiles and markdown tables.

Benchmarks run clusters with ``checkers=[]`` and message tracing off: they
measure algorithmic behaviour, and the invariants are already exercised by
the test suite. Every run is seeded, so the tables are reproducible.
"""

from __future__ import annotations

import math
import statistics
from typing import Sequence


def pct(values: Sequence[float], p: float) -> float:
    """Nearest-rank percentile (p in 0..100)."""
    ordered = sorted(values)
    k = max(0, math.ceil(p / 100 * len(ordered)) - 1)
    return ordered[k]


def summary(values: Sequence[float]) -> tuple[str, str, str]:
    if not values:
        return "n/a", "n/a", "n/a"
    return (
        f"{statistics.median(values):g}",
        f"{pct(values, 95):g}",
        f"{max(values):g}",
    )


def table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---:" for _ in headers) + "|",
    ]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return "\n".join(lines)
