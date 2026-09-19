"""Benchmark 2: time-to-commit vs message drop rate.

5 nodes, network delay uniform in [1, 3] ticks. After a leader is elected, a
retrying client commits 20 commands one after another; we measure ticks from
first submission to commit for each, over 100 seeds per drop rate. Heavy loss
also costs heartbeats, so followers time out and depose healthy leaders; the
"elections" column counts terms started per 1000 ticks.

    uv run python benchmarks/bench_commit.py
"""

from __future__ import annotations

from _common import summary, table
from raftlab import Cluster
from raftlab.client import CommitTimeout, commit

SEEDS = range(100)
DROP_RATES = [0.0, 0.05, 0.15, 0.30]
COMMANDS = 20


def run(seed: int, drop: float) -> tuple[list[int], int, int, int]:
    """(ticks per committed command, timeouts, terms elapsed, ticks elapsed)."""
    cluster = Cluster(n=5, seed=seed, checkers=[], trace_messages=False)
    cluster.network.delay(1, 3)
    cluster.run_until(lambda c: bool(c.leaders()), 500)
    cluster.network.drop_rate(drop)
    start_tick = cluster.now
    start_term = max(n.current_term for n in cluster.nodes.values())

    latencies, timeouts = [], 0
    for k in range(COMMANDS):
        before = cluster.now
        try:
            commit(cluster, f"SET k{k}={k}", max_ticks=1000)
            latencies.append(cluster.now - before)
        except CommitTimeout:
            timeouts += 1
    terms = max(n.current_term for n in cluster.nodes.values()) - start_term
    return latencies, timeouts, terms, cluster.now - start_tick


def main() -> None:
    rows = []
    for drop in DROP_RATES:
        latencies, timeouts, terms, ticks = [], 0, 0, 0
        for seed in SEEDS:
            lat, to, tm, tk = run(seed, drop)
            latencies += lat
            timeouts += to
            terms += tm
            ticks += tk
        rows.append([
            f"{drop:.0%}",
            *summary(latencies),
            f"{1000 * terms / ticks:.1f}",
            timeouts,
        ])
    print(f"Time-to-commit vs drop rate (5 nodes, delay 1-3, {len(SEEDS)} seeds x {COMMANDS} commands)\n")
    print(table(
        ["drop rate", "median ticks", "p95 ticks", "max ticks",
         "elections / 1000 ticks", "gave up (>1000 ticks)"],
        rows,
    ))


if __name__ == "__main__":
    main()
