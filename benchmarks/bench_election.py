"""Benchmark 1: election latency vs election-timeout jitter.

Cold-start a 5-node cluster whose election timeouts are drawn from
[10, 10 + jitter] ticks and measure ticks until the first leader is elected,
over 100 seeds per jitter value. With zero jitter every node times out on the
same tick, every election is a split vote, and no leader ever emerges.

    uv run python benchmarks/bench_election.py
"""

from __future__ import annotations

import statistics

from _common import summary, table
from raftlab import Cluster, RaftConfig

SEEDS = range(100)
JITTERS = [0, 1, 2, 5, 10, 20, 40]
CAP = 2000


def first_leader(seed: int, jitter: int) -> tuple[int, int] | None:
    """(ticks, term) at which the first leader appeared, or None within CAP."""
    config = RaftConfig(election_timeout_min=10, election_timeout_max=10 + jitter)
    cluster = Cluster(n=5, seed=seed, config=config, checkers=[], trace_messages=False)
    if not cluster.run_until(lambda c: bool(c.leaders()), CAP):
        return None
    return cluster.now, cluster.leaders()[0].current_term


def main() -> None:
    rows = []
    for jitter in JITTERS:
        results = [first_leader(seed, jitter) for seed in SEEDS]
        elected = [r for r in results if r is not None]
        ticks = [t for t, _ in elected]
        terms = [term for _, term in elected]
        split = sum(term > 1 for term in terms) + (len(SEEDS) - len(elected))
        rows.append([
            f"[10, {10 + jitter}]",
            *summary(ticks),
            f"{statistics.mean(terms):.2f}" if terms else "n/a",
            f"{split}%",
            f"{len(SEEDS) - len(elected)}%",
        ])
    print("Election latency vs timeout jitter (5 nodes, 100 seeds, cold start)\n")
    print(table(
        ["timeout window", "median ticks", "p95 ticks", "max ticks",
         "mean terms", "had split vote", f"no leader in {CAP}"],
        rows,
    ))


if __name__ == "__main__":
    main()
