"""Benchmark 3: availability vs cluster size under rolling crashes.

Every K ticks, the node killed last round is restarted and a uniformly random
node is killed (sometimes the leader). We measure the fraction of ticks with
a *stable leader*: a live leader whose term and identity are acknowledged by
a majority of the whole cluster. That's the condition under which a client
request could actually commit.

    uv run python benchmarks/bench_availability.py
"""

from __future__ import annotations

import random
import statistics

from _common import table
from raftlab import Cluster, Role

SEEDS = range(50)
SIZES = [3, 5, 7]
KILL_EVERY = [50, 100, 200]
TICKS = 3000


def has_stable_leader(cluster: Cluster) -> bool:
    for leader in cluster.leaders():
        followers = sum(
            1
            for n in cluster.live_nodes()
            if n.current_term == leader.current_term and n.leader_id == leader.id
        )
        if followers >= leader.majority:
            return True
    return False


def run(seed: int, n: int, every: int) -> tuple[float, int, list[int]]:
    """(availability, leader kills, outage lengths after each leader kill)."""
    rng = random.Random(f"avail:{seed}")
    cluster = Cluster(n=n, seed=seed, checkers=[], trace_messages=False)
    cluster.network.delay(1, 2)
    cluster.run_until(has_stable_leader, 500)

    up, leader_kills, outages = 0, 0, []
    victim: int | None = None
    outage_start: int | None = None
    for t in range(TICKS):
        if t % every == 0:
            if victim is not None:
                cluster.restart(victim)
            victim = rng.choice(cluster.node_ids)
            if cluster.nodes[victim].role is Role.LEADER:
                leader_kills += 1
                outage_start = cluster.now
            cluster.crash(victim)
        cluster.step()
        if has_stable_leader(cluster):
            up += 1
            if outage_start is not None:
                outages.append(cluster.now - outage_start)
                outage_start = None
    return up / TICKS, leader_kills, outages


def main() -> None:
    rows = []
    for every in KILL_EVERY:
        for n in SIZES:
            results = [run(seed, n, every) for seed in SEEDS]
            avail = statistics.mean(a for a, _, _ in results)
            kills = sum(k for _, k, _ in results)
            outages = [o for _, _, os in results for o in os]
            rows.append([
                every,
                n,
                f"{avail:.2%}",
                f"{kills / (len(SEEDS) * TICKS // every):.0%}",
                f"{statistics.median(outages):g}" if outages else "n/a",
            ])
    print(f"Availability vs cluster size ({len(SEEDS)} seeds x {TICKS} ticks, one node down at a time)\n")
    print(table(
        ["kill every K ticks", "nodes", "ticks with stable leader",
         "kills that hit the leader", "median outage (ticks)"],
        rows,
    ))


if __name__ == "__main__":
    main()
