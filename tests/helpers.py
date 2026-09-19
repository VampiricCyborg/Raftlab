"""Shared test helpers."""

from __future__ import annotations

from raftlab import Cluster, RaftNode


def stable_leader(cluster: Cluster, max_ticks: int = 300) -> RaftNode:
    """Run until exactly one live node believes it is leader, and return it."""
    ok = cluster.run_until(lambda c: len(c.leaders()) == 1, max_ticks)
    assert ok, f"no single leader within {max_ticks} ticks (seed={cluster.seed})"
    return cluster.leaders()[0]


class LeaderRecorder:
    """Checker that remembers every (term, node) that was ever leader."""

    def __init__(self) -> None:
        self.seen: set[tuple[int, int]] = set()

    def __call__(self, cluster: Cluster) -> None:
        self.seen |= {(n.current_term, n.id) for n in cluster.leaders()}

    @property
    def leader_ids(self) -> set[int]:
        return {node_id for _, node_id in self.seen}
