"""Shared test helpers."""

from __future__ import annotations

from raftlab import Cluster, RaftNode


def stable_leader(cluster: Cluster, max_ticks: int = 300) -> RaftNode:
    """Run until exactly one live node believes it is leader, and return it."""
    ok = cluster.run_until(lambda c: len(c.leaders()) == 1, max_ticks)
    assert ok, f"no single leader within {max_ticks} ticks (seed={cluster.seed})"
    return cluster.leaders()[0]


def commit(cluster: Cluster, command: str, max_ticks: int = 200) -> int:
    """Behave like a retrying client: submit to the current leader and wait
    until the command is committed; return its log index.

    If the leader that accepted it is deposed or crashes first, the entry may
    be lost, so resubmit to whoever leads next (SET is idempotent).
    """
    placed: list[int] = []
    owner: tuple[int, int] | None = None  # (leader id, term) holding our latest copy
    for _ in range(max_ticks):
        for index in placed:
            if cluster.is_committed(index, command):
                return index
        leaders = cluster.leaders()
        still_leading = any((n.id, n.current_term) == owner for n in leaders)
        if leaders and not still_leading:
            leader_id, index = cluster.submit(command)
            placed.append(index)
            owner = (leader_id, cluster.nodes[leader_id].current_term)
        cluster.step()
    raise AssertionError(f"{command!r} not committed within {max_ticks} ticks (seed={cluster.seed})")


class LeaderRecorder:
    """Checker that remembers every (term, node) that was ever leader."""

    def __init__(self) -> None:
        self.seen: set[tuple[int, int]] = set()

    def __call__(self, cluster: Cluster) -> None:
        self.seen |= {(n.current_term, n.id) for n in cluster.leaders()}

    @property
    def leader_ids(self) -> set[int]:
        return {node_id for _, node_id in self.seen}
