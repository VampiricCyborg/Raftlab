"""A retrying client, as used by tests, benchmarks and the demo."""

from __future__ import annotations

from raftlab.cluster import Cluster


class CommitTimeout(AssertionError):
    pass


def commit(cluster: Cluster, command: str, max_ticks: int = 200) -> int:
    """Submit ``command`` to the current leader and step until it's committed.

    Returns the log index it committed at. If the leader holding our copy is
    deposed or crashes first, the entry may be lost, so resubmit to whoever
    leads next. (Raft doesn't deduplicate client retries; that needs client
    sessions, which are out of scope. SET is idempotent, so a duplicate is
    harmless here.)
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
    raise CommitTimeout(
        f"{command!r} not committed within {max_ticks} ticks (seed={cluster.seed})"
    )
