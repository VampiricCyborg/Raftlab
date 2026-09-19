"""Safety invariants, checked after every simulator step.

``Cluster`` installs an ``InvariantChecker`` by default, so every test gets
these checks for free. A violation raises immediately, with the tick and seed,
so the failing run can be replayed exactly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from raftlab.node import Role

if TYPE_CHECKING:
    from raftlab.cluster import Cluster


class InvariantViolation(AssertionError):
    pass


class InvariantChecker:
    """Stateful: some invariants are about history, not a single snapshot."""

    def __init__(self) -> None:
        # I1: the first node ever seen as leader of each term.
        self.leader_of_term: dict[int, int] = {}

    def __call__(self, cluster: Cluster) -> None:
        self.check_election_safety(cluster)
        # TODO(block 4): I2 leader append-only, I3 log matching,
        # I4 leader completeness, I5 state machine safety.

    def check_election_safety(self, cluster: Cluster) -> None:
        """I1: at most one leader per term, ever (not just at the same instant)."""
        for node in cluster.live_nodes():
            if node.role is not Role.LEADER:
                continue
            first = self.leader_of_term.setdefault(node.current_term, node.id)
            if first != node.id:
                _fail(
                    cluster,
                    "I1",
                    f"node{first} and node{node.id} both leader in term {node.current_term}",
                )


def _fail(cluster: Cluster, invariant: str, detail: str) -> None:
    raise InvariantViolation(
        f"{invariant} violated at t={cluster.now} (seed={cluster.seed}): {detail}"
    )
