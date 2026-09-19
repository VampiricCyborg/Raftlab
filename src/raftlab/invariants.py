"""Safety invariants I1–I5, checked after every simulator step.

``Cluster`` installs an ``InvariantChecker`` by default, so every test, the
fuzzer, and the demo get these checks for free. A violation raises
immediately with the tick and seed, so the failing run can be replayed
exactly (``python -m raftlab.chaos --seed N``).

The checker observes; it never influences the run. Some invariants are about
history rather than a single snapshot, so the checker keeps its own records:
who led each term (I1), each leader's log as of the previous step (I2), every
entry ever seen committed (I4), and every command ever applied (I5).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from raftlab.log import LogEntry
from raftlab.node import Role

if TYPE_CHECKING:
    from raftlab.cluster import Cluster


class InvariantViolation(AssertionError):
    pass


class InvariantChecker:
    def __init__(self) -> None:
        # I1: first node ever seen as leader of each term.
        self.leader_of_term: dict[int, int] = {}
        # I2: node id -> (term, log entries) while it is leader.
        self.leader_logs: dict[int, tuple[int, tuple[LogEntry, ...]]] = {}
        # I4: index -> (entry, the term in which it was first seen committed).
        self.committed: dict[int, tuple[LogEntry, int]] = {}
        self.commit_checked: dict[int, int] = {}  # node id -> commit index already recorded
        # I5: index -> command applied there (by anyone), and how much of each
        # node's apply history has been checked already.
        self.applied: dict[int, str] = {}
        self.history_checked: dict[int, int] = {}
        self.steps = 0

    def __call__(self, cluster: Cluster) -> None:
        self.steps += 1
        self.check_election_safety(cluster)
        self.check_leader_append_only(cluster)
        self.check_log_matching(cluster)
        self.check_leader_completeness(cluster)
        self.check_state_machine_safety(cluster)

    def check_election_safety(self, cluster: Cluster) -> None:
        """I1: at most one leader per term, ever (not just at the same instant)."""
        for node in cluster.live_nodes():
            if node.role is not Role.LEADER:
                continue
            first = self.leader_of_term.setdefault(node.current_term, node.id)
            if first != node.id:
                _fail(
                    cluster,
                    "I1 election safety",
                    f"node{first} and node{node.id} both leader in term {node.current_term}",
                )

    def check_leader_append_only(self, cluster: Cluster) -> None:
        """I2: while a node leads a term, its log only ever grows at the end."""
        for node in cluster.nodes.values():
            live_leader = node.id not in cluster.crashed and node.role is Role.LEADER
            if not live_leader:
                self.leader_logs.pop(node.id, None)
                continue
            now = node.log.entries()
            before = self.leader_logs.get(node.id)
            if before is not None and before[0] == node.current_term:
                old = before[1]
                if now[: len(old)] != old:
                    _fail(
                        cluster,
                        "I2 leader append-only",
                        f"leader node{node.id} rewrote its log: {_terms(old)} -> {_terms(now)}",
                    )
            self.leader_logs[node.id] = (node.current_term, now)

    def check_log_matching(self, cluster: Cluster) -> None:
        """I3: if two logs hold an entry with the same index and term, the logs
        are identical up to and including that index.

        Checked for every pair of nodes, crashed ones included (their logs are
        persistent state). It suffices to find the highest index where the
        terms agree and compare the prefixes up to there.
        """
        logs = [(n.id, n.log.entries()) for n in cluster.nodes.values()]
        for i, (a_id, a) in enumerate(logs):
            for b_id, b in logs[i + 1 :]:
                k = min(len(a), len(b))
                while k > 0 and a[k - 1].term != b[k - 1].term:
                    k -= 1
                if a[:k] != b[:k]:
                    _fail(
                        cluster,
                        "I3 log matching",
                        f"node{a_id} {_terms(a)} and node{b_id} {_terms(b)} share"
                        f" index {k} term {a[k - 1].term} but differ before it",
                    )

    def check_leader_completeness(self, cluster: Cluster) -> None:
        """I4: an entry committed in term T is in the log of every leader of a
        term > T.

        "Committed" is observed, not assumed: whenever any live node's
        commit_index covers an entry, record it, along with the observer's
        term. The observer's term is >= the term the commit really happened
        in, so using it can only make the check more lenient, never produce a
        false alarm. Two different entries committed at one index are
        reported too: that alone is already a lost commit.
        """
        for node in cluster.live_nodes():
            # A restart resets commit_index to 0, so the watermark can move back.
            start = min(self.commit_checked.get(node.id, 0), node.commit_index)
            self.commit_checked[node.id] = node.commit_index
            for index in range(start + 1, node.commit_index + 1):
                entry = node.log.entry(index)
                seen = self.committed.get(index)
                if seen is None:
                    self.committed[index] = (entry, node.current_term)
                elif seen[0] != entry:
                    _fail(
                        cluster,
                        "I4 leader completeness",
                        f"index {index} committed as {seen[0]} earlier but node{node.id}"
                        f" has committed {entry}",
                    )
        for leader in cluster.leaders():
            for index, (entry, commit_term) in self.committed.items():
                if leader.current_term <= commit_term:
                    continue
                have = leader.log.entry(index) if index <= leader.log.last_index else None
                if have != entry:
                    _fail(
                        cluster,
                        "I4 leader completeness",
                        f"leader node{leader.id} (term {leader.current_term}) lacks {entry}"
                        f" committed at index {index} in term {commit_term}",
                    )

    def check_state_machine_safety(self, cluster: Cluster) -> None:
        """I5: no two nodes ever apply different commands at the same index.

        Uses each node's append-only apply history (which survives restarts),
        so a node re-applying after a crash is held to what it applied before.
        """
        for node in cluster.nodes.values():
            history = node.state_machine.history
            start = self.history_checked.get(node.id, 0)
            for index, command in history[start:]:
                first = self.applied.setdefault(index, command)
                if first != command:
                    _fail(
                        cluster,
                        "I5 state machine safety",
                        f"node{node.id} applied {command!r} at index {index},"
                        f" but {first!r} was applied there before",
                    )
            self.history_checked[node.id] = len(history)


def _terms(entries: tuple[LogEntry, ...]) -> list[int]:
    return [e.term for e in entries]


def _fail(cluster: Cluster, invariant: str, detail: str) -> None:
    raise InvariantViolation(
        f"{invariant} violated at t={cluster.now} (seed={cluster.seed}): {detail}"
    )
