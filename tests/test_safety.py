"""Safety under the scenarios Raft's rules exist for."""

import pytest

from helpers import commit, stable_leader
from raftlab import Cluster, LogEntry, RaftNode, Role
from raftlab.invariants import InvariantViolation


def set_state(node: RaftNode, term: int, voted_for: int | None, entries: list[LogEntry]) -> None:
    node.current_term, node.voted_for = term, voted_for
    for e in entries:
        node.log.append(e)


def figure8_cluster(seed: int) -> Cluster:
    """Raft paper Figure 8, state (c), as a 5-node cluster (S1..S5 = node0..node4).

    - (a) node0 led term 2 and replicated index 2 (term 2) only to node1.
    - (b) node0 crashed; node4 won term 3 with votes from node3 and itself...
          and appended index 2 (term 3) to its own log only; then crashed.
    - (c) node0 restarted and won term 4 (votes 0, 1, 2). It is now about to
          replicate its term-2 entry at index 2 onto a majority.

    Returned just after node0 becomes leader of term 4, with node4 crashed.
    """
    one = LogEntry(1, "SET x=1")
    cluster = Cluster(n=5, seed=seed)
    set_state(cluster.nodes[0], 4, 0, [one, LogEntry(2, "SET x=2")])
    set_state(cluster.nodes[1], 4, 0, [one, LogEntry(2, "SET x=2")])
    set_state(cluster.nodes[2], 4, 0, [one])
    set_state(cluster.nodes[3], 3, 4, [one])
    set_state(cluster.nodes[4], 3, 4, [one, LogEntry(3, "SET x=3")])
    cluster.crash(4)

    leader = cluster.nodes[0]
    leader.votes_received = [0, 1, 2]
    cluster._after_node_call(0, leader._become_leader(cluster.now))
    return cluster


def play_figure8(cluster: Cluster) -> None:
    """(c) node0 replicates index 2 to a majority; (d) node0 crashes and node4,
    whose last log term (3) beats everyone else's (2), wins and overwrites
    index 2 everywhere."""
    old = cluster.nodes[0]
    cluster.run(10)
    holders = [n.id for n in cluster.nodes.values() if n.log.term_at(2) == 2]
    assert len(holders) >= 3, "term-2 entry should now sit on a majority"

    cluster.crash(0)
    cluster.restart(4)
    # In the paper's story node4 is the one that times out first. (If node1-3
    # won instead, that would be safe too, just not the scenario under test.)
    # Granting a vote resets a node's timer, so keep node1-3 passive until then.
    cluster.nodes[4].election_deadline = cluster.now + 1
    for _ in range(100):
        if cluster.nodes[4].role is Role.LEADER:
            break
        for i in (1, 2, 3):
            cluster.nodes[i].election_deadline = cluster.now + 1_000
        cluster.step()
    assert cluster.nodes[4].role is Role.LEADER
    commit(cluster, "SET x=5")  # a term-5 entry commits index 2 (term 3) indirectly
    cluster.restart(0)
    assert cluster.run_until(Cluster.converged, 100)
    assert old.role is Role.FOLLOWER


@pytest.mark.parametrize("seed", range(5))
def test_no_stale_term_commit(seed):
    cluster = figure8_cluster(seed)
    leader = cluster.nodes[0]
    cluster.run(10)
    # R6: index 2 is on a majority, but it's from term 2 and we're in term 4.
    assert sum(n.log.term_at(2) == 2 for n in cluster.nodes.values()) >= 3
    assert leader.commit_index == 0
    assert all(n.state_machine.history == [] for n in cluster.nodes.values())

    # Which makes step (d) harmless: node4 legally overwrites index 2.
    play_figure8(cluster)
    for node in cluster.nodes.values():
        assert [e.command for e in node.log.entries()] == ["SET x=1", "SET x=3", "SET x=5"]
        assert node.state_machine.data == {"x": "5"}


def test_figure8_without_r6_term_check_loses_a_committed_entry(monkeypatch):
    """The same story with the naive rule ("majority has it => committed").

    node0 commits and applies index 2 (SET x=2) in term 4; node4 then wins
    term 5 without that entry. The invariant checker catches it.
    """

    def naive_advance_commit_index(self: RaftNode) -> None:
        for n in range(self.log.last_index, self.commit_index, -1):
            replicas = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
            if replicas >= self.majority:
                self.commit_index = n
                self._apply_committed()
                return

    monkeypatch.setattr(RaftNode, "_advance_commit_index", naive_advance_commit_index)
    cluster = figure8_cluster(seed=0)
    with pytest.raises(InvariantViolation, match="I4 leader completeness"):
        play_figure8(cluster)
    assert (2, "SET x=2") in cluster.nodes[0].state_machine.history


@pytest.mark.parametrize("seed", range(10))
@pytest.mark.parametrize("n", [3, 5, 7])
def test_committed_entry_survives_n_minus_one_over_two_crashes(n, seed):
    cluster = Cluster(n=n, seed=seed)
    leader = stable_leader(cluster)
    index = commit(cluster, "SET x=1")

    # Crash the leader plus enough others to leave a bare majority alive.
    victims = [leader.id] + [i for i in cluster.node_ids if i != leader.id][: (n - 1) // 2 - 1]
    for v in victims:
        cluster.crash(v)
    new = stable_leader(cluster)
    assert new.log.entry(index) == LogEntry(leader.current_term, "SET x=1")
    commit(cluster, "SET y=2")

    for v in victims:
        cluster.restart(v)
    assert cluster.run_until(Cluster.converged, 300)
    for node in cluster.nodes.values():
        assert node.state_machine.data == {"x": "1", "y": "2"}
