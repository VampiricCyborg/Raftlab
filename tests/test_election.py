"""Block 2: leader election across a simulated cluster.

The InvariantChecker (I1: one leader per term) runs after every step of every
cluster here, so each test also asserts election safety throughout.
"""

import pytest

from helpers import LeaderRecorder, stable_leader
from raftlab import Cluster, LogEntry, RaftConfig, Role

SEEDS = range(10)


@pytest.mark.parametrize("seed", SEEDS)
def test_single_leader_elected_from_cold_start(seed):
    cluster = Cluster(n=5, seed=seed)
    leader = stable_leader(cluster, max_ticks=100)
    term = leader.current_term
    cluster.run(200)
    assert cluster.leaders() == [leader]
    assert all(n.current_term == term for n in cluster.nodes.values())
    assert all(n.leader_id == leader.id for n in cluster.nodes.values())


@pytest.mark.parametrize("seed", SEEDS)
def test_split_vote_resolves_via_randomized_timeout(seed):
    cluster = Cluster(n=5, seed=seed)
    for node in cluster.nodes.values():
        node.election_deadline = 1  # everyone times out on the same tick

    cluster.run(5)
    # Term 1: every node voted for itself, so nobody could reach 3 votes.
    assert cluster.leaders() == []
    assert all(n.current_term == 1 and n.role is Role.CANDIDATE for n in cluster.nodes.values())

    # Randomized re-timeouts break the symmetry within a few terms.
    leader = stable_leader(cluster, max_ticks=200)
    assert leader.current_term <= 4


def test_split_vote_never_resolves_without_jitter():
    """The pathology randomization exists to prevent: lockstep split votes forever."""
    no_jitter = RaftConfig(election_timeout_min=10, election_timeout_max=10)
    cluster = Cluster(n=5, seed=0, config=no_jitter)
    for node in cluster.nodes.values():
        node.election_deadline = 1

    recorder = LeaderRecorder()
    cluster.checkers.append(recorder)
    cluster.run(500)
    assert recorder.seen == set()
    assert {n.current_term for n in cluster.nodes.values()} == {50}


@pytest.mark.parametrize("seed", SEEDS)
def test_leader_reelected_after_leader_crash(seed):
    cluster = Cluster(n=5, seed=seed)
    old = stable_leader(cluster)
    cluster.crash(old.id)
    new = stable_leader(cluster)
    assert new.id != old.id
    assert new.current_term > old.current_term

    # The old leader comes back as a follower and accepts the new regime.
    cluster.restart(old.id)
    cluster.run(50)
    assert cluster.leaders() == [new]
    assert old.role is Role.FOLLOWER and old.current_term == new.current_term


@pytest.mark.parametrize("seed", SEEDS)
def test_candidate_with_stale_log_cannot_win(seed):
    cluster = Cluster(n=3, seed=seed)
    # Nodes 0 and 1 hold an entry from term 1 that node 2 never received.
    for node in cluster.nodes.values():
        node.current_term = 1
        node.log.append(LogEntry(1, "SET x=1"))
    for i in (0, 1):
        cluster.nodes[i].log.append(LogEntry(1, "SET y=2"))

    # Give node 2 a head start of 40 ticks: it will run several elections alone.
    cluster.nodes[2].election_deadline = 1
    cluster.nodes[0].election_deadline = 40
    cluster.nodes[1].election_deadline = 40

    recorder = LeaderRecorder()
    cluster.checkers.append(recorder)
    cluster.run(39)
    assert cluster.nodes[2].current_term > 2, "node 2 should have tried repeatedly"
    stable_leader(cluster)
    cluster.run(100)
    assert 2 not in recorder.leader_ids
    assert recorder.leader_ids <= {0, 1}


@pytest.mark.parametrize("seed", SEEDS)
def test_no_leader_in_minority_partition(seed):
    cluster = Cluster(n=5, seed=seed)
    cluster.partition({0, 1}, {2, 3, 4})
    recorder = LeaderRecorder()
    cluster.checkers.append(recorder)
    cluster.run(300)
    assert recorder.leader_ids.isdisjoint({0, 1})
    # The minority keeps trying (its terms inflate) but can never reach 3 votes.
    assert all(cluster.nodes[i].current_term > 1 for i in (0, 1))
    [leader] = cluster.leaders()
    assert leader.id in {2, 3, 4}


def _election_trace(seed: int) -> list[str]:
    cluster = Cluster(n=5, seed=seed)
    cluster.network.delay(1, 4)
    cluster.network.drop_rate(0.1)
    cluster.network.duplicate_rate(0.05)
    cluster.run(100)
    cluster.crash(cluster.leaders()[0].id if cluster.leaders() else 0)
    cluster.run(200)
    return [str(e) for e in cluster.trace]


def test_election_trace_is_deterministic():
    assert _election_trace(3) == _election_trace(3)
    assert _election_trace(3) != _election_trace(4)
