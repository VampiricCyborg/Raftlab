"""Log replication and commit across a simulated cluster."""

import pytest

from helpers import commit, stable_leader
from raftlab import Cluster, LogEntry

SEEDS = range(5)


@pytest.mark.parametrize("seed", SEEDS)
def test_entry_replicated_to_all_followers(seed):
    cluster = Cluster(n=5, seed=seed)
    stable_leader(cluster)
    commit(cluster, "SET x=1")
    assert cluster.run_until(Cluster.converged, 50)
    for node in cluster.nodes.values():
        assert node.log.entries() == (LogEntry(node.current_term, "SET x=1"),)
        assert node.state_machine.data == {"x": "1"}


@pytest.mark.parametrize("seed", SEEDS)
def test_entry_committed_after_majority_ack(seed):
    cluster = Cluster(n=5, seed=seed)
    leader = stable_leader(cluster)
    followers = [i for i in cluster.node_ids if i != leader.id]

    # Leader + 2 followers = 3 of 5: enough.
    for f in followers[:2]:
        cluster.isolate(f)
    _, index = cluster.submit("SET x=1")
    assert leader.commit_index < index  # accepted is not committed
    assert cluster.run_until(lambda c: leader.commit_index >= index, 20)

    # Leader + 1 follower = 2 of 5: never.
    cluster.isolate(followers[2])
    _, index = cluster.submit("SET y=2")
    cluster.run(100)
    assert leader.commit_index < index
    assert "y" not in leader.state_machine.data


@pytest.mark.parametrize("seed", SEEDS)
def test_lagging_follower_catches_up_after_rejoin(seed):
    cluster = Cluster(n=5, seed=seed)
    leader = stable_leader(cluster)
    lagger = next(i for i in cluster.node_ids if i != leader.id)
    cluster.isolate(lagger)
    for k in range(10):
        commit(cluster, f"SET k{k}={k}")
    assert cluster.nodes[lagger].log.last_index == 0

    cluster.rejoin(lagger)
    # The isolated node inflated its term while alone, so rejoining may force
    # an election; either way it must end up with the full committed log.
    assert cluster.run_until(Cluster.converged, 300)
    assert cluster.nodes[lagger].state_machine.data == {f"k{k}": str(k) for k in range(10)}


@pytest.mark.parametrize("seed", SEEDS)
def test_conflicting_follower_suffix_is_truncated(seed):
    cluster = Cluster(n=3, seed=seed)
    shared = [LogEntry(1, "SET a=1"), LogEntry(2, "SET b=2")]
    stale = [LogEntry(1, "SET a=1"), LogEntry(1, "SET x=bad"), LogEntry(1, "SET y=bad"), LogEntry(1, "SET z=bad")]
    for i, entries in ((0, shared), (1, shared), (2, stale)):
        node = cluster.nodes[i]
        node.current_term = 2
        for e in entries:
            node.log.append(e)
    cluster.nodes[0].election_deadline = 1
    for i in (1, 2):
        cluster.nodes[i].election_deadline = 50

    leader = stable_leader(cluster)
    assert leader.id == 0
    commit(cluster, "SET c=3")
    assert cluster.run_until(Cluster.converged, 50)
    follower = cluster.nodes[2]
    assert [e.command for e in follower.log.entries()] == ["SET a=1", "SET b=2", "SET c=3"]
    assert follower.state_machine.data == {"a": "1", "b": "2", "c": "3"}
    assert any("log conflict at index 2" in e.text for e in cluster.trace if e.node == 2)


@pytest.mark.parametrize("seed", SEEDS)
def test_heartbeat_prevents_election(seed):
    cluster = Cluster(n=5, seed=seed)
    leader = stable_leader(cluster)
    term, elected_at = leader.current_term, cluster.now
    cluster.run(500)
    assert cluster.leaders() == [leader]
    assert all(n.current_term == term for n in cluster.nodes.values())
    assert not any("CANDIDATE" in e.text for e in cluster.trace if e.now > elected_at)


def test_replication_survives_lossy_reordering_network():
    cluster = Cluster(n=5, seed=11)
    cluster.network.delay(1, 5)
    cluster.network.drop_rate(0.2)
    cluster.network.duplicate_rate(0.1)
    for k in range(20):
        commit(cluster, f"SET k{k}={k}", max_ticks=500)
    cluster.network.drop_rate(0.0)
    assert cluster.run_until(Cluster.converged, 300)
    for node in cluster.nodes.values():
        assert node.state_machine.data == {f"k{k}": str(k) for k in range(20)}
