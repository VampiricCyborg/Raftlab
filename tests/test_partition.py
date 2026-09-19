"""Partitions and split brain.

Every cluster here runs the full I1–I5 checker after every step, so these
tests assert safety *throughout* the partition, not only once it's healed.
"""

import pytest

from helpers import commit, stable_leader
from raftlab import Cluster, Role

SEEDS = range(10)


def split_brain(seed: int):
    """Elect a leader, cut it off alone, and wait for the majority to elect another.

    Returns (cluster, old_leader, new_leader).
    """
    cluster = Cluster(n=5, seed=seed)
    old = stable_leader(cluster)
    commit(cluster, "SET x=1")
    cluster.partition({old.id}, set(cluster.node_ids) - {old.id})
    ok = cluster.run_until(
        lambda c: any(n.role is Role.LEADER and n.id != old.id for n in c.live_nodes()), 200
    )
    assert ok, f"majority side never elected a leader (seed={seed})"
    [new] = [n for n in cluster.leaders() if n.id != old.id]
    return cluster, old, new


@pytest.mark.parametrize("seed", SEEDS)
def test_minority_leader_cannot_commit(seed):
    cluster = Cluster(n=5, seed=seed)
    leader = stable_leader(cluster)
    buddy = next(i for i in cluster.node_ids if i != leader.id)
    cluster.partition({leader.id, buddy}, set(cluster.node_ids) - {leader.id, buddy})

    _, index = cluster.submit("SET x=42", node_id=leader.id)
    cluster.run(200)
    assert cluster.nodes[buddy].log.last_index >= index  # replicated to its partner...
    assert leader.commit_index < index  # ...but 2 of 5 is not a majority
    assert all("x" not in n.state_machine.data for n in cluster.nodes.values())


@pytest.mark.parametrize("seed", SEEDS)
def test_old_leader_steps_down_after_heal(seed):
    """The headline test: two nodes believe they're leader, only one can act."""
    cluster, old, new = split_brain(seed)

    # Split brain, as seen from inside: both believe they lead, in different terms.
    assert old.role is Role.LEADER and new.role is Role.LEADER
    assert new.current_term > old.current_term

    x99 = commit(cluster, "SET x=99")
    _, x42 = cluster.submit("SET x=42", node_id=old.id)
    cluster.run(30)
    assert new.commit_index >= x99
    assert old.commit_index < x42  # the stale leader can accept but never commit

    cluster.heal()
    assert cluster.run_until(lambda c: old.role is Role.FOLLOWER, 30)
    assert old.current_term >= new.current_term
    assert cluster.run_until(Cluster.converged, 200)
    assert len(cluster.leaders()) == 1
    for node in cluster.nodes.values():
        assert node.state_machine.data == {"x": "99"}


@pytest.mark.parametrize("seed", SEEDS)
def test_uncommitted_entries_from_old_leader_are_overwritten(seed):
    cluster, old, new = split_brain(seed)
    stale = [cluster.submit(f"SET stale{k}=1", node_id=old.id)[1] for k in range(3)]
    for k in range(3):
        commit(cluster, f"SET fresh{k}=1")
    stale_terms = {old.log.term_at(i) for i in stale}

    cluster.heal()
    assert cluster.run_until(Cluster.converged, 300)
    commands = [e.command for e in old.log.entries()]
    assert not any(c.startswith("SET stale") for c in commands)
    assert all(f"SET fresh{k}=1" in commands for k in range(3))
    assert all(old.log.term_at(i) not in stale_terms for i in stale)
    assert any("log conflict" in e.text for e in cluster.trace if e.node == old.id)
