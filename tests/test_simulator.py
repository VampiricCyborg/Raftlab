"""Block 1: the simulator skeleton, with no Raft logic involved yet."""

import random

import pytest

from raftlab import Cluster, LogEntry, Network, RaftLog, Role
from raftlab.messages import RequestVoteResp


def ping(term: int = 0) -> RequestVoteResp:
    """Any frozen message will do as a payload for transport tests."""
    return RequestVoteResp(term=term, vote_granted=False)


def drain(net: Network, until: int) -> list[tuple[int, int, int]]:
    """Deliver everything up to ``until``; return (tick, src, term) per message."""
    got = []
    for now in range(until + 1):
        got += [(now, e.src, e.msg.term) for e in net.deliver_due(now)]
    return got


# --- log ------------------------------------------------------------------


def test_log_sentinel_append_and_truncate():
    log = RaftLog()
    assert (log.last_index, log.last_term, log.term_at(0)) == (0, 0, 0)
    assert log.term_at(1) is None
    for term in (1, 1, 2):
        log.append(LogEntry(term, f"SET x={term}"))
    assert (log.last_index, log.last_term) == (3, 2)
    assert log.entries_from(2) == (LogEntry(1, "SET x=1"), LogEntry(2, "SET x=2"))
    assert log.entries_from(4) == ()
    log.truncate_from(2)
    assert log.entries() == (LogEntry(1, "SET x=1"),)
    with pytest.raises(ValueError):
        log.truncate_from(0)


# --- transport ------------------------------------------------------------


def test_message_arrives_after_its_delay():
    net = Network([0, 1], random.Random(0))
    net.delay(3, 3)
    net.send(0, 1, ping(), now=10)
    assert net.deliver_due(12) == []
    [env] = net.deliver_due(13)
    assert (env.src, env.dst, env.sent_at, env.deliver_at) == (0, 1, 10, 13)


def test_random_delay_reorders_messages():
    net = Network([0, 1], random.Random(1))
    net.delay(1, 10)
    for i in range(20):
        net.send(0, 1, ping(term=i), now=0)
    terms = [term for _, _, term in drain(net, 10)]
    assert sorted(terms) == list(range(20))
    assert terms != list(range(20))


def test_partition_blocks_cross_group_traffic_only():
    net = Network(range(5), random.Random(0))
    net.partition({2}, {0, 1, 3, 4})
    assert not net.can_reach(2, 0) and not net.can_reach(0, 2)
    assert net.can_reach(0, 4)
    net.send(2, 0, ping(), now=0)
    net.send(0, 4, ping(), now=0)
    assert [src for _, src, _ in drain(net, 5)] == [0]
    with pytest.raises(ValueError):
        net.partition({0, 1}, {2})  # nodes 3 and 4 missing


def test_partition_destroys_messages_already_in_flight():
    net = Network([0, 1], random.Random(0))
    net.delay(5, 5)
    net.send(0, 1, ping(), now=0)
    net.partition({0}, {1})
    assert drain(net, 10) == []


def test_isolate_rejoin_and_heal():
    net = Network(range(3), random.Random(0))
    net.isolate(1)
    assert not net.can_reach(0, 1) and not net.can_reach(1, 2)
    assert net.can_reach(0, 2)
    net.rejoin(1)
    assert net.can_reach(0, 1)
    net.partition({0}, {1, 2})
    net.heal()
    assert all(net.can_reach(a, b) for a in range(3) for b in range(3))


def test_drop_rate_extremes_and_seeded_pattern():
    def delivered(seed: int, p: float) -> list[int]:
        net = Network([0, 1], random.Random(seed))
        net.drop_rate(p)
        for i in range(100):
            net.send(0, 1, ping(term=i), now=0)
        return [term for _, _, term in drain(net, 1)]

    assert len(delivered(0, 0.0)) == 100
    assert delivered(0, 1.0) == []
    assert 0 < len(delivered(0, 0.3)) < 100
    assert delivered(0, 0.3) == delivered(0, 0.3)
    assert delivered(0, 0.3) != delivered(1, 0.3)


def test_duplication_delivers_copies():
    net = Network([0, 1], random.Random(0))
    net.duplicate_rate(1.0)
    net.send(0, 1, ping(), now=0)
    assert len(drain(net, 1)) == 2


# --- cluster --------------------------------------------------------------


def test_three_nodes_tick_with_seeded_timeouts():
    cluster = Cluster(n=3, seed=42)
    cfg = cluster.config
    deadlines = [cluster.nodes[i].election_deadline for i in cluster.node_ids]
    assert all(cfg.election_timeout_min <= d <= cfg.election_timeout_max for d in deadlines)
    assert deadlines == [Cluster(n=3, seed=42).nodes[i].election_deadline for i in range(3)]
    cluster.run(25)
    assert cluster.now == 25
    assert all(n.role is Role.FOLLOWER for n in cluster.nodes.values())


def test_cluster_delivers_messages_between_nodes():
    cluster = Cluster(n=3, seed=0)
    cluster.network.send(0, 2, ping(term=7), cluster.now)
    cluster.step()
    assert [str(e) for e in cluster.trace] == [
        "t=001 node2: recv RequestVoteResp(term=7) from node0"
    ]


def test_crashed_node_receives_nothing_and_restart_keeps_persistent_state():
    cluster = Cluster(n=3, seed=0)
    node = cluster.nodes[1]
    node.current_term, node.voted_for = 3, 2
    node.log.append(LogEntry(3, "SET x=1"))
    node.commit_index = 1

    cluster.crash(1)
    cluster.network.send(0, 1, ping(), cluster.now)
    cluster.run(30)
    assert not any("recv" in e.text for e in cluster.trace if e.node == 1)

    cluster.restart(1)
    assert (node.current_term, node.voted_for, node.log.last_index) == (3, 2, 1)
    assert (node.commit_index, node.last_applied, node.role) == (0, 0, Role.FOLLOWER)
    assert node.election_deadline > cluster.now


def run_scripted(seed: int) -> list[str]:
    cluster = Cluster(n=5, seed=seed)
    cluster.network.delay(1, 6)
    cluster.network.drop_rate(0.2)
    cluster.network.duplicate_rate(0.1)
    for t in range(200):
        if t == 50:
            cluster.partition({0, 1}, {2, 3, 4})
        if t == 80:
            cluster.crash(3)
        if t == 120:
            cluster.heal()
            cluster.restart(3)
        src, dst = t % 5, (t * 3 + 1) % 5
        cluster.network.send(src, dst, ping(term=t), cluster.now)
        cluster.step()
    return [str(e) for e in cluster.trace]


def test_same_seed_same_trace():
    assert run_scripted(7) == run_scripted(7)
    assert run_scripted(7) != run_scripted(8)
