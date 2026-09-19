"""Log replication. Block 2 adds the heartbeat test; Block 3 adds the rest."""

import pytest

from helpers import stable_leader
from raftlab import Cluster


@pytest.mark.parametrize("seed", range(5))
def test_heartbeat_prevents_election(seed):
    cluster = Cluster(n=5, seed=seed)
    leader = stable_leader(cluster)
    term, elected_at = leader.current_term, cluster.now
    cluster.run(500)
    assert cluster.leaders() == [leader]
    assert all(n.current_term == term for n in cluster.nodes.values())
    assert not any("CANDIDATE" in e.text for e in cluster.trace if e.now > elected_at)
