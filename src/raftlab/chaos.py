"""Seeded chaos: random crashes, restarts, partitions and client traffic.

Every random choice comes from ``random.Random(f"chaos:{seed}")`` (plus the
cluster's own seeded RNGs), so a seed *is* a complete, replayable run:

    python -m raftlab.chaos --seed 17            # summary
    python -m raftlab.chaos --seed 17 --trace    # full event trace

The InvariantChecker runs after every step, so a safety bug surfaces as an
InvariantViolation naming the invariant, the tick and the seed.
"""

from __future__ import annotations

import argparse
import random
import sys

from raftlab.cluster import Cluster
from raftlab.invariants import InvariantViolation
from raftlab.node import RaftConfig


class Chaos:
    """A cluster plus a seeded fault schedule. ``run`` then ``recover``."""

    def __init__(self, seed: int, n: int = 5) -> None:
        self.seed = seed
        self.rng = random.Random(f"chaos:{seed}")
        # Half the seeds use a tight timeout window: more split votes, so more
        # concurrent candidates in one term, which stresses one-vote-per-term.
        jitter = self.rng.choice([2, 10])
        config = RaftConfig(election_timeout_min=10, election_timeout_max=10 + jitter)
        self.cluster = Cluster(n=n, seed=seed, config=config)
        net = self.cluster.network
        net.drop_rate(self.rng.choice([0.0, 0.05, 0.15, 0.3]))
        low = self.rng.randint(1, 3)
        net.delay(low, low + self.rng.randint(0, 4))
        net.duplicate_rate(self.rng.choice([0.0, 0.05]))
        self.bounce_back: dict[int, int] = {}  # node id -> tick to restart it at

    def run(self, ticks: int = 500) -> None:
        """``ticks`` steps of randomized faults and client load."""
        cluster, rng, n = self.cluster, self.rng, len(self.cluster.node_ids)
        for _ in range(ticks):
            for node_id, at in sorted(self.bounce_back.items()):
                if at <= cluster.now:
                    del self.bounce_back[node_id]
                    if node_id in cluster.crashed:
                        cluster.restart(node_id)
            live = sorted(set(cluster.node_ids) - cluster.crashed)
            down = sorted(cluster.crashed)
            r = rng.random()
            if r < 0.010 and len(live) > 1:
                cluster.crash(rng.choice(live))
            elif r < 0.040 and down:
                cluster.restart(rng.choice(down))
            elif r < 0.050:
                order = list(cluster.node_ids)
                rng.shuffle(order)
                cut = rng.randint(1, n - 1)
                cluster.partition(order[:cut], order[cut:])
            elif r < 0.058:
                cluster.isolate(rng.choice(cluster.node_ids))
            elif r < 0.085:
                cluster.heal()
            elif r < 0.100 and len(live) > 1:
                # Bounce: a quick crash-and-restart. It lands inside elections
                # far more often than independent crashes, which is where a
                # node forgetting its vote would let it vote twice in a term.
                victim = rng.choice(live)
                cluster.crash(victim)
                self.bounce_back[victim] = cluster.now + rng.randint(1, 5)
            if rng.random() < 0.3:
                # Clients talk to whichever node claims leadership, including
                # stale leaders stranded in a minority: the dangerous case.
                leaders = sorted(cluster.leaders(), key=lambda node: node.id)
                if leaders:
                    t = cluster.now
                    cluster.submit(f"SET k{t % 7}={self.seed}.{t}", node_id=rng.choice(leaders).id)
            cluster.step()

    def recover(self, max_ticks: int = 1000) -> bool:
        """End the chaos (heal, restart everyone, stop dropping) and check the
        cluster converges: one leader, identical logs, everything applied."""
        cluster = self.cluster
        cluster.heal()
        for node_id in sorted(cluster.crashed):
            cluster.restart(node_id)
        cluster.network.drop_rate(0.0)
        cluster.network.duplicate_rate(0.0)

        def settled(c: Cluster) -> bool:
            leaders = c.leaders()
            if len(leaders) != 1:
                return False
            leader = leaders[0]
            # A leader only commits its own-term entries (R6): make sure one exists.
            if leader.log.last_term != leader.current_term:
                c.submit("NOOP", node_id=leader.id)
                return False
            return c.converged()

        return cluster.run_until(settled, max_ticks)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--ticks", type=int, default=500)
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--trace", action="store_true", help="print every event")
    args = parser.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")

    chaos = Chaos(args.seed, args.nodes)
    cluster = chaos.cluster
    try:
        chaos.run(args.ticks)
        recovered = chaos.recover()
    except InvariantViolation as err:
        print(f"VIOLATION: {err}")
        return 1
    finally:
        if args.trace:
            for event in cluster.trace:
                print(event)

    leader = cluster.leaders()[0] if recovered else None
    print(f"seed={args.seed} ticks={cluster.now} nodes={args.nodes}")
    print(f"network: {dict(cluster.network.stats)}")
    print(f"invariant checks passed: {cluster.checkers[0].steps} steps x I1-I5")
    if leader is None:
        print("did NOT converge after recovery")
        return 2
    print(
        f"converged: leader node{leader.id} term {leader.current_term}, "
        f"{leader.log.last_index} entries, state {leader.state_machine.data}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
