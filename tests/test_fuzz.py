"""Randomized chaos. I1–I5 are asserted after *every* step by the cluster's
InvariantChecker; these tests add liveness-after-recovery on top.

A failure's test id carries the seed. Replay it exactly with:

    uv run python -m raftlab.chaos --seed <seed> --trace
"""

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from raftlab import Cluster
from raftlab.chaos import Chaos


@pytest.mark.parametrize("seed", range(200))
def test_random_chaos_preserves_all_invariants(seed):
    chaos = Chaos(seed)
    chaos.run(500)  # raises InvariantViolation (with the seed) on any breach
    assert chaos.recover(), f"no convergence after chaos; replay: python -m raftlab.chaos --seed {seed}"
    assert chaos.cluster.checkers[0].steps == chaos.cluster.now


# --- property-based: Hypothesis chooses the fault schedule --------------------

N = 5
nodes = st.integers(0, N - 1)
ops = st.one_of(
    st.tuples(st.just("crash"), nodes),
    st.tuples(st.just("restart"), nodes),
    st.tuples(st.just("isolate"), nodes),
    st.tuples(st.just("partition"), st.sets(nodes, min_size=1, max_size=N - 1)),
    st.tuples(st.just("heal"), st.none()),
    st.tuples(st.just("submit"), nodes),
)
schedules = st.lists(st.tuples(ops, st.integers(1, 25)), min_size=1, max_size=40)


@settings(max_examples=60, derandomize=True, deadline=None,
          suppress_health_check=[HealthCheck.too_slow])
@given(seed=st.integers(0, 2**16), schedule=schedules)
def test_arbitrary_fault_schedules_preserve_invariants(seed, schedule):
    """Hypothesis explores schedules no hand-written chaos loop would, and
    shrinks any failure to a minimal schedule. ``derandomize`` keeps the
    examples identical on every run, so the suite stays deterministic."""
    cluster = Cluster(n=N, seed=seed)
    cluster.network.delay(1, 4)
    cluster.network.drop_rate(0.1)
    for step, ((op, arg), gap) in enumerate(schedule):
        if op == "crash" and arg not in cluster.crashed and len(cluster.crashed) < N - 1:
            cluster.crash(arg)
        elif op == "restart" and arg in cluster.crashed:
            cluster.restart(arg)
        elif op == "isolate":
            cluster.isolate(arg)
        elif op == "partition":
            cluster.partition(arg, set(range(N)) - arg)
        elif op == "heal":
            cluster.heal()
        elif op == "submit" and arg not in cluster.crashed and cluster.nodes[arg].role.value == "leader":
            cluster.submit(f"SET k{step}={seed}", node_id=arg)
        cluster.run(gap)
