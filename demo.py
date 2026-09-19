"""RaftLab demo: split brain, and why it's harmless.

A 5-node cluster elects a leader and commits a write. The leader is then
partitioned away alone: the majority elects a new leader and keeps committing,
while the old leader still believes it is in charge and accepts a write it can
never commit. When the partition heals, the old leader steps down and its
uncommitted entry is overwritten. I1–I5 are checked after every step.

    uv run python demo.py            # the narrated run (fixed seed)
    uv run python demo.py --seed 7   # same script, different timings
"""

from __future__ import annotations

import argparse
import re
import sys

from raftlab import Cluster, Role
from raftlab.client import commit

SEED = 3


class Narrator:
    """Turns the cluster's trace into the story, skipping per-message noise."""

    def __init__(self, cluster: Cluster, out) -> None:
        self.cluster = cluster
        self.out = out
        self.seen = 0
        self.saw_term: dict[int, str] = {}

    def say(self, text: str, now: int | None = None) -> None:
        now = self.cluster.now if now is None else now
        print(f"t={now:03d}  {text}", file=self.out)

    def flush(self) -> None:
        events = self.cluster.trace[self.seen :]
        self.seen = len(self.cluster.trace)
        for e in events:
            line = self.render(e.node, e.text)
            if line:
                self.say(line, e.now)

    def render(self, node: int | None, text: str) -> str | None:
        who = f"node{node}"
        if node is None:
            if text.startswith("PARTITION"):
                return "⚡ " + text
            if text == "HEAL":
                return "✓ HEAL"
            return None
        if m := re.fullmatch(r"times out -> CANDIDATE, term (\d+)", text):
            return f"{who} times out → candidate, term {m[1]}"
        if m := re.fullmatch(r"elected LEADER, term (\d+) \(votes: (.*)\)", text):
            return f"{who} elected LEADER, term {m[1]} (votes: {m[2]})"
        if m := re.fullmatch(r"saw term (\d+) > term (\d+), adopts it", text):
            self.saw_term[node] = f"sees term {m[1]} > term {m[2]}"
            return None
        if text.startswith("steps down to FOLLOWER"):
            why = self.saw_term.pop(node, "")
            return f"{who} {why} → steps down to FOLLOWER".replace("  ", " ")
        if m := re.fullmatch(r"log conflict at index (\d+) .* discarded (.*)", text):
            return f"{who} log conflict at index {m[1]} → truncated, {m[2]} discarded"
        return None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=SEED)
    args = parser.parse_args(argv)
    run_demo(args.seed, sys.stdout)
    return 0


def run_demo(seed: int, out) -> None:
    cluster = Cluster(n=5, seed=seed)
    narrate = Narrator(cluster, out)
    narrate.say("cluster of 5 started, all followers")

    cluster.run_until(lambda c: len(c.leaders()) == 1, 200)
    narrate.flush()
    old = cluster.leaders()[0]

    index = commit(cluster, "SET x=1")
    narrate.flush()
    narrate.say(f"client: SET x=1    → committed at index {index}")

    cluster.run(10)
    rest = set(cluster.node_ids) - {old.id}
    cluster.partition({old.id}, rest)
    narrate.flush()
    cluster.step()
    narrate.say(f"node{old.id} still believes it is leader (term {old.current_term})")

    cluster.run_until(
        lambda c: any(n.role is Role.LEADER and n.id != old.id for n in c.live_nodes()), 200
    )
    narrate.flush()
    index = commit(cluster, "SET x=99")
    narrate.flush()
    narrate.say(f"client: SET x=99   → committed at index {index} (majority side)")

    cluster.run(5)
    narrate.flush()
    _, index = cluster.submit("SET x=42", node_id=old.id)
    narrate.say(f"node{old.id} accepts SET x=42 at index {index} — NOT committed (no majority)")
    cluster.run(15)
    narrate.flush()
    assert old.commit_index < index

    cluster.heal()
    narrate.flush()
    cluster.run_until(Cluster.converged, 200)
    narrate.flush()

    values = {n.state_machine.data.get("x") for n in cluster.nodes.values()}
    [value] = values
    steps = cluster.checkers[0].steps
    narrate.say(f"✓ all 5 nodes agree: x={value}.  I1–I5 held after every one of {steps} steps.")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    raise SystemExit(main())
