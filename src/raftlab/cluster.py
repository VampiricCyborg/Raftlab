"""The simulator: logical clock, event loop, crash/restart.

One call to ``step()`` is one tick of logical time:

1. ``now += 1``
2. deliver every message due at ``now`` (in deterministic order) to live nodes
3. ``tick(now)`` every live node, in id order
4. run every registered checker (the invariant checker plugs in here)

Everything is single-threaded and seeded. The network and each node get their
own ``random.Random`` derived from the cluster seed, so a node's timeouts don't
change just because the amount of network traffic changed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable, Iterable

from raftlab.invariants import InvariantChecker
from raftlab.messages import Message
from raftlab.node import Outbound, RaftConfig, RaftNode, Role
from raftlab.transport import Network

Checker = Callable[["Cluster"], None]


@dataclass(frozen=True)
class TraceEvent:
    now: int
    node: int | None  # None for cluster-level events (partition, heal, ...)
    text: str

    def __str__(self) -> str:
        who = "cluster" if self.node is None else f"node{self.node}"
        return f"t={self.now:03d} {who}: {self.text}"


class Cluster:
    def __init__(
        self,
        n: int = 3,
        seed: int = 0,
        config: RaftConfig | None = None,
        checkers: Iterable[Checker] | None = None,
        trace_messages: bool = True,
    ) -> None:
        self.seed = seed
        self.config = config or RaftConfig()
        self.now = 0
        self.node_ids = tuple(range(n))
        # String seeds are hashed with SHA-512 by random.seed, so they are
        # stable across runs and unaffected by PYTHONHASHSEED.
        self.network = Network(self.node_ids, random.Random(f"net:{seed}"))
        self.nodes = {
            i: RaftNode(i, self.node_ids, self.config, random.Random(f"node{i}:{seed}"))
            for i in self.node_ids
        }
        self.crashed: set[int] = set()
        # Invariants are on unless a caller (e.g. a benchmark) opts out explicitly.
        self.checkers: list[Checker] = (
            [InvariantChecker()] if checkers is None else list(checkers)
        )
        self.trace_messages = trace_messages
        self.trace: list[TraceEvent] = []

    # --- the event loop ----------------------------------------------------

    def step(self) -> None:
        self.now += 1
        for env in self.network.deliver_due(self.now):
            if env.dst in self.crashed:
                continue
            if self.trace_messages:
                self._record(env.dst, f"recv {_describe(env.msg)} from node{env.src}")
            out = self.nodes[env.dst].handle(env.src, env.msg, self.now)
            self._after_node_call(env.dst, out)
        for i in self.node_ids:
            if i not in self.crashed:
                self._after_node_call(i, self.nodes[i].tick(self.now))
        for check in self.checkers:
            check(self)

    def run(self, ticks: int) -> None:
        for _ in range(ticks):
            self.step()

    def run_until(self, predicate: Callable[["Cluster"], bool], max_ticks: int) -> bool:
        """Step until ``predicate(self)`` holds; False if it never did."""
        for _ in range(max_ticks):
            if predicate(self):
                return True
            self.step()
        return predicate(self)

    def _after_node_call(self, node_id: int, out: list[Outbound]) -> None:
        node = self.nodes[node_id]
        for text in node.events:
            self._record(node_id, text)
        node.events.clear()
        for dst, msg in out:
            self.network.send(node_id, dst, msg, self.now)

    # --- fault injection ---------------------------------------------------

    def crash(self, node_id: int) -> None:
        self.crashed.add(node_id)
        self._record(node_id, "CRASHED")

    def restart(self, node_id: int) -> None:
        self.crashed.discard(node_id)
        node = self.nodes[node_id]
        node.restart(self.now)
        self._after_node_call(node_id, [])

    def partition(self, *groups: Iterable[int]) -> None:
        groups = tuple(set(g) for g in groups)
        self.network.partition(*groups)
        self._record(None, "PARTITION " + " | ".join(_fmt_group(g) for g in groups))

    def isolate(self, node_id: int) -> None:
        self.network.isolate(node_id)
        self._record(None, f"ISOLATE node{node_id}")

    def rejoin(self, node_id: int) -> None:
        self.network.rejoin(node_id)
        self._record(None, f"REJOIN node{node_id}")

    def heal(self) -> None:
        self.network.heal()
        self._record(None, "HEAL")

    # --- observation helpers ----------------------------------------------

    def live_nodes(self) -> list[RaftNode]:
        return [self.nodes[i] for i in self.node_ids if i not in self.crashed]

    def leaders(self) -> list[RaftNode]:
        """Live nodes that currently *believe* they are leader (can be >1)."""
        return [n for n in self.live_nodes() if n.role is Role.LEADER]

    def _record(self, node_id: int | None, text: str) -> None:
        self.trace.append(TraceEvent(self.now, node_id, text))


def _describe(msg: Message) -> str:
    return f"{type(msg).__name__}(term={msg.term})"


def _fmt_group(group: set[int]) -> str:
    return "{" + ",".join(str(i) for i in sorted(group)) + "}"
