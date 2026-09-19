"""Simulated network: an in-flight queue plus seeded failure injection.

Every random decision (drop, duplicate, delay) is drawn from the ``rng`` the
simulator hands in, and drawn unconditionally so that the sequence of random
numbers doesn't depend on which knobs are turned on. Same seed, same knobs,
same run.

Partitions are modelled as a set of blocked directed links. A link is checked
both when a message is sent and when it is delivered, so a partition also
destroys messages that were already in flight across it.
"""

from __future__ import annotations

import heapq
import random
from collections import Counter
from dataclasses import dataclass
from typing import Iterable

from raftlab.messages import Message


@dataclass(frozen=True)
class Envelope:
    src: int
    dst: int
    msg: Message
    sent_at: int
    deliver_at: int


class Network:
    def __init__(self, node_ids: Iterable[int], rng: random.Random) -> None:
        self.node_ids = tuple(node_ids)
        self.rng = rng
        self.drop_probability = 0.0
        self.duplicate_probability = 0.0
        self.min_delay = 1
        self.max_delay = 1
        self._blocked: set[tuple[int, int]] = set()
        # Heap of (deliver_at, seq, envelope); seq breaks ties in send order.
        self._queue: list[tuple[int, int, Envelope]] = []
        self._seq = 0
        self.stats: Counter[str] = Counter()

    # --- failure-injection knobs -------------------------------------------

    def drop_rate(self, p: float) -> None:
        if not 0.0 <= p <= 1.0:
            raise ValueError("drop rate must be in [0, 1]")
        self.drop_probability = p

    def duplicate_rate(self, p: float) -> None:
        if not 0.0 <= p <= 1.0:
            raise ValueError("duplicate rate must be in [0, 1]")
        self.duplicate_probability = p

    def delay(self, min_ticks: int, max_ticks: int) -> None:
        """Every message takes uniform[min, max] ticks. A range causes reordering."""
        if not 1 <= min_ticks <= max_ticks:
            raise ValueError("need 1 <= min_ticks <= max_ticks")
        self.min_delay, self.max_delay = min_ticks, max_ticks

    def partition(self, *groups: Iterable[int]) -> None:
        """Replace the current topology: nodes can only talk within their group.

        Every node must appear in exactly one group, e.g.
        ``partition({2}, {0, 1, 3, 4})``.
        """
        sets = [set(g) for g in groups]
        listed = [n for g in sets for n in g]
        if sorted(listed) != sorted(self.node_ids):
            raise ValueError(f"groups must cover every node exactly once: {sets}")
        group_of = {n: i for i, g in enumerate(sets) for n in g}
        self._blocked = {
            (a, b)
            for a in self.node_ids
            for b in self.node_ids
            if group_of[a] != group_of[b]
        }

    def isolate(self, node_id: int) -> None:
        """Cut every link to and from ``node_id``, leaving other links alone."""
        for other in self.node_ids:
            if other != node_id:
                self._blocked.add((node_id, other))
                self._blocked.add((other, node_id))

    def rejoin(self, node_id: int) -> None:
        """Restore every link to and from ``node_id``."""
        self._blocked = {(a, b) for a, b in self._blocked if node_id not in (a, b)}

    def heal(self) -> None:
        self._blocked.clear()

    def can_reach(self, src: int, dst: int) -> bool:
        return (src, dst) not in self._blocked

    # --- traffic -----------------------------------------------------------

    def send(self, src: int, dst: int, msg: Message, now: int) -> None:
        self.stats["sent"] += 1
        dropped = self.rng.random() < self.drop_probability
        duplicated = self.rng.random() < self.duplicate_probability
        if dropped:
            self.stats["dropped_random"] += 1
            return
        if not self.can_reach(src, dst):
            self.stats["dropped_partition"] += 1
            return
        self._schedule(src, dst, msg, now)
        if duplicated:
            self.stats["duplicated"] += 1
            self._schedule(src, dst, msg, now)

    def _schedule(self, src: int, dst: int, msg: Message, now: int) -> None:
        deliver_at = now + self.rng.randint(self.min_delay, self.max_delay)
        env = Envelope(src, dst, msg, sent_at=now, deliver_at=deliver_at)
        heapq.heappush(self._queue, (deliver_at, self._seq, env))
        self._seq += 1

    def deliver_due(self, now: int) -> list[Envelope]:
        """Pop every message due at or before ``now``, in (deliver_at, send order)."""
        due: list[Envelope] = []
        while self._queue and self._queue[0][0] <= now:
            _, _, env = heapq.heappop(self._queue)
            if self.can_reach(env.src, env.dst):
                self.stats["delivered"] += 1
                due.append(env)
            else:
                self.stats["dropped_partition"] += 1
        return due

    @property
    def in_flight(self) -> int:
        return len(self._queue)

    def pending(self) -> list[Envelope]:
        """Messages still in flight, in delivery order (read-only view for visualizers)."""
        return [env for _, _, env in sorted(self._queue, key=lambda item: item[:2])]

    @property
    def blocked_links(self) -> frozenset[tuple[int, int]]:
        return frozenset(self._blocked)
