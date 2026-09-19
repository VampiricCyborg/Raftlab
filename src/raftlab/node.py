"""RaftNode: the Follower / Candidate / Leader state machine.

The node is *pure* with respect to the outside world: it never touches the
network and never reads a clock. Time arrives only as the ``now`` argument of
``tick`` and ``handle``, and outbound traffic leaves only as the returned list
of ``(destination, message)`` pairs. The simulator decides what happens to
those messages. This is what makes every run replayable from a seed.

Rules R1–R7 from the brief are implemented as named methods (Blocks 2 and 3)
so the README can point at them.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum
from typing import Sequence

from raftlab.log import RaftLog
from raftlab.messages import (
    AppendEntriesReq,
    AppendEntriesResp,
    Message,
    RequestVoteReq,
    RequestVoteResp,
)
from raftlab.statemachine import KVStateMachine

Outbound = tuple[int, Message]


class Role(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass(frozen=True)
class RaftConfig:
    # Election timeout is drawn uniformly from [min, max] ticks each time the
    # timer is reset. max - min is the "jitter" swept by benchmark 1.
    election_timeout_min: int = 10
    election_timeout_max: int = 20
    # Must be comfortably below election_timeout_min, or followers will time
    # out between heartbeats of a perfectly healthy leader.
    heartbeat_interval: int = 3


class RaftNode:
    def __init__(
        self,
        node_id: int,
        cluster_ids: Sequence[int],
        config: RaftConfig,
        rng: random.Random,
    ) -> None:
        self.id = node_id
        self.peers = tuple(i for i in cluster_ids if i != node_id)
        self.config = config
        self.rng = rng
        self.events: list[str] = []  # drained by the simulator into its trace

        # Persistent state: survives crash/restart.
        self.current_term = 0
        self.voted_for: int | None = None
        self.log = RaftLog()

        # Not Raft state: the applied dict is volatile, its history is
        # instrumentation for I5 (see statemachine.py).
        self.state_machine = KVStateMachine()

        self._reset_volatile(now=0)

    # --- lifecycle ---------------------------------------------------------

    def _reset_volatile(self, now: int) -> None:
        # Volatile, all nodes.
        self.role = Role.FOLLOWER
        self.leader_id: int | None = None
        self.commit_index = 0
        self.last_applied = 0
        # Volatile, candidate only.
        self.votes_received: set[int] = set()
        # Volatile, leader only.
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self.heartbeat_due = 0
        self.reset_election_timer(now)

    def restart(self, now: int) -> None:
        """Come back from a crash with only (current_term, voted_for, log)."""
        self._reset_volatile(now)
        self.state_machine.reset()
        self._emit(f"restarted as follower, term {self.current_term}")

    @property
    def cluster_size(self) -> int:
        return len(self.peers) + 1

    @property
    def majority(self) -> int:
        return self.cluster_size // 2 + 1

    def reset_election_timer(self, now: int) -> None:
        timeout = self.rng.randint(
            self.config.election_timeout_min, self.config.election_timeout_max
        )
        self.election_deadline = now + timeout

    def _emit(self, text: str) -> None:
        self.events.append(text)

    # --- entry points called by the simulator ------------------------------

    def tick(self, now: int) -> list[Outbound]:
        """Advance this node's timers to ``now``."""
        if self.role is Role.LEADER:
            if now >= self.heartbeat_due:
                return self._send_heartbeats(now)
            return []
        if now >= self.election_deadline:
            return self._on_election_timeout(now)
        return []

    def handle(self, src: int, msg: Message, now: int) -> list[Outbound]:
        """Process one inbound message and return the replies to send."""
        match msg:
            case RequestVoteReq():
                return self._handle_request_vote(src, msg, now)
            case RequestVoteResp():
                return self._handle_request_vote_resp(src, msg, now)
            case AppendEntriesReq():
                return self._handle_append_entries(src, msg, now)
            case AppendEntriesResp():
                return self._handle_append_entries_resp(src, msg, now)
        raise TypeError(f"unknown message type: {type(msg).__name__}")

    # --- election (Block 2) ------------------------------------------------

    def _on_election_timeout(self, now: int) -> list[Outbound]:
        # TODO(block 2): become candidate and request votes.
        # Placeholder so the Block 1 skeleton can tick without electing anyone.
        self._emit("election timeout (elections not implemented yet)")
        self.reset_election_timer(now)
        return []

    def _handle_request_vote(
        self, src: int, msg: RequestVoteReq, now: int
    ) -> list[Outbound]:
        return []  # TODO(block 2)

    def _handle_request_vote_resp(
        self, src: int, msg: RequestVoteResp, now: int
    ) -> list[Outbound]:
        return []  # TODO(block 2)

    # --- replication (Blocks 2 and 3) --------------------------------------

    def _send_heartbeats(self, now: int) -> list[Outbound]:
        return []  # TODO(block 2)

    def _handle_append_entries(
        self, src: int, msg: AppendEntriesReq, now: int
    ) -> list[Outbound]:
        return []  # TODO(block 2 heartbeat, block 3 entries)

    def _handle_append_entries_resp(
        self, src: int, msg: AppendEntriesResp, now: int
    ) -> list[Outbound]:
        return []  # TODO(block 3)

    # --- state machine -----------------------------------------------------

    def _apply_committed(self) -> None:
        """Apply every committed-but-unapplied entry, in index order."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.entry(self.last_applied)
            self.state_machine.apply(self.last_applied, entry.command)
