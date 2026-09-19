"""RaftNode: the Follower / Candidate / Leader state machine.

The node is *pure* with respect to the outside world: it never touches the
network and never reads a clock. Time arrives only as the ``now`` argument of
``tick`` and ``handle``, and outbound traffic leaves only as the returned list
of ``(destination, message)`` pairs. The simulator decides what happens to
those messages. This is what makes every run replayable from a seed.

Rules R1–R7 from the brief are implemented as named methods so the README can
point at them.
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
        # Volatile, candidate only. A list in arrival order, for the narration.
        self.votes_received: list[int] = []
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
        """Process one inbound message and return the replies to send.

        Every message passes through the two term rules first, so by the time
        a handler below runs, ``msg.term == self.current_term`` is guaranteed.
        """
        old_term = self.current_term
        self._step_down_if_newer_term(msg.term, now)  # R1
        if self.current_term != old_term:
            self._emit(f"saw term {self.current_term} > term {old_term}, adopts it")

        rejection = self._reject_stale(msg)  # R2
        if rejection is not None:
            return [(src, reply) for reply in rejection]

        assert msg.term == self.current_term, "R1/R2 must leave only current-term messages"
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

    # --- R1, R2: terms (paper §5.1) ----------------------------------------
    # Tests: tests/test_rules_election.py -k "r1 or r2"

    def _step_down_if_newer_term(self, term: int, now: int) -> None:
        """R1: any RPC with ``term > current_term`` => set term, ``voted_for = None``,
        become Follower.

        Called by ``handle`` on *every* inbound message (requests and
        responses) before anything else looks at it.

        - ``term`` is the term carried by the incoming message.
        - Use ``self._become_follower(now)`` to change role; it handles the
          narration and the timer reset for a deposed leader.
        - Why clear the vote? ``voted_for`` means "who I voted for in
          ``current_term``". A new term is a new election, and holding on to
          the old vote would make this node refuse everyone in the new term.

        Primer §2. Paper Figure 2, "Rules for Servers / All Servers".
        """
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._become_follower(now)

    def _reject_stale(self, msg: Message) -> list[Message] | None:
        """R2: any RPC with ``term < current_term`` => reject, reply with ``current_term``.

        Return ``None`` if ``msg`` is from the current term (``handle`` then
        dispatches it normally). Otherwise return the list of replies to send
        back to the sender; ``handle`` addresses them for you:

        - a stale ``RequestVoteReq`` gets a ``RequestVoteResp`` that refuses
          the vote,
        - a stale ``AppendEntriesReq`` gets an ``AppendEntriesResp`` that
          fails (use ``match_index=0``),
        - a stale *response* gets no reply at all: it answers a question from
          an old term that nobody is asking any more.

        Both refusals must carry *our* ``current_term``. That's the whole
        point: when the stale sender receives it, its own R1 fires and it
        steps down. A stale message must not change any of our state.

        Primer §2. Paper Figure 2, "Receiver implementation" step 1 of both RPCs.
        """
        if msg.term >= self.current_term:
            return None
        match msg:
            case RequestVoteReq():
                return [RequestVoteResp(term=self.current_term, vote_granted=False)]
            case AppendEntriesReq():
                return [AppendEntriesResp(term=self.current_term, success=False, match_index=0)]
        return []

    # --- R3: voting (paper §5.2, §5.4.1) -----------------------------------
    # Tests: tests/test_rules_election.py -k r3, tests/test_election.py

    def _should_grant_vote(self, msg: RequestVoteReq) -> bool:
        """R3: grant only if ``voted_for in (None, candidate_id)`` **and** the
        candidate's log is at least as up-to-date as ours.

        Only decide here; don't mutate state. The caller
        (``_handle_request_vote``) records ``voted_for`` and resets the timer
        when you return True. R1 and R2 have already run, so
        ``msg.term == self.current_term``.

        Why is re-granting to the *same* candidate allowed? The network can
        duplicate or retransmit a RequestVote. Answering "yes" again is
        harmless, since it's the same vote.

        Primer §4. Paper §5.2 (one vote per term), Figure 2 RequestVote step 2.
        """
        already_voted_for_other = self.voted_for not in (None, msg.candidate_id)
        if already_voted_for_other:
            return False
        return self._candidate_log_is_up_to_date(msg.last_log_index, msg.last_log_term)

    def _candidate_log_is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
        """R3, second half: is the candidate's log at least as up-to-date as ours?

        Compare the *term of the last entry* first; the higher term wins
        outright, whatever the lengths. Only if the last terms are equal does
        the *length* (last index) decide, and a tie counts as up-to-date.
        ``self.log.last_index`` / ``self.log.last_term`` give our side (both
        0 for an empty log).

        A longer log from an older term is *less* up-to-date: its extra
        entries were never committed (primer §6, Figure 8, step (d)).

        Primer §6 "Leader completeness". Paper §5.4.1.
        """
        if last_log_term != self.log.last_term:
            return last_log_term > self.log.last_term
        return last_log_index >= self.log.last_index

    # --- role transitions --------------------------------------------------

    def _become_follower(self, now: int) -> None:
        was = self.role
        self.role = Role.FOLLOWER
        self.leader_id = None
        if was is not Role.FOLLOWER:
            self._emit(f"steps down to FOLLOWER, term {self.current_term}")
        if was is Role.LEADER:
            # A leader's election deadline is long stale; without this reset it
            # would time out on its very next tick and disrupt the new leader.
            self.reset_election_timer(now)

    def _on_election_timeout(self, now: int) -> list[Outbound]:
        """Follower or candidate heard nothing for a full timeout: start an election."""
        self.current_term += 1
        self.role = Role.CANDIDATE
        self.leader_id = None
        self.voted_for = self.id
        self.votes_received = [self.id]
        self.reset_election_timer(now)
        self._emit(f"times out -> CANDIDATE, term {self.current_term}")
        if len(self.votes_received) >= self.majority:  # single-node cluster
            return self._become_leader(now)
        req = RequestVoteReq(
            term=self.current_term,
            candidate_id=self.id,
            last_log_index=self.log.last_index,
            last_log_term=self.log.last_term,
        )
        return [(peer, req) for peer in self.peers]

    def _become_leader(self, now: int) -> list[Outbound]:
        self.role = Role.LEADER
        self.leader_id = self.id
        self.next_index = {p: self.log.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        votes = ",".join(str(v) for v in self.votes_received)
        self._emit(f"elected LEADER, term {self.current_term} (votes: {votes})")
        return self._send_heartbeats(now)  # assert leadership immediately

    # --- election RPC handlers ---------------------------------------------

    def _handle_request_vote(
        self, src: int, msg: RequestVoteReq, now: int
    ) -> list[Outbound]:
        granted = self._should_grant_vote(msg)  # R3
        if granted:
            self.voted_for = msg.candidate_id
            # Granting a vote counts as hearing from a (future) leader: don't
            # start a competing election right after helping someone else win.
            self.reset_election_timer(now)
            self._emit(f"votes for node{msg.candidate_id}, term {self.current_term}")
        return [(src, RequestVoteResp(term=self.current_term, vote_granted=granted))]

    def _handle_request_vote_resp(
        self, src: int, msg: RequestVoteResp, now: int
    ) -> list[Outbound]:
        if self.role is not Role.CANDIDATE or not msg.vote_granted:
            return []
        if src in self.votes_received:  # duplicated message: count each voter once
            return []
        self.votes_received.append(src)
        if len(self.votes_received) >= self.majority:
            return self._become_leader(now)
        return []

    # --- replication (heartbeats in Block 2, entries in Block 3) -----------

    def _send_heartbeats(self, now: int) -> list[Outbound]:
        self.heartbeat_due = now + self.config.heartbeat_interval
        return [(peer, self._append_entries_for(peer)) for peer in self.peers]

    def _append_entries_for(self, peer: int) -> AppendEntriesReq:
        """Everything ``peer`` is believed to be missing, anchored at next_index - 1."""
        prev = self.next_index[peer] - 1
        return AppendEntriesReq(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev,
            prev_log_term=self.log.term_at(prev),
            entries=self.log.entries_from(prev + 1),
            leader_commit=self.commit_index,
        )

    def _handle_append_entries(
        self, src: int, msg: AppendEntriesReq, now: int
    ) -> list[Outbound]:
        if self.role is Role.LEADER:
            # Another leader in *our* term would violate I1. Don't paper over
            # it here; the invariant checker reports it.
            return []
        if self.role is Role.CANDIDATE:
            self._become_follower(now)  # someone already won this term
        self.leader_id = msg.leader_id
        self.reset_election_timer(now)
        # TODO(block 3): R4 consistency check, R5 truncate + append, R7 commit.
        return [(src, AppendEntriesResp(term=self.current_term, success=False, match_index=0))]

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
