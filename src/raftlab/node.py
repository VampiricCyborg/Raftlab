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

from raftlab.log import LogEntry, RaftLog
from raftlab.messages import (
    AppendEntriesReq,
    AppendEntriesResp,
    Message,
    RequestVoteReq,
    RequestVoteResp,
)
from raftlab.statemachine import KVStateMachine

Outbound = tuple[int, Message]


class NotLeader(Exception):
    pass


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

    # --- client entry point ------------------------------------------------

    def propose(self, command: str, now: int) -> tuple[int, list[Outbound]]:
        """Leader only: append a client command and start replicating it.

        Returns the entry's log index plus the AppendEntries to send. The
        entry is *not* committed yet; that happens once R6 is satisfied.
        """
        if self.role is not Role.LEADER:
            raise NotLeader(f"node{self.id} is {self.role.value}, leader is {self.leader_id}")
        index = self.log.append(LogEntry(self.current_term, command))
        self._emit(f"accepts {command!r} at index {index}, term {self.current_term}")
        self._advance_commit_index()  # a single-node cluster commits alone
        return index, self._send_heartbeats(now)

    # --- replication, leader side (paper §5.3) -----------------------------

    def _send_heartbeats(self, now: int) -> list[Outbound]:
        self.heartbeat_due = now + self.config.heartbeat_interval
        return [(peer, self._append_entries_for(peer)) for peer in self.peers]

    def _append_entries_for(self, peer: int) -> AppendEntriesReq:
        """Everything ``peer`` is believed to be missing, anchored at next_index - 1.

        An empty ``entries`` tuple is a heartbeat. Unacknowledged entries are
        simply re-sent on every heartbeat, which doubles as retransmission.
        """
        prev = self.next_index[peer] - 1
        return AppendEntriesReq(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev,
            prev_log_term=self.log.term_at(prev),
            entries=self.log.entries_from(prev + 1),
            leader_commit=self.commit_index,
        )

    def _handle_append_entries_resp(
        self, src: int, msg: AppendEntriesResp, now: int
    ) -> list[Outbound]:
        if self.role is not Role.LEADER:
            return []  # a late reply to a leadership we no longer hold
        if msg.success:
            # Replies can arrive reordered or duplicated: never move backwards.
            self.match_index[src] = max(self.match_index[src], msg.match_index)
            self.next_index[src] = max(self.next_index[src], self.match_index[src] + 1)
            self._advance_commit_index()
            return []
        # Consistency check failed (R4). Jump back to the follower's hint, but
        # never below what we already know it holds, then retry right away.
        self.next_index[src] = max(
            self.match_index[src] + 1,
            min(self.next_index[src] - 1, msg.match_index + 1),
        )
        return [(src, self._append_entries_for(src))]

    def _advance_commit_index(self) -> None:
        """R6: commit N only if a majority has ``match_index >= N`` **and**
        ``log[N].term == current_term``.

        The term check is the Figure 8 rule (primer §6): an entry from an
        earlier term can sit on a majority and still be overwritten later, so
        counting replicas alone proves nothing about it. Earlier-term entries
        become committed indirectly, when a current-term entry after them
        commits (by Log Matching, everything before it is identical on that
        majority).
        """
        for n in range(self.log.last_index, self.commit_index, -1):
            if self.log.term_at(n) != self.current_term:
                break  # terms only decrease going backwards: nothing below qualifies
            replicas = 1 + sum(1 for p in self.peers if self.match_index[p] >= n)
            if replicas >= self.majority:
                self.commit_index = n
                self._emit(f"commits index {n} (term {self.current_term}, {replicas} replicas)")
                self._apply_committed()
                return

    # --- replication, follower side (paper §5.3) ---------------------------

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

        if not self._log_matches(msg.prev_log_index, msg.prev_log_term):  # R4
            hint = self._conflict_hint(msg.prev_log_index)
            return [(src, AppendEntriesResp(self.current_term, success=False, match_index=hint))]

        last_new = self._append_new_entries(msg.prev_log_index, msg.entries)  # R5
        self._follow_leader_commit(msg.leader_commit, last_new)  # R7
        return [(src, AppendEntriesResp(self.current_term, success=True, match_index=last_new))]

    def _log_matches(self, prev_log_index: int, prev_log_term: int) -> bool:
        """R4: succeed only if we hold an entry at prev_log_index with prev_log_term.

        By Log Matching (I3), agreeing on that single entry means agreeing on
        the entire prefix before it, so one comparison checks the whole log.
        """
        return self.log.term_at(prev_log_index) == prev_log_term

    def _conflict_hint(self, prev_log_index: int) -> int:
        """Where the leader should retry from after an R4 failure.

        If our log is too short, retry just after our last entry. If it has
        the wrong term at prev_log_index, skip back over that whole term in
        one step instead of one index per round trip (paper §5.3, end). The
        hint only affects speed: the leader's retry is re-checked by R4.
        """
        conflict_term = self.log.term_at(prev_log_index)
        if conflict_term is None:
            return self.log.last_index
        i = prev_log_index
        while i > 1 and self.log.term_at(i - 1) == conflict_term:
            i -= 1
        return i - 1

    def _append_new_entries(self, prev_log_index: int, entries: tuple[LogEntry, ...]) -> int:
        """R5: on conflict, delete the conflicting entry and all after it, then append.

        Returns the index of the last entry covered by this message.

        Only an entry with the *same index but a different term* is a
        conflict. An entry with the same index and term is the same entry (by
        Log Matching) and is kept. This matters: a delayed, reordered
        AppendEntries carrying a shorter prefix must not truncate entries a
        later message already delivered.
        """
        index = prev_log_index
        for entry in entries:
            index += 1
            existing = self.log.term_at(index)
            if existing == entry.term:
                continue
            if existing is not None:
                dropped = [e.command for e in self.log.entries_from(index)]
                self._emit(
                    f"log conflict at index {index} (term {existing} vs {entry.term})"
                    f" -> truncated, discarded {', '.join(dropped)}"
                )
                assert index > self.commit_index, "R5 must never truncate committed entries"
                self.log.truncate_from(index)
            self.log.append(entry)
        return prev_log_index + len(entries)

    def _follow_leader_commit(self, leader_commit: int, last_new_index: int) -> None:
        """R7: ``commit_index = min(leader_commit, last_new_index)``.

        Capped at last_new_index because only entries up to there are known
        to match the leader; anything after might be a stale suffix. Never
        moves backwards (a reordered old message can carry a smaller value).
        """
        target = min(leader_commit, last_new_index)
        if target > self.commit_index:
            self.commit_index = target
            self._apply_committed()

    # --- state machine -----------------------------------------------------

    def _apply_committed(self) -> None:
        """Apply every committed-but-unapplied entry, in index order."""
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.entry(self.last_applied)
            self.state_machine.apply(self.last_applied, entry.command)
