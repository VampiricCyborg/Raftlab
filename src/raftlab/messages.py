"""Raft RPC messages (paper Figure 2).

All messages are frozen: once a message is in flight nobody can mutate it,
which removes a whole class of "the leader changed the entries tuple after
sending it" simulation bugs. The sender and receiver ids live on the network
envelope, not in the message.
"""

from __future__ import annotations

from dataclasses import dataclass

from raftlab.log import LogEntry


@dataclass(frozen=True)
class RequestVoteReq:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteResp:
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntriesReq:
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]  # empty tuple == heartbeat
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesResp:
    term: int
    success: bool
    # On success: the index of the follower's last entry that now matches the
    # leader. On failure: a hint for where the leader should retry from, which
    # avoids walking next_index back one entry per round trip.
    match_index: int


Message = RequestVoteReq | RequestVoteResp | AppendEntriesReq | AppendEntriesResp
