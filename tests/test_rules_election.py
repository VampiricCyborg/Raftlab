"""Block 2 unit tests: R1, R2, R3 exercised on a single node, no network.

These are the fast feedback loop while you implement the rules. Each test
feeds one message into ``node.handle`` and inspects the reply and the state.
"""

import random

from raftlab import LogEntry, RaftConfig, RaftNode, Role
from raftlab.messages import (
    AppendEntriesReq,
    AppendEntriesResp,
    RequestVoteReq,
    RequestVoteResp,
)


def make_node(term: int = 0, log_terms: tuple[int, ...] = (), role: Role = Role.FOLLOWER) -> RaftNode:
    """Node 0 in a 3-node cluster {0, 1, 2}."""
    node = RaftNode(0, range(3), RaftConfig(), random.Random(0))
    node.current_term = term
    for t in log_terms:
        node.log.append(LogEntry(t, f"SET x={t}"))
    node.role = role
    return node


def vote_req(term: int, candidate: int = 1, last_index: int = 0, last_term: int = 0) -> RequestVoteReq:
    return RequestVoteReq(term=term, candidate_id=candidate, last_log_index=last_index, last_log_term=last_term)


def heartbeat(term: int, leader: int = 1) -> AppendEntriesReq:
    return AppendEntriesReq(term=term, leader_id=leader, prev_log_index=0, prev_log_term=0, entries=(), leader_commit=0)


def granted(replies) -> bool:
    [(dst, resp)] = replies
    assert isinstance(resp, RequestVoteResp)
    return resp.vote_granted


# --- R1: higher term => adopt it, clear vote, become follower -------------


def test_r1_follower_adopts_higher_term_and_forgets_its_vote():
    node = make_node(term=1)
    node.voted_for = 2
    node.handle(1, heartbeat(term=3), now=0)
    assert node.current_term == 3
    assert node.voted_for is None
    assert node.role is Role.FOLLOWER


def test_r1_leader_steps_down_on_higher_term_response():
    node = make_node(term=2, role=Role.LEADER)
    node.voted_for = 0
    node.handle(1, RequestVoteResp(term=5, vote_granted=False), now=0)
    assert (node.current_term, node.voted_for, node.role) == (5, None, Role.FOLLOWER)


def test_r1_candidate_steps_down_on_higher_term_request():
    node = make_node(term=2, role=Role.CANDIDATE)
    node.voted_for = 0
    node.handle(2, heartbeat(term=4, leader=2), now=0)
    assert (node.current_term, node.role) == (4, Role.FOLLOWER)


def test_r1_equal_term_changes_nothing():
    node = make_node(term=3)
    node.voted_for = 2
    node.handle(1, RequestVoteResp(term=3, vote_granted=False), now=0)
    assert (node.current_term, node.voted_for) == (3, 2)


# --- R2: lower term => reject, reply with current_term --------------------


def test_r2_stale_vote_request_is_refused_with_current_term():
    node = make_node(term=5)
    replies = node.handle(1, vote_req(term=3), now=0)
    assert replies == [(1, RequestVoteResp(term=5, vote_granted=False))]
    assert node.voted_for is None


def test_r2_stale_append_entries_is_refused_and_does_not_reset_timer():
    node = make_node(term=5)
    deadline = node.election_deadline
    replies = node.handle(1, heartbeat(term=4), now=7)
    assert replies == [(1, AppendEntriesResp(term=5, success=False, match_index=0))]
    # A deposed leader's heartbeats must not keep this node from electing a new one.
    assert node.election_deadline == deadline
    assert node.leader_id is None


def test_r2_stale_responses_get_no_reply_and_are_not_counted():
    node = make_node(term=3, role=Role.CANDIDATE)
    node.voted_for, node.votes_received = 0, [0]
    assert node.handle(1, RequestVoteResp(term=2, vote_granted=True), now=0) == []
    assert node.handle(2, AppendEntriesResp(term=1, success=True, match_index=9), now=0) == []
    assert node.votes_received == [0]
    assert node.role is Role.CANDIDATE


# --- R3: one vote per term, and only for an up-to-date log ----------------


def test_r3_grants_first_vote_of_the_term():
    node = make_node(term=1)
    assert granted(node.handle(1, vote_req(term=1, candidate=1), now=0))
    assert node.voted_for == 1


def test_r3_refuses_a_second_candidate_in_the_same_term():
    node = make_node(term=1)
    node.handle(1, vote_req(term=1, candidate=1), now=0)
    assert not granted(node.handle(2, vote_req(term=1, candidate=2), now=0))
    assert node.voted_for == 1


def test_r3_regrants_same_candidate_so_duplicate_requests_are_harmless():
    node = make_node(term=1)
    node.handle(1, vote_req(term=1, candidate=1), now=0)
    assert granted(node.handle(1, vote_req(term=1, candidate=1), now=0))


def test_r3_new_term_means_a_fresh_vote():
    node = make_node(term=1)
    node.handle(1, vote_req(term=1, candidate=1), now=0)
    assert granted(node.handle(2, vote_req(term=2, candidate=2), now=0))
    assert node.voted_for == 2


def test_r3_refuses_candidate_whose_last_term_is_older_even_if_longer():
    node = make_node(term=3, log_terms=(1, 3))
    assert not granted(node.handle(1, vote_req(term=4, last_index=5, last_term=2), now=0))


def test_r3_grants_candidate_whose_last_term_is_newer_even_if_shorter():
    node = make_node(term=3, log_terms=(1, 1, 1, 2))
    assert granted(node.handle(1, vote_req(term=4, last_index=2, last_term=3), now=0))


def test_r3_same_last_term_compares_length():
    node = make_node(term=2, log_terms=(1, 2, 2))
    assert not granted(node.handle(1, vote_req(term=3, candidate=1, last_index=2, last_term=2), now=0))
    # Refusing on log grounds must not burn the vote: an equal log still wins it.
    assert granted(node.handle(2, vote_req(term=3, candidate=2, last_index=3, last_term=2), now=0))


def test_r3_empty_logs_are_equally_up_to_date():
    node = make_node(term=0)
    assert granted(node.handle(1, vote_req(term=1, last_index=0, last_term=0), now=0))
