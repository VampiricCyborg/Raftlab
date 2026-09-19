"""Block 3 unit tests: R4–R7 exercised on a single node, no network."""

import random

from raftlab import LogEntry, RaftConfig, RaftNode, Role
from raftlab.messages import AppendEntriesReq, AppendEntriesResp


def E(term: int, cmd: str = "") -> LogEntry:
    return LogEntry(term, cmd or f"SET t={term}")


def make_node(term: int, entries: tuple[LogEntry, ...] = (), n: int = 3) -> RaftNode:
    node = RaftNode(0, range(n), RaftConfig(), random.Random(0))
    node.current_term = term
    for e in entries:
        node.log.append(e)
    return node


def ae(term, prev_index, prev_term, entries=(), commit=0, leader=1) -> AppendEntriesReq:
    return AppendEntriesReq(term, leader, prev_index, prev_term, tuple(entries), commit)


def reply(replies) -> AppendEntriesResp:
    [(_, resp)] = replies
    return resp


# --- R4: consistency check ------------------------------------------------


def test_r4_prev_index_zero_always_matches():
    node = make_node(term=1)
    assert reply(node.handle(1, ae(1, 0, 0, [E(1, "SET a=1")]), now=0)).success
    assert node.log.entries() == (E(1, "SET a=1"),)


def test_r4_rejects_when_follower_log_is_too_short():
    node = make_node(term=2, entries=(E(1),))
    resp = reply(node.handle(1, ae(2, 3, 2, [E(2)]), now=0))
    assert not resp.success
    assert resp.match_index == 1  # hint: "I only have up to index 1"
    assert node.log.last_index == 1


def test_r4_rejects_term_mismatch_and_hints_before_the_conflicting_term():
    node = make_node(term=3, entries=(E(1), E(2), E(2), E(2)))
    resp = reply(node.handle(1, ae(3, 4, 3, [E(3)]), now=0))
    assert not resp.success
    assert resp.match_index == 1  # skip the whole run of term-2 entries at once
    assert node.log.last_index == 4  # R4 failure changes nothing


# --- R5: truncate only on real conflicts ----------------------------------


def test_r5_truncates_conflicting_suffix_then_appends():
    node = make_node(term=3, entries=(E(1, "SET a=1"), E(2, "SET b=stale"), E(2, "SET c=stale")))
    resp = reply(node.handle(1, ae(3, 1, 1, [E(3, "SET b=new")]), now=0))
    assert resp.success and resp.match_index == 2
    assert node.log.entries() == (E(1, "SET a=1"), E(3, "SET b=new"))


def test_r5_reordered_old_message_does_not_truncate_newer_entries():
    node = make_node(term=2)
    full = [E(2, "SET a=1"), E(2, "SET b=2"), E(2, "SET c=3")]
    node.handle(1, ae(2, 0, 0, full), now=0)
    # A delayed earlier AppendEntries carrying only the first entry arrives late.
    resp = reply(node.handle(1, ae(2, 0, 0, full[:1]), now=1))
    assert resp.success and resp.match_index == 1
    assert node.log.entries() == tuple(full)


def test_r5_duplicate_delivery_is_idempotent():
    node = make_node(term=2)
    msg = ae(2, 0, 0, [E(2, "SET a=1")])
    node.handle(1, msg, now=0)
    node.handle(1, msg, now=0)
    assert node.log.last_index == 1


# --- R7: follower commit ---------------------------------------------------


def test_r7_commit_is_capped_at_last_new_entry():
    node = make_node(term=2, entries=(E(1), E(1), E(1)))  # indexes 2-3 not yet verified
    node.handle(1, ae(2, 1, 1, [], commit=3), now=0)
    assert node.commit_index == 1
    assert node.state_machine.history == [(1, "SET t=1")]


def test_r7_commit_never_moves_backwards():
    node = make_node(term=2)
    node.handle(1, ae(2, 0, 0, [E(2, "SET a=1"), E(2, "SET b=2")], commit=2), now=0)
    node.handle(1, ae(2, 0, 0, [E(2, "SET a=1")], commit=1), now=1)
    assert node.commit_index == 2
    assert node.state_machine.data == {"a": "1", "b": "2"}


# --- R6: leader commit ------------------------------------------------------


def make_leader(term: int, entries, n: int = 5) -> RaftNode:
    node = make_node(term, entries, n)
    node.role = Role.LEADER
    node.next_index = {p: node.log.last_index + 1 for p in node.peers}
    node.match_index = {p: 0 for p in node.peers}
    return node


def ok(node: RaftNode, src: int, match: int) -> None:
    node.handle(src, AppendEntriesResp(node.current_term, True, match), now=0)


def test_r6_commits_current_term_entry_once_a_majority_holds_it():
    leader = make_leader(3, [E(3)])
    ok(leader, 1, 1)
    assert leader.commit_index == 0  # 2 of 5
    ok(leader, 2, 1)
    assert leader.commit_index == 1  # 3 of 5


def test_r6_never_commits_previous_term_entry_by_counting_replicas():
    leader = make_leader(4, [E(1), E(2)])
    for peer in (1, 2, 3, 4):
        ok(leader, peer, 2)
    assert leader.commit_index == 0  # even all 5 holding it is not enough


def test_r6_previous_term_entries_commit_indirectly_via_current_term_entry():
    leader = make_leader(4, [E(1), E(2), E(4)])
    ok(leader, 1, 3)
    ok(leader, 2, 3)
    assert leader.commit_index == 3
    assert [i for i, _ in leader.state_machine.history] == [1, 2, 3]


def test_r6_reordered_success_reply_does_not_lower_match_index():
    leader = make_leader(2, [E(2), E(2)])
    ok(leader, 1, 2)
    ok(leader, 1, 1)
    assert leader.match_index[1] == 2
