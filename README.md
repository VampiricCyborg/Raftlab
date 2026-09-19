# RaftLab

A from-scratch implementation of the **Raft consensus algorithm** (leader
election, log replication, and the safety rules), running as a deterministic
simulated cluster inside a single process. The simulated network can drop,
delay, reorder, duplicate and partition messages on demand, and nodes can
crash and restart. After **every simulation step**, a checker asserts all five
of Raft's safety properties, and it fails loudly the moment one breaks.

This is not a distributed database. It's a small, correct, testable,
observable consensus core, plus the failure scenarios that show it is correct.
Python 3.11+, standard library only (`pytest` and `hypothesis` for tests).

```bash
uv sync
uv run pytest                              # 388 tests (64 functions), ~15 s
uv run python demo.py                      # the transcript below
uv run python -m raftlab.chaos --seed 17   # one chaos run, replayable by seed
uv run python benchmarks/bench_election.py # (also bench_commit, bench_availability)
```

## Split brain in fourteen lines

`python demo.py`. The output is deterministic, and `tests/test_demo.py`
fails if this block ever stops matching it byte-for-byte:

<!-- demo:start -->
```text
t=000  cluster of 5 started, all followers
t=011  node2 times out → candidate, term 1
t=013  node2 elected LEADER, term 1 (votes: 2,0,1)
t=015  client: SET x=1    → committed at index 1
t=025  ⚡ PARTITION {2} | {0,1,3,4}
t=026  node2 still believes it is leader (term 1)
t=034  node0 times out → candidate, term 2
t=036  node0 elected LEADER, term 2 (votes: 0,1,3)
t=038  client: SET x=99   → committed at index 2 (majority side)
t=043  node2 accepts SET x=42 at index 2 — NOT committed (no majority)
t=058  ✓ HEAL
t=061  node2 sees term 2 > term 1 → steps down to FOLLOWER
t=061  node2 log conflict at index 2 → truncated, SET x=42 discarded
t=061  ✓ all 5 nodes agree: x=99.  I1–I5 held after every one of 61 steps.
```
<!-- demo:end -->

From t=034 to t=061, two nodes believe they are the leader. That is allowed.
What Raft forbids is two leaders *in the same term* (I1), and a leader that
can't reach a majority committing anything. node2 accepts `SET x=42` but can
never get it onto 3 of 5 logs. When the partition heals, the first message it
sees carries term 2, so it steps down, and the new leader's AppendEntries
overwrite its uncommitted entry.

## Safety properties, and why they hold

The rules are implemented literally, as named methods in
[`src/raftlab/node.py`](src/raftlab/node.py):

| Rule | Method | Statement |
|---|---|---|
| R1 | `_step_down_if_newer_term` | Any RPC with `term > current_term`: adopt the term, clear `voted_for`, become Follower |
| R2 | `_reject_stale` | Any RPC with `term < current_term`: reject it, reply with `current_term` |
| R3 | `_should_grant_vote`, `_candidate_log_is_up_to_date` | Vote only if not already voted for someone else this term **and** the candidate's log is at least as up-to-date (last term first, then length) |
| R4 | `_log_matches` | AppendEntries succeeds only if the follower has an entry at `prev_log_index` with `prev_log_term` |
| R5 | `_append_new_entries` | On conflict (same index, different term), delete that entry and everything after it, then append. Never truncate on a match |
| R6 | `_advance_commit_index` | Leader commits N only if a majority has `match_index ≥ N` **and** `log[N].term == current_term` |
| R7 | `_follow_leader_commit` | Follower sets `commit_index = min(leader_commit, last new entry index)`, never lowering it |

The invariants are checked by [`src/raftlab/invariants.py`](src/raftlab/invariants.py)
after every step of every test, fuzz run and demo. Each argument below rests
on one fact: **any two majorities of the same cluster share at least one
node.**

**I1: Election safety.** *At most one leader per term.* A node votes at most
once per term (R3), and `voted_for` is persistent, so it survives crashes.
Winning needs a majority, two majorities overlap, and the node in the overlap
can't have voted for both candidates.
Tests: `test_single_leader_elected_from_cold_start`,
`test_restarted_node_does_not_vote_twice_in_one_term`. The checker records
the first leader of each term and flags any second one, even if the two are
never leader at the same instant.

**I2: Leader append-only.** *A leader never overwrites or deletes its own
entries.* This holds by construction. The only code that truncates is R5,
which runs in the follower path, and a leader that receives an AppendEntries
in its own term drops it instead of processing it. The checker compares each
leader's log to its log one step earlier.

**I3: Log matching.** *If two logs have an entry with the same index and term,
they are identical up to that index.* A leader creates at most one entry per
index in its term and never changes it (I2), so `(index, term)` names a unique
command. By induction, a follower appends only after R4 confirms it agrees with
the leader on the entry just before. That is also why R4 needs to check only
the single pair `(prev_log_index, prev_log_term)`: agreeing on that entry
implies agreeing on the whole prefix.
Test: `test_conflicting_follower_suffix_is_truncated`. The checker compares
every pair of logs, crashed nodes included.

**I4: Leader completeness.** *An entry committed in term T is in the log of
every leader of any term > T.* A committed entry is on a majority, and a
winning candidate got votes from a majority. Some voter is in both, and by R3
it refuses any candidate whose log is less up-to-date than its own. So a
candidate missing a committed entry can't win.
This depends on R6. Without the current-term condition, an entry can sit on a
majority and still be overwritten; that's Raft paper Figure 8.
`test_no_stale_term_commit` replays that scenario exactly, and
`test_figure8_without_r6_term_check_loses_a_committed_entry` swaps in the naive
rule and shows the checker catching the lost commit. Also covered by
`test_candidate_with_stale_log_cannot_win`.

**I5: State machine safety.** *No two nodes apply different commands at the
same index.* Nodes apply only committed entries, in index order. By I4, every
future leader holds each committed entry, and by I3 every follower that
matches that leader holds the same prefix. So a committed index can never
come to hold a different command. The checker keeps each node's append-only
apply history (it survives restarts) and compares them.
Tests: `test_old_leader_steps_down_after_heal`,
`test_committed_entry_survives_n_minus_one_over_two_crashes`, and all 200
chaos seeds.

### The five questions this project answers

1. **Why are randomized election timeouts required?** With identical timeouts
   every node becomes a candidate on the same tick, votes for itself, and no
   one reaches a majority, forever. `test_split_vote_never_resolves_without_jitter`
   runs 500 ticks: 50 terms, no leader. Benchmark 1 measures the curve.
2. **Why may a leader never commit a previous-term entry by counting
   replicas?** Figure 8. An old-term entry on a majority can still be
   overwritten by a candidate whose last entry has a newer term. See I4.
3. **Why is `(prev_log_index, prev_log_term)` enough for the consistency
   check?** Log Matching. See I3.
4. **Why can't a candidate with a staler log win?** Every committed entry is
   on a majority, and every majority contains a voter who refuses it (R3).
   See I4.
5. **Why is split brain impossible even though two nodes can think they're
   leader?** They are leaders of *different* terms. The stale one can't reach
   a majority, because a majority has already moved to the newer term and
   rejects it (R2). The moment it hears the newer term, it steps down (R1).
   See the demo.

## Architecture

```
        ┌─────────────────────────────────────────────┐
        │              Simulator (cluster.py)          │
        │  logical clock · seeded RNGs · event loop    │
        └───────────────┬─────────────────────────────┘
                        │ step(): deliver due messages, tick nodes, check
        ┌───────────────▼─────────────────────────────┐
        │           Network (transport.py)             │
        │  in-flight heap · drop/delay/dup/partition   │
        └───────┬───────────┬───────────┬─────────────┘
                │           │           │
            ┌───▼───┐   ┌───▼───┐   ┌───▼───┐
            │ Node0 │   │ Node1 │   │ Node2 │   ...   (node.py)
            └───┬───┘   └───┬───┘   └───┬───┘
            ┌───▼───────────▼───────────▼───┐
            │  KV state machine (SET only)   │    (statemachine.py)
            └────────────────────────────────┘
        ┌─────────────────────────────────────────────┐
        │   InvariantChecker (invariants.py): I1–I5    │
        │   runs after EVERY step                      │
        └─────────────────────────────────────────────┘
```

**The key decision: the node is a pure state machine driven by a logical
clock.** `RaftNode` never touches the network and never reads a clock. Time
enters only as the `now` argument to `tick(now)` and `handle(src, msg, now)`,
and messages leave only as the returned list of `(destination, message)`
pairs. The simulator decides what happens to them. One `Cluster.step()`
advances `now` by one tick, delivers every message due at that tick in a fixed
order, ticks every live node in id order, then runs the invariant checker.
There are no threads, no asyncio and no `time.sleep`.

What that buys:

- **Determinism.** Every source of randomness is a `random.Random` seeded from
  the cluster seed. The network and each node get their own stream, so extra
  traffic doesn't perturb a node's timeouts. Same seed ⇒ same trace, which
  is tested.
- **Replayable failures.** Any failing fuzz seed replays exactly with
  `python -m raftlab.chaos --seed N --trace`.
- **Cheap invariant checking.** Between steps the whole cluster is quiescent,
  so the checker can inspect every node's state directly.

Other details:

- **Messages are frozen dataclasses**, so nothing can mutate a message while
  it's in flight.
- **The log is 1-indexed**, with a virtual index-0 entry of term 0, so
  `prev_log_index = 0` always matches.
- **A crash** stops a node from receiving messages or ticking. **A restart**
  keeps `current_term`, `voted_for` and `log` (what real Raft keeps on disk)
  and resets everything else.
- **Partitions** are a set of blocked directed links. A link is checked at
  send *and* at delivery, so cutting a link also destroys messages already in
  flight across it.
- **AppendEntries carries at most 8 entries.** Leaders pipeline the next batch
  as soon as a reply arrives, and followers' failure replies include a hint
  that lets the leader skip back over a whole conflicting term per round trip.
  Batching matters for correctness testing: without it R7's cap could never
  come into play, and a bug removing it went undetected (see below).
- Defaults: election timeout 10–20 ticks, heartbeat every 3 ticks, message
  delay 1 tick (all configurable).

## Testing: do the tests have teeth?

64 test functions (388 cases with parametrization), all seeded, all
deterministic (the suite has been run twice and its output diffed). Unit
tests for each rule, cluster tests for each scenario in the brief,
[`tests/test_fuzz.py`](tests/test_fuzz.py) with 200 chaos seeds × 500 ticks
(random crashes, quick crash/restart "bounces", partitions, isolation, 0–30%
loss, duplication, reordering delays, tight and loose timeout windows, and
client writes sent to *any* node claiming leadership, stale ones included),
plus a derandomized Hypothesis test that generates fault schedules. After the
chaos, each fuzz run heals everything and asserts the cluster converges.

To check that the tests would catch real mistakes, each rule was broken in
turn and the suite re-run:

| Deliberate bug | Fuzzer alone | Full suite |
|---|---|---|
| R1 keeps its old vote on a new term | caught | caught |
| R3 votes twice in a term | caught (I1) | caught (I1) |
| R3 skips the up-to-date check | caught (I4) | caught |
| R4 accepts any existing prev entry | caught (I3) | caught (I3) |
| R5 truncates even on matching entries | caught | caught |
| R6 commits old-term entries by count | caught (I4, seed 181) | caught (Figure 8 test) |
| R7 caps at log end, not last new entry | caught (I4) | caught |
| `voted_for` forgotten on restart | not caught | caught (targeted test) |

Seed 181 is worth a look. With R6's term check removed, random chaos
rediscovered a Figure 8 interleaving by itself. The one bug the fuzzer misses
(voting twice across a crash) needs a node to crash and restart between two
RequestVotes of one term. Only about 1 seed in 1000 produces that, which is why
it has a dedicated test.

## Benchmarks

These measure algorithmic behaviour in ticks, not throughput. Every run is
seeded and reproducible (`uv run python benchmarks/<script>.py`).

**1. Election latency vs timeout jitter** (5 nodes, 100 seeds, cold start,
`bench_election.py`)

| timeout window | median ticks | p95 ticks | max ticks | mean terms | had split vote | no leader in 2000 |
|---:|---:|---:|---:|---:|---:|---:|
| [10, 10] | n/a | n/a | n/a | n/a | 100% | 100% |
| [10, 11] | 12 | 23 | 43 | 1.29 | 23% | 0% |
| [10, 12] | 12 | 23 | 35 | 1.09 | 8% | 0% |
| [10, 15] | 12 | 14 | 25 | 1.01 | 1% | 0% |
| [10, 20] | 13 | 16 | 18 | 1.00 | 0% | 0% |
| [10, 30] | 14.5 | 20 | 24 | 1.00 | 0% | 0% |
| [10, 50] | 17 | 29 | 37 | 1.00 | 0% | 0% |

*Reading:* with no jitter, elections never converge. A little jitter fixes
the median but leaves a split-vote tail (23% at ±1 tick). Beyond roughly one
network round trip of jitter, split votes vanish and a wider window only adds
latency. The default [10, 20] sits at the bottom of that curve.

**2. Time-to-commit vs message drop rate** (5 nodes, delay 1–3 ticks,
100 seeds × 20 sequential commands, retrying client, `bench_commit.py`)

| drop rate | median ticks | p95 ticks | max ticks | elections / 1000 ticks | gave up (>1000 ticks) |
|---:|---:|---:|---:|---:|---:|
| 0% | 3 | 4 | 5 | 0.0 | 0 |
| 5% | 4 | 5 | 6 | 0.0 | 0 |
| 15% | 4 | 5 | 7 | 0.0 | 0 |
| 30% | 5 | 7 | 55 | 3.7 | 0 |

*Reading:* only a majority has to acknowledge, and unacknowledged entries are
resent with every heartbeat, so moderate loss barely moves commit latency. At
30% loss, followers start missing enough consecutive heartbeats to time out
and depose a healthy leader. Those elections, not retransmission, create the
long tail.

**3. Availability vs cluster size** (50 seeds × 3000 ticks; every K ticks the
last victim restarts and a random node is killed, `bench_availability.py`)

| kill every K ticks | nodes | ticks with stable leader | kills that hit the leader | median outage (ticks) |
|---:|---:|---:|---:|---:|
| 50 | 3 | 85.67% | 35% | 19 |
| 50 | 5 | 92.60% | 22% | 17 |
| 50 | 7 | 95.68% | 13% | 16 |
| 100 | 3 | 92.58% | 34% | 19 |
| 100 | 5 | 96.46% | 21% | 17 |
| 100 | 7 | 97.52% | 15% | 16 |
| 200 | 3 | 96.48% | 35% | 18 |
| 200 | 5 | 98.42% | 19% | 17 |
| 200 | 7 | 98.81% | 15% | 16 |

*Reading:* killing a follower costs nothing, because a majority is still up.
Only killing the leader costs an election (≈ one timeout, ~17 ticks). A kill
hits the leader with probability 1/n, so availability loss ≈ (1/n) × outage / K,
and larger clusters are more available mainly because the leader is a smaller
target.

## Explicit non-goals

These are deliberate scope cuts, not omissions:

- **Real networking.** No TCP, gRPC or sockets. In-process message queues only.
- **Disk persistence.** `current_term`, `voted_for` and `log` live in memory. A
  crash is a volatile-state reset with that triple preserved, which is enough
  to exercise the algorithm.
- **Log compaction, snapshotting and InstallSnapshot.**
- **Cluster membership changes** (joint consensus). N is fixed at startup.
- **Client sessions, linearizable reads and exactly-once semantics.** A retried
  command can be appended twice; `SET` is idempotent, so this is harmless here.
  Relatedly, leaders don't append a no-op on election (paper §8); previous-term
  entries commit when the next client write in the new term does.
- **PreVote and leader stickiness.** A node that rejoins after isolation, with
  an inflated term, can depose a healthy leader. That's safe, and it shows up
  in the tests as an extra election.
- **Performance tuning.** The benchmarks measure behaviour (ticks, elections),
  not throughput.

## What I'd add next

1. **Persistence**: write the persistent triple through a fake disk that can
   also lose unsynced writes on crash.
2. **Snapshotting**: log compaction plus InstallSnapshot, with invariants over
   the snapshot boundary.
3. **Membership changes**: joint consensus, and a checker for the
   "no two disjoint majorities" property during transitions.
4. **Real transport**: the pure-node design means only `transport.py` and
   the event loop would need to change.

## Layout

```
src/raftlab/
  messages.py      RequestVote / AppendEntries request + response (frozen)
  log.py           LogEntry, RaftLog (1-indexed, index 0 sentinel)
  node.py          RaftNode: Follower / Candidate / Leader, rules R1–R7
  transport.py     simulated network: delay, reorder, drop, duplicate, partition
  cluster.py       simulator: logical clock, event loop, crash/restart, submit
  statemachine.py  toy KV store (SET key=value)
  invariants.py    I1–I5, checked after every step
  client.py        retrying client used by tests, benchmarks and the demo
  chaos.py         seeded chaos runs; python -m raftlab.chaos --seed N
tests/             unit tests per rule, cluster scenarios, safety, fuzz, demo
benchmarks/        the three tables above
demo.py            the transcript above
documentation/     project brief and a Raft primer
```

## References

- Diego Ongaro and John Ousterhout, *In Search of an Understandable Consensus
  Algorithm (Extended Version)*: §5.2 leader election, §5.3 log replication,
  §5.4 safety (§5.4.1 election restriction, §5.4.2 committing entries from
  previous terms / Figure 8), and Figure 2, the one-page summary of the rules.
- [`documentation/raft-primer.md`](documentation/raft-primer.md): a short
  primer mapping the paper's ideas onto R1–R7 and I1–I5 in this codebase.
