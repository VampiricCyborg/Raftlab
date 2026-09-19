# RaftLab — Project Brief

**Day 3 of 10 · One-day build · Standalone repo**

---

## 1. What this is

A from-scratch implementation of the **Raft consensus algorithm**, running as a
deterministic simulated cluster inside a single process, with an injectable
network layer that can drop, delay, reorder, and partition messages on demand.

The deliverable is not "a distributed database." It is a **correct, testable,
observable consensus core** plus the failure scenarios that prove it correct.

## 2. Why this project (learning goal)

The single genuine gap in the current skill map is **distributed systems under
failure**. Everything built so far — DocuQuery, Weir, Cairn, Vigilarch — assumes
either a single coordinating process or eventual-consistency semantics (CRDTs).
Raft is the other half of that space: *strong* consistency with a single elected
leader and an explicitly replicated, ordered log.

Concretely, by end of day you should be able to explain, from code you wrote:

- Why randomized election timeouts are required (and what happens without them)
- Why a leader may **never** commit an entry from a previous term by counting replicas alone
- Why the log-matching property makes AppendEntries consistency-checkable with just `(prevLogIndex, prevLogTerm)`
- Why a candidate with a shorter/staler log cannot win an election
- Why split-brain is impossible even though two nodes can simultaneously believe they are leader

If any of those five are still hand-wavy at 11pm, the project is not done.

## 3. Scope

### In scope (core — must ship)

| Area | Requirement |
|---|---|
| Leader election | RequestVote RPC, randomized timeouts, term-based voting, one-vote-per-term |
| Log replication | AppendEntries RPC, consistency check, follower log truncation, `nextIndex`/`matchIndex` tracking |
| Safety rules | Higher-term → step down; election restriction (up-to-date log check); commit-only-current-term rule |
| Commit advancement | Majority `matchIndex` → `commitIndex` → apply to a toy state machine |
| Failure injection | Drop / delay / reorder / partition / crash / restart, driven by a seeded RNG |
| Determinism | Same seed ⇒ same execution trace, every run |
| Invariant checker | Runs after every simulation step, fails loudly on violation |

### Explicitly cut (document these as cuts in the README)

- Real networking — no TCP, no gRPC, no sockets. In-process message queues only.
- Disk persistence — `currentTerm` / `votedFor` / `log[]` live in memory. A "crash" is a state reset with the persistent triple preserved; that is enough to exercise the algorithm.
- Log compaction / snapshotting / InstallSnapshot RPC
- Cluster membership changes (joint consensus) — fixed N at startup
- Client session semantics / linearizable read leases / exactly-once dedup
- Performance tuning — benchmarks measure algorithmic behaviour (election latency, rounds-to-commit), not throughput

Scope discipline is itself a deliverable here. A documented cut is worth as much
as a documented decision.

## 4. Language and stack

**Python 3.11+, standard library only** for the core. Rationale: the value of
this project is the algorithm, and Python gets you to the interesting failure
scenarios hours earlier than Rust would. Vigilarch already covers Rust on the
GitHub profile.

- Core: stdlib only (`dataclasses`, `enum`, `random`, `heapq`)
- Tests: `pytest` + `hypothesis` (property tests)
- Nothing async. The simulation is a **single-threaded discrete-event loop** with
  a logical clock. This is non-negotiable — real threads or asyncio destroy
  determinism and will eat the whole day in flaky-test debugging.

## 5. Architecture

```
        ┌─────────────────────────────────────────────┐
        │              Simulator (cluster.py)          │
        │  logical clock · seeded RNG · event queue    │
        └───────────────┬─────────────────────────────┘
                        │ tick(), deliver(msg)
        ┌───────────────▼─────────────────────────────┐
        │           Network (transport.py)             │
        │  in-flight queue · drop/delay/partition sets │
        └───────┬───────────┬───────────┬─────────────┘
                │           │           │
            ┌───▼───┐   ┌───▼───┐   ┌───▼───┐
            │ Node0 │   │ Node1 │   │ Node2 │      (node.py)
            └───┬───┘   └───┬───┘   └───┬───┘
                │           │           │
            ┌───▼───────────▼───────────▼───┐
            │  StateMachine (toy KV apply)   │
            └────────────────────────────────┘
                        │
        ┌───────────────▼─────────────────────────────┐
        │        Invariants (invariants.py)            │
        │  checked after EVERY simulator step          │
        └─────────────────────────────────────────────┘
```

**Key design decision:** `Node` is a *pure* state machine. Its RPC handlers take
a message and return `(new_state_effects, outbound_messages)`. It never calls the
network directly and never reads a wall clock. Time enters only via
`node.tick(now)`. This makes every test deterministic and makes the invariant
checker trivial to write.

## 6. Repo layout

```
raftlab/
├── src/raftlab/
│   ├── __init__.py
│   ├── messages.py      # RequestVote/AppendEntries req+resp dataclasses
│   ├── log.py           # LogEntry, RaftLog (append, truncate, match check)
│   ├── node.py          # RaftNode: Follower/Candidate/Leader state machine
│   ├── transport.py     # Network sim: partitions, drops, delays, reorder
│   ├── cluster.py       # Simulator: logical clock, event loop, crash/restart
│   ├── statemachine.py  # Toy KV applied from committed entries
│   └── invariants.py    # Safety property checks
├── tests/
│   ├── test_election.py
│   ├── test_replication.py
│   ├── test_partition.py
│   ├── test_safety.py
│   └── test_fuzz.py     # randomized/property-based chaos
├── benchmarks/
│   ├── bench_election.py
│   └── bench_commit.py
├── demo.py
├── README.md
└── pyproject.toml
```

## 7. Component specs

### 7.1 `messages.py`

```python
@dataclass(frozen=True)
class RequestVoteReq:
    term: int; candidate_id: int
    last_log_index: int; last_log_term: int

@dataclass(frozen=True)
class RequestVoteResp:
    term: int; vote_granted: bool

@dataclass(frozen=True)
class AppendEntriesReq:
    term: int; leader_id: int
    prev_log_index: int; prev_log_term: int
    entries: tuple[LogEntry, ...]      # empty tuple == heartbeat
    leader_commit: int

@dataclass(frozen=True)
class AppendEntriesResp:
    term: int; success: bool
    match_index: int    # follower's last matching index — avoids the O(n) decrement walk
```

All messages frozen. Immutability removes an entire class of simulation bugs.

### 7.2 `node.py` — required state

*Persistent (survives crash):* `current_term`, `voted_for`, `log`
*Volatile (all nodes):* `commit_index`, `last_applied`, `role`, `election_deadline`
*Volatile (leader only):* `next_index[]`, `match_index[]`

**Non-negotiable rules to implement literally** (write each as a named method or
a commented block — the README will reference them):

1. `R1` — any RPC with `term > current_term` ⇒ set term, `voted_for = None`, become Follower
2. `R2` — any RPC with `term < current_term` ⇒ reject, reply with `current_term`
3. `R3` — grant vote only if `voted_for in (None, candidate_id)` **and** candidate log is at least as up-to-date (compare `last_log_term` first, then `last_log_index`)
4. `R4` — AppendEntries succeeds only if the follower has an entry at `prev_log_index` with term `prev_log_term`
5. `R5` — on conflict, delete the follower's conflicting entry and everything after it, then append
6. `R6` — leader advances `commit_index` to `N` only if a majority has `match_index >= N` **and** `log[N].term == current_term`
7. `R7` — followers set `commit_index = min(leader_commit, last_new_entry_index)`

Rule `R6` is the one almost every from-scratch implementation gets wrong. Write
the test for it before the code (see §8, `test_no_stale_term_commit`).

### 7.3 `transport.py`

Must support, all seeded and reproducible:

- `partition(group_a, group_b)` / `heal()`
- `drop_rate(p)` — per-message probability
- `delay(min_ticks, max_ticks)` — causes natural reordering
- `isolate(node_id)` / `rejoin(node_id)`
- Message duplication (optional, one line, catches idempotency bugs)

### 7.4 `invariants.py` — checked after **every** step

| ID | Invariant |
|---|---|
| `I1` | **Election safety** — at most one leader per term |
| `I2` | **Leader append-only** — a leader never overwrites or deletes its own log entries |
| `I3` | **Log matching** — if two logs share an entry with the same index+term, all preceding entries are identical |
| `I4` | **Leader completeness** — an entry committed in term T is present in the log of every leader of term > T |
| `I5` | **State machine safety** — no two nodes apply a different command at the same log index |

`I5` is the one that turns this from a toy into a demonstrable proof. Keep an
append-only per-node apply history and diff them.

## 8. Test plan

Tests are the product here. Aim for **~20 tests, all deterministic, all seeded.**

**Election (`test_election.py`)**
- `test_single_leader_elected_from_cold_start`
- `test_split_vote_resolves_via_randomized_timeout` — force identical timeouts on tick 0, assert convergence within N terms
- `test_leader_reelected_after_leader_crash`
- `test_candidate_with_stale_log_cannot_win` — the election restriction
- `test_no_leader_in_minority_partition`

**Replication (`test_replication.py`)**
- `test_entry_replicated_to_all_followers`
- `test_entry_committed_after_majority_ack`
- `test_lagging_follower_catches_up_after_rejoin`
- `test_conflicting_follower_suffix_is_truncated`
- `test_heartbeat_prevents_election`

**Partition & safety (`test_partition.py`, `test_safety.py`)**
- `test_minority_leader_cannot_commit`
- `test_old_leader_steps_down_after_heal` ← **the headline test**
- `test_uncommitted_entries_from_old_leader_are_overwritten`
- `test_no_stale_term_commit` — construct the Raft §5.4.2 figure-8 scenario; assert `R6` prevents the bad commit
- `test_committed_entry_survives_n_minus_one_over_two_crashes`

**Chaos (`test_fuzz.py`)**
- `test_random_chaos_preserves_all_invariants` — 200 seeds × 500 ticks, random crashes/partitions/drops, assert `I1`–`I5` after every step. Print the failing seed on violation so any failure is replayable.

## 9. Benchmarks

Not throughput. Behavioural curves, plotted or tabulated in the README:

1. **Election latency vs timeout jitter** — sweep the randomization window, measure ticks-to-stable-leader over 100 seeds. Shows the split-vote pathology at low jitter.
2. **Time-to-commit vs drop rate** — 0%, 5%, 15%, 30% message loss.
3. **Availability vs cluster size** — 3, 5, 7 nodes: fraction of ticks with a stable leader while a random node is killed every K ticks.

Each should be one table in the README with a one-sentence reading of what it shows.

## 10. Demo (`demo.py`)

A single scripted scenario with narrated, timestamped console output:

```
t=000  cluster of 5 started, all followers
t=014  node2 times out → candidate, term 1
t=016  node2 elected LEADER (votes: 2,0,3)
t=020  client: SET x=1     → committed at index 1
t=030  ⚡ PARTITION  {2} | {0,1,3,4}
t=031  node2 still believes it is leader (term 1)
t=044  node0 elected LEADER, term 2
t=050  client: SET x=99    → committed at index 2 (majority side)
t=055  node2 accepts SET x=42 into its log — NOT committed (no majority)
t=070  ✓ HEAL
t=072  node2 sees term 2 > term 1 → steps down to FOLLOWER
t=074  node2 log conflict at index 2 → truncated, x=42 discarded
t=076  ✓ all 5 nodes agree: x=99.  I1–I5 hold.
```

This output *is* the README's hero section. It demonstrates split-brain
prevention in fifteen lines.

## 11. README outline

1. One-paragraph what/why
2. The demo transcript above, verbatim
3. **Safety properties and why they hold** — I1–I5, each with a two-sentence argument and a link to the rule (`R1`–`R7`) and test that enforces it
4. Architecture diagram + the "pure state machine + logical clock" decision and why
5. Benchmark tables with readings
6. **Explicit non-goals** — the §3 cut list, stated as deliberate scope, not omission
7. What I'd add next: persistence → snapshotting → membership changes → real transport
8. References: Ongaro & Ousterhout, *In Search of an Understandable Consensus Algorithm* (Raft extended paper), §5.2, §5.3, §5.4

The "why it holds" section in (3) is what distinguishes this from the thousands
of half-finished `raft-in-python` repos on GitHub. Most have code and no
argument. Write the argument.

## 12. Timeline

| Block | Target | Done when |
|---|---|---|
| 09:00–11:00 | Simulator skeleton: logical clock, event queue, transport, node roles, terms | 3 nodes tick, messages deliver, no election yet |
| 11:00–13:00 | Leader election (`R1`–`R3`) + election tests | Single leader from cold start; re-election after crash |
| 14:00–16:00 | Log replication + commit (`R4`–`R7`) + replication tests | Entry commits and applies on all nodes |
| 16:00–18:00 | Partitions, invariant checker, the headline split-brain test | `I1`–`I5` checked every step; figure-8 test passes |
| 18:00–19:30 | Fuzz harness + benchmarks | 200 seeds green; three benchmark tables produced |
| 19:30–21:00 | `demo.py` + README | Transcript reproduces; safety arguments written |

**Cut order if behind:** benchmarks → fuzz seeds (drop to 50) → message
duplication → delay/reorder. Never cut: the invariant checker, the figure-8
test, or the README safety arguments.

## 13. Done criteria

- [ ] `pytest` green, every test seeded and deterministic (run the suite twice, diff output)
- [ ] `I1`–`I5` asserted after every simulation step, not just at test end
- [ ] `test_no_stale_term_commit` passes — and fails if `R6`'s term check is removed (verify this by deleting the check and re-running; then restore it)
- [ ] `python demo.py` reproduces the transcript byte-for-byte from a fixed seed
- [ ] README explains all five safety properties in your own words
- [ ] Non-goals section written
- [ ] Repo pushed to `VampiricCyborg/raftlab`, description set, topics tagged

## 14. Risks

| Risk | Mitigation |
|---|---|
| **Rabbit-holing on real networking** | Hard rule: no sockets today. If you type `import socket`, stop. |
| **Flaky tests from real time/threads** | Logical clock only. No `time.sleep`, no threads, no asyncio. |
| **Getting `R6` subtly wrong and not noticing** | Write `test_no_stale_term_commit` from the paper's Figure 8 *before* writing commit logic. |
| **Running out of day before the README** | README's safety section is a done criterion, not a nice-to-have. Reserve 90 minutes. |
| **Scope creep into a KV database** | The state machine is a dict with `SET`. That's it. |

## 15. Resume line (draft — refine once shipped)

> **RaftLab** — Implemented the Raft consensus algorithm (leader election, log
> replication, and safety rules) over a deterministic discrete-event network
> simulator with injectable partitions and message loss; verified all five Raft
> safety invariants continuously under randomized chaos testing across 200 seeds.

Do not claim "production" or "distributed KV store." The honest framing is
stronger, and anyone who actually knows Raft will recognize the invariant
checking as the hard part.
