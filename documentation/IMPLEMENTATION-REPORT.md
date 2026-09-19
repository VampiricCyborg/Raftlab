# RaftLab: Implementation Report for Independent Verification

**Purpose of this document.** This is a complete account of what was built
against `documentation/RaftLab-BRIEF.md`, written so that a reviewer (human or
LLM) can check every claim. Each requirement from the brief is mapped to
concrete evidence: a file, a function, a test name, or a command with its
expected output. Deviations from the brief and unfinished items are listed
explicitly in §4 and §5. **Treat every claim here as unverified until you
check it against the code or by running the listed commands.**

- Repository: `https://github.com/VampiricCyborg/RaftLab` (branch `main`)
- Local path: `C:\Users\Madhav\PROJECTS\learning\RaftLab`
- Environment used: Windows 11, Python 3.13.1, uv 0.9.8, pytest 9.1.1, hypothesis 6.168.0
- Size: about 2,800 lines across `src/`, `tests/`, `benchmarks/` and `demo.py`
- Authorship: all code was written by Claude (the AI assistant). The original
  plan had the user write rules R1–R7 by hand; the user later chose to have
  the whole project completed for them. See §4.

---

## 1. Suggested verification procedure

Run from the repo root. Expected results are in brackets.

```bash
uv sync                                         # installs pytest + hypothesis
uv run pytest -q                                # [388 passed, ~12-15 s]
uv run pytest -q tests/test_safety.py -k stale_term   # [5 passed]
uv run python demo.py                           # [the 14-line transcript in README]
uv run python -m raftlab.chaos --seed 17        # [invariant checks passed: 581 steps; converged]
uv run python benchmarks/bench_election.py      # [table identical to README]
uv run python benchmarks/bench_commit.py        # [table identical to README]
uv run python benchmarks/bench_availability.py  # [table identical to README, ~15 s]
```

Checks worth doing by hand (these are what the brief cares most about):

1. **R6 done criterion.** In `src/raftlab/node.py`, `_advance_commit_index`,
   delete the two lines
   `if self.log.term_at(n) != self.current_term: break`. Run
   `uv run pytest -q tests/test_safety.py -k stale_term`: expect **5 failed**.
   Restore the lines: expect 5 passed.
2. **Invariants run after every step.** In `src/raftlab/cluster.py`,
   `Cluster.step()` calls every checker at the end of each tick, and
   `Cluster.__init__` installs `InvariantChecker()` by default whenever
   `checkers` is not passed. Only the benchmarks opt out (`checkers=[]`).
3. **Determinism.** Run the suite twice and diff the per-test results. Also run
   `PYTHONHASHSEED=1 uv run python -m raftlab.chaos --seed 17 --trace` and the
   same command with `PYTHONHASHSEED=999`: expect byte-identical output
   (1,279 lines). Both checks were done before this report was written.
4. **No forbidden machinery.**
   `grep -rnE "socket|thread|asyncio|time\.sleep|import time" src demo.py benchmarks`
   should find only the word "single-threaded" in a docstring in `cluster.py`.
5. **Core is stdlib-only.** `src/raftlab/*.py` imports only `argparse`,
   `collections`, `dataclasses`, `enum`, `heapq`, `random`, `sys`, `typing`
   and `raftlab.*`. `pyproject.toml` has `dependencies = []`, with pytest and
   hypothesis in the `dev` group.

---

## 2. What the system is

A Raft consensus core running as a **single-threaded discrete-event
simulation** inside one process. It has five parts:

- **Simulator (`cluster.py`).** An integer logical clock `now`.
  `Cluster.step()` runs one tick:
  1. `now += 1`
  2. Deliver every message due at `now`, in (deliver tick, send order).
     Messages addressed to crashed nodes are discarded.
  3. Call `tick(now)` on each live node in id order.
  4. Run the checkers.
  Every node call returns outbound messages, which the simulator hands to the
  network. Node events are collected into `cluster.trace` as `TraceEvent`s.
- **Network (`transport.py`).** A heap of in-flight `Envelope`s. Delay is
  uniform in [min, max] ticks, which causes reordering. It supports a drop
  probability, a duplicate probability, and partitions modelled as a set of
  blocked directed links. Links are checked both at send and at delivery, so a
  partition also destroys messages already in flight. Every random draw is
  made unconditionally, from a seeded `random.Random`.
- **Node (`node.py`).** Follower / Candidate / Leader. It never touches the
  network or a clock: `tick(now)` and `handle(src, msg, now)` mutate the node
  and return a list of `(destination, message)`.
- **State machine (`statemachine.py`).** A dict supporting `SET key=value`
  (plus `NOOP`). It keeps an append-only `history` of `(index, command)`
  applied, which survives restarts, for checking I5.
- **Invariant checker (`invariants.py`).** Checks I1–I5 after every step and
  raises `InvariantViolation`, whose message includes the tick and seed.

**Seeding.** The network uses `Random(f"net:{seed}")` and node *i* uses
`Random(f"node{i}:{seed}")`. String seeds are hashed with SHA-512 by
`random.seed`, so they are unaffected by `PYTHONHASHSEED`. Chaos runs add
`Random(f"chaos:{seed}")`.

**Crash model.** `Cluster.crash(i)` stops node *i* from receiving messages
or ticking. `Cluster.restart(i)` calls `RaftNode.restart`, which resets the
role, `commit_index`, `last_applied`, vote tally, `next_index`,
`match_index` and the KV dict, and keeps `current_term`, `voted_for` and
`log`.

**Defaults (`RaftConfig`).** Election timeout uniform in [10, 20] ticks,
heartbeat every 3 ticks, at most 8 entries per AppendEntries, network delay
1 tick, no drops or duplicates.

---

## 3. Requirement-by-requirement traceability

Status legend: **DONE**, **DONE (deviation)** = done with a difference
explained in §4, **NOT DONE**.

### 3.1 Brief §3: core scope

| Requirement | Status | Evidence |
|---|---|---|
| Leader election: RequestVote, randomized timeouts, term-based voting, one vote per term | DONE | `node.py`: `_on_election_timeout`, `_handle_request_vote`, `_handle_request_vote_resp`, `_become_leader`, `reset_election_timer`; tests in `test_election.py`, `test_rules_election.py` |
| Log replication: AppendEntries, consistency check, follower truncation, `nextIndex`/`matchIndex` | DONE | `node.py`: `_append_entries_for`, `_handle_append_entries`, `_handle_append_entries_resp`, `_log_matches`, `_append_new_entries`, `_conflict_hint` |
| Safety rules: step down on higher term; election restriction; commit only current term | DONE | R1 `_step_down_if_newer_term`, R3 `_candidate_log_is_up_to_date`, R6 `_advance_commit_index` |
| Commit advancement: majority `matchIndex` → `commitIndex` → apply | DONE | `_advance_commit_index`, `_follow_leader_commit`, `_apply_committed` |
| Failure injection: drop / delay / reorder / partition / crash / restart, seeded | DONE | `transport.py`: `drop_rate`, `delay`, `duplicate_rate`, `partition`, `isolate`, `rejoin`, `heal`; `cluster.py`: `crash`, `restart` |
| Determinism: same seed ⇒ same trace | DONE | `test_same_seed_same_trace`, `test_election_trace_is_deterministic`, `test_demo_is_deterministic`; cross-process trace diff (§1 check 3) |
| Invariant checker after every step, fails loudly | DONE | `invariants.py` `InvariantChecker.__call__`; installed by default in `Cluster.__init__`, invoked at the end of `Cluster.step` |

### 3.2 Brief §3: explicit cuts (should be documented in the README)

| Cut | Status | Evidence |
|---|---|---|
| No real networking | DONE | README "Explicit non-goals"; no socket imports (§1 check 4) |
| No disk persistence (crash = volatile reset, triple kept) | DONE | README; `RaftNode.restart`; `test_crashed_node_receives_nothing_and_restart_keeps_persistent_state` |
| No compaction / snapshots / InstallSnapshot | DONE | README non-goals |
| No membership changes | DONE | README non-goals; fixed `n` in `Cluster` |
| No client sessions / linearizable reads / exactly-once | DONE | README non-goals (also notes that retried commands can be duplicated, and that there's no leader no-op) |
| No performance tuning | DONE | README non-goals; benchmarks measure ticks |
| (extra) No PreVote | documented | README non-goals |

### 3.3 Brief §4: language and stack

| Requirement | Status | Evidence |
|---|---|---|
| Python 3.11+ | DONE (deviation) | `requires-python = ">=3.11"`, but only run on 3.13.1 (§4) |
| Core is stdlib only (`dataclasses`, `enum`, `random`, `heapq`) | DONE | §1 check 5; also uses `collections`, `typing`, `argparse`, `sys` |
| Tests: pytest + hypothesis property tests | DONE | `tests/`; Hypothesis in `test_fuzz.py::test_arbitrary_fault_schedules_preserve_invariants` (`derandomize=True`, 60 examples) |
| Nothing async; single-threaded discrete-event loop with a logical clock | DONE | `Cluster.step`; §1 check 4 |

### 3.4 Brief §5: architecture

| Requirement | Status | Evidence |
|---|---|---|
| Simulator / Network / Nodes / StateMachine / Invariants layering | DONE | files as named; diagram in README "Architecture" |
| Node is a pure state machine: handlers take a message and return `(new_state_effects, outbound_messages)`; never calls the network; never reads a clock; time only via `tick(now)` | DONE (deviation) | handlers mutate `self` and return only outbound messages (§4); no network or clock access holds; `now` is passed to both `tick` and `handle` |

### 3.5 Brief §6: repository layout

| Brief path | Status | Actual |
|---|---|---|
| `src/raftlab/{__init__,messages,log,node,transport,cluster,statemachine,invariants}.py` | DONE | all present |
| `tests/test_{election,replication,partition,safety,fuzz}.py` | DONE | all present |
| `benchmarks/bench_election.py`, `bench_commit.py` | DONE | present |
| `demo.py`, `README.md`, `pyproject.toml` | DONE | present |
| (extra) | n/a | `src/raftlab/chaos.py`, `src/raftlab/client.py`, `tests/helpers.py`, `tests/test_rules_election.py`, `tests/test_rules_replication.py`, `tests/test_simulator.py`, `tests/test_demo.py`, `benchmarks/bench_availability.py`, `benchmarks/_common.py`, `documentation/raft-primer.md`, `uv.lock`, `.gitignore` |
| Top-level folder named `raftlab/` | DONE (deviation) | the repo root *is* the project folder (`RaftLab/`), with no nested `raftlab/` directory |

### 3.6 Brief §7.1: messages

| Requirement | Status | Evidence |
|---|---|---|
| `RequestVoteReq(term, candidate_id, last_log_index, last_log_term)` | DONE | `messages.py` |
| `RequestVoteResp(term, vote_granted)` | DONE | `messages.py` |
| `AppendEntriesReq(term, leader_id, prev_log_index, prev_log_term, entries: tuple[LogEntry,...], leader_commit)`, empty tuple = heartbeat | DONE | `messages.py` |
| `AppendEntriesResp(term, success, match_index)` | DONE (deviation) | on success `match_index` = last index covered by the request; on failure it is a retry *hint* (§4) |
| All messages frozen | DONE | `@dataclass(frozen=True)` on all four; `LogEntry` also frozen |

### 3.7 Brief §7.2: node state and rules R1–R7

State: persistent `current_term`, `voted_for`, `log`; volatile
`commit_index`, `last_applied`, `role`, `election_deadline`; leader
`next_index`, `match_index`. All present in `RaftNode.__init__` /
`_reset_volatile`. **DONE.**

| Rule | Method (`node.py`) | Unit tests | Cluster-level tests |
|---|---|---|---|
| R1 higher term ⇒ adopt, clear vote, follower | `_step_down_if_newer_term` (called first in `handle`) | `test_r1_*` (4) | `test_old_leader_steps_down_after_heal` |
| R2 lower term ⇒ reject with `current_term` | `_reject_stale` (called in `handle` after R1; stale responses get no reply) | `test_r2_*` (3) | via partition tests |
| R3 vote once per term + up-to-date log (last term, then index) | `_should_grant_vote`, `_candidate_log_is_up_to_date` | `test_r3_*` (8) | `test_candidate_with_stale_log_cannot_win`, `test_restarted_node_does_not_vote_twice_in_one_term` |
| R4 AE succeeds only if entry at `prev_log_index` has `prev_log_term` | `_log_matches` | `test_r4_*` (3) | `test_conflicting_follower_suffix_is_truncated` |
| R5 delete conflicting entry and all after, then append | `_append_new_entries` (truncates only on same index + different term; asserts it never truncates a committed index) | `test_r5_*` (3) | `test_uncommitted_entries_from_old_leader_are_overwritten` |
| R6 commit N only with majority `match_index ≥ N` **and** `log[N].term == current_term` | `_advance_commit_index` | `test_r6_*` (4) | `test_no_stale_term_commit`, `test_figure8_without_r6_term_check_loses_a_committed_entry` |
| R7 follower `commit_index = min(leader_commit, last_new_entry_index)` | `_follow_leader_commit` (never lowers `commit_index`) | `test_r7_*` (2) | `test_entry_replicated_to_all_followers` |

Also in the node: `handle()` asserts `msg.term == current_term` after
R1/R2 as a guard; duplicate votes are counted once per voter; `match_index`
and `next_index` never go backwards on reordered replies; a deposed leader's
election timer is reset in `_become_follower`; batches are capped at
`max_entries_per_message` and the next batch is pipelined immediately on
success.

### 3.8 Brief §7.3: transport

| Requirement | Status | Evidence |
|---|---|---|
| `partition(group_a, group_b)` / `heal()` | DONE (deviation) | `partition(*groups)` accepts two or more groups, which must cover every node exactly once (raises `ValueError` otherwise); `heal()` clears all blocks |
| `drop_rate(p)` | DONE | `Network.drop_rate`; `test_drop_rate_extremes_and_seeded_pattern` |
| `delay(min_ticks, max_ticks)` causing reordering | DONE | `Network.delay`; `test_random_delay_reorders_messages`, `test_message_arrives_after_its_delay` |
| `isolate(node_id)` / `rejoin(node_id)` | DONE | `test_isolate_rejoin_and_heal` |
| Message duplication (optional) | DONE | `Network.duplicate_rate`; `test_duplication_delivers_copies` |
| All seeded and reproducible | DONE | see seeding in §2 |

### 3.9 Brief §7.4: invariants, checked after every step

| ID | Check (`invariants.py`) | How it works |
|---|---|---|
| I1 election safety | `check_election_safety` | remembers the first leader of each term over the whole run and flags any other node that later leads that term |
| I2 leader append-only | `check_leader_append_only` | while a node stays leader in the same term, its previous-step log must be a prefix of its current log |
| I3 log matching | `check_log_matching` | for every pair of nodes (crashed included), finds the highest index where terms agree and requires identical prefixes up to it |
| I4 leader completeness | `check_leader_completeness` | records every entry observed committed on any live node, with the observer's term; every leader of a strictly higher term must hold that entry; two different entries committed at one index also fail |
| I5 state machine safety | `check_state_machine_safety` | diffs every node's append-only apply history against the first command applied at each index |

Status: **DONE.** Evidence that the checker is not vacuous:
`test_figure8_without_r6_term_check_loses_a_committed_entry` expects an
`InvariantViolation` matching "I4 leader completeness"; §6 lists the
mutations it catches.

### 3.10 Brief §8: test plan (brief-named tests)

All 16 test names in the brief exist:

| Brief test | File | Seeds / cases |
|---|---|---|
| `test_single_leader_elected_from_cold_start` | test_election.py | 10 |
| `test_split_vote_resolves_via_randomized_timeout` | test_election.py | 10 (all deadlines forced to tick 1; asserts no leader in term 1, then a leader by term ≤ 4) |
| `test_leader_reelected_after_leader_crash` | test_election.py | 10 |
| `test_candidate_with_stale_log_cannot_win` | test_election.py | 10 |
| `test_no_leader_in_minority_partition` | test_election.py | 10 |
| `test_entry_replicated_to_all_followers` | test_replication.py | 5 |
| `test_entry_committed_after_majority_ack` | test_replication.py | 5 |
| `test_lagging_follower_catches_up_after_rejoin` | test_replication.py | 5 |
| `test_conflicting_follower_suffix_is_truncated` | test_replication.py | 5 |
| `test_heartbeat_prevents_election` | test_replication.py | 5 |
| `test_minority_leader_cannot_commit` | test_partition.py | 10 |
| `test_old_leader_steps_down_after_heal` (headline) | test_partition.py | 10 |
| `test_uncommitted_entries_from_old_leader_are_overwritten` | test_partition.py | 10 |
| `test_no_stale_term_commit` (Figure 8) | test_safety.py | 5 |
| `test_committed_entry_survives_n_minus_one_over_two_crashes` | test_safety.py | 30 (n = 3, 5, 7 × 10 seeds) |
| `test_random_chaos_preserves_all_invariants` | test_fuzz.py | 200 seeds × 500 ticks |

Additional tests beyond the brief (48 functions): 12 simulator/transport/log
tests, 15 R1–R3 unit tests, 12 R4–R7 unit tests,
`test_split_vote_never_resolves_without_jitter`,
`test_election_trace_is_deterministic`,
`test_replication_survives_lossy_reordering_network`,
`test_figure8_without_r6_term_check_loses_a_committed_entry`,
`test_restarted_node_does_not_vote_twice_in_one_term`, the Hypothesis
schedule test, and 3 demo tests.

**Totals: 64 test functions, 388 collected cases. All pass.** The brief
aimed for "~20 tests, all deterministic, all seeded". Every cluster is seeded;
Hypothesis is run with `derandomize=True`.

**Fuzz specifics (`raftlab/chaos.py`, class `Chaos`).**
- Per-seed network settings: drop rate from {0, 5, 15, 30}%, delay window
  within [1, 7], duplication from {0, 5}%, and a timeout window of [10, 12]
  or [10, 20].
- Each tick, with small probabilities: crash, restart, "bounce" (crash, then
  restart 1–5 ticks later), random two-way partition, isolate one node, heal.
- Client writes are sent to a randomly chosen node that currently *claims*
  leadership, stale leaders included.
- After 500 ticks, `recover()` heals the network, restarts all nodes, stops
  drops, and requires convergence (one leader, identical logs, all applied)
  within 1000 ticks.
- The failing seed appears in the pytest id and the `InvariantViolation`
  message, and replays with `python -m raftlab.chaos --seed N --trace`.

### 3.11 Brief §9: benchmarks

| Brief benchmark | Status | Script | README table |
|---|---|---|---|
| Election latency vs timeout jitter, 100 seeds | DONE | `bench_election.py` (windows [10,10] … [10,50]; ticks to first leader, mean terms, split-vote %, no-leader %) | yes, with reading |
| Time-to-commit vs drop rate 0/5/15/30% | DONE | `bench_commit.py` (100 seeds × 20 sequential commands, retrying client; median/p95/max ticks, elections per 1000 ticks) | yes, with reading |
| Availability vs cluster size 3/5/7 with a node killed every K ticks | DONE (deviation) | `bench_availability.py` (separate file, not in the brief's layout; 50 seeds × 3000 ticks; K = 50/100/200; one node down at a time) | yes, with reading |

"Stable leader" means a live leader whose term and identity are recognized
by a majority of the cluster (`has_stable_leader` in the script).

Key numbers, as stated in the README:
- Zero jitter: 100% of seeds never elect a leader in 2000 ticks.
- [10, 11]: 23% split votes. [10, 20]: 0%, median 13 ticks.
- Drop rate 30%: median commit 5 ticks, max 55, 3.7 elections per 1000 ticks
  (0 at ≤ 15%).
- Availability at K = 50: 85.67% (3 nodes), 92.60% (5 nodes), 95.68% (7 nodes).

### 3.12 Brief §10: demo

| Requirement | Status | Evidence |
|---|---|---|
| Single scripted scenario, narrated, timestamped | DONE | `demo.py` (`run_demo`, `Narrator`) |
| Story: elect → commit x=1 → partition leader alone → it still believes it leads → new leader → commit x=99 on majority → old leader accepts x=42 uncommitted → heal → step down → truncate x=42 → all agree x=99, I1–I5 hold | DONE | seed 3 output; `test_demo_story_holds_for_other_seeds` checks the story for seeds 0–9 |
| Reproduces byte-for-byte from a fixed seed | DONE (deviation) | tick numbers and vote lists differ from the brief's illustrative sample (agreed with the user); `test_readme_transcript_matches_demo_byte_for_byte` ties README to output |

Actual output (seed 3):

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

### 3.13 Brief §11: README outline

| Item | Status |
|---|---|
| 1. One-paragraph what/why | DONE |
| 2. Demo transcript verbatim | DONE (test-enforced) |
| 3. I1–I5, each with an argument, linked to rules and tests | DONE ("Safety properties, and why they hold"; rule table R1–R7 with method names) |
| 4. Architecture diagram + pure-state-machine/logical-clock decision | DONE |
| 5. Benchmark tables with readings | DONE (3 tables) |
| 6. Explicit non-goals | DONE |
| 7. What's next: persistence → snapshotting → membership → real transport | DONE (in that order) |
| 8. References: Raft extended paper §5.2, §5.3, §5.4 | DONE |
| (extra) "The five questions this project answers", testing/mutation section, layout, quickstart | present |

### 3.14 Brief §12: timeline

The work was done as six blocks matching the brief's timeline, one commit
each (`608d3c4` skeleton, `89d5719` election, `feac917` replication,
`866cc75` invariants/partitions/safety, `475ca78` fuzz/benchmarks, `ef7d371`
demo/README). Wall-clock times were not tracked. Nothing on the brief's
cut-order list was cut.

### 3.15 Brief §13: done criteria

| Criterion | Status | Evidence |
|---|---|---|
| `pytest` green, every test seeded and deterministic (run twice, diff) | DONE | 388 passed; two runs diffed identical; cross-process trace diff identical |
| I1–I5 asserted after every simulation step | DONE | §1 check 2 |
| `test_no_stale_term_commit` passes, and fails if R6's term check is removed | DONE | verified: 5 failed without the check, 5 passed with it (§1 check 1) |
| `python demo.py` reproduces the transcript byte-for-byte from a fixed seed | DONE | `test_readme_transcript_matches_demo_byte_for_byte`, `test_demo_is_deterministic` |
| README explains all five safety properties "in your own words" | DONE (deviation) | written by Claude, not by the user (§4) |
| Non-goals section written | DONE | README |
| Repo pushed to `VampiricCyborg/raftlab`, description set, topics tagged | **PARTIAL** | pushed to `VampiricCyborg/RaftLab` (capitalization kept deliberately, §5); **description and topics NOT set**, because `gh` is not authenticated (§5) |

### 3.16 Brief §14: risk mitigations

| Risk / mitigation | Status |
|---|---|
| No `import socket` | DONE (§1 check 4) |
| Logical clock only; no `time.sleep`, threads, asyncio | DONE |
| Write the Figure 8 test *before* the commit logic | **NOT followed as a process step.** R6 and its unit tests (`test_r6_*`) were written together in Block 3; the full Figure 8 cluster test came in Block 4. The outcome criterion (the test fails without the check) is verified. |
| Reserve time for the README safety section | DONE (section exists) |
| No scope creep into a KV database | DONE (`SET` and `NOOP` only) |

### 3.17 Brief §2: learning goals (the five "whys")

The README section "The five questions this project answers" gives each
answer and points to a test or benchmark:

1. Randomized timeouts: `test_split_vote_never_resolves_without_jitter` and benchmark 1.
2. No previous-term commit by counting: `test_no_stale_term_commit` and the naive-R6 test.
3. `(prevLogIndex, prevLogTerm)` suffices: the I3 argument, R4.
4. Stale candidate can't win: the I4 argument, `test_candidate_with_stale_log_cannot_win`.
5. Split brain impossible: demo, `test_old_leader_steps_down_after_heal`.

Whether the *user* can explain these without notes is a human criterion
that code cannot verify (§4).

---

## 4. Deviations from the brief

1. **Authorship and learning goal.** The brief intends the user to be able to
   explain the rules "from code you wrote". All code, including R1–R7 and the
   README safety arguments, was written by Claude after the user chose to have
   the project completed for them. The criterion "README explains all five
   safety properties in your own words" is therefore met in content but not in
   authorship.
2. **Node return shape.** The brief says handlers return
   `(new_state_effects, outbound_messages)`. The implementation mutates the
   node in place and returns only outbound messages. The underlying purpose
   (no network or clock access, time only via arguments) holds.
3. **`AppendEntriesResp.match_index` on failure** is a retry hint: the
   follower's last index if its log is too short, otherwise the index just
   before the first entry of the conflicting term. The leader clamps
   `next_index` to `≥ match_index[peer] + 1` and R4 re-validates every retry,
   so the hint only affects speed.
4. **Batching, which the brief doesn't mention.** At most 8 entries per
   AppendEntries, with pipelining. It was added because without it R7's cap
   is unreachable (a leader always sent every entry through its last index),
   so a mutant removing the cap went undetected.
5. **`partition(*groups)`** generalizes `partition(group_a, group_b)` and
   requires every node to be listed.
6. **Demo transcript** follows the brief's story but with real tick numbers
   and votes from seed 3 (14 lines rather than "fifteen").
7. **Extra files** listed in §3.5, including a third benchmark script and
   package modules `chaos.py` (replayable chaos CLI) and `client.py`
   (retrying client shared by tests, benchmarks and demo).
8. **Python version.** `requires-python >= 3.11`, but only Python 3.13.1 was
   run. The code uses `match` statements and `X | Y` union syntax, both
   3.10+, so 3.11 should work, but it is untested.
9. **Figure 8 test ordering** (§3.16).
10. **Split-vote test** forces identical deadlines at tick 1 rather than
    "tick 0", because the first `step()` is tick 1.

---

## 5. Not done / open items

1. **GitHub description and topics are not set.** This requires
   `gh auth login` and then, for example,
   `gh repo edit VampiricCyborg/RaftLab --description "..." --add-topic raft,consensus,distributed-systems,python,simulation`.
2. **Repository name capitalization (decided, not a defect).** The brief says
   `raftlab`; the repo is `RaftLab`. The owner chose to keep it. GitHub URLs
   are case-insensitive and the Python package is `raftlab` either way.
3. **The mutation-testing script is not in the repo.** It lived in a temporary
   directory. §6 describes each mutation precisely so it can be reproduced by
   hand.
4. **Not tested on Python 3.11 or 3.12**, and not tested on Linux or macOS.

---

## 6. Mutation testing (evidence the tests can fail)

Each change below was applied alone to `src/raftlab/node.py`, then
`tests/test_fuzz.py` alone and the full suite were run.

| # | Mutation (exact change) | Fuzz alone | Full suite |
|---|---|---|---|
| 1 | R1: remove `self.voted_for = None` in `_step_down_if_newer_term` | caught | caught |
| 2 | R3: remove the `if already_voted_for_other: return False` check | caught (I1) | caught (I1) |
| 3 | R3: `_should_grant_vote` returns True instead of calling the up-to-date check | caught (I4) | caught (`test_candidate_with_stale_log_cannot_win`, all 10 seeds) |
| 4 | R4: `_log_matches` returns `term_at(prev) is not None` | caught (I3) | caught |
| 5 | R5: never `continue` on a matching term (always truncate and re-append) | caught | caught |
| 6 | R6: delete the current-term check in `_advance_commit_index` | caught (I4, seed 181) | caught (`test_r6_*`, `test_no_stale_term_commit`) |
| 7 | R7: `target = min(leader_commit, self.log.last_index)` | caught (I4) | caught (`test_r7_commit_is_capped_at_last_new_entry`) |
| 8 | `restart()` also sets `voted_for = None` | **not caught** by fuzz (~1 in 1000 chaos seeds would catch it) | caught (`test_restarted_node_does_not_vote_twice_in_one_term`, `test_crashed_node_receives_nothing_and_restart_keeps_persistent_state`) |

Before the batching and chaos changes, the fuzzer missed mutations 6, 7 and
8. After adding batching, bounce faults, and tight-jitter seeds, it catches 6
and 7. Mutation 6 was also run with 2,000 seeds under the old chaos settings
(0 caught) and 1,000 seeds under the new ones (1 caught, seed 181, which is
inside the suite's 200).

---

## 7. Places a reviewer should scrutinize

These are judgment calls or weaker spots, not known bugs:

1. **I4 is lenient by design.** The commit term recorded is the observer's
   `current_term` when the commit is first seen, which is ≥ the true commit
   term. The check then applies only to leaders of a strictly higher term.
   This avoids false alarms but could miss a violation involving a leader
   whose term falls between the true and observed commit terms. Since the
   checker runs every step, the observer is usually the committing leader in
   the same step.
2. **I2 compares only consecutive steps** while the node stays leader in the
   same term. A rewrite followed by a restore within one step would be missed,
   but a node call can't do both within one step in practice.
3. **The Figure 8 test manipulates internals.** `figure8_cluster` sets
   `current_term`, `voted_for` and logs directly, and makes node0 leader via
   `leader._become_leader(...)` plus `cluster._after_node_call(...)`.
   `play_figure8` holds nodes 1–3's election deadlines back until node4
   leads, so that the paper's exact interleaving happens. Worth confirming
   that the scenario matches paper Figure 8 (a)–(d).
4. **Production-code asserts.** `handle()` asserts the message term equals
   `current_term` after R1/R2, and `_append_new_entries` asserts it never
   truncates at or below `commit_index`. They are checks, not control flow;
   running with `python -O` would skip them.
5. **No leader no-op on election.** Previous-term entries commit only when a
   new current-term entry commits. `Chaos.recover()` submits a `NOOP` to force
   this. Confirm this doesn't hide liveness issues.
6. **Retrying client duplicates.** `client.commit` resubmits when the leader
   holding its copy is deposed, so the same command can appear at two
   indexes. Safe because `SET` is idempotent; documented as a non-goal.
7. **The benchmarks run without invariant checks** (`checkers=[]`) for speed.
   Their correctness relies on the same code the tests check.
8. **The 200-seed fuzz ran in 4–14 s.** Logs stay small (tens to ~100
   entries); the I3 pairwise check is O(nodes² × log length) per step.

---

## 8. File index

| File | Contents |
|---|---|
| `src/raftlab/messages.py` | 4 frozen RPC dataclasses; `Message` union |
| `src/raftlab/log.py` | `LogEntry`, `RaftLog` (1-indexed; `term_at(0) == 0`; `entry`, `entries_from`, `append`, `truncate_from`) |
| `src/raftlab/node.py` | `RaftConfig`, `Role`, `NotLeader`, `RaftNode` (R1–R7, election, replication, `propose`, `restart`) |
| `src/raftlab/transport.py` | `Envelope`, `Network` (fault knobs, `send`, `deliver_due`, `stats`) |
| `src/raftlab/cluster.py` | `Cluster` (`step`, `run`, `run_until`, `submit`, `is_committed`, `crash`, `restart`, `partition`, `isolate`, `rejoin`, `heal`, `leaders`, `converged`), `TraceEvent` |
| `src/raftlab/statemachine.py` | `KVStateMachine` (`apply`, `reset`, `history`) |
| `src/raftlab/invariants.py` | `InvariantChecker` (I1–I5), `InvariantViolation` |
| `src/raftlab/chaos.py` | `Chaos` (`run`, `recover`), CLI `python -m raftlab.chaos --seed N [--trace]` |
| `src/raftlab/client.py` | `commit()` retrying client, `CommitTimeout` |
| `tests/*.py` | see §3.10 |
| `benchmarks/*.py` | see §3.11 |
| `demo.py` | narrated split-brain scenario, `SEED = 3`, `--seed` option |
| `documentation/RaftLab-BRIEF.md` | the original brief |
| `documentation/raft-primer.md` | Raft primer mapped to R1–R7 / I1–I5 |
| `README.md` | project documentation (sections listed in §3.13) |
