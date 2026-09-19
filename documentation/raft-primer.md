# Raft Primer (read before Block 2)

A compact tour of Raft, mapped onto the rule IDs (`R1`–`R7`) and invariant IDs
(`I1`–`I5`) from the brief. The source of truth is the extended Raft paper
(Ongaro & Ousterhout), §5. Figure 2 of that paper is a one-page summary of
everything below; keep it open while coding.

---

## 1. The problem

We have N servers. Clients send commands (`SET x=1`). We want every server to
apply **the same commands in the same order**, so every copy of the state
machine ends up identical — even while servers crash and the network drops,
delays, and partitions messages.

Raft solves this by keeping a **replicated log**: an ordered list of commands.
If every server's log agrees on the prefix it has applied, the state machines
agree.

**The one fact everything rests on:** a *majority* is `N // 2 + 1`. Any two
majorities of the same cluster share at least one server. In a 5-node cluster,
majorities are 3 nodes; two groups of 3 out of 5 must overlap. Raft tolerates
`(N - 1) // 2` failures (2 of 5) because a majority can still be formed.

Every safety argument in this project ends with "...and because the two
majorities overlap, some server saw both, so the bad thing can't happen."

---

## 2. Terms — Raft's logical clock

Time is divided into **terms**: 1, 2, 3, ... Each term starts with an election.
If the election succeeds, that leader rules for the rest of the term. If it
fails (split vote), a new term starts.

- Every server stores `current_term`. It only ever increases.
- **Every message carries the sender's term.**
- `R1`: if you receive any message with `term > current_term`, you are out of
  date. Adopt the higher term, clear `voted_for`, become Follower.
- `R2`: if you receive a message with `term < current_term`, the sender is out
  of date. Reject it and reply with your `current_term` (which, via `R1`,
  makes the sender step down).

Terms let servers detect stale information without synchronized clocks. A
leader from term 3 is powerless once a majority has moved to term 4.

---

## 3. Roles

```
                 times out,                 receives votes from
                 starts election            majority of servers
  ┌──────────┐ ─────────────────▶ ┌───────────┐ ────────────────▶ ┌────────┐
  │ Follower │                    │ Candidate │                   │ Leader │
  └──────────┘ ◀───────────────── └───────────┘                   └────────┘
       ▲        discovers current leader │  ▲ times out, new election   │
       │        or higher term           └──┘                           │
       └──────────────────────────────────────────────────────────────── ┘
                           discovers server with higher term (R1)
```

- **Follower**: passive. Answers RPCs. If it hears nothing from a leader for
  an *election timeout*, it becomes a candidate.
- **Candidate**: trying to become leader for a new term.
- **Leader**: accepts client commands, replicates them, sends heartbeats.

---

## 4. Leader election (`R1`–`R3`, invariant `I1`)

When a follower's election timer expires it:

1. increments `current_term`,
2. becomes Candidate and votes for itself (`voted_for = self`),
3. resets its election timer,
4. sends `RequestVote(term, candidate_id, last_log_index, last_log_term)` to
   everyone.

A server grants its vote (`R3`) only if **both**:

- it has not already voted for someone else in this term
  (`voted_for in (None, candidate_id)`), and
- the candidate's log is **at least as up-to-date** as its own: compare the
  *last entry's term* first (higher wins); if equal, compare *last index*
  (longer wins, ties OK).

A candidate that collects votes from a majority (itself included) becomes
Leader. If it hears an AppendEntries from a leader of the same or higher term,
it becomes Follower. If its timer expires again, it starts a fresh election in
the next term.

**Why at most one leader per term (`I1`)?** Each server votes at most once per
term, and `voted_for` survives crashes (it's in the persistent triple). Winning
needs a majority; two majorities overlap; the overlapping server can't have
voted for both. Done.

**Why randomized timeouts?** If all followers time out at the same tick, they
all become candidates, all vote for themselves, nobody gets a majority, and
they all time out again at the same tick — forever. Randomizing the timeout
(e.g. uniform in 10–20 ticks) means one server usually times out first, wins,
and starts heartbeating before the others wake up. Benchmark 1 will measure
this directly.

---

## 5. Log replication (`R4`, `R5`, `R7`)

A log entry is `(term, command)` at a 1-based index. The leader appends client
commands to its own log, then sends each follower:

```
AppendEntries(term, leader_id,
              prev_log_index, prev_log_term,   # "the entry just before these"
              entries,                         # empty = heartbeat
              leader_commit)
```

The follower:

- `R4`: accepts only if its own log has an entry at `prev_log_index` whose
  term is `prev_log_term`. Otherwise it says "no", and the leader backs up and
  retries from earlier.
- `R5`: for each new entry, if the follower already has an entry at that index
  with a *different* term, that's a conflict: delete it and everything after
  it, then append. (If the term matches, it's the same entry — keep it. Don't
  truncate blindly: a delayed, older AppendEntries must not erase newer
  entries.)
- `R7`: sets `commit_index = min(leader_commit, index of last new entry)`.

The leader tracks, per follower:

- `next_index[f]`: the next entry to send to `f` (optimistic; starts at
  `leader.last_index + 1`).
- `match_index[f]`: the highest index known to be replicated on `f`
  (pessimistic; starts at 0).

**Log Matching property (`I3`):** if two logs have an entry with the same index
and term, then the logs are identical up to and including that entry. Why:

1. A leader creates at most one entry per index per term, and never changes it
   (leader append-only, `I2`). So `(index, term)` identifies a unique command.
2. By induction: a follower only appends after `R4` confirms it matches the
   leader at `prev_log_index`. So whenever an entry is added, the prefix before
   it already matches the leader.

That's why a single `(prev_log_index, prev_log_term)` pair is enough to verify
an entire prefix — the consistency check is O(1), not O(log length).

---

## 6. Commitment (`R6`, invariants `I4`, `I5`)

An entry is **committed** once it is safe to apply to the state machine:
Raft guarantees that it will never be removed from any future leader's log.
Every server applies committed entries in index order (`last_applied` catches
up to `commit_index`).

The obvious rule, "committed once it's on a majority", is **wrong**.

### Figure 8 — why `R6` exists (paper §5.4.2)

Five servers S1–S5. Notation: `2` means "an entry from term 2".

```
       index:  1  2  3
(a) S1 leader term 2, replicates index 2 only to S2.
    S1: 1  2
    S2: 1  2
    S3: 1
    S4: 1
    S5: 1
(b) S1 crashes. S5 wins term 3 (votes S3, S4, S5), appends index 2 (term 3)
    locally only.
    S5: 1  3
(c) S5 crashes. S1 restarts and wins term 4 (votes S1, S2, S3). It continues
    replicating its old term-2 entry, so now S1, S2, S3 all have index 2 = term 2.
    That's a majority! Can S1 call index 2 committed?
    S1: 1  2  4
    S2: 1  2
    S3: 1  2
(d) If it did: S1 crashes. S5 restarts and runs for term 5. Its last log term is
    3, which beats S2/S3/S4 (last term 2 or 1), so they vote for it (R3 is
    satisfied). S5 becomes leader and overwrites index 2 on everyone with its
    term-3 entry. A "committed" entry just vanished. State machine safety broken.
```

**`R6`:** the leader only advances `commit_index` to `N` if a majority has
`match_index >= N` **and** `log[N].term == current_term`. Old-term entries are
never committed by counting replicas; they get committed *indirectly*, when an
entry from the current term after them commits (by Log Matching, everything
before it is committed too).

In scenario (c), S1 must first get its term-4 entry at index 3 onto a
majority. Once that happens, S5 (last term 3) can no longer win: a majority has
last term 4, and they refuse to vote for it.

### Leader completeness (`I4`) and why a stale log can't win

A committed entry lives on a majority. A candidate needs votes from a majority.
The two majorities overlap, so at least one voter has the committed entry, and
`R3` makes that voter refuse any candidate whose log is less up-to-date than its
own. So every elected leader already holds every committed entry. With `R6`,
"committed" really means "on a majority *and* protected by the up-to-date check".

### State machine safety (`I5`)

With leader completeness plus Log Matching, no two servers ever apply different
commands at the same index. `I5` is the end-to-end property the other four
exist to protect, which is why our checker records every `(index, command)`
each node applies and compares them.

---

## 7. Split brain: why two "leaders" is harmless

After a partition `{2} | {0,1,3,4}`, node 2 still thinks it's leader of
term 1. The majority side elects node 0 in term 2. For a while, **two nodes
believe they are leader**. That's allowed. What's forbidden is two leaders *in
the same term* (`I1`), and a minority leader committing anything:

- Node 2 can append client commands to its own log, but it can only reach 1 of
  5 servers, so its entries never reach a majority and never commit.
- When the partition heals, node 2's next message reaches a term-2 server,
  which rejects it (`R2`) and replies with term 2. Node 2 sees the higher term,
  steps down (`R1`), and the new leader's AppendEntries truncate its
  uncommitted entries (`R5`).

---

## 8. Cheat sheet: concept → rule → invariant → test

| Concept | Rules | Invariant | Headline test |
|---|---|---|---|
| Terms detect stale nodes | R1, R2 | — | `test_old_leader_steps_down_after_heal` |
| One vote per term | R3 (first half) | I1 election safety | `test_single_leader_elected_from_cold_start` |
| Up-to-date vote check | R3 (second half) | I4 leader completeness | `test_candidate_with_stale_log_cannot_win` |
| Consistency check | R4 | I3 log matching | `test_conflicting_follower_suffix_is_truncated` |
| Conflict truncation | R5 | I3 | `test_uncommitted_entries_from_old_leader_are_overwritten` |
| Current-term commit only | R6 | I4, I5 | `test_no_stale_term_commit` |
| Follower commit | R7 | I5 | `test_entry_replicated_to_all_followers` |
| Leader never rewrites itself | (by construction) | I2 leader append-only | fuzz |

---

## 9. How our simulator models all this

- **Time** is an integer `now` that only moves when `Cluster.step()` runs. One
  step = deliver every message due at this tick, then `tick(now)` every live
  node. No wall clock, no threads.
- **Nodes are pure-ish state machines.** `node.handle(src, msg, now)` and
  `node.tick(now)` update the node's own fields and *return* the messages to
  send. They never touch the network.
- **The network** holds in-flight messages in a heap keyed by delivery tick.
  Random delays cause reordering; drops, duplicates, and partitions are applied
  with a seeded RNG.
- **A crash** stops a node from receiving or ticking. **A restart** wipes
  volatile state (role, `commit_index`, leader bookkeeping, the KV dict) but
  keeps `current_term`, `voted_for`, and `log`, which is exactly what real Raft
  would have on disk.
- **Same seed ⇒ same run.** Every source of randomness is a `random.Random`
  seeded from the cluster seed.
