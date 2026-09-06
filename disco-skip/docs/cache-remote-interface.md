# Disco-Skip: cache/remote interface

**What this is.** A design input from the compute-side local cache, describing what the
cache now assumes about the remote skip vector and what it needs supplied. Written to be
a starting point for the remote/RDMA side of the design.

**How to treat it.**

- Sections marked **Settled** describe behaviour that already exists in
  `include/skipvector_disco.h` and passes its structural `verify()` across 1–32 threads.
  Treat those as facts about the other side of the interface.
- Sections marked **Proposed** are one side's suggestion and have not been agreed.
- Items **A1–A8** are **open questions, not requirements.** Where this document says
  "assumed", that is an assumption that may be wrong. Do not resolve them by picking an
  option and implementing it — surface them for a decision between the two authors.
- The cache implementation (`include/skipvector_disco.h`, `include/test.cc`) is the
  other author's half of the repo. Read it freely; don't modify it from this side.

---

## 0. System context — Settled

A disaggregated-memory key/value store.

- **Memory servers are passive.** They run no server-side logic. Compute nodes operate on
  the data structure entirely through **one-sided RDMA** — reads, writes, and CAS.
- **The data structure is a skip vector** living on the memory servers: a chunked
  skip-list variant that flattens each layer into vectors, trading pointer-chasing for
  spatial locality. That matters more here than in shared memory, because each pointer hop
  is a network round trip.
- **Three-way replication** for fault tolerance, with CAS-ABD linearization. A write
  linearizes at 2/3 CAS success; a read linearizes at a 2/3 quorum read taking the max
  tag, where tag order is `(struct_ver, content_ver)` lexicographic. Replication is a
  compile-time toggle, `DS_N_REPLICAS ∈ {1, 3}`.
- **Failure model:** fail-stop memory nodes. No Byzantine faults, no partitions, no client
  crashes during evaluation.
- **Linearizable range queries** are a target capability, served by snapshot timestamps
  plus the `old_ver*` chain, so that readers never block writers.
- **Target scale: up to 8 client machines, 8–16 cores each.** Each *machine* holds one local
  cache, shared by all of its threads. Throughout this document **N (or M) means machines**,
  never total cores — the two behave differently and §8 works out why.
- The caches do **not** coordinate with each other — no invalidation broadcast, no coherence
  directory. Each is independently stale and independently repaired. Anything requiring
  cross-machine agreement is out of scope by construction.

**Authoritative source for the above is [`docs/invariants.md`](invariants.md)**, in the
same repo. That file governs; this one only elaborates the cache boundary. The sections
that bear directly on this interface are §2 (version rules), §4 (CAS-ABD linearization),
§6 (cache staleness, C1–C5) and §7 (structural-modification state machines).

### Remote node layout — as understood, please correct

The cache's assumptions depend on this, so it is worth confirming that the understanding
below is still accurate:

```
{ struct_ver (14b) | content_ver (18b) | offset (4B) | k_min (8B) | next* (8B)
  | content_ver | struct_ver | padding }
```

with the first 8 bytes forming a **handle** that supports RDMA CAS, and the trailing
bookend versions used to validate that a remote read was internally consistent. If any of
that has changed, several assumptions below need re-checking.

## 1. The split — Settled

The cache is a compute-local mirror of the remote *index* structure. Values never live in
it: the bottom level holds remote addresses, the levels above hold local pointers.

- Remote is the source of truth. An operation linearizes at the remote CAS.
- The cache is a hint. Staleness costs extra RDMAs, never a wrong answer.
- The cache and the remote layer never call each other. An orchestrator composes cache
  calls and RDMA calls. Every item below is really a question about what the orchestrator
  carries across that gap.

## 2. Level numbering — Settled on the cache side, see A2

An inconsistency here already produced three off-by-one bugs in the cache, so it is worth
pinning down before anything else.

| Term | Meaning |
|---|---|
| level 0 | The directory level. Entries are `(key, remote data-node address)`. This is the level that yields something to RDMA-read. |
| levels 1 … L−1 | Index levels, `L` = total level count. An entry is `(key, pointer to a node one level down)`. |
| height `h` | A key of height `h` occupies levels `0 … h−1`. So `h` ranges over `0 … L`. |
| height 0 | Requires **no cache update at all** — a data-layer-only change does not alter index structure. Don't spend a call on it. |

**Skip vector invariant the cache maintains.** At every level *below* a key's top level,
the key becomes the **minimum of a newly created node** (a split at K). At the key's top
level (`h−1`) the key is instead **inserted into the existing node** that already covers
it. Split reports from the remote side need to line up with this.

## 3. Head nodes — Settled on the cache side, depends on A1

Each cache level begins with a leftmost ("head") node covering everything below the first
boundary. These were originally empty local sentinels with no remote counterpart. That
does not work:

> The first key ever inserted at height 2 must be written into whatever node covers it at
> level 1 — the leftmost node. If that node cannot hold entries, the write has nowhere to
> go. Neither can the next key, nor a smaller one. There is no first entry, so level 1
> never acquires one and the index levels never bootstrap. Measured: 3 nodes at level 1
> across ~70 inserts, nearly all updates silently dropped.

Heads are therefore now ordinary entry-holding nodes, and **each head mirrors the remote
leftmost node at its level, one to one.** Every local node at a level corresponds to
exactly one remote node; the head is not an exception. The other correspondences the cache
learns from split reports — the head's it can only be told, once, at bootstrap.

This survives splits: when the remote leftmost node splits at K, the cache's head keeps
entries below K and a new local node after it takes the rest, so head and remote-leftmost
stay paired. **A1 asks you to confirm that property.**

## 4. The `REMOTE_ADDR` type — Settled

`skipvector` is templated over the address type, so the remote side defines it. The cache
requires only that it is **default-constructible and copyable**.

Notably, the cache **never inspects the value** — no equality test, no null check. It
stores `REMOTE_ADDR{}` into nodes it created on its own, and returns whatever it holds.
So:

> The remote address type must be defined such that a **default-constructed value is a
> distinguishable null**, and the orchestrator is responsible for recognising it.

## 5. Call surface

Exact signatures from `include/skipvector_disco.h`. `K` is the key type, `MAX_LAYERS` the
compile-time level cap.

```cpp
// Bootstrap. addrs[n] = remote leftmost node at level n. Sequential-only;
// must be called before any concurrent use.                        [see A1]
void set_head_remote_addrs(std::array<REMOTE_ADDR, MAX_LAYERS> const &addrs);

// Returns the remote data-node address that may contain k, or a
// default-constructed (null) address meaning cache miss.
REMOTE_ADDR locate_data(K const &k);

// Fills prev_addrs[0 .. height-1]; prev_addrs[L] is the covering node's
// address at level L. Any entry may come back null.
bool gather_prevs(K const &k, uint32_t height, REMOTE_ADDR *&prev_addrs);

// Called after a remote structural change commits. Only when height >= 1.  [see A3]
void mirror_insert(K const &k, int height,
                   REMOTE_ADDR const &new_remote_data_addr,
                   std::array<REMOTE_ADDR, MAX_LAYERS> const &new_remote_index_addrs);

// Entry repair, after locate_data() missed or a read showed a k_min mismatch.
// Idempotent and purely additive. Built and tested.
void mirror_reconcile(K const &data_k_min, REMOTE_ADDR const &data_addr);
```

**On `locate_data` and the two address families at level 0.** A directory node carries
*two* unrelated remote addresses: its own `remote_addr`, identifying the remote level-0
*index* node it mirrors, and its vector entries, which map keys to remote *data* node
addresses. `locate_data` returns the **data node address**, so the orchestrator reads the
data node directly rather than reading the remote index node and resolving `k` against its
entries. That is what makes caching the directory contents worth anything.

**What `mirror_reconcile` needs from a traversal.** Only the data node's `k_min` and
address — not a whole path. This is because level-0 staleness is **purely additive**: node
addresses are stable (CoW replaces a node's vector and updates its offset; a split by F2
creates a new node while the existing one keeps its address and lower range), so a cached
entry `(k', D)` never becomes *wrong*, only incomplete. A cheap `insert` therefore suffices,
with no invalidation, no upsert, and no stale-value case.

Structural repair — a remote *index* boundary the cache does not know about — is a separate
and rarer operation, and it is just `mirror_insert()` called with that boundary key and the
addresses learned from the traversal. There is no third function.

**Does not exist:** there is no `mirror_remove`. See A7.

Note that `gather_prevs` and the refresh payload collect the same information from
opposite directions. If one remote traversal can serve both, they should probably be one
call rather than two.

## 6. Where the two sides may diverge — Settled

Vector capacity is a purely local property on each side. The cache's node capacity is
currently 16 entries. If the remote's differs, the cache will sometimes split where the
remote did not, and vice versa.

This is handled rather than prevented: a node the cache created on its own carries a null
remote address, the universal signal for "no known remote counterpart, treat as a cache
miss". Null addresses arise in exactly two ways — a head not yet bootstrapped, and a
local-only node born from a local capacity overflow.

This is the mechanism that lets the mirror be approximate without ever being wrong.

## 7. Refresh triggers — Proposed, see A8

The rule turns on **which** version bumped, which V1–V3 make cleanly separable: V3 says a
single operation bumps at most one of the two, V1 says a `content_ver` bump leaves `k_min`
unchanged, and V2 says a `struct_ver` bump means split/merge or a `k_min` change — which is
exactly when the cache's structural picture goes stale.

| Condition | Refresh? | Why |
|---|---|---|
| Cache miss (`locate_data` returned null) | **Yes** | The orchestrator must traverse remotely anyway, and that traversal *is* the refresh payload. |
| `k_min` mismatch — `k ∉ [k_min, next.k_min)` (C4) | **Yes** | Address was real, read was consistent, but the node is no longer the right one. |
| **`struct_ver` mismatch** | **Yes** | By V2 this means a split/merge or a `k_min` change, so the cache's structural picture is now wrong. This is the case C3 is pointing at. |
| **`content_ver` mismatch** | **No** | By V1 the contents changed with `k_min` unchanged, so the cache's picture is still correct. Retry the operation; for a Get, follow `old_ver*` per C2. |
| Torn read — bookend versions disagree | **No** | The read raced mid-write. Re-read; nothing about the cache was wrong. |
| Mirror update abandoned by the cache | **No** | The cache doesn't know remote truth, so it would have to call back through the orchestrator to issue RDMAs, inverting the layering. It also fires under contention, so many threads hit it at once — amplifying a local contention event into a burst of remote traffic. The next operation over that key range will miss and repair it anyway. |

**Relationship to C3.** An earlier draft of this document said flatly that a version
mismatch should not trigger a refresh, which contradicted C3 and was wrong. The table above
supersedes it: C3 is correct for `struct_ver`, and the "no refresh" case applies only to
`content_ver` and torn reads. The distinction matters because conflating them produces
refresh traffic proportional to *all* write load rather than to structural churn alone.

---

## 8. Staleness under scale — Analysis, not yet agreed

The concern: with N compute nodes all mutating the structure, each individual cache goes
stale faster while repairing at its own unchanged rate. This is real, and worth stating
precisely rather than reassuring around.

**Machines and cores are different axes.** Throughout this document N refers to the number
of client *machines*, since that is the number of independent caches. Cores per machine are
a separate variable, and they behave completely differently. Let `M` = machines,
`C` = cores per machine, `λ` = per-core op rate, `w` = write fraction, `r` = the level ratio
(8 at `IDX_EXP = 3`), `n_ℓ` = node count at level ℓ. For one level-ℓ entry on one machine:

- Invalidation rate `s = (M−1) · C · λ · w · r^−(ℓ+1) / n_ℓ` — structural writes from *other*
  machines landing in that node's span. A machine's own writes do not stale its own cache,
  because `mirror_insert` updates it in the same operation.
- Access rate `a = C · λ / n_ℓ`.

so

```
s/a  =  (M−1) · w · r^−(ℓ+1)
```

**`C` cancels, and so does `n_ℓ`.** Adding cores to a machine raises the rate at which its
cache is invalidated and the rate at which it repairs, in lockstep — core count has no
effect on staleness at all. Only machine count does, and it enters as `(M−1)`, so a single
machine is perfectly fresh by construction.

**The scaling in `M` is genuinely linear.** The probability that an access finds an entry
stale is roughly `1 − e^(−s/a)`, and repair-on-touch does not defeat it: a machine repairs
at rate `a` while the rest of the cluster breaks things at rate `s`. What bounds it in
practice is that `r^−(ℓ+1)` term, below.

### At the target scale — 8 machines, 8–16 cores each

Stale-on-access probability at `M = 8`, `r = 8`:

| write fraction | level 0 | level 1 | level 2 |
|---|---|---|---|
| 50% | ~35% | ~5% | ~0.7% |
| 10% | ~8% | ~1% | ~0.14% |

Only the directory level degrades meaningfully, and a level-0 miss costs one extra `next_dn`
hop rather than a traversal. **Crossover** — where expected extra hops equal the `L−1` round
trips the cache saves — sits near `M ≈ 50` machines at 50% writes and `M ≈ 240` at 10%. At 8
machines there is a 6–30× margin.

**Conclusion: this deployment is firmly in the small-N regime**, which settles A6 (see there)
and means the linear-in-`M` growth described here is a documented property rather than
a problem to engineer around.

Core count is not a staleness question but a **local contention** question: 8–16 threads
share one cache and contend on its sequence locks and hazard pointers. More cores per machine
is in fact mildly *good* for hit rate, since all of a machine's threads warm and repair the
same shared structure. Note that `MAX_THREADS` in `include/common/machine_defines.h` is
currently 16; it only tunes hazard-pointer sizing rather than capping anything, but it should
be raised if a machine ever runs more than 16 threads.

**What makes it survivable is the level geometry, not a mechanism.** With `IDX_EXP = 3` the
level ratio is 8. A level-0 entry goes stale when a height-≥1 key lands in its span — about
1 insert in 8. A level-1 entry needs height ≥ 2: 1 in 64. Level 2: 1 in 512. Staleness rate
decays geometrically with level, while the cost of being wrong at level ℓ grows only
linearly, because being wrong at ℓ costs a re-traverse from ℓ rather than from the root.
That is what C4's "traverse from cached ancestor" buys.

So the cache degrades to *degraded*, not to *useless*. As N grows you lose the bottom of the
index first, and by the time level 0 is thoroughly stale, level 2 is still 64× fresher. What
remains still lands a lookup within a node or two of the answer — one or two extra RDMAs
instead of L round trips. This falls out of the skip vector's geometry, so it needs no
mechanism to obtain, and cannot be tuned except through `IDX_EXP`.

**The thing that would actually make the cache useless is not staleness.** Nothing in the
design ever shrinks the local structure; `mirror_reconcile` adds and repairs but never
removes. Under sustained churn, local level 0 accumulates nodes mirroring nothing remote.
That is not a correctness problem — such nodes carry null or wrong addresses and get
bypassed — it is a traversal-length problem. Once the local level-0 chain holds several
times more nodes than the remote one, walking it locally costs more than the RDMA it was
meant to save. See A9.

**Amortisation is the one lever that improves the constant.** A refresh that installs a
whole node's contents teaches the cache about all ~8 of that node's children from a single
remote read, rather than one entry. That makes repair rate proportional to
`access rate × fanout` instead of to access rate alone. It does not change the linear-in-N
scaling, but an 8× constant may be the difference between the cache paying for itself at 8
clients and at 64. See A6.

---

## Open items — A1 to A9

**Do not resolve these unilaterally.** `Decide` = a real choice with consequences.
`Confirm` = an assumption that is probably already true but must be written down.

### A1 — `Decide` — Does the remote SV have a stable leftmost node per level, and can its address be exposed at bootstrap?

- **Assumed:** one leftmost node per level `0 … L−1`, address fixed for the lifetime of
  the structure, known at bootstrap, surviving its own splits as described in §3.
- **Needed:** those `L` addresses, once, before concurrent use. Until they arrive, every
  lookup left of the first boundary reports a miss and falls back to remote traversal —
  correct but slow.
- **If not:** if the remote leftmost is an empty sentinel, or the root can move, the
  cache's head design must change — and the obvious alternative is the one shown in §3 to
  be unable to bootstrap. Needs a conversation before either side commits.

### A2 — `Confirm` — Do both sides share the level numbering and height-to-level mapping?

- **Assumed:** the table in §2.
- **Needed:** the same convention, or an explicit written mapping. Cheap to settle,
  expensive to get wrong.

### A3 — `Confirm` — How is `new_remote_index_addrs` indexed?

- **Assumed:** indexed *by level*, not by "i-th node created". Entry `L` holds the address
  of the remote node the split created at level `L`, for `L` in `0 … h−2`. At the top
  level `h−1` the entry is the address of an orphan if the remote node overflowed, or null
  if it did not.
- **Needed:** confirmation of the indexing and of the top slot. Also: can the remote report
  a split at a level where the cache has no node to split, or vice versa? Both are believed
  survivable, but should be known rather than discovered.

### A4 — `Decide` — Should node capacity match on both sides?

- **Situation:** cache is 16 entries per vector. Divergence is already handled (§6), so
  nothing breaks if they differ.
- **Trade-off:** matching removes a whole class of divergence for free and makes the mirror
  much easier to reason about when debugging. Not matching lets each side tune for its own
  cache-line and RDMA characteristics, which may matter more on the remote side. Worth
  choosing deliberately rather than ending up with it by accident.

### A5 — `Decide` — Can one remote traversal return the whole descent path?

- **Question:** on a miss, can a single traversal return `(address, k_min, next_k_min)` per
  level, or is it a round trip per level?
- **Why it matters:** the biggest lever on the refresh design. One traversal ⇒ refresh is
  essentially free on any miss and should be eager. A round trip per level ⇒ refresh must
  be lazy and path-only, and `gather_prevs` needs rethinking since it wants the same data.

### A6 — `Confirm` — Refresh payload: path addresses only, or whole node snapshots?

**Recommendation: path-only**, on the strength of the numbers in §8. Downgraded from
`Decide` to `Confirm` now that the target scale is known.

- **Complication:** at level 0 a snapshot installs verbatim, because the cache's value type
  there already *is* a remote address. At levels ≥ 1 the snapshot's entries are remote
  down-addresses while the cache stores local pointers, so installing a whole vector needs
  an address-to-local-node map that does not exist. Path-only sidesteps this entirely,
  because the chain is built bottom-up.
- **Why path-only wins here.** Whole-node refresh repairs ~`r` entries per remote read
  instead of one, which is the only lever against the linear-in-`M` staleness growth in §8.
  But at `M = 8` that growth is 6–30× away from the crossover, so the lever buys margin
  nobody needs, in exchange for building the address-to-pointer map. Spend the simplicity.
- **Revisit if** the machine count ever heads toward the crossover, or if measured level-0
  hop counts come in far above the model. The whole-node option stays open; nothing in the
  path-only design forecloses it.
- **Still coupled to A5.** If a traversal cannot return node *contents* cheaply, path-only
  is the only affordable option anyway.

### A7 — `Decide` — What happens to the cache on delete?

- **Gap:** there is no `mirror_remove`. A key deleted and re-inserted can return at a
  different height, leaving a stale local chain at the old height's levels.
- **Status:** not a correctness hole — the cache detects the resulting inconsistency and
  abandons the update rather than corrupting itself. But it is a staleness source whose
  only repair is a later miss.
- **Needed:** a decision on whether deletes are reported to the cache at all, and if so with
  what payload. Reasonable to defer if deletes are rare in the evaluation — but deliberately.

### A8 — `Confirm` — Is the refresh trigger rule agreed, and should C3 be amended?

- **Proposal:** the table in §7, which keys the decision on `struct_ver` vs `content_ver`
  rather than on "version mismatch" as a single condition.
- **Needed:** agreement, because this lives in the orchestrator and must be written once.
  The failure mode of getting it wrong is a refresh storm that scales with total write load
  instead of with structural churn.
- **Also:** C3 in `docs/invariants.md` currently reads "on version mismatch in
  Insert/Delete → restart, refresh cache". If we adopt the table, C3 should be narrowed to
  say `struct_ver` explicitly, so the two documents don't drift.

### A9 — `Decide` — Does local structure reclamation get built, and is it in scope for the evaluation?

- **Gap:** nothing shrinks the local structure. Over a long run with churn, local node count
  at each level grows without bound relative to remote node count, until local traversal
  costs more than the RDMA it saves (§8). This, rather than staleness, is the real
  "cache became useless" failure mode.
- **Available fix:** the upstream skip vector had opportunistic orphan merging, currently
  commented out in `check_next` in `include/skipvector_disco.h`. Re-enabling a version of it
  would let traversals compact local nodes on the way past, bounding the local-to-remote
  node ratio. It fits the existing cleanup-on-touch philosophy.
- **Note on C5:** "entries live until version-mismatch invalidation" is correct about
  *correctness* but silent about *reclamation*. Those are separate questions, and C5 should
  not be read as settling this one.
- **Not governed by machine count.** Unlike everything else in §8, this does not improve at
  `M = 8`. Bloat is driven by *total elapsed churn*, and specifically by structure the remote
  side removes that the cache never hears about. So its severity is coupled to **A7**: with
  no `mirror_remove` and a delete-bearing workload, local structure grows monotonically while
  remote shrinks, which is the case that bites. An insert-mostly evaluation makes it mild.
- **Mostly cache-side work,** so this is less a question for the remote side than a joint
  decision on whether long-run cache viability is something the evaluation has to
  demonstrate. If runs are short and delete-light, it can be deferred — but deliberately, and
  metric 2 below will say whether that holds.

---

## What to measure jointly

The cache's justification is fewer RDMAs, and §8 says the numbers that matter are the ones
taken **as a function of N**.

1. **RDMAs per operation against N**, per operation type, versus a no-cache baseline. The
   headline result. Flat in N means the cache holds up; the slope says where the ceiling is
   and at what client count it stops paying for itself.
2. **Local node count ÷ remote node count, per level.** The bloat metric, and the one that
   predicts the failure mode in §8 and A9. Watch it especially if reclamation is not built.
3. **Hop count on stale hits — the distribution, not the mean.** The tail is what hurts, and
   it is what says when a client should stop walking forward and re-traverse from the top.

**Refresh rate is not a convergence metric under sustained churn.** An earlier draft of this
document claimed it should decay over time, and that a plateau indicates the reconcile logic
failing to converge. That holds only for a cache warming up on a static workload; with
ongoing writes it should plateau at a nonzero value proportional to N, and the plateau means
nothing is wrong.

## If only two items get answered

**A1 and A5.** A1 decides whether the head design just built is sound at all. A5 decides
whether refresh can be eager and — because whole-node refresh needs node contents rather
than addresses — largely settles A6 too, which §8 shows is the only real lever against
staleness growth as client count rises. Everything else can follow.

A9 is the other one worth settling early, but it is cache-side work rather than a question
for the remote side: it decides whether the cache stays useful over a long run or only over
a short one.
