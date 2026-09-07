# Disco-Skip: cache/remote interface

**What this is.** A design input from the compute-side local cache, describing what the
cache now assumes about the remote skip vector and what it needs supplied. Written to be
a starting point for the remote/RDMA side of the design.

**How to treat it.**

- Sections marked **Settled** describe behaviour that already exists in
  `include/skipvector_disco.h` and passes its structural `verify()` across 1–32 threads.
  Treat those as facts about the other side of the interface.
- Sections marked **Proposed** are one side's suggestion and have not been agreed.
- Items **A1–A10** marked `Decide` or `Confirm` are **open questions, not requirements.**
  Where this document says "assumed", that is an assumption that may be wrong. Do not
  resolve them by picking an option and implementing it — surface them for a decision
  between the two authors. Items marked `Resolved` have been agreed by both sides and
  carry the answer inline; treat those as settled.
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
  plus the `old_ver*` chain, so that readers never block writers. The guarantee is narrower
  and more clock-dependent than that sentence suggests -- point operations are clock-free and
  unconditionally linearizable; only ranges need a timestamp. See §9.
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

### Vocabulary used throughout this document

**Node minimum** — the smallest key in a node's vector, i.e. where that node's key range
starts. `[40|50|60]` has minimum 40 and covers `[40, next_node_min)`.

**Entry vs. boundary** — an *entry* is any `(key, pointer)` pair inside a node's vector. A
*boundary* at level L is a key that is some level-L node's minimum. Every boundary is an
entry — the first one in its node — and most entries are not boundaries. A node's entries at
level L are exactly the boundaries at level L−1.

**The fidelity property** — the invariant the cache is held to:

> For every local node that something points down to, its minimum equals the minimum of some
> real remote node at that level.

Equivalently: the cache never routes to a node whose range starts at a key where the remote
has no node starting. Two details it turns on:

- **Local orphans are exempt.** Nothing points at them, so their minimum makes no routing
  claim — it is just an artifact of a local vector filling up.
- **Remote orphans count as real.** An orphan is a real node with a real minimum; it is
  merely one that nothing routes to. So the property is about *node minimums*, not about
  which of those minimums have parents.

**Invented boundary** — a local node minimum that is *not* any remote node's minimum. This is
the failure the property rules out, and the quantity asserted to be zero in
`test_reconcile.cc`. It arises when an overflow split picks whatever key happens to land
first in the new node as its minimum, and something then routes to it.

**What the property buys.** It makes the **routed** local partition a *coarsening* of the
remote one: taking only the nodes something points down to, each one's range is a union of
consecutive remote node ranges, beginning exactly where the first of them begins. So
`remote_addr` names a remote node whose range is a *prefix* of the local node's range, which
is what gives C4 a clean reading — fetch the named node, and either `k` falls in its range
and you are done, or you are in the part of the local range that address does not cover and
you re-traverse.

Local orphans sit outside that statement in both directions, which is worth being exact
about. They *subdivide* a routed node's range further, so the full local partition is not a
pure coarsening. And an orphan's address may name a remote node whose `k_min` differs from
the orphan's own minimum: `mirror_insert_at_top_level` tags an overflow orphan with the
remote orphan's address, but `steal_half_and_insert` chooses the local split point from
*local* contents, which are not identical to the remote node's, so the two can split at
different keys. Neither is unsafe — nothing routes to an orphan, and C4 validates its address
like any other — but an orphan's address is a hint in the loosest sense and should not be
read as identity.

**On its strength, honestly.** Violating it produces no crash anyone has demonstrated; C4
still catches the mismatch. What it costs is the one-to-one correspondence — a node with an
invented minimum corresponds to *no* remote node, it merely overlaps one — and that
correspondence is what §3's head mirroring, the `gather_prevs` contract and §6's divergence
bound all rest on. It is asserted mechanically rather than assumed, so that it cannot be
quietly eroded by a later change that looks locally reasonable.

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
leftmost node at its level, one to one.** Every local node that something *routes to*
corresponds to exactly one remote node; the head is not an exception. The other
correspondences the cache learns from split reports — the head's it can only be told, once,
at bootstrap.

**The exception, stated precisely:** local *orphans* born of capacity overflow correspond to
no remote node. Their minimum is whatever key happened to land first when a full vector
split, and they carry a null remote address, which reads as a cache miss. They are exempt
from the fidelity property because nothing points at them — they are reachable only by
walking `next`, and they make no routing claim. The correspondence holds for every node the
cache actually routes to, which is the version the tests assert.

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
// Bootstrap. addrs[n] = remote leftmost node at level n. Sequential-only and
// write-once; must be called before any concurrent use.            [A1: agreed]
void set_head_remote_addrs(std::array<REMOTE_ADDR, MAX_LAYERS> const &addrs);

// Did bootstrap actually run? Leaving it uncalled is safe but silently turns
// every gather_prevs() that lands on a head into a per-level miss, so this
// exists to be asserted rather than inferred from a high miss rate.
bool heads_bootstrapped() const;

// Returns the remote data-node address that may contain k, or a
// default-constructed (null) address meaning cache miss.
REMOTE_ADDR locate_data(K const &k);

// Fills prev_addrs[0 .. height-1]; prev_addrs[L] is the covering node's
// address at level L. Any entry may come back null. Levels at or above
// /height/ are left untouched.
bool gather_prevs(K const &k, uint32_t height, REMOTE_ADDR *prev_addrs);

// Called after a remote structural change commits. Only when height >= 1.  [see A3]
void mirror_insert(K const &k, int height,
                   REMOTE_ADDR const &new_remote_data_addr,
                   std::array<REMOTE_ADDR, MAX_LAYERS> const &new_remote_index_addrs);

// What the descent saw at one level. The client already holds this for every
// level it descended through, so collecting it costs no extra RDMA.
struct path_step {
  K           k_min;       // k_min of the remote node covering the sought key
  REMOTE_ADDR addr;        // that node's remote address
  REMOTE_ADDR first_down;  // LEVEL 0 ONLY: value of that node's first entry,
                           // i.e. the data node whose k_min is this k_min.
                           // Needed to create a local directory node with that
                           // minimum. At levels >= 1 the value is a local
                           // pointer the cache resolves itself, so it is unused.
};

// Repair after locate_data() missed or a read showed a k_min mismatch.
// Installs the level-0 routing entry, and promotes any node that the resulting
// overflow splits off into the layer above, using /path/ to give the nodes it
// creates a covering remote address.  Idempotent.  Built and tested.  [A5]
void mirror_reconcile(K const &data_k_min, REMOTE_ADDR const &data_addr,
                      path_step const *path, uint32_t levels);

// Convenience overload for a caller with no path. Repairs the entry, but nodes
// created by promotion get null addresses and so read as cache misses.
void mirror_reconcile(K const &data_k_min, REMOTE_ADDR const &data_addr);

// Local node count at /level/, 0 = directory. Sequential-only. This is the
// local-to-remote node ratio metric.
size_t node_count(int level) const;

// Visit every node at /level/ as f(minimum_key, is_orphan, entry_count).
// Sequential-only. Lets a test assert the fidelity property in A5.
template <typename F> void for_each_node(int level, F &&f) const;
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

A node the cache created on its own carries a null remote address — the universal signal
for "no known remote counterpart, treat as a cache miss". Null addresses arise in exactly
two ways: a head not yet bootstrapped, and a local-only node born from a local capacity
overflow. This is the mechanism that lets the mirror be approximate without ever being
wrong.

**A4 (matched capacity) narrows the second case considerably.** Level-0 staleness is
additive (§5), so a local node's entries are always a *subset* of its remote counterpart's.
With equal capacities that means **the cache can never overflow before the remote does** —
if a local node reaches capacity then the remote node did too, so the remote has split and
either already reported it or is about to. Local-only orphans therefore stop being a
systematic outcome and become a narrow race window. They still have to be handled, but they
should be rare enough not to shape the design.

A related consequence, worth stating so the measurement is not misread: the orphan fraction
reported in A9 is **not divergence**. Orphans are ordinary skip vector structure produced by
the capacity-versus-promotion geometry, and with matched capacities the remote generates them
at the same rate. Given `orphan_remote_addr` (A3), the local ones mirror remote ones rather
than being local inventions.

**Divergence has a stated bound.** Since A5, the local partition is a *coarsening* of the
remote one and never a different one: every node that something points down to has a minimum
that is a real remote boundary, asserted in the tests. Local can hold **fewer** boundaries
than remote — for any it has not learned yet — but never a boundary the remote does not have.
A local node may therefore span several remote nodes, in which case its `remote_addr` names
only one of them. That is a hint that fails validation, not a wrong answer: the reader checks
`k ∈ [k_min, next.k_min)` per C4 and re-traverses, at the cost of one round trip. Reconcile
then splits the node at the boundary it just learned, so the same miss does not recur.

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

**Repair now covers both consumers.** `locate_data` self-heals through entry installation,
and since A5 `gather_prevs` does too: a coarse node gets split at the boundary the descent
saw, and a node whose minimum matches the remote's adopts its address. Before that, index
addresses degraded monotonically with no repair path.

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

## 7a. Worked example — a stale cache, and the path that repairs it

Scaled down for readability; the shape is real. `layers = 3`, and a key of height `h` is an
entry at levels `0 … h-1` and a node minimum at levels `0 … h-2`.

Keys with height >= 1: `10 20 30 40 50 60 70 80`. Of those, height >= 2: `10 40 70`.
Height >= 3: `10 70`.

### Remote state

```
level 2   R2_10 [ 10 | 70 ]
                   |    |
                   |    +-----------------------+
                   v                            v
level 1   R1_10 [ 10 | 40 ]              R1_70 [ 70 ]
                   |    |                        |
             +-----+    +--------+               |
             v                   v               v
level 0   R0_10             R0_40           R0_70
          [10|20|30]        [40|50|60]      [70|80]
             |                  |               |
             v                  v               v
data      DN10 DN20 DN30   DN40 DN50 DN60  DN70 DN80
```

### Stale local cache

This client learned the left of the tree, never heard about the split at 40 (another client
made it), and knows nothing of 60, 70, 80.

```
level 2   T_local [ 10 ]                  addr = R2_10
                    |
                    v
level 1   L1_a    [ 10 ]                  addr = R1_10
                    |
                    v
level 0   L0_a    [10|20|30]              addr = R0_10
```

`L0_a` claims to be `R0_10`, but its local range is `[10, inf)` — it already spans `R0_10`,
`R0_40` and `R0_70`. That is the coarse case: local holds *fewer* boundaries than remote,
never more.

### The miss

`locate_data(55)` descends to `L0_a`, does `find_lte(55)` over `{10,20,30}` and returns
`DN30`. The orchestrator reads `DN30`: `k_min = 30`, and 55 is past the last key in its
vector, so the node cannot answer for 55 on its own.

**That last step is conditional, and worth being precise about.** Having read a data node,
three cases:

| condition | conclusion | extra hop? |
|---|---|---|
| `k` is in the vector | hit | no |
| `k` absent and `k < max_key(vector)` | `k` sorts inside this node's range and is not there, so it does not exist | no |
| `k > max_key(vector)` | either absent from this node's range, or this is the wrong node | **yes** — needs `next_dn.k_min` |

Only the third case costs a further round trip. This mirrors the local traversal, where
`check_next()` advances only when `!curr->v.last(last) || k > last`.

**Two ways to avoid even that hop**, both worth considering:

- **Co-locate `next_dn.k_min` with `next_dn` in the node header.** They change together —
  a split of `curr` rewrites `curr->next_dn`, and a split of `next` leaves `next`'s own
  `k_min` alone — so the value can ride along in the write F2 already performs. Eight bytes
  buys a definitive range check from a single node read.
- **Have the cache supply it.** A directory node's entries *are* the data-node boundaries, so
  the entry after the one `find_lte` matched is exactly `next_dn.k_min`. If `locate_data`
  returned that alongside the address, the orchestrator could bound the range with no remote
  read at all — and in this example it would learn up front that the cache cannot vouch for
  55 (30 is the last entry in `L0_a`), skipping the wasted read of `DN30` entirely. This
  would change `locate_data`'s return type, so it is not built.

Either way, 55 is not covered, so the orchestrator re-traverses remotely — reading one node
per level, and thereby holding the whole path for free.

### The path handed back

```cpp
path_step path[3] = {
  /* level 0 */ { .k_min = 40, .addr = R0_40, .first_down = DN40 },
  /* level 1 */ { .k_min = 10, .addr = R1_10, .first_down = {} },
  /* level 2 */ { .k_min = 10, .addr = R2_10, .first_down = {} },
};

sv.mirror_reconcile(/*data_k_min=*/50, /*data_addr=*/DN50, path, 3);
```

`k_min` is coarser at each level up — 40, then 10, then 10. That is the staircase.
`first_down` is `DN40` because 40 is `R0_40`'s first entry and its value is the data node
whose `k_min` is 40. Only level 0 needs it; higher levels' down values are local pointers
the cache resolves itself.

### What the cache does with it

**1. Install the data entry.** `L0_a` becomes `{10,20,30,50}`, so `locate_data(55)` would
now return `DN50`.

**2. Split at the observed boundary.** `b = path[0].k_min = 40`. The node covering 40 is
`L0_a`, minimum 10, so it splits — entries above 40 move across and 40 is inserted with
`DN40`:

```
L0_a [10|20|30]  addr = R0_10        L0_b [40|50]  addr = R0_40
```

`L0_a` now covers `[10,40)` and `L0_b` covers `[40, …)`, both matching real remote nodes.
The coarseness is repaired.

**3. Install the parent, then stop.** `path[1].k_min` is 10, not 40, so 40 is an *entry* at
level 1 but not a boundary. Installing it is faithful — `R1_10` really does hold an entry
keyed 40 — and it gives `L0_b` its parent:

```
level 1   L1_a [ 10 | 40 ]
                  |     |
                  v     v
level 0        L0_a    L0_b
```

The climb stops there. Had the lookup been for `k = 15`, all three `k_min`s would be 10 and
it would have climbed to the top.

Two things this deliberately does not fix: `L0_a`'s address is not re-checked, because
`R0_10` was not on this descent; and 60/70/80 remain unknown. A later miss repairs them.

### Variation: the descent lands on a remote orphan

The remote has orphans too — A3 confirms a node that overflows splits off a successor that
nothing above points at. Say `R0_40` had held `{40,50,60}` and inserting 55 overflowed it:

```
level 1   R1_10 [ 10 | 40 ]              <- no entry for 55
                   |    |
             +-----+    +----+
             v               v
level 0   R0_10          R0_40      R0_55o  (orphan)
          [10|20|30]     [40|50]    [55|60]
                              \_______/
                            reached only by next
```

Looking up `k = 57`: level 1 gives `find_lte(57) -> 40 -> R0_40`; at level 0, 57 is past
`R0_40`'s last key, so the descent walks `next` and **lands on the orphan**.

```cpp
path[0] = { .k_min = 55, .addr = R0_55o, .first_down = DN55 };  // an orphan
path[1] = { .k_min = 10, .addr = R1_10 };
path[2] = { .k_min = 10, .addr = R2_10 };
mirror_reconcile(/*data_k_min=*/55, DN55, path, 3);
```

The cache takes the `split_at` branch here rather than `split_insert`, because step 1 has
just inserted 55 as an ordinary entry of `L0_b` and step 2 then splits at that existing
entry: `L0_b` keeps `{40,50}`, and a new `L0_c = {55}` takes `addr = R0_55o`. Step 3 installs
`(55 -> L0_c)` at level 1.

**The consequence is worth understanding.** Remotely `R1_10` has *no* entry keyed 55 — that
is what makes `R0_55o` an orphan. Locally `L1_a` now does. So the local index is finer than
the remote index **at the entry level**. That is sound, and mildly good:

- Every local node still mirrors exactly one real remote node. `L0_c` ↔ `R0_55o`, same
  minimum. The fidelity property is about *node minimums*, not about which minimums have
  parents, and an orphan's minimum is a real node minimum.
- `gather_prevs` stays correct: for 57 it returns `R0_55o`, which really is the level-0 node
  covering 57.
- Local routing beats remote — the cache reaches the orphan's mirror by a down pointer where
  the remote has to walk. Reproducing the remote's walk would be strictly worse for no
  correctness gain.

Cost is bounded at roughly the orphan rate: the local index carries order 20% more entries
than the remote index.

**An orphan cannot break the staircase**, which is why the climb logic stays safe: a level-L
orphan's minimum is drawn from the level below's boundary set (its entries came from
splitting a level-L node, and those entries are level-(L-1) boundaries), so
`path[L].k_min <= path[L-1].k_min` still holds.

**Open design point.** At level L, should the descent report *the node it landed on*, or *the
last node that had a parent*? Reporting what you landed on is recommended, for the reasons
above — but it should be decided deliberately rather than falling out of how the traversal
loop happens to be written. The cache cannot tell the difference: `path_step` carries no
orphan flag, and does not need one.

### Contrast: what `mirror_insert` receives

Had this client instead *inserted* key 45 at height 2, the remote would split `R0_40` and add
45 as an entry in `R1_10`:

```cpp
new_remote_index_addrs[0] = R0_45;  // the node the split created at level 0
new_remote_index_addrs[1] = {};     // top level (h-1): orphan address, or null
sv.mirror_insert(45, /*height=*/2, DN45, new_remote_index_addrs);
```

Indexed by level, same as `path_step[]`, but carrying *newly created* nodes rather than
*observed* ones — which is why the top slot is an orphan address or null rather than a
covering node.

---

## 9. Linearizability, clocks, and range queries

Orchestrator and remote-layer territory rather than the cache boundary, but it decides what
the paper can claim, so it belongs in shared context.

### Scope it first -- it is narrower than it looks

**Point operations involve no clocks at all.** L1 linearizes a write at 2/3 CAS success and
L2 linearizes a read at the max-tag quorum read, where the tag is
`(struct_ver, content_ver)`. No cross-machine time comparison enters either path. **Only
range queries need a snapshot timestamp**, because they alone have to fix a point in time and
read every vector as of it.

So the claim to make is:

> Point get / insert / delete are linearizable, unconditionally. Range queries are
> linearizable under commit-wait, or ε-linearizable without it.

That is far easier to defend than "the system is ε-linearizable", and it is what the
invariants already imply.

### Commit-wait is a correctness mechanism, not an optimisation

If a writer waits ε after taking its timestamp before making the write visible, then any
operation starting after that write is visible provably carries a larger timestamp. That is
Spanner's argument, and it converts ε-linearizability into linearizability outright. The
price is ε of added write latency.

Which changes what reviewers are being asked to accept:

- *"ε is sub-nanosecond, so misordering is practically irrelevant"* -- a correctness claim
  resting on timing hardware we do not have.
- *"We implement commit-wait, so we are linearizable; it costs ε per write; on our testbed
  that is X and costs Y%; with White-Rabbit-class clocks ε is sub-ns and the cost goes to
  zero"* -- a correctness claim that holds unconditionally, plus a performance projection.

The second is much the stronger position, and reviewers accept performance projections far
more readily than correctness ones. If testbed ε on commodity PTP is around 100 ns against
~2 µs operations, commit-wait costs roughly 5% -- a graph rather than an argument. White
Rabbit then explains why that cost disappears, instead of propping up correctness.

### Sub-nanosecond on the wire is not sub-nanosecond at the application

White Rabbit synchronises clocks across its switch and NIC ecosystem. The ε that matters is
the error in the timestamp a *thread* obtains, and that path runs
reference -> host clock -> application read.

- If the disciplined clock lives in the NIC, reading it is likely a PCIe read of roughly a
  microsecond -- worse than the operation being timestamped.
- If the system clock is disciplined via PTP and read through vDSO (~20-25 ns), achievable ε
  is capped at PTP-to-system-clock quality: tens of nanoseconds, not sub-ns.

That last hop is not a network problem, so WR does not address it. **Before any ε number goes
in the paper**, work out concretely how a thread reads a WR-disciplined time and what that
read costs. The shape of the story survives either answer; only the number changes.

### One quantitative note

Comparing ε to operation *duration* is the wrong measure. A violation needs two operations
that are **non-overlapping in real time, conflicting, and separated by less than ε** --
overlapping operations may legally be ordered either way, so duration does not enter it. The
right quantity is the rate of conflicting non-overlapping pairs separated by less than ε,
which follows from the arrival process and the conflict density. At 128 cores the cluster
inter-arrival gap is tens of nanoseconds, so if ε lands in that range the at-risk fraction is
not automatically negligible and wants computing from measured rates.

### The clock-free alternative, worth citing even if unimplemented

An RDMA fetch-and-add on a well-known counter in memory-server memory yields a genuine total
order for one round trip, keeps the server passive, and is HCA-offloaded. At eight machines
the single-cacheline contention is very likely fine. Citing it costs a paragraph and
pre-empts "why not just serialise?".

### None of this touches the cache

The cache stores node addresses and never timestamps, so commit-wait, ε and snapshot
acquisition are entirely orchestrator and remote-layer concerns. No cache-side work follows
from this section.

---

## Open items — A1 to A10

**Do not resolve these unilaterally.** `Decide` = a real choice with consequences.
`Confirm` = an assumption that is probably already true but must be written down.

### A1 — `Resolved` — Does the remote SV have a stable leftmost node per level, and can its address be exposed at bootstrap?

**Answered yes (remote side).** There is a stable leftmost node per level and the addresses
can be exposed at bootstrap. The head design in §3 stands; `set_head_remote_addrs()` should
be wired into the bootstrap sequence.

- **Assumed:** one leftmost node per level `0 … L−1`, address fixed for the lifetime of
  the structure, known at bootstrap, surviving its own splits as described in §3.
- **Needed:** those `L` addresses, once, before concurrent use. Until they arrive, every
  lookup left of the first boundary reports a miss and falls back to remote traversal —
  correct but slow.
- **If not:** if the remote leftmost is an empty sentinel, or the root can move, the
  cache's head design must change — and the obvious alternative is the one shown in §3 to
  be unable to bootstrap. Needs a conversation before either side commits.

### A2 — `Resolved` — Do both sides share the level numbering and height-to-level mapping?

**Agreed.** Both sides use the numbering in §2.

- **Assumed:** the table in §2.
- **Needed:** the same convention, or an explicit written mapping. Cheap to settle,
  expensive to get wrong.

### A3 — `Resolved` — How is `new_remote_index_addrs` indexed?

**Answered: indexed by level, and the top slot populated as described.** So entry `L` holds
the address of the remote node the split created at level `L`, for `L` in `0 … h−2`, and at
`h−1` the entry is an orphan address if the remote node overflowed, or null if it did not.

**One sub-question went unanswered, and checking it found a latent bug.** I had also asked
whether the remote can report a split at a level where the cache has nothing to split, or
the reverse. Tracing that through `mirror_insert` showed it bailed out mid-way on a
`nullptr` from `mirror_split_at_level`, leaving the node it had already created at the level
below as a non-orphan with no parent — which `verify_index()` rejects. It now installs that
node's parent entry wherever it stops, the same way `mirror_reconcile` does.

The boundary-nesting invariant — a key that is a boundary at level L is a boundary at every
level below it — probably makes that case unreachable, since it would require `k` to be a
level-L boundary without being a level-0 one. But the argument is delicate under concurrency
and the guard costs nothing, so the code no longer depends on it.

The reverse direction (the cache splitting where the remote did not) is now bounded by A4:
with matched capacities and additive level-0 staleness, a local node's entries are a subset
of its remote counterpart's, so the cache cannot overflow first except in a narrow race.

### A4 — `Resolved` — Should node capacity match on both sides?

**Agreed: capacities will match.** This is worth more than it looks — see the consequence
noted in §6.

- **Situation:** cache is 16 entries per vector. Divergence is already handled (§6), so
  nothing breaks if they differ.
- **Trade-off:** matching removes a whole class of divergence for free and makes the mirror
  much easier to reason about when debugging. Not matching lets each side tune for its own
  cache-line and RDMA characteristics, which may matter more on the remote side. Worth
  choosing deliberately rather than ending up with it by accident.

### A5 — `Confirm` — After a miss, does the orchestrator feed back what the descent saw?

**Implemented on the cache side. What remains is your descent collecting `path_step[]` and
passing it.** The cache-side answer is settled and measured.

**Why it was needed.** `mirror_reconcile` installs level-0 *entries*, while node
*boundaries* would otherwise arrive only via `mirror_insert`. A read-mostly client therefore
filled the directory with unindexed overflow nodes and the layer degenerated into a linked
list — worse than having no cache at all, and unbounded as more keys are touched.

**What it does now.** It installs the data routing entry as before, then **splits the local
structure at the remote boundaries the descent actually saw**. It climbs while the same key
is a boundary at the next level up, and where it stops it installs that node's parent entry
— still faithful, because a key that is a boundary at level L−1 really does appear as an
entry in the remote node covering it at level L.

**The fidelity property, asserted in the tests:** *every node that something points down to
has a minimum that is a real remote boundary.* The cache never invents one. Orphans are
exempt, since nothing points at them and they claim to be nobody's boundary.

It also repairs the **coarse** case, which was previously unfixable. A local node that
recorded a boundary as an ordinary entry — because some *other* client created that boundary,
which is the normal situation — now gets split there and stops spanning two remote nodes.
That is what makes `gather_prevs` self-healing; before this it degraded monotonically and
never recovered.

**Measured**, 4 layers, against a simulated remote whose boundaries are 8× sparser per level
with stable addresses:

| population | directory | level 1 | level 2 | orphans | invented boundaries |
|---|---|---|---|---|---|
| entries only (the original gap) | 1771 | 1 | 1 | **100%** | — |
| local promotion (rejected) | 1774 | 156 | 14 | 1 | many |
| **faithful boundary split** | 26705 | 3923 | 486 | **3%** | **0** |

Fanout settles at ~7–8 per layer, matching the structure's own geometry. `verify()` passes,
and the path is exercised concurrently by `test.cc` at up to 64 threads.

**How far up one descent can reach.** The covering boundary is coarser at each level — for
`k = 350` the level-0 node may start at 300 while the level-1 node starts at 100 — so the
chain below a higher boundary passes through nodes that descent never read. The climb
therefore stops when the key stops being a boundary at the next level. Later reconciles fill
the rest in: `path[0].k_min` equals `path[1].k_min` whenever the key falls in the first
sub-node, roughly one lookup in `TARGET_IDX_RATIO`.

**Rejected alternative, recorded because it nearly shipped.** The cache could promote its own
overflow nodes into the layer above, keeping itself balanced without needing real boundaries
at all. It works and it is simpler, but it makes the local partition finer than the remote at
keys whose height does not deserve a boundary. It is still compiled in behind
`PROMOTE_INDEX_ORPHANS = false` so the residual orphan rate can be measured with it on and
off.

**The original question was partly malformed**, worth recording so it is not re-asked: it
assumed a traversal might not be able to return the path. With passive memory servers the
client *performs* the descent, so it necessarily already holds every node on it. The path
costs **zero marginal RDMA**; the only question was ever whether the orchestrator keeps it.

### A6 — `Confirm` — Refresh payload: path addresses only, or whole node snapshots?

**Recommendation: path-only**, on the strength of the numbers in §8, and now implemented
that way — see A5. The payload is `path_step[]`: per level a `(k_min, addr)` pair, plus the
first-entry value at level 0. Not whole-node snapshots.

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

### A8 — `Resolved` — Is the refresh trigger rule agreed, and should C3 be amended?

**Agreed, and C3 has been amended** in `docs/invariants.md` to name `struct_ver` explicitly
and to state that a `content_ver` mismatch triggers a restart but no refresh.

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
- **Not to be confused with A5's boundary split.** That keeps the *local* partition lined up
  with the remote one and is now implemented. A9 is about local structure the remote has
  *removed*, which no amount of splitting recovers.
- **Orphan merging is NOT the fix either,** despite appearances. The upstream skip vector had
  opportunistic merging, still commented out in `check_next`. It was evaluated and left off:
  `should_merge()` returns false for non-orphans on its first line, while the divergence A9
  is about — remote merges and deletes the cache never hears of — produces cached nodes that
  are stale but *non-orphans*. Merging leaves the actual case untouched. It was also found to
  be mildly divergent in its own right, since an orphan created by `node_t::insert` overflow
  can carry a real remote address (per A3), and absorbing it discards the knowledge that a
  second remote node exists — which degrades `gather_prevs`, though not `locate_data`, since
  that reads entry values rather than node identities.
- **Measured: insert-side bloat is bounded.** Directory-level orphan fraction holds at ~21%
  across a 64× range of workload size (22.6% / 21.0% / 21.1% / 21.0% at 10k / 40k / 160k /
  640k ops per thread, 8 threads). So the orphans inherent to the capacity-vs-promotion
  geometry are a stable constant, not unbounded growth, and cheap next to an RDMA. **This
  does not measure A9's actual concern** — the workload is insert-only and cannot produce
  remote removals the cache misses.
- **The fix is not a separate mechanism: A9 collapses into A7.** Reclaiming stale
  non-orphans means learning that a remote node is gone — and if the cache is told that, the
  right response is to merge the corresponding local node into its predecessor, because the
  remote absorbed that key range into a neighbour. `node_t::merge()` and the hazard-pointer
  reclamation it needs already exist; only the *trigger* changes, from local-size-driven
  (which is the version that was rejected above) to remote-driven. So implementing
  `mirror_remove` gives reclamation as a consequence, and the local-to-remote node ratio
  stays ~1 by construction. There is no separate GC to design.

**On the 10-second experiment duration.** It helps, but it is not what decides this, and the
duration argument is weaker than it sounds: 10 s at RDMA rates is on the order of 10^8
cluster-wide operations, which is not a short workload by op count. What actually bounds the
damage is that bloat is capped by the number of *remote node removals the cache had cached*.
Even in the worst case that is bounded by the number of nodes ever created, so local
structure can at most roughly double over a run — inconvenient, not fatal.

**The decisive question is upstream of duration: does the remote SV remove nodes at all?**
`docs/invariants.md` §7 gives a state machine for Split and for Update_Index, but none for
merge or delete, while V2 contemplates a `struct_ver` bump on merge. If deletes only remove
KVs from vectors and never unlink a node, then remote node count is monotonically
non-decreasing, the cache mirrors that faithfully, and **A9 cannot arise at all** — at any
duration. If node merging is implemented, then over 10 s it is bounded as above and still
almost certainly deferrable.

**Recommendation: defer A9 for the evaluation**, and confirm the merge question so the paper
can say *why* it is deferrable rather than that it went unmeasured. Keep metric 2 below
instrumented regardless — it is nearly free, and it converts "we believe bloat is bounded"
into "we measured that it is", which is worth having when a reviewer asks.
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

### A10 — `Decide` — How are range queries linearized, and what ε do we report?

Not a cache-boundary question, but it is the one that decides what the paper can claim, so it
belongs in the same list. Reasoning in §9.

- **Proposed:** implement commit-wait, so the guarantee is unconditional rather than resting
  on hardware we do not have; report *measured* testbed ε; project the cost to zero for
  White-Rabbit-class clocks. Point operations need nothing here — they are already
  linearizable without clocks.
- **Needed:** a decision on whether commit-wait is implemented for the evaluation, and where
  the snapshot timestamp is read from. The second decides both the achievable ε and the
  per-operation cost of obtaining it, and "sub-ns on the wire" does not settle it.
- **Also:** if a guarantee parameterised by ε is claimed, ε should be measured on the testbed
  rather than quoted from a datasheet, and the range-query path should be checked for
  violations during the evaluation. Finding none is a result; not looking is a gap.

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
