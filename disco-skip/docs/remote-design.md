# Disco-Skip: the remote (RDMA) side, as built

Companion to [`cache-remote-interface.md`](cache-remote-interface.md), which is the
*cache* author's document, and to [`invariants.md`](invariants.md), which governs both
halves. This one describes the remote/RDMA side: what is in memory-server memory, how
operations manipulate it, and which of the two other documents' proposals were superseded
in the process.

**Read this before the node layout in `cache-remote-interface.md` §0.** That section was
the cache author's best guess at this side and is explicitly marked *"as understood, please
correct"*. It has since been corrected, and several of its details are now wrong. Where the
two disagree, this file is what the code does.

Nothing here changes the cache's call surface. `REMOTE_ADDR` is one `uint64_t`, node
capacity is 16 (A4), and the cache never sees a node record.

---

## 1. The layout

```
NodeRecord = 64 B, one cache line, bookended
  Handle   handle;          // struct_ver:14 | content_ver:18 | offset:32
  Key      k_min;
  uint64_t next_id;
  Key      next_k_min;
  uint32_t level;
  uint32_t tail_struct_ver; // bookend partner for handle.struct_ver
  ... padding to 64 B

VecRecord = 320 B, allocated out of line, write-once except `ts`
  uint32_t struct_ver, content_ver;  // version identity, not a bookend
  uint32_t size, is_orphan;          // I4 lives here
  uint64_t ts;                       // kNullTs (0) == pending
  uint64_t old_ver;                  // the previous version; 0 ends the chain
  uint64_t next_id;                  // split descriptor, populated only
  Key      k_min_next;               //   during a split
  Entry    e[16];                    // {Key key; uint64_t val;}
```

`Entry::val` is read three ways depending on level: the payload in a data node, a data
node's `RemoteAddr::id` in a level-0 (directory) node, and the node one level down in a
level-≥1 index node.

### Why vectors are out of line

`cache-remote-interface.md` §0 implies it, and an earlier iteration of this side inlined
two copy-on-write slots in the node to make a node read one RDMA. That does not work:

- **Range queries need unbounded version history.** A snapshot at `T` needs, per node, the
  version with `ts <= T`. A node written twice since `T` has no such version in a two-slot
  ring, so the range cannot be served. `old_ver*` only means anything as a real chain.
- **A node read is two RDMAs regardless**, because the timestamp cannot be published with
  the data. See §3.

### Why `next_id` / `next_k_min` are in the header

So that a right-walk along a level costs only 64-byte header reads. A node being *passed
over* never needs its 320-byte vector fetched, and passing over is the common case while
splitting is rare — a node must fill first. Putting the link in the vector makes every hop
cost both reads, which optimises rare splits at the expense of common traversal.

`next_k_min` is `cache-remote-interface.md` §7a's own recommendation, and it earns its
eight bytes: it makes `k ∈ [k_min, next_k_min)` answerable from one header read, removing
the one case in three that otherwise needs a further hop.

**A stale `next_k_min` is the dangerous kind of stale.** Too large a bound turns "I need
another hop" into "definitively absent", so the node answers for a range it no longer
covers. That is a wrong answer, not a wasted hop, which is why §2's stability marker
exists and why readers must check it before trusting the header.

### The handle

`struct_ver` above `content_ver` above `offset` is deliberate: `tag()` — the raw value
shifted past the offset — is then a plain unsigned integer whose natural order *is* L3's
lexicographic `(struct_ver, content_ver)` order. L2's max-tag quorum read becomes one
integer compare, and a differing offset cannot perturb it.

`content_ver` exists **only in the handle**; there is no `tail_content_ver`. Its sole job is
to order two content states of one node for the quorum read. Split propagation is tracked
by `struct_ver` against its tail copy and an unfixed timestamp by `ts` itself, so
bookending `content_ver` would be dead weight.

Widths are `14/18/32`. Wrap is not expected to bind at the evaluation's run length and is a
paper discussion rather than a mechanism. For that discussion: **`content_ver` wraps roughly
16× sooner than `struct_ver`**, because a node takes about a vector's worth of inserts to
earn one split — so if the widths are ever re-cut, the bits belong to `content_ver` rather
than being split evenly.

### Why the vector has no bookends

It is write-once. F2 writes it in full before the fence, the CAS publishes it, and a later
version goes to a *new* offset rather than overwriting — so once reachable its bytes never
change and there is nothing for a reader to tear against. The asymmetry is the point: the
**node header is mutated in place** (the CAS writes the handle, a later CAS writes
`next_id`), so it needs bookends; the vector does not.

The one exception is `ts`, which is fixed after the version is visible. That is a single
8-byte aligned write, atomic under F4, and its intermediate value (`kNullTs`) is a
first-class protocol state rather than a torn read.

The version pair in the vector is *identity*, not a bookend. Today the offset alone
identifies a version — offsets are bump-allocated and never reused — so a speculative read
is validated by comparing the offset it assumed against the handle's. The fields are kept
because that stops being true once epoch GC lands: with offsets recycled, offset equality
admits an ABA and the version is what disambiguates.

---

## 2. Non-blocking reads, by helping

A writer's work does not finish at the CAS, so a reader can arrive mid-operation. Rather
than wait for the bookends to match — which is what `invariants.md` V5 implies and would
make reads blocking — the reader **completes the operation**. The new vector carries
everything needed, so it doubles as an operation descriptor.

Two markers say what is outstanding:

| Marker | Meaning | Visible from |
|---|---|---|
| `handle.struct_ver != tail_struct_ver` | a split is mid-propagation, so the header's `next_id` / `next_k_min` are not yet trustworthy | a **header-only** read |
| `vec.ts == kNullTs` | the version is visible but its timestamp is unfixed | the vector |

That the first is visible from a header-only read is what lets a passing reader skip the
vector on the overwhelmingly common stable node.

### The protocol

**Plain write (no split).** Stage the new vector, fence, CAS the handle (`content_ver + 1`,
new offset — `struct_ver` unchanged, so the bookends stay matched). The only outstanding
step is the timestamp, marked by `ts == kNullTs`.

**Split.** *Before* the handle CAS: write the new node, both vectors, and `next_id` /
`k_min_next` into the existing node's new vector. Then fence and CAS the handle
(`struct_ver + 1`, new offset) — the bookends are now mismatched. Then, by the writer or any
helper, in **any order**: CAS `ts` from `kNullTs`, CAS `next_id`, CAS `k_min_next`. Finally
CAS `tail_struct_ver`, which closes the window and must be last.

Every step is a CAS whose result the caller ignores: a failure means another thread already
performed it, which is as good as doing it oneself. Concurrent splits on one node therefore
serialise through the marker — a second writer sees the mismatch and must help the first to
completion before starting its own.

`tail_struct_ver` is 4 bytes but RDMA CAS is 8, so it is swapped as the word it shares with
`level`. `level` never changes after creation, and including it makes the CAS fail if the
node is not the one we read — a free extra check. **If those two fields are ever reordered,
the final CAS starts clobbering a live field**; there is a test asserting they stay
adjacent.

### When a reader helps, and when it does not

- **Hopping past a node** consults only where its range ends and what comes next, never its
  entries, so it needs no help. The vector's descriptor is preferred over the header's
  whenever it is populated, which makes such a reader immune to the propagation window
  without depending on any ordering between the header's CASes.
- **Using a node's entries** (`find_lte`) requires the version to be settled first. Using
  contents whose timestamp is unfixed would leave that write unordered against this read.

An earlier draft of this design imposed an order on the two header CASes
(`k_min_next` before `next_id`). **That is unnecessary** — the analysis behind it assumed a
reader that trusts `next_k_min` without checking the bookends, which is not a legal reader.

### Why the timestamp is written after visibility

Not incidental: load-bearing. A writer that publishes `ts` atomically with the data lets a
reader holding a *larger* snapshot miss a write it should have seen — it reads before the
CAS lands, and the write's timestamp is already fixed below the reader's. Commit-wait does
not close that; it only guarantees the timestamp is past by the time the write is visible,
which says nothing about a reader that started earlier. Spanner closes it with locks; a
one-sided RDMA design has none, so the version becomes visible with an *undetermined*
timestamp and readers resolve it.

A helper stamps with its own clock, which orders the write at the moment somebody first
needed it. A reader whose snapshot then falls below that timestamp walks `old_ver` for an
older version, which is exactly what the chain is for.

---

## 3. Arenas and allocation

Two flat arrays per memory server, identical on every replica:

```
[ node arena   : NodeRecord, 64 B each  ]
[ vector arena : VecRecord, 320 B each  ]
```

A node's identity is its index into the first — that is what a `RemoteAddr` is, and why it
is replica-independent and must be resolved against a particular connection's
`remoteBuf()`. A version's identity is its index into the second, which is the handle's
32-bit offset.

Both are per-client bump allocators over private stripes, with no free list:
`invariants.md` §5 puts epoch GC out of scope. They are separate because they are consumed
at wildly different rates — a node only by a split, a vector by **every write**.

**The vector arena is the binding resource.** With no reclamation it must hold one vector
per write for the whole run, so arena size bounds run length rather than the other way
round. At 320 B that is roughly 32 GB cluster-wide for 10⁸ writes. Both sizes are printed at
startup and the vector one names the write cap explicitly.

Reserved ids: `0` is null and never allocated; `1 .. kMaxLayers` are the per-level heads in
level order; `kMaxLayers + 1` is the initial data node. Fixing heads at known ids is what
makes A1 cheap — `set_head_remote_addrs()` needs no discovery and no memstore round trip,
only a barrier until the initialising client has written the records.

**Orphan flags mean different things on the two sides.** Remotely, heads are *not* orphans:
I4's `is_orphan` means "reachable only by walking `next`", and every head but the root is a
down-pointer target. In the cache, heads *must* be flagged orphan, because `verify_index()`
only tolerates an unreferenced node in a layer if it is one (interface doc §3). Both are
right; they answer different questions. Easy to trip over when comparing the selftest's
orphan count against the cache's.

### The offset hint, speculated inside the quorum read

A node read is a header and then the vector its handle names, and those are
ordinarily serialised because the offset is not known until the header lands.
The hint breaks that dependency: with a guess, `readNode` issues the vector read
*alongside* the header reads — replica 0's header and the speculation chained on
one queue pair, one doorbell, one completion — and `readVec` is then answered
from it with no round trip at all.

The policy lives in `ds_quorum.hpp` rather than in the replica set, because
validating a guess means knowing which handle won the quorum. Validation is the
strict reading of L4: accepted only when **replica 0 voted with the winning
handle** and the guess equalled that offset. A vector is write-once and written
to every replica before the handle CAS publishes it, so a correct guess would
give identical bytes from any replica that has it — the strict check costs
nothing and does not lean on that.

**A wrong guess now costs a wasted 320-byte read and no latency**, because the
speculation rides in the same doorbell as the headers and the real read lands
where the unspeculated one would have. That is the trade the toggle exists to
measure: rNIC and PCIe bandwidth against round trips.

Measured on the cluster over the selftest script, identical at one and three
servers since the speculation is per-read rather than per-replica:

```
speculation:  153 reads speculated, 140 hit / 13 miss;
              140 vector read(s) served without a round trip
```

**The 13 misses are exactly the 13 commits.** A write moves its node's vector,
so the next read of that node necessarily misses — which means the hint misses
once per published version and never otherwise. That is the floor: on this
workload it is as accurate as any such hint could be. It also makes the
write-heavy prediction concrete rather than hand-waved — miss rate tracks the
*write* rate, so the crossover is where writes stop being rare.

### The offset hint

Header and vector reads are ordinarily serialised, because the offset is not known until
the header lands. A client-local table of last-seen offsets, direct-mapped by node id, lets
both go out in one doorbell batch when it has a guess.

A wrong guess costs a wasted 320-byte read — bandwidth, a work request and a completion —
but **not latency**, since the real read lands where the unspeculated one would have. So
`--offset-hint` trades rNIC and PCIe bandwidth, which is the scarce resource under
write-heavy load, and the hit rate is instrumented so the crossover is measured rather than
assumed. It favours read-heavy workloads; under write-heavy load every write moves the
vector and the guess is usually wrong.

It deliberately does **not** live in the cache. `install_data_entry()` ignores `insert()`'s
"already present" return, so cache entries are write-once: a hint packed into a
`REMOTE_ADDR` could never be refreshed, installed on first touch and stale forever after
the first write. It would also undercut the interface doc §5 additivity argument, which
rests on node addresses being stable.

---

## 4. Superseded proposals, and why

Recorded so they are not re-proposed.

| Proposal | Where | Superseded because |
|---|---|---|
| Node layout with `offset` and bookended `content_ver` | interface doc §0 | Vectors moved out of line; `content_ver` has no tail copy (§1) |
| F2 writes `next_dn` *after* the CAS | invariants.md §3, §7 S4 | Correct as written — the header's bookends make the intermediate state detectable. An earlier revision of this side moved the link into the vector to avoid a window that does not exist; that cost a vector read on every hop and was reverted |
| Two inline CoW slots for a one-RDMA node read | this side, earlier revision | Bounds version history at one step, which cannot serve a range query (§1) |
| `k_min_next` CAS'd before `next_id` | this side, earlier revision | Unnecessary; any order is safe for a reader that checks the bookends (§2) |
| Widen versions to 16/32, dropping the offset from the handle | this side, earlier revision | The offset needs 32 bits, so `14/18/32` stands (§1) |
| Publish `ts` atomically with the data | this side, earlier revision | Lets a reader with a larger snapshot miss a visible write (§2) |

---

## 5. What is built

| Component | File | State |
|---|---|---|
| Node and vector layout, handle encoding | `src/ds_node.hpp` | done, tested |
| Arenas, allocators, offset hint | `src/layout.hpp` | done, tested |
| Bootstrap of the initial structure | `src/ds_bootstrap.hpp` | done, tested |
| I1–I4 structural verifier | `src/ds_verify.hpp` | done, tested (rejects 18 hand-broken structures) |
| Remote descent + helping | `src/ds_descend.hpp` | done, tested |
| Point-read orchestration | `src/ds_get.hpp` | done, tested against the real cache |
| Cache glue | `src/ds_cache.hpp` | done |
| Blocking RDMA + `RdmaOps` | `src/ds_rdma.hpp` | written, compiles, validated only via `--selftest` |
| **F1 insert, F2 split** | `src/ds_insert.hpp` | done, tested |
| **Write orchestration + Update_Index** | `src/ds_put.hpp` | done, tested |
| `--selftest` body | `src/ds_selftest.hpp` | done; templated so it runs off-cluster too |
| Chained work-request batches | `src/ds_batch.hpp` | done, tested |
| CAS-ABD quorum (L1-L5) | `src/ds_quorum.hpp` | done, tested against a fake replica set; not yet run on 3 servers |
| Future-based (pipelined) path | — | not started; blocking helpers are sequential, so the offset hint's doorbell batching is not yet realised |
| 3-way replication | `src/ds_quorum.hpp` | done; passes on 3 servers with `WRITE PASS`, readback 9/9 and I1-I4 holding |
| Deletes, reclamation, range queries | — | deferred (A7, A9, A10) |

### How the write path is split across the two headers

`ds_insert.hpp` is the mechanism and knows nothing about levels: `insertEntry` is F1,
`splitAt` is F2, `insertWithOverflow` is F1 with a capacity split behind it. All three are
level-agnostic, because a split of an index node and a split of a data node are the same
operation on the same layout.

`ds_put.hpp` is the policy, and is where height enters — the climb of §5, plus the one
`mirror_insert` call. It is the write-side twin of `ds_get.hpp`, and like it composes the
cache and the wire without either calling the other.

Two properties worth knowing before changing either:

- **The helping protocol lives in one place.** `settleNode()` in `ds_descend.hpp` is shared
  by the descent and by both write primitives rather than copied into each. A write must
  settle a node *and find it stable* before CASing its handle, because a stable node has had
  any split descriptor propagated into its header — which is what makes it safe for the new
  version to carry no descriptor of its own.
- **Every step is idempotent, which is what makes a partially applied climb safe to
  abandon.** `splitAt` at a key that is already a node's `k_min` is a no-op returning that
  node, so a retry after the data split succeeded but a level-1 split lost its CAS re-derives
  the same structure instead of building a second copy. That no-op path must still apply the
  seed: skipping it made a repeated height-driven put report success while never writing the
  value, which is a silently stale payload over a structurally correct index.

### Round trips, and why writes go out as batches

The operations of §2 are sequences -- F1 is a write then two CASes, F2 is three
writes then six. Issued one at a time each costs a round trip, and at three
replicas a per-replica loop multiplies that, so replication would cost *latency*
rather than only bandwidth. That is the wrong shape: the whole reason the cache
exists is that round trips are the scarce thing.

So the Ops surface takes **batches** (`ds_batch.hpp`) at the points where a
sequence is issued. A batch becomes a linked list of work requests posted with
one `postSend`, which is one round trip for the whole chain -- the idiom
`swarm-kv/src/unreliable_maxreg.hpp` uses. Replicas are fanned out the way
`chimera/src/put_future.hpp` does it: every replica's chain is posted *before*
any of them is drained, so the three overlap.

Measured on the cluster over the `--selftest` script, nine puts, one client:

| | 1 server | 3 servers | ratio |
|---|---|---|---|
| **round trips** | 16 | **16** | **1.0x** |
| operations carried | 57 | 133 | 2.3x |
| bytes written | 5,312 | 15,936 | 3.0x |
| CAS total | 38 | 114 | 3.0x |
| replica reads | 335 | 657 | 2.0x |

The round trips do not move; the bytes triple. That is the property to preserve,
and `quorum_test.cc` asserts it directly rather than leaving it to be
re-derived. The figures match what the fake replica set predicts exactly, which
is the cross-check worth having: the local tests are not a rough model of the
cluster here, they are the same arithmetic.

**The 2.0x on reads rather than 3.0x is L4 working.** A header read fans out to
all three replicas, but the vector is read only from a replica that voted with
the winning handle: 161 x 3 + 174 x 1 = 657. So replication triples the header
traffic and leaves the 320-byte vector reads alone, which is the opposite of
where the cost would have fallen without L4.

Two things the chain buys beyond speed:

- **Ordering becomes structural.** F2 needs `tail_struct_ver` CAS'd last, and
  that used to hold because the calls appeared in the right order in the source.
  Same-QP RC ordering now enforces it.
- **The fence has somewhere to live.** F3 needs the staged writes visible at the
  remote HCA before the publishing CAS. With blocking helpers that came free --
  each waited for its own completion, which is strictly stronger. In a chain
  nothing waits, so the publishing CAS carries `IBV_SEND_FENCE`. **dory never
  sets that flag** (nothing in `conn/`, `swarm-kv/`, `chimera/` or `fusee/`
  does), and `prepareSingleCas` *assigns* `send_flags` rather than OR-ing, so it
  has to be added after preparing. F3 was, until now, an invariant no code
  implemented.

### Replication: two decisions beyond what invariants.md spells out

**The commit attempts every replica, not a fixed majority subset.**
`DsState::quorum_indices` picks a fixed majority per client and the legacy
register path uses it; doing that here deadlocks. Client A holding `{0,1}` commits
and leaves R2 lagged; client B holding `{1,2}` then CASes the value it read on
`{1,2}`, R1 succeeds, R2 fails because it still holds the old handle -- one
success, short of majority, forever, because nothing in that loop repairs R2. So
L1's "2/3 CAS success" must mean *attempt three, require two*, which also demotes
L5's writeback to the accelerant §4.1 claims it is rather than something
correctness quietly depends on.

**The read takes the highest tag with majority support**, which is stronger than
L2 as written. L2's max-tag rule is right whenever every distinct handle is either
committed or absent. A *partially* applied commit breaks that: two writers bumping
from the same predecessor produce two different handles with the **same** tag,
since both bump `content_ver` once, so L3's order cannot separate them and picking
the uncommitted one would return contents no majority ever held. Reading every
replica makes it decidable. **This is a deliberate strengthening of L2 and
`invariants.md` should say so** -- flagged rather than adopted silently, since
that document governs both halves.

### What DS_N_REPLICAS actually does: nothing

`invariants.md` §9 describes it as the toggle selecting a "single-replica code
path". In this half it selects nothing — `kNumReplicas` is a constant with a
static_assert and drives no logic. **The replication factor is the number of
memory servers the run connects to**, and `QuorumOps` derives its majority from
that at runtime, so at one server the majority is one and every quorum rule
degenerates to the direct operation.

That is deliberate, and better than two code paths: "zero overhead at N=1" is
true by construction rather than by keeping two implementations in step, and it
is what makes the comparison above a single binary run twice rather than two
builds. The toggle is left as a declared expectation rather than wired into an
assert, because an assert would forbid exactly that comparison.

### Height-driven splits

Confirmed with the cache author: **splits are height-driven, not capacity-driven.** A key of
height `h` is split in at levels `0 … h-2` whether or not those nodes are full, and inserted
as an ordinary entry at `h-1`. Capacity overflow is a *separate* mechanism producing an
orphan with no parent, never a parented boundary. Height is drawn geometrically with
`p = 1/TARGET_IDX_RATIO = 1/8` at `IDX_EXP = 3`, clamped to `[0, layers]`; height 0 alters
no index structure and needs no cache call at all.

---

## 6. Open questions

1. **The quorum tag at `DS_N_REPLICAS=3`.** `content_ver` orders content states, so L2/L3
   work. But if `ts` were ever to become the tag, it lives in the vector and a tag read
   would drag the vector along. Fine as is; revisit with replication.
2. **The helper's clock.** `RdmaOps::now()` is `steady_clock`: monotonic per process, which
   keeps `old_ver` locally ordered and orders **nothing across clients** — its epoch is boot
   time, so two nodes' values differ by their uptime difference. A10 needs a disciplined read
   and a measured ε. A wrong clock here produces wrong range-query results rather than a
   crash.

   **Now measured** — see [`clock-measurements.md`](clock-measurements.md). Relative TSC
   frequency error across the 12 testbed nodes is **14.31 ppm** (noise floor 0.43 ppm), which
   from a single sync point accumulates to **143 µs over a 10 s run** against ~2 µs
   operations. So `rdtsc` plus a one-shot reset does not work: the reset fixes offset and
   leaves rate error to accumulate. A disciplined `CLOCK_REALTIME` read through the vDSO
   costs only 11 ns more than raw `rdtscp` (31.4 ns vs 20.1 ns), so keeping `rdtsc` and
   dropping the one-shot reset is not a trade-off. Three consequences, which differ and
   should not be conflated: **no effect** on point reads, F1 or F2, since nothing compares
   timestamps; **detectable** in the verifier, whose `old_ver` chain check becomes a
   cross-machine clock assertion once F1 runs multi-client; **a silent wrong answer** only
   in A10. Step 3/4 is therefore unblocked.
3. **A7 / A9** — deletes and reclamation, both deferred deliberately.
