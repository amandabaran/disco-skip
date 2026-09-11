# Clocks on the testbed: what we measured, and what it decides

Measured to settle open question 2 in [`remote-design.md`](remote-design.md) §6 — what
`RdmaOps::now()` should read — and to give A10 (`cache-remote-interface.md`) a real ε
instead of a quoted one. Every number here is from this testbed, and the harness that
produced them is checked in, so they can be regenerated rather than trusted.

**Reproduce:**

```sh
cd /users/adb321/disco-skip-artifacts/experiments/clock
./measure_tsc_rate.sh 30 3          # 30 s x 3 windows, all 12 nodes, concurrent
```

Raw data for the run quoted below: `experiments/clock/results/20260910T230809Z/`.

---

## 1. The headline

**The relative TSC frequency error across the 12 testbed nodes is 14.31 ppm.** From a
single synchronisation point that accumulates to **143 µs of skew over a 10 s run**,
against operations that take about 2 µs.

That one number is what decides the timestamp design, so the rest of this document is
mostly about how much weight it can bear.

## 2. The hardware

Uniform across all 12 nodes (`w1`–`w12`), from `*.env` in the results directory:

| | |
|---|---|
| CPU | Intel Xeon E5-2450 0 @ 2.10 GHz |
| TSC flags | `constant_tsc`, `nonstop_tsc`, `rdtscp` |
| `clocksource` | `tsc` |
| PTP devices | `/dev/ptp0`, `/dev/ptp1`, `/dev/ptp2` |
| `ptp4l` | **not running** |
| NTP synchronised | yes |

Two consequences worth pulling out, because they shape everything below:

- **The TSC is invariant.** `constant_tsc` + `nonstop_tsc` means it ticks at a fixed rate
  regardless of P-state and does not halt in idle. So a TSC-based timestamp is sound
  *within* a machine, which is the half of the rdtsc plan that works.
- **`clocksource` is already `tsc`.** So `clock_gettime()` through the vDSO *is* a
  `rdtsc` plus a scale-and-offset maintained by kernel timekeeping. This matters later:
  choosing a disciplined clock does not mean giving up rdtsc, and does not mean the ~1 µs
  PCIe read of a NIC clock that `cache-remote-interface.md` §9 worries about. `ptp4l`
  disciplines the NIC PHC and `phc2sys` steers the *system* clock, which is TSC-backed.

## 3. Method, and why it is the right measurement

A one-shot reset corrects **offset**. What it cannot correct is **rate**: two invariant
TSCs tick at constant but *different* frequencies, and that difference accumulates for the
whole run. So the quantity that decides whether reset-once is viable is not the accuracy of
the reset — it is the relative frequency error between machines.

`CLOCK_REALTIME` is NTP-disciplined, and NTP corrects frequency as well as offset, so over
a window of tens of seconds each node's own `CLOCK_REALTIME` is an accurate *interval*
reference. Dividing a `rdtscp` delta by it gives that node's TSC frequency; differencing
two nodes' frequencies gives their relative error. All nodes are measured **concurrently**,
so they share one wall-clock interval and similar thermal conditions and the differences
are not confounded by having been taken at different times.

**The noise floor is reported alongside the signal**, because residual NTP frequency error
is the dominant error term and the claim is worthless without it. Three back-to-back
windows per node; the within-node spread across windows is the error bar.

## 4. Results

30 s × 3 windows, 12 nodes, concurrent. Reference is the fleet median,
2,100,009,528 Hz.

| host | mean Hz | window spread Hz | vs median |
|---|---|---|---|
| w1 | 2,100,018,839.2 | 25.5 | +4.43 ppm |
| w2 | 2,100,018,968.3 | 23.8 | **+4.50 ppm** |
| w3 | 2,100,002,299.6 | 803.2 | −3.44 ppm |
| w4 | 2,099,995,866.4 | 870.8 | −6.51 ppm |
| w5 | 2,100,018,685.9 | 24.7 | +4.36 ppm |
| w6 | 2,100,012,781.5 | 840.8 | +1.55 ppm |
| w7 | 2,100,003,976.8 | 20.8 | −2.64 ppm |
| w8 | 2,100,014,595.0 | 25.4 | +2.41 ppm |
| w9 | 2,099,999,881.8 | 42.4 | −4.59 ppm |
| w10 | 2,100,010,401.7 | 19.4 | +0.42 ppm |
| w11 | 2,099,988,921.6 | 912.4 | **−9.81 ppm** |
| w12 | 2,100,008,654.9 | 15.0 | −0.42 ppm |

- **Worst pair: w2 vs w11 = 14.31 ppm.**
- **Noise floor: 0.43 ppm** (worst within-node window spread). The signal is 33× the
  floor, so the spread is resolved and not a measurement artifact.
- Every node is within ±10 ppm of the median, which is unremarkable for commodity server
  crystals. The finding is not that this hardware is bad; it is that 14 ppm is *ordinary*
  and still far too much.

Accumulated skew for the worst pair, from one synchronisation point:

| elapsed | skew | in operation widths (~2 µs) |
|---|---|---|
| 0.14 s | 2 µs | 1 |
| 1 s | 14.3 µs | 7 |
| **10 s** | **143 µs** | **72** |
| 60 s | 859 µs | 429 |

### The cost of reading a clock

Measured on w5, 2M iterations each:

| source | cost | note |
|---|---|---|
| `rdtscp` | 20.1 ns | raw counter, undisciplined |
| `clock_gettime(CLOCK_MONOTONIC_RAW)` | 23.5 ns | raw TSC via vDSO |
| `steady_clock::now()` | 24.9 ns | what `now()` does today |
| `clock_gettime(CLOCK_REALTIME)` | 31.4 ns | **NTP/PTP-disciplined**, via vDSO |

So a disciplined, cross-machine-comparable read costs **11.3 ns more than raw `rdtscp`** —
0.6% of a 2 µs operation — and 6.5 ns more than the current `steady_clock`. This answers
the question `cache-remote-interface.md` §9 says must be answered before any ε goes in the
paper: a thread reads a disciplined time in **31.4 ns through the vDSO**, not a microsecond
through PCIe.

## 5. What this does to the reset-once plan

The plan was: `rdtsc`, a reset message before the experiment to establish a common origin,
and a resync if a large skew is detected. Taking the parts separately:

**`rdtsc` as the source is right, and today's code is worse than it.** `now()` currently
returns `steady_clock`, which is `CLOCK_MONOTONIC` — whose epoch is *boot time*. Two nodes'
values therefore differ by their uptime difference, which is hours or days, not
microseconds. So the reset plan is a large improvement on the status quo, and the instinct
behind it is sound.

**The one-shot reset is the part that does not survive.** It fixes offset and leaves the
14.31 ppm rate error to accumulate: 143 µs over a 10 s run, which is 72× an operation.
Ordering by such a timestamp is not approximately right, it is uninformative for anything
that happened within the last 143 µs — roughly the last 70 operations on a node.

**"Resync if a large skew is detected" cannot close it**, for two reasons:

1. Detecting skew requires comparing clocks, which requires the same synchronisation
   machinery whose accuracy is the question. The gross cases are detectable; the ε-scale
   ones are invisible by construction, and those are exactly the ones that reorder
   operations.
2. Here the skew exceeds the operation latency after **0.14 s**, so a tripwire would fire
   more or less continuously. Resyncing on every trip *is* continuous synchronisation —
   i.e. reimplementing PTP, but without rate correction or filtering.

### A concrete inversion, with real values

Per the design's own rule, a helper stamps a version with **its own** clock, so one
`old_ver` chain can carry stamps from several machines. Take data node **4210**, a 10 s run,
both machines reset at true *t* = 0, and w11 as the reference:

- True *t* = 8.99990 s: a helper on **w2** settles the version at vector offset **81004**.
  w2's clock is ahead by 14.31 ppm × 8.9999 s = 128.8 µs, so it writes
  `ts = 9,000,028,790`.
- True *t* = 9.00000 s — **100 µs later** — a client on **w11** completes an F1 insert of
  key 5100, producing the version at offset **88123** with `old_ver = 81004`. A helper on
  w11 settles it and writes `ts = 9,000,000,000`.

The chain is now `88123 → 81004`, newest first, with the newer version carrying the
*smaller* timestamp. [`ds_verify.hpp:242`](../src/ds_verify.hpp) checks
`older.ts >= newer_ts` and reports:

```
node 4210: old_ver chain is not decreasing in ts -- offset 81004 has ts
9000028790 at or above its successor's 9000000000
```

The two writes were genuinely 100 µs apart in real time. A 128.8 µs skew inverts them. And
this is not a narrow race: any two versions of one node separated by less than 128.8 µs of
true time can invert, which at ~2 µs per operation is a window about **64 operations**
wide, widening as the run goes on.

### Severity, stated as three different things

These get conflated, and they are not the same:

| path | effect | why |
|---|---|---|
| Point reads, F1, F2 — **Step 3/4** | **no effect** | Nothing compares timestamps. `casTs(kNullTs → now())` ignores the value's magnitude; ordering comes from the CAS and the version tags, per `cache-remote-interface.md` §9. |
| The I1–I4 verifier | **detectable, loud** | `checkOldVerChain` errors exactly as above. |
| Range queries (A10) | **wrong answer, silent** | A snapshot walks back for the version with `ts <= T` and stops at the first that qualifies; an inverted chain makes it stop early and serve the wrong version. `ds_verify.hpp:216` already says this is "the failure mode a range query cannot detect for itself." |

So: **this does not block Step 3/4** — the write path needs no cross-machine clock. But it
does bite before range queries exist, because the verifier's `ts` check becomes a
cross-machine clock assertion as soon as F1 lands and two client machines write the same
node. Worth knowing, so that failure is read as a clock problem and not a chain bug.

### It also decides whether commit-wait is affordable

`cache-remote-interface.md` §9 argues that commit-wait converts ε-linearizability into
linearizability outright, at ε of added write latency. Against a ~2 µs write:

| ε | commit-wait cost | verdict |
|---|---|---|
| 143 µs (reset-once, 10 s run) | +7200% | prohibitive |
| ~1 µs (commodity hardware-timestamped PTP, one rack) | +50% | arguable |
| ~100 ns | +5% | a graph, not an argument |

The bottom row is the ~5% the interface doc already projects. So reset-once does not merely
weaken the claim to "ε-linearizable with a large ε" — **it removes the option of making the
claim unconditional**, because commit-wait at 143 µs is not a cost anyone would pay. That is
the strongest argument here, and it is stronger than the correctness argument.

## 6. Options

1. **Discipline the system clock and read it through the vDSO.** `ptp4l` on `/dev/ptp0`
   (hardware timestamping is present on every node), `phc2sys` steering the system clock,
   `now()` reading `CLOCK_REALTIME`. Keeps `rdtsc` as the underlying source, adds
   continuous *rate* correction from the kernel, costs 11 ns per read over raw `rdtscp`,
   and yields an ε that can be measured directly from `ptp4l`'s own offset statistics
   rather than argued for. **This is the cheapest thing that works, and it is not in
   tension with the rdtsc plan — it is the rdtsc plan with the reset replaced by
   continuous discipline.**
2. **RDMA fetch-and-add on a well-known counter** for snapshot acquisition — already
   recorded in `cache-remote-interface.md` §9. One round trip (~2 µs), a genuine total
   order, no clock and no ε at all, server stays passive, HCA-offloaded. At 8 machines the
   single-cacheline contention is very likely fine. This makes the whole ε discussion
   optional rather than load-bearing, which is worth a lot in review.
3. **Software rate-corrected sync over RDMA** — resync periodically and fit frequency as
   well as offset. This is what HUYGENS (NSDI'18) and Sundial (OSDI'20) do, and their
   contribution is precisely that naive offset estimation is insufficient. Defensible, but
   it is a project of its own.

**Recommendation, for your decision rather than mine** — A10 is an open item and
`cache-remote-interface.md` is explicit that these are not to be resolved one-sidedly:

- Take option 1 for `ts`. It is a one-line change at the `now()` seam plus cluster
  configuration, it makes the verifier's chain check meaningful across machines, and it
  turns ε into something measured.
- Seriously consider option 2 for A10 snapshots. Being able to say "range queries are
  linearizable, no clock assumption" is a much better position than any ε.
- Keep the reset idea only as the offset half of option 1, never on its own.

Either way, `now()` stays behind the `RdmaOps` seam, so this can be decided after Step 3/4
without re-laying out anything.

## 6a. The clock-free alternative, costed

Option 2 above (an RDMA fetch-and-add on a global counter) can be priced on this hardware
rather than guessed at. All numbers w6 → w1, `mlx4_0`, 8-byte operations.

**The NICs are ConnectX-3 on `mlx4`** (`MT27500`, fw 2.42.5000), which matters: RDMA
atomics on this generation are handled by a serialising unit in the NIC.

| operation | latency (typical) | p99 | throughput, 1 QP | throughput, 32 QPs |
|---|---|---|---|---|
| `ib_read_lat` / `ib_read_bw` | 1.92 µs | 2.28 µs | 7.05 Mops/s | 5.14 Mops/s (8 QPs) |
| `ib_atomic_lat` / `_bw`, FETCH_AND_ADD | **1.89 µs** | 2.07 µs | **2.70 Mops/s** | **2.70 Mops/s** |

Two results, and the first is the opposite of what the RDMA-atomics folklore suggests:

- **Latency is free.** An uncontended FAA costs the same as an 8-byte read — 1.89 µs
  against 1.92 µs, with a *better* p99. Acquiring a snapshot is therefore one ordinary
  round trip. The common claim that RDMA atomics are "2× a read" is about throughput, not
  latency, and conflating the two overstates the cost of this option.
- **Throughput is the constraint, and it does not scale.** 2.70 Mops/s at one QP,
  2.70 at eight, 2.70 at thirty-two — flat to three decimal places. Adding queue pairs buys
  nothing, which is the signature of a single serialising atomic unit rather than a per-QP
  limit. Against 7.05 Mops/s for reads, **atomics run 2.6× slower in throughput and
  identical in latency.**

### Why that ratio decides something

A global counter lives at one address on one NIC, so it does not scale with the four memory
servers while the data path does. `workloads/oops-workloade*` is `scanproportion=0.95`, and
every scan needs one snapshot, so on that workload the FAA rate is essentially the operation
rate.

Rough sizing: a YCSB-E scan costs a 4-level traversal plus the data nodes in range, call it
~6 round trips. Four memory servers at ~7 Mops/s of reads support on the order of 4.7 M
scans/s, each wanting one FAA — against a 2.7 Mops/s counter. So the counter would cap
scan-heavy throughput at roughly **1.7× below what the data path can sustain**.

That is a real cost, and also a modest and quotable one. It is not a reason to reject the
option; it is the number that makes "we also implement a variant with no timing assumption
at all, and it costs 1.7× on scans" a sentence the paper can defend.

### Measured on the real configuration

`ib_atomic_bw` cannot answer the question that matters, because it pairs one client process
with one server process, each with its own region — its numbers describe *independent*
addresses. `experiments/rdma-counter/` is a purpose-built program that puts every QP on
every client machine onto **one 8-byte word in one server process**, with a `--stride`
control that gives each QP its own cache line instead. Full write-up in
`experiments/rdma-counter/RESULTS.md`; aggregate Mops/s:

| op | addressing | 1 mach | 2 mach | 4 mach | 8 mach |
|---|---|---|---|---|---|
| FAA | shared word | 2.7074 | 2.6412 | 2.6223 | **2.6928** |
| FAA | own cache line | 2.6972 | 2.6932 | 2.6960 | 2.6724 |
| READ | shared word | 6.0548 | 12.2683 | 17.8013 | **26.4962** |
| READ | own cache line | 6.3760 | 12.9193 | 24.1569 | 26.4835 |

**The FAA ceiling is ~2.7 Mops/s and it is not same-address contention.** The control
settles it: own-cache-line is equally flat. The limit is the responder NIC's atomic unit, so
sharding the counter within a server buys nothing, and — the good news — eight machines on
one word cost no more than one machine on it. The ceiling is flat rather than degrading.

**The reader path is a non-issue.** A single shared word serves reads at 26.50 Mops/s
against 26.48 for distinct lines: no same-address penalty, ~10× the FAA ceiling. The worry
above about per-scan counter reads eating a memory server's read budget was based on a
7 Mops/s single-QP figure and was too pessimistic — at ~4 M scans/s it is ~15% of capacity.

**The binding workload is the write-heavy one, which inverts the expectation above.** The
FAA is on the *write* path, so `workloade` (5% insert) has ~13× headroom while
`workloada` (50% update) caps aggregate throughput at roughly **5.4 M ops/s**. Whether that
binds depends on what the remote write path sustains, which cannot be known until F1 exists.

Note also that the older `rdma-scaling-tests` sweep appearing to scale to ~21 Mops/s was on
**c6525-25g** — 25 GbE RoCE, later silicon whose atomics do scale. Do not carry it over.

**Batching the FAA is not a free lever.** Reserving a block per writer would cut the FAA
rate but breaks the property that makes reading the raw counter safe: a reader seeing
`C = 164` would infer everything below 164 is published while the reserving writer has
published three of sixty-four. That is §5's backfill hazard again, so batching needs a
published-watermark rather than just a reservation counter.

**One implementation detail, checked rather than assumed.** The IB spec describes atomic
operands in big-endian network order, so the expectation is that a counter driven by FAA and
sampled by RDMA READ needs `be64toh()`. **It does not, on this hardware.** After 522046 FAAs
the word reads 522047 natively, and an RDMA READ of it returns the same native value — three
views agreeing (client count, server CPU, RDMA READ). mlx4 does the read-modify-write in
host byte order on x86, so `ts` needs no swap. This is a device property rather than a
guarantee, so `--verify` keeps it measurable if the adapter generation changes.

**Also note the older CSVs are not this cluster.** `faa_latency.csv` (4.10 µs) and
`faa_throughput.csv` are dated with the **c6525-25g** run — 25 GbE RoCE hardware. Do not
cite them for the r320/ConnectX-3/InfiniBand testbed; the FAA latency here is 1.89 µs, not
4.10 µs.

### Vector clocks are the wrong shape for this

Raised as a possible cheaper option; recording why it is not, so it is not revisited:

- **No room, and a re-layout.** `VecRecord` carries 8 bytes of `ts`. A vector clock over 8
  machines is 64 bytes and over 128 threads is 1 KB, against 256 bytes of payload in a
  320-byte record. Adding it means re-laying-out every already-written structure.
- **Partial order, but a snapshot needs a total one.** The `old_ver` walk asks "is this
  version's stamp ≤ T". For two concurrent writes a vector clock answers "incomparable",
  which does not say whether to stop walking or keep going. Any deterministic tiebreak
  restores a total order that is no longer the vector clock's.
- **Nobody maintains them.** Vector clocks are kept by processes that observe each other.
  Here writers are one-sided RDMA clients that never communicate; reading the cluster's
  vector state means reading all N clients' slots — `invariants.md` §5 E2's epoch read, so
  N round trips where the FAA needs one. Strictly worse.
- **A helper's clock is the wrong clock.** Per §2 of the design a *helper* stamps the
  version, and a helper's vector clock describes the helper's causal history, not the
  writer's.

**Hybrid logical clocks are the idea from that family that does fit**, and are worth
considering: 8 bytes, a total order that respects causality, physical time in the high bits.
The part that suits this design is that a reader which observes a larger `ts` advances its
own clock — and these readers already read `ts`, so skew self-corrects in the parts of the
structure actually being touched, with no synchronisation protocol. CockroachDB uses exactly
this. It does not remove the need for an uncertainty window on linearizable reads, so it
complements a disciplined clock rather than replacing it.

**One cheap consequence worth taking regardless of the A10 decision.** The verifier
inversion in §5 is fixable with no clock infrastructure at all: have whoever stamps take
`max(own_clock, predecessor_ts + 1)` instead of `own_clock`. That makes the `old_ver` chain
monotonic *by construction* at any skew, removing the "detectable" failure category
entirely. It does not make cross-machine snapshots correct — that still needs real time —
and it costs either a read of the predecessor's `ts` or somewhere to stash it, so it is a
small design decision rather than a free win. But it decouples chain monotonicity from clock
quality, which is worth having before F1 runs multi-client.

## 7. What still needs measuring

- **ε itself.** The 14.31 ppm figure bounds what reset-once loses; it is *not* ε for a
  disciplined clock. With `ptp4l` running, ε comes from its offset statistics and should be
  measured on this testbed, not quoted from a datasheet.
- **Drift stability.** One 90 s sample. Crystal frequency moves with temperature, so the
  spread should be re-measured over a full run length and under load before the number is
  final. Nodes w3, w4, w6 and w11 showed ~0.4 ppm of window-to-window variation against
  ~0.01 ppm on the rest, which is probably NTP slewing but has not been chased down.
- **The violation rate**, if an ε-parameterised claim is made. Per
  `cache-remote-interface.md` §9 the right quantity is the rate of conflicting,
  non-overlapping operation pairs separated by less than ε — not ε against operation
  duration. That needs the measured arrival process.
