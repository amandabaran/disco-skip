# Building, deploying and running disco-skip

Everything below was learned the hard way. Each item cost at least one wasted round trip.

## Two repos, easily confused

| Path (on toad) | Repo | Branch |
|---|---|---|
| `/users/adb321/disco-skip-artifacts` | artifacts: harness, scripts, deploy | `disco-skip-deploy` |
| `/users/adb321/disco-skip-artifacts/bin/disco-skip` | disco-skip: the code | `cache-glue` |

The second is nested inside the first. `git checkout` in the wrong one is the single most
common mistake. Note also that `~` on toad is `/home/amanda` while the checkout is under
`/users/adb321`, and the conan cache is under `/home/amanda/.conan` — three different roots.

## Local gate: run this before every cluster build

```sh
make -C disco-skip/tests        # toggle combinations, verifier, traversal, get, strict, stub
make -C disco-skip/include check   # the cache author's own suite; must keep passing
```

`disco-skip/tests` is the off-cluster gate. It covers everything that does not need
libibverbs, and it exists because four separate cluster builds were spent on compile errors.
Two limits worth knowing:

- **The gate now matches the cluster's warning set exactly**, including `-Wstrict-overflow=5`
  and `-Wsign-promo`, at `-O2`. Earlier revisions dropped the GCC-only flags because the dev
  box was a Mac; the build actually runs on toad with the same `g++`, so there is no reason
  to approximate. See the comment block in `tests/Makefile` before weakening anything there.
- `main.cpp` still cannot be compiled off-cluster — it needs lyra, fmt and the dory control
  plane. **So keep logic out of it.** The `--selftest` body lives in `src/ds_selftest.hpp`,
  templated over the reader and the Ops surface, which gets it checked twice off-cluster:
  `tests/rdma_compile.cc` instantiates it against the real `RdmaNodeReader`/`RdmaOps` so it
  type-checks under the strict set, and `tests/selftest_test.cc` instantiates it against
  `FakeOps` and *runs* it. What remains in `main.cpp` is argument parsing and building those
  two objects out of `DsState`.

  This was worth doing: six cluster builds have been lost to compile errors, the last to a
  mistyped `VerifyReport` field in the selftest — about as cheap a mistake as exists, and it
  still cost a full round trip. Anything added to `main.cpp` from here is unverified until a
  build, so put it in a header and instantiate it from `tests/`.

The stub in `tests/dory_stub/` is **compile-only** and moves no data. Never assert behaviour
through it.

## Cluster build

```sh
cd /users/adb321/disco-skip-artifacts/bin/disco-skip && git pull && ./build.py disco-skip
```

This builds only the remote side, which is what you want while iterating. It does **not**
package or deploy anything — see **Deploy** below, and note that `cloudlab_deploy.sh` runs
its own (full, `distclean`) build, so there is no need to do both.

The dory toolchain compiles with `-Werror` and a much stricter warning set than the cache
half is written for. That is handled by marking the cache include directories `SYSTEM` in
`src/CMakeLists.txt`, so diagnostics originating inside them are suppressed while our own
code stays fully strict. **Do not** replace that with blanket `-Wno-error=`; it would weaken
the same checks on our code, and the cache is the other author's half and not ours to edit.

## Deploy

```sh
cd /users/adb321/disco-skip-artifacts && ./cloudlab_deploy.sh
```

`cloudlab_deploy.sh` is the entry point and it does **all** the steps, from
`/users/adb321/disco-skip-artifacts`:

```
build.py distclean buildclean clean && build.py all   # conan stack
bin/dlsm/build.sh clean && build                      # independent CMake
bin/zip-binaries.sh      # build/bin/disco-skip  -> bin/bin.zip
prepare-deployment.sh    # bin/bin.zip + scripts -> deployment.zip
send-deployment.sh       # deployment.zip        -> all 12 nodes
```

**Use it rather than the individual scripts.** `send-deployment.sh` never builds
`deployment.zip` — it ships whatever copy is already on disk, and prints
`Parallel deployment successful across all nodes!` either way. Calling it directly after a
fresh compile shipped a **two-hour-old** binary to all 12 nodes and reported success; the
failure surfaced one step later as `Error in command line: Unrecognized token: --ts`, i.e.
wearing a *code* costume rather than a deploy costume, which is the expensive kind of
misdirection. `prepare-deployment.sh` is the step that rebuilds the payload and the one
that is easy to leave out.

If a full `cloudlab_deploy.sh` is too slow to iterate on — it `distclean`s and rebuilds the
whole conan stack plus dLSM, none of which the remote side touches — the minimum honest
chain is the last four lines above with `build.py disco-skip` in place of the first. Then
verify rather than trust — **by hash**, using the check under *A deploy can silently skip a
node* below. Same reasoning as the freshness check under **Cluster build**: conan prints
`Already installed!` and `Copied 1 file` for a no-op, so neither its output nor a deploy
script's exit status is evidence that the bytes on the workers are the bytes you compiled.

Do not substitute `strings | grep <new flag>` for the hash. It works only when the thing
you grep for really is in the binary: grepping for text that turned out to be a C++ comment
returned `0` and was indistinguishable from a failed deploy, when the deploy had in fact
succeeded. A hash has no such failure mode, and `bin.zip` carries several binaries now
(disco-skip, swarmkv, fusee, and dLSM's three), any of which can be the stale one.

`bin/zip-binaries.sh` **discovers** the built binary rather than hardcoding a path, and fails
loudly if it cannot find one. That matters because `send-deployment.sh` renames
`staging/disco-skip` with `|| true`, so a binary missing from `bin.zip` fails *silently*: the
worker simply has no executable, the tmux window dies instantly, and
`wait-till-completion.sh` reports `can't find window: server1`. A packaging problem wearing a
tmux costume.

There is deliberately **no `LD_LIBRARY_PATH`**: `conanfile.py` builds with `shared=False`, so
the binaries link statically and `ldd` reports nothing missing. The
`export LD_LIBRARY_PATH=/bin/chimera/...` lines at the top of `experiments/*.sh` are dead
twice over — a spurious leading slash, and they run on the gateway while
`remote-invoker.sh` forwards only `DORY_REGISTRY_IP`.

## Run

```sh
./scripts/run.sh disco-skip-exe selftest/vN oops-workloada-uniform 1 1 --selftest 1
```

- **`--selftest 1`, not `--selftest`.** Every toggle in this binary takes a value, because
  `lyra::opt(value, "name")` declares an option with an argument — same as `--cache 1` and
  `--ml 1`, which the artifact scripts already pass.
- The workload argument is ignored: `--selftest` returns before it touches YCSB, which is
  not in the repo.
- Roles come from `proc_id > num_servers`, so **`-i 1` is the server and `-i 2` the client**.
  `run.sh` assigns them; the *client* does the bootstrap and the verification.
- Servers land on w1–w4, clients on **w5**–w12, so `logs/<folder>/client1.txt` is on **w5**,
  not w1. That log is the one to read.

```sh
ssh w5 'cat /users/adb321/disco-skip-artifacts/logs/selftest/vN/client1.txt'
```

Read the teed log, not the tmux pane. Panes can vanish; the log persists.

## Running at three replicas

The replication factor is the **server count**, not a build flag —
`DS_N_REPLICAS` drives no logic (see `remote-design.md`). So both arms of the
comparison are the same binary:

```sh
./scripts/run.sh disco-skip-exe selftest/n1 oops-workloada-uniform 1 1 --selftest 1
./scripts/run.sh disco-skip-exe selftest/n3 oops-workloada-uniform 3 1 --selftest 1
```

The `Quorum` block reports what CAS-ABD actually did. On a healthy cluster with
one client, expect **0 stale votes, 0 tag ties, 0 re-polls and 0 writebacks** —
nothing lags and nothing contends, so none of the interesting paths execute.
That is the point of `tests/quorum_test.cc`: a passing 3-server run proves the
three connections work, and proves nothing about the disagreement handling.

Expected 3-server write phase, against the 1-server numbers:

| | 1 server | 3 servers |
|---|---|---|
| round trips | 16 | 16 |
| operations carried | 57 | 133 |
| bytes written | 5,312 | 15,936 |
| CAS total | 38 | 114 |

One failure mode specific to this path, since it cost a run: a chained batch
dying with `RDMA chained batch failed: status 4`. Status 4 is
`IBV_WC_LOC_PROT_ERR` — a *local* protection error, meaning the source buffer is
outside the registered MR. It appeared only at three servers, from indexing the
staging region per replica when the layout allocates it once. A fake arena has
no memory region to be outside of, so this class is cluster-only; if it recurs,
look at what `stageBatchPayloads` was handed rather than at the remote side.

## Two recurring cluster faults

**`Failed to set to the store ... (SERVER HAS FAILED AND IS DISABLED UNTIL TIMED RETRY)`**
is a memcached registry failure, and it has meant two different things:

1. `swarm-w1` does not resolve from the client node. `config.sh` built `DORY_REGISTRY_IP`
   as `swarm-${machine1}`, a name that only exists in `/etc/hosts`. **CloudLab's node
   watchdog re-syncs `/etc/hosts` from the testbed database on a ~10–15 minute cron and
   discards local additions**, so `update_hosts_file.sh` had to be re-run and kept having
   to be — it was observed being wiped again *within the same session*, between running
   that script and launching the experiment.

   **Fixed.** `config.sh`'s hostname block now holds the `10.10.1.N` experiment-LAN
   addresses directly, so nothing depends on `/etc/hosts` any more. Two things that made
   this land correctly, both worth keeping in mind:

   - **The workers' `config.sh` is authoritative, not the gateway's.** `remote-invoker.sh`
     forwards `DORY_REGISTRY_IP` over ssh, but `invoker.sh` then *sources `config.sh` on
     the worker*, which re-exports it unconditionally and clobbers the forwarded value —
     and the tmux window sources it a second time. So editing `config.sh` on toad alone
     changes nothing: it has to reach every worker, either through a deployment or by
     copying `scripts/config.sh` to all 12.
   - Verify the value end to end rather than assuming, from the worker:
     ```sh
     ssh w5 'source /users/adb321/disco-skip-artifacts/scripts/config.sh
             timeout 3 bash -c "exec 3<>/dev/tcp/$DORY_REGISTRY_IP/11211 &&
                                printf \"version\r\n\" >&3 && head -1 <&3"'
     ```
     `nc` on these nodes gave a false "cannot reach" for a port that was in fact open;
     `/dev/tcp` did not.

2. `update_hosts_file.sh` itself kills memcached — its Step 5 runs
   `sudo pkill -9 -x memcached`. Harmless, because `run.sh` restarts it, but it means
   `Connection refused` immediately after running that script is expected. It also cannot
   update toad's own `/etc/hosts` without an interactive sudo password; that failure is
   harmless, since only the workers need to resolve anything.

**A deploy can silently skip a node while reporting success.** `send-deployment.sh` runs
each node in a subshell piped to `sed`, so the pipeline's exit status is `sed`'s and a
per-node failure does not trip `set -e`; the closing
`Parallel deployment successful across all nodes!` prints unconditionally. Observed: w5's
ssh dropped with `client_loop: send disconnect: Broken pipe`, the script declared success,
and w5 kept a **binary from seven hours earlier** while the other eleven nodes had the new
one. Since w5 is the client that does the bootstrap and the verification, the selftest
would have silently reported on stale code.

So **check the binary actually landed** before believing a run:

```sh
md5sum bin/disco-skip/disco-skip/build/bin/disco-skip
for h in w1 w2 w3 w4 w5 w6 w7 w8 w9 w10 w11 w12; do
  printf "%-4s " $h; ssh $h 'md5sum /users/adb321/disco-skip-artifacts/bin/disco-skip-exe'
done
```

To repair one node without re-running the whole deployment (which `cloudlab_deploy.sh`
would precede with a full `distclean` rebuild of every target):

```sh
scp -C bin/bin.zip w5:/users/adb321/disco-skip-artifacts/bin/bin.zip
ssh w5 'cd /users/adb321/disco-skip-artifacts/bin && mkdir -p staging &&
        unzip -o -q bin.zip -d staging/ && mv -f staging/disco-skip ./disco-skip-exe &&
        rm -rf staging'
```

**The workers are on MDT and toad is on EDT.** A two-hour offset, so a log that looks two
hours stale is probably current. Compare `date` on both before concluding a run produced no
output. (Unrelated to the clock measurements in
[`clock-measurements.md`](clock-measurements.md), which use UTC internally.)

**`can't find window: server1`** has also meant two different things: the binary was never
deployed (see the packaging note above), or the binary started and exited immediately —
which for `--selftest` is *normal*, since it exits by design. `invoker.sh`'s trailing
`sleep 10` keeps the window alive past the ~0.9 s polling interval.

One related trap: `memc.sh` used to kill and recreate `$TMUX_SESSION` — the same session
`setup-all-tmux.sh` had just made, and on the registry machine those are the same machine.
Killing the last session kills the tmux server, discarding the global `remain-on-exit on`,
so a crashed run left no pane to read. The registry now gets its own `REGISTRY_SESSION`.

## Expected selftest output

```
Node arena:   65546 nodes, ... GiB per memory server
Vector arena: 262154 vectors, ... GiB -- caps writes at 262144 per client
Bootstrap: wrote 5 nodes + 5 vectors (4 heads + 1 data node) to 1 replica(s)
index nodes:  4 (0 orphans, 4 entries)
data nodes:   1 (0 entries)
old versions: 0
rdma reads:   20 (10 headers, 10 vectors, 3840 bytes)
              0 mid-split retries, 0 stale-vector retries
offset hint:  hit rate 1 (5 hit / 0 miss)
SELFTEST PASS: I1-I4 hold
probes:       5 resolved, 5 absent (expected: all, the structure is empty)
rdma:         25 headers, 25 vectors, 0 CAS
TRAVERSAL PASS: every probe reached the data node and reported absent

################ Write path:
puts:         9 resolved, 0 failed (6 data-only, 3 structural)
splits:       2 data, 1 index, 0 capacity; 1 boundary no-op(s)
rdma writes:  3 nodes, 16 vectors, 5312 bytes; 38 CAS total
round trips:  16 chained batches carrying 57 operations
readback:     9/9 keys match
index nodes:  5 (0 orphans, 7 entries)
data nodes:   3 (8 entries)
old versions: 13
WRITE PASS: F1/F2 committed over RDMA, every key reads back, I1-I4 hold
```

The write phase runs a fixed script of nine puts at fixed heights, so those counts are an
exact expectation rather than a distribution — if they move, something changed. Reading them:
three of the nine have height >= 1, producing two data splits (at 400 and 500) and one index
split (level 0, for the height-2 key); the repeat of key 400 finds a data node whose `k_min`
is already 400 and so takes the boundary no-op path. Sixteen vectors for nine writes is
copy-on-write working as intended, and `old versions: 13` is the `old_ver` chain those
superseded versions form.

**`0 CAS` is expected on the traversal line and emphatically not on the write line.** A write
is CASes by construction — one handle CAS per published version, plus the timestamp and, for
a split, the two header fields and the tail word.

Verified on the cluster at `736cfcb` + the `findLte` fix, run as `selftest/v2`. The cache
block that follows reports 1 orphan per level against the verifier's 0, which is the
expected disagreement noted below.

`0 CAS` on the traversal line is meaningful: nothing is in flight, so nothing should need
helping.

**Do not expect vector reads below header reads here.** An earlier version of this document
said to, as the check that the header-only hop (`15623ee`) is working. That is wrong, and
the selftest cannot show it either way. The bootstrap structure has exactly **one node per
level**, so a traversal passes over nothing: it reads header *and* vector at each of the four
index levels plus the data node, and there are zero right-hops to save. The measured
`25 headers, 25 vectors` for 5 probes is 1:1 **by construction** — 5 nodes × 5 probes — and
is neither evidence for the optimisation nor against it.

The header-only hop only pays when a level holds several nodes, which needs inserts. It is
measured off-cluster in `tests/get_test.cc`, where the uncached path reads **9504 headers
against 2880 vectors (3.3:1)**. That is where to look for this, until F1 exists and the
selftest can build a structure with right-hops in it.

The cache reporting **1 orphan per level** while the verifier reports 0 is expected, not a
disagreement — see the orphan-flag note in [`remote-design.md`](remote-design.md) §3.

The **offset hint is now a real mechanism, but its hit rate is still not a performance
number.** It used to warm and score without doing anything, because the blocking helpers
posted one work request at a time. It now chains the speculative vector read into the same
doorbell as the quorum's header reads, so a hit genuinely removes a round trip -- the
selftest reports `140 vector read(s) served without a round trip`.

What makes the *rate* unrepresentative is the workload, not the mechanism. The script is 9
puts against 153 reads, and the misses are exactly the 13 commits: a write moves its node's
vector, so the next read of that node necessarily misses, and nothing else does. 91.5% here
is the floor on miss count for a read-heavy script over a nearly static structure. Miss rate
tracks the write rate, so expect it to fall away under write-heavy load -- which is the
crossover the toggle exists to find.
