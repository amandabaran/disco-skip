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
make -C disco-skip/tests        # toggle combinations, verifier, descent, get, strict, stub
make -C disco-skip/include check   # the cache author's own suite; must keep passing
```

`disco-skip/tests` is the off-cluster gate. It covers everything that does not need
libibverbs, and it exists because four separate cluster builds were spent on compile errors.
Two limits worth knowing:

- **`-Wsign-promo` is GCC-only** and there is no GCC on the dev Mac, so the class of bug it
  catches (an unsigned enum picking the `int` overload) stays cluster-only.
- `main.cpp` cannot be compiled off-cluster at all. It includes only `src/ds.hpp`, and
  `tests/rdma_compile.cc` compiles that same umbrella against `tests/dory_stub/`, which is
  what keeps a missing include in `main.cpp` from costing a round trip.

The stub in `tests/dory_stub/` is **compile-only** and moves no data. Never assert behaviour
through it.

## Cluster build

```sh
cd /users/adb321/disco-skip-artifacts/bin/disco-skip && git pull && ./build.py disco-skip
```

The dory toolchain compiles with `-Werror` and a much stricter warning set than the cache
half is written for. That is handled by marking the cache include directories `SYSTEM` in
`src/CMakeLists.txt`, so diagnostics originating inside them are suppressed while our own
code stays fully strict. **Do not** replace that with blanket `-Wno-error=`; it would weaken
the same checks on our code, and the cache is the other author's half and not ours to edit.

## Deploy

```sh
cd /users/adb321/disco-skip-artifacts && ./cloudlab_deploy.sh
```

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

## Two recurring cluster faults

**`Failed to set to the store ... (SERVER HAS FAILED AND IS DISABLED UNTIL TIMED RETRY)`**
is a memcached registry failure, and it has meant two different things:

1. `swarm-w1` does not resolve from the client node. `config.sh` builds `DORY_REGISTRY_IP`
   as `swarm-${machine1}`, a name that only exists in `/etc/hosts`. **CloudLab's node
   watchdog re-syncs `/etc/hosts` from the testbed database on a ~10–15 minute cron and
   discards local additions**, so `update_hosts_file.sh` has to be re-run and will keep
   having to be. `getent hosts swarm-w1` on w5 tells you immediately.
   *Proper fix, not yet applied:* put the `10.10.1.x` addresses into `config.sh`'s hostname
   block. `DORY_REGISTRY_IP` is the only thing that ever needed those names, dory is happy
   with an IP, the experiment-LAN addresses are stable, and a change in `config.sh` ships
   with the deployment where the watchdog cannot undo it. Needs the `wN → 10.10.1.x` mapping
   collected once.
2. `update_hosts_file.sh` itself kills memcached — its Step 5 runs
   `sudo pkill -9 -x memcached`. Harmless, because `run.sh` restarts it, but it means
   `Connection refused` immediately after running that script is expected.

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
SELFTEST PASS: I1-I4 hold
probes:       5 resolved, 5 absent (expected: all, the structure is empty)
DESCENT PASS: every probe reached the data node and reported absent
```

`0 CAS` on the descent line is meaningful: nothing is in flight, so nothing should need
helping. Vector reads should be well below header reads — the descent reads one vector per
level plus one for the data node, but a header for every hop.

The cache reporting **1 orphan per level** while the verifier reports 0 is expected, not a
disagreement — see the orphan-flag note in [`remote-design.md`](remote-design.md) §3.

The **offset hint's hit rate is not a performance number** yet. The blocking helpers post one
work request at a time, so nothing is doorbell-batched; the hint only warms and scores. On a
static structure it reads 100%, which is the best possible case by construction.
