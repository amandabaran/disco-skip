# Compile-only stub of the dory surface

`ds_rdma.hpp` (and, from Step 2 on, the operation state machines) sit directly
on `dory::conn::ReliableConnection` and libibverbs, neither of which exists off
the cluster. Without this stub, none of that code can even be type-checked
without a cluster round trip.

This stub declares just enough of that surface for those headers to *compile*.
It is deliberately **not** a fake fabric: nothing here moves data, and no test
should assert on behaviour through it. Its only job is to turn "wrong on the
cluster" into "wrong locally".

## Be honest about what this does and does not catch

It was added after three cluster builds failed on compile errors. It would have
caught **none of them**, which is worth stating plainly so nobody trusts it
further than it goes:

- `memset` over a non-trivially-default-constructible `NodeRecord` — was in
  `ds_node.hpp`, which the strict target already covers without any stub.
- `std::to_string(ibv_wc_status)` picking the `int` overload — `-Wsign-promo`
  is a GCC-only warning and there is no GCC on the development Mac.
- `headAddrs(layout.cache_layers)` narrowing `uint64_t` to `uint32_t` — that is
  in `main.cpp`, which needs lyra, fmt and the dory control plane too, and is
  well past what is worth stubbing.

What it *does* buy is type-checking of the RDMA-facing headers and everything
built on them: wrong argument order, a signature that drifted from
`conn/src/rc.hpp`, a template that does not instantiate, and narrowing of
runtime (as opposed to constant) values. That matters most for Step 2, where the
descent state machine is both large and entirely dory-facing.

Anything about actual RDMA semantics — completion ordering, fencing, whether a
CAS really landed — is untestable here by construction.

If it ever becomes worth testing the descent logic against a simulated fabric,
this is where that would grow from.
