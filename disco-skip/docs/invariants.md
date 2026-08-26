# Disco-Skip Invariants

## 1. Structural invariants
- I1: For any reachable Data Node D, D.k_min <= min(keys in D's Data Vector).
- I2: The level-0 next_dn chain is sorted by k_min; no cycles.
- I3: If a Data Vector has height H, then for every level L in [1..H],
      there exists an Index Vector routing entry to D at level L.
- I4: A vector with is_orphan=1 is reachable only via old_ver* or next_dv*.

## 2. Version rules
- V1: content_ver bumps <=> KV added/removed, k_min unchanged.
- V2: struct_ver bumps <=> split/merge OR k_min change.
- V3: A single op bumps at most one of {content_ver, struct_ver}.
- V4: Handle.(struct_ver,content_ver) is authoritative.
- V5: Vector.(struct_ver,content_ver) must equal Handle.(...) for a
      consistent read; else follow old_ver*.

## 3. Ordering / fence points
- F1 (Insert_h0):  WRITE(newvec, all replicas)
                   -> SEND_FENCE
                   -> CAS(handle, all replicas, quorum wait)
                   -> WRITE(ts) chained WRITE(content_ver on node)
                     [with piggyback re-CAS on lagged replicas]
- F2 (Split):      WRITE(new_node, new_vec, updated_vec, all replicas)
                   -> SEND_FENCE
                   -> CAS(cur_node.handle, all replicas, quorum wait)
                   -> WRITE(ts on both vectors) chained
                      WRITE(new next_dn + struct_ver on cur_node)
- F3: All chained WRs live on one QP per replica. Same-QP RC ordering
      + SEND_FENCE guarantees WRITE-visible-before-CAS at the remote HCA.
- F4: PCIe writes to 64B-aligned regions are atomic on this HW.

## 4. CAS-ABD linearization
- L1: Write linearizes at 2/3 CAS success.
- L2: Read linearizes at quorum node read (max tag).
- L3: Tag order = (struct_ver, content_ver) lex.
- L4: Vector read targets a replica that voted with the max tag.
- L5: Async writeback: correctness argument in Section 4.1.

### 4.1 Async writeback linearizability sketch
- Any two 2/3 quorums intersect in >=1 replica.
- Post-commit, >=2 replicas hold the new tag.
- A subsequent 2/3 quorum read intersects the committed set -> sees new tag.
- Async writeback only accelerates convergence of the lagged 1/3 replica;
  it does not affect the visibility of committed writes.

## 5. GC / epoch
- This will not be implemented but discussed in the paper
- E1: Every client publishes local epoch to a well-known per-replica slot
      every N=1024 ops.
- E2: Memory-node passive: no server logic. Clients read all peers'
      epochs (RDMA READ into a 64-byte cacheline batch) to compute min-epoch
      before reclaiming.
- E3: On CoW, old vector tagged retire_epoch = current+1 in client's freelist.
- E4: A vector at retire_epoch R is reclaimable once global_min_epoch > R.
- E5: Because slabs are client-partitioned, freelist mgmt is thread-local.

## 6. Cache staleness
- C1: Cache may be stale.
- C2: On version mismatch in Get -> follow old_ver*.
- C3: On version mismatch in Insert/Delete -> restart, refresh cache.
- C4: On k_min mismatch (k not in [k_min, next.k_min)) -> invalidate cache,
      remote traverse from cached ancestor.
- C5: Never cache with cross-op TTL; entries live until version-mismatch invalidation.

## 7. Structural mod state machines
### Split (state diagram)
  S0 Read cur_node+vec (versions consistent)
  S1 Prepare new_node, new_vec, updated_vec locally
  S2 WRITE all three to 3 replicas (chained, per-replica QP)
  S3 CAS cur_node.handle on 3 replicas, wait for 2/3
     -- fail: goto S0
  S4 WRITE ts on both vectors + WRITE new next_dn + struct_ver on cur_node
  S5 Update local SV
  S6 if height H>0: kick off Update_Index(H, k)

### Update_Index
  Completed during the operation. Not sure if we can use async operations.

## 8. Failure model
- Fail-stop memory nodes. No Byzantine, no partition, no client crashes
  during eval (client crash safe via epoch GC but not evaluated).

## 9. Toggle semantics
- Replication toggle: compile-time flag DS_N_REPLICAS in {1,3}.
  N=1: quorum=1, single-replica code path. Zero overhead vs. N=3
  is verified via a microbench (single-op RDMA count).
- Cache toggle: DS_CACHE_ENABLED as in chimera.