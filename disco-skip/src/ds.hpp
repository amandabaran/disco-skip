#pragma once

// Umbrella header for the remote side.
//
// main.cpp includes this rather than picking headers individually, because it
// is the one translation unit that cannot be compiled off-cluster -- so a
// missing include there costs a full cluster round trip to discover, and has
// already done so once. With one include there is nothing to get wrong.
//
// The individual headers stay independently includable, and the tests include
// them directly on purpose: that is what keeps them from silently growing
// dependencies on each other.
//
// ds_cache.hpp is deliberately NOT here -- it pulls in the cache half and is
// only valid under DS_CACHE_ENABLED, which callers guard for themselves.

#include "ds_defs.hpp"
#include "ds_remote_addr.hpp"

#include "ds_node.hpp"
#include "layout.hpp"

#include "ds_async.hpp"
#include "ds_bootstrap.hpp"
#include "ds_traverse.hpp"
#include "ds_get.hpp"
#include "ds_insert.hpp"
#include "ds_put.hpp"
#include "ds_quorum.hpp"
#include "ds_selftest.hpp"
#include "ds_verify.hpp"

// Needs dory, so it comes last and is what makes this header cluster-only.
#include "ds_rdma.hpp"
