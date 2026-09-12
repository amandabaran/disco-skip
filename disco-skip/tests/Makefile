# Local (off-cluster) gate for the remote side.
#
# The RDMA half needs libibverbs and a fabric, so it only builds on the
# cluster. Everything here is plain C++: the RemoteAddr contract, the cache
# instantiation, and the remote node layout. Running it costs a second and
# catches the class of mistake -- a toggle that only compiles one way, a
# RemoteAddr that stops being lock-free -- that is otherwise invisible until a
# cluster deploy.
#
#   make          build and run every toggle combination
#   make debug    same, with ASan + UBSan
#
# The cache half is header-only, so both targets depend on its headers too;
# otherwise editing skipvector_disco.h leaves a stale binary.

CXX      ?= g++
CXXFLAGS := -std=c++17 -O1 -g -Wall -Wextra -Wpedantic -pthread -Wno-nested-anon-types
LDFLAGS  := -pthread

SRC_DIR   := ../src
CACHE_DIR := ../include
INCLUDES  := -I$(SRC_DIR) -I$(CACHE_DIR) -I$(CACHE_DIR)/include-cache

# The cache reads MAX_THREADS from a header that is gitignored and generated
# (see src/CMakeLists.txt for the build-tree copy). Off-cluster we fall back to
# the checked-in template, which is what its own instructions say to do.
MACHINE_DEFINES := $(CACHE_DIR)/include-cache/common/machine_defines.h

HDRS := $(wildcard $(SRC_DIR)/ds_*.hpp) $(SRC_DIR)/layout.hpp \
        $(wildcard $(CACHE_DIR)/*.h) \
        $(wildcard $(CACHE_DIR)/include-cache/*/*.h)

# Every combination of the two toggles that invariants.md §9 defines.
CONFIGS := cache1-repl1 cache1-repl3 cache0-repl1 cache0-repl3
BINS    := $(addprefix obj/smoke-,$(CONFIGS))

# The structure verifier and the bootstrap builder, against a fake arena. Needs
# no cache and no toggle sweep -- it is pure remote-side logic.
VERIFY  := obj/verify
TRAVERSE := obj/traverse
GET     := obj/get
INSERT  := obj/insert
PUT     := obj/put
SELFTEST := obj/selftest
QUORUM  := obj/quorum
ASYNC   := obj/async
TS      := obj/ts
RANGE   := obj/range

# The dory toolchain's warning set, copied from the actual compile line rather
# than approximated. This target's job is to fail here instead of on the
# cluster, so it must not be weaker than the real build in any respect.
#
# Reproduce the reference list with:
#   ./build.py disco-skip  (then read the g++ invocation for main.cpp)
#
# Three things this must keep matching:
#   * -O2, not -O1. -Wstrict-overflow and -Wdisabled-optimization are
#     optimiser-driven, so a lower level silently stops reporting them.
#   * -Wstrict-overflow=5 and -Wsign-promo are GCC-only. Earlier revisions of
#     this file dropped them as unavailable, because the dev box was a Mac with
#     no GCC -- but the cluster build runs on toad, which has the same g++ the
#     dory toolchain uses, so there is no reason to approximate. Dropping
#     -Wstrict-overflow=5 cost a cluster build (findLte in ds_node.hpp).
#   * the -Wno-error= list. The real build demotes four of these to warnings;
#     matching that keeps the gate from failing on something the cluster
#     tolerates, which would be its own kind of false signal.
#
# -Wno-nested-anon-types exempts register.hpp's pre-existing anonymous struct in
# an anonymous union (chimera's code, and GCC accepts it).
STRICTWARN := -Werror -Wall -Wextra -Wpedantic -Wconversion -Wfloat-equal \
              -Wpointer-arith -Wswitch-default -Wpacked -Winvalid-pch \
              -Wmissing-field-initializers -Wunreachable-code -Wcast-align \
              -Wcast-qual -Wdisabled-optimization -Wformat=2 -Wformat-nonliteral \
              -Wuninitialized -Wformat-security -Wformat-y2k -Winit-self \
              -Wmissing-declarations -Wmissing-include-dirs -Wredundant-decls \
              -Wstrict-overflow=5 -Wundef -Wno-unused -Wctor-dtor-privacy \
              -Wsign-promo -Woverloaded-virtual -Wold-style-cast -Wlogical-op \
              -Wstrict-null-sentinel -Wnoexcept

# Exactly what src/CMakeLists.txt demotes, so the gate matches the build.
STRICTNOERR := -Wno-error=switch-default -Wno-error=conversion \
               -Wno-error=missing-declarations -Wno-error=undef

STRICTFLAGS := -std=c++17 -O2 -g -DNDEBUG $(STRICTWARN) $(STRICTNOERR) \
               -Wno-nested-anon-types

# ds_rdma.hpp sits on dory and libibverbs, which do not exist off-cluster, so it
# is compiled against the stub in dory_stub/ instead. Compile-only: the stub
# moves no data, so nothing asserts behaviour through it.
#
# LIMIT: -Wsign-promo is GCC-only and there is no GCC here, so the class of bug
# it catches (an unsigned enum picking the int overload) stays cluster-only.
STUBFLAGS := -I dory_stub

.PHONY: all check strict rdma debug clean
all: check strict rdma

$(MACHINE_DEFINES): $(CACHE_DIR)/include-cache/common/machine_defines.template.h
	cp $< $@

obj:
	@mkdir -p obj

define SMOKE_RULE
obj/smoke-cache$(1)-repl$(2): local_smoke.cc $$(HDRS) $$(MACHINE_DEFINES) | obj
	@echo "[CXX] cache=$(1) replicas=$(2)"
	@$$(CXX) $$(CXXFLAGS) $$(INCLUDES) \
	  -DDS_CACHE_ENABLED=$(1) -DDS_N_REPLICAS=$(2) \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 \
	  $$< -o $$@ $$(LDFLAGS)
endef

$(eval $(call SMOKE_RULE,1,1))
$(eval $(call SMOKE_RULE,1,3))
$(eval $(call SMOKE_RULE,0,1))
$(eval $(call SMOKE_RULE,0,3))

$(VERIFY): verify_test.cc $(HDRS) $(MACHINE_DEFINES) | obj
	@echo "[CXX] verify"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(TRAVERSE): traverse_test.cc $(HDRS) $(MACHINE_DEFINES) | obj
	@echo "[CXX] traverse"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(GET): get_test.cc $(HDRS) fake_ops.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] get"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=1 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(INSERT): insert_test.cc $(HDRS) fake_ops.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] insert"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(PUT): put_test.cc $(HDRS) fake_ops.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] put"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(SELFTEST): selftest_test.cc $(HDRS) fake_ops.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] selftest"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(QUORUM): quorum_test.cc $(HDRS) fake_ops.hpp fake_replicas.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] quorum"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=3 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(RANGE): range_test.cc $(HDRS) fake_ops.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] range"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(TS): ts_test.cc $(HDRS) fake_ops.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] ts"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(ASYNC): async_test.cc $(HDRS) fake_ops.hpp fake_replicas.hpp fake_async.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] async"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=3 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

check: $(BINS) $(VERIFY) $(TRAVERSE) $(GET) $(INSERT) $(PUT) $(SELFTEST) $(QUORUM) $(ASYNC) $(TS) $(RANGE)
	@echo "---- $(VERIFY) ----"; $(VERIFY) || exit 1
	@echo "---- $(TRAVERSE) ----"; $(TRAVERSE) || exit 1
	@echo "---- $(GET) ----"; $(GET) || exit 1
	@echo "---- $(INSERT) ----"; $(INSERT) || exit 1
	@echo "---- $(PUT) ----"; $(PUT) || exit 1
	@echo "---- $(SELFTEST) ----"; $(SELFTEST) || exit 1
	@echo "---- $(QUORUM) ----"; $(QUORUM) || exit 1
	@echo "---- $(ASYNC) ----"; $(ASYNC) || exit 1
	@echo "---- $(TS) ----"; $(TS) || exit 1
	@echo "---- $(RANGE) ----"; $(RANGE) || exit 1
	@for b in $(BINS); do echo "---- $$b ----"; $$b || exit 1; done
	@echo "==== all toggle combinations pass ===="

debug: CXXFLAGS := -std=c++17 -O0 -g3 -Wall -Wextra -Wpedantic -pthread -fsanitize=address,undefined
debug: LDFLAGS  := -pthread -fsanitize=address,undefined
debug: clean check

# Catches the class of failure that only shows up under the cluster's -Werror.
strict: | obj
	@echo "[CXX] strict warning set"
	@$(CXX) $(STRICTFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 strict_headers.cc -o obj/strict $(LDFLAGS)
	@obj/strict > /dev/null; echo "==== headers clean under the strict warning set ===="

# The cache half is written for a laxer warning set, so the real build marks its
# include directories SYSTEM (src/CMakeLists.txt) to suppress diagnostics from
# inside them while our own code stays strict. Mirrored here, or the cache's own
# headers fail this target for reasons that are not ours.
SYSINCLUDES := -I$(SRC_DIR) -isystem $(CACHE_DIR) -isystem $(CACHE_DIR)/include-cache

rdma: | obj
	@echo "[CXX] ds_rdma.hpp against the dory stub"
	@$(CXX) $(STRICTFLAGS) $(INCLUDES) $(STUBFLAGS) \
	  -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 \
	  -c rdma_compile.cc -o obj/rdma_compile.o
	@echo "==== ds_rdma.hpp compiles clean against the dory surface ===="
	@echo "[CXX] the same, with the cache compiled in"
	@# -Wno-error=strict-overflow matches src/CMakeLists.txt, and only on THIS
	@# arm. The cache's signed descending loops inline into our wrappers and
	@# the diagnostic is then attributed to our function with a useless
	@# location, so it cannot be scoped away -- see the comment by the flag in
	@# CMakeLists. The arm above keeps it fatal, and that arm is all of src/
	@# with none of the cache, so our own code is still covered.
	@$(CXX) $(STRICTFLAGS) -Wno-error=strict-overflow $(SYSINCLUDES) $(STUBFLAGS) \
	  -DDS_CACHE_ENABLED=1 -DDS_N_REPLICAS=3 -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 \
	  -c rdma_compile.cc -o obj/rdma_compile_cache.o
	@echo "==== and clean with DS_CACHE_ENABLED=1, which is what the build uses ===="

clean:
	rm -rf obj
