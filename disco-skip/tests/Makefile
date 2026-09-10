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
DESCEND := obj/descend
GET     := obj/get

# Approximation of the dory toolchain's warning set, which is far stricter than
# the cache half's own Makefile. Clang-only equivalents are used where GCC's
# spelling differs, and the GCC-specific flags it does not know are dropped.
# -Wno-nested-anon-types exempts register.hpp's pre-existing anonymous struct in
# an anonymous union (chimera's code, and GCC accepts it).
STRICTFLAGS := -std=c++17 -O1 -Werror -Wall -Wextra -Wpedantic -Wconversion \
               -Wfloat-equal -Wpointer-arith -Wpacked -Wmissing-field-initializers \
               -Wunreachable-code -Wcast-align -Wcast-qual -Wformat=2 \
               -Wformat-nonliteral -Wuninitialized -Wformat-security -Winit-self \
               -Wold-style-cast -Wredundant-decls -Wundef -Wno-unused \
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

$(DESCEND): descend_test.cc $(HDRS) $(MACHINE_DEFINES) | obj
	@echo "[CXX] descend"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

$(GET): get_test.cc $(HDRS) fake_ops.hpp $(MACHINE_DEFINES) | obj
	@echo "[CXX] get"
	@$(CXX) $(CXXFLAGS) $(INCLUDES) -DDS_CACHE_ENABLED=1 -DDS_N_REPLICAS=1 \
	  -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 $< -o $@ $(LDFLAGS)

check: $(BINS) $(VERIFY) $(DESCEND) $(GET)
	@echo "---- $(VERIFY) ----"; $(VERIFY) || exit 1
	@echo "---- $(DESCEND) ----"; $(DESCEND) || exit 1
	@echo "---- $(GET) ----"; $(GET) || exit 1
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

rdma: | obj
	@echo "[CXX] ds_rdma.hpp against the dory stub"
	@$(CXX) $(STRICTFLAGS) $(INCLUDES) $(STUBFLAGS) \
	  -DDS_CACHE_ENABLED=0 -DDS_N_REPLICAS=1 -DDS_MAX_LAYERS=8 -DDS_IDX_EXP=3 \
	  -c rdma_compile.cc -o obj/rdma_compile.o
	@echo "==== ds_rdma.hpp compiles clean against the dory surface ===="

clean:
	rm -rf obj
