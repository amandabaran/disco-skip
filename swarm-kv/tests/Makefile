# Unit tests for the range table lock's two decision rules.
#
# NO INCLUDE PATHS AND NO LIBRARIES, deliberately. range_lock_rules.hpp has no
# dependencies so this stays a pure-logic test; an earlier version included the
# lock itself and spent its time failing on spdlog and fmt link errors, which
# look like toolchain faults rather than test failures.
CXX      ?= g++
CXXFLAGS ?= -std=c++17 -O1 -g -Wall -Wextra -Werror

obj/range_table_lock_test: range_table_lock_test.cc ../src/range_lock_rules.hpp
	@mkdir -p obj
	$(CXX) $(CXXFLAGS) -o $@ $<

.PHONY: check
check: obj/range_table_lock_test
	@./obj/range_table_lock_test
