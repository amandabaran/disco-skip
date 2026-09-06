# Makefile for skipvector mirror_insert test

CXX      := g++
CXXFLAGS := -std=c++17 -O2 -g -Wall -Wextra -Wpedantic -pthread
LDFLAGS  := -pthread

# Adjust these to point to your source tree.
INCLUDES := -I. -I./include-cache

# If you have architecture-specific flags:
# CXXFLAGS += -march=native

TARGET   := test
SRCS     := test.cc

# Single-threaded, assertion-driven coverage of locate_data() and
# mirror_reconcile(), which the concurrent harness does not exercise.
RECON    := test_reconcile

# This is a header-only data structure, so the targets have to depend on the
# headers too -- otherwise editing skipvector_disco.h leaves a stale binary.
HDRS     := $(wildcard *.h) $(wildcard include-cache/*/*.h)

.PHONY: all clean run debug check

all: $(TARGET) $(RECON)

$(TARGET): $(SRCS) $(HDRS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(SRCS) -o $(TARGET) $(LDFLAGS)

$(RECON): $(RECON).cc $(HDRS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(RECON).cc -o $(RECON) $(LDFLAGS)

# Run both suites. Exits nonzero if either fails.
check: $(TARGET) $(RECON)
	./$(RECON)
	./$(TARGET) 8 20000 100000 6

debug: CXXFLAGS := -std=c++17 -O0 -g3 -Wall -Wextra -Wpedantic -pthread -fsanitize=address,undefined
debug: LDFLAGS  := -pthread -fsanitize=address,undefined
debug: $(TARGET)

# Convenience run target with default args.
run: $(TARGET)
	./$(TARGET) 4 10000 100000 4

# Higher-thread stress test.
stress: $(TARGET)
	./$(TARGET) 16 100000 1000000 6

clean:
	rm -f $(TARGET) $(RECON)