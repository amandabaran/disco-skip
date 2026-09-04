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

.PHONY: all clean run debug

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(SRCS) -o $(TARGET) $(LDFLAGS)

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
	rm -f $(TARGET)