#pragma once
// Compile-only stub of the fmt surface DsState uses. See dory_stub/README.md.
//
// LIMITATION, stated because it is easy to over-trust: this accepts any format
// string with any arguments, so it does NOT check that placeholders match the
// arguments supplied. Real fmt 7.1.3 parses the format string at runtime, so a
// mismatch there is a runtime throw rather than a compile error anyway -- but do
// not read a clean stub compile as "the fmt calls are right". It exists so that
// everything AROUND them type-checks.
#include <string>
#include <utility>

namespace fmt {

template <typename... Args>
inline void print(char const *, Args &&...) {}

template <typename... Args>
inline std::string format(char const *, Args &&...) {
  return std::string{};
}

}  // namespace fmt
