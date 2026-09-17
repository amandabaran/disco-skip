#pragma once
// Compile-only stub of the slice of fmt this project uses. See ../README.md.
//
// fmt is a conan dependency and is not present off-cluster, but src/ds.hpp now
// reaches it transitively (ds_futures.hpp -> disco_skip_state.hpp ->
// latency.hpp). Without this the umbrella cannot be type-checked locally at
// all, which is the one thing that gate exists for.
//
// Deliberately does no formatting: the signatures are what matter, since the
// point is to catch a call that does not compile, not to produce output. Only
// fmt::print and fmt::format are used (33 and 4 call sites respectively).
#include <cstdio>
#include <string>

namespace fmt {

template <typename... Args>
inline void print(char const *, Args &&...) {}

template <typename... Args>
inline void print(std::FILE *, char const *, Args &&...) {}

template <typename... Args>
inline std::string format(char const *fmt_str, Args &&...) {
  return std::string(fmt_str);
}

}  // namespace fmt
