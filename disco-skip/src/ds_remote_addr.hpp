#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <type_traits>

namespace ds {

/// A remote node address: an index into the node arena, identical on every
/// replica.
///
/// Why an id and not a virtual address. Each replica registers its own MR, so
/// the same logical node sits at a different VA on each one. An address read
/// out of replica 0 would be meaningless against replica 1. The id is
/// replica-independent; Layout::nodeAddr(replica, id) resolves it by adding
/// that connection's remoteBuf(). The rkey does not belong here either --
/// dory's ReliableConnection takes it from the connection, not the caller
/// (see conn/src/rc.hpp postSendSingle, which has no rkey parameter).
///
/// Why exactly one uint64_t, and why this must never grow. The cache stores
/// directory entries as rlx_atomic<REMOTE_ADDR>, i.e. std::atomic<RemoteAddr>,
/// inside a plain C array that vector_sfra shuffles with raw std::memcpy
/// (include/include-cache/vector/vector_sfra.h). memcpy over a std::atomic is
/// only survivable while that atomic is lock-free and holds no embedded lock;
/// a 16-byte payload is not lock-free on x86-64 without -mcx16, and memcpy
/// would then be relocating a mutex. The static_asserts below are the guard --
/// if someone later adds a field, the build breaks here rather than corrupting
/// silently under load.
///
/// id 0 is null. The cache never inspects the value: it stores a
/// default-constructed REMOTE_ADDR into nodes it created on its own and hands
/// back whatever it holds, so "default-constructed is a distinguishable null"
/// is a contract the remote side has to honour (interface doc §4). The
/// allocator consequently never hands out id 0.
struct RemoteAddr {
  uint64_t id;

  constexpr RemoteAddr() noexcept : id(0) {}

  /// Explicit so an integer never turns into an address by accident. Kept
  /// convertible-by-static_cast on purpose: entry<K,V,EMPTY>'s default ctor
  /// does `val(static_cast<V>(0))`, and while vector_sfra never instantiates
  /// it in this configuration, relying on that is not worth a build break.
  explicit constexpr RemoteAddr(uint64_t node_id) noexcept : id(node_id) {}

  [[nodiscard]] constexpr bool isNull() const noexcept { return id == 0; }
  [[nodiscard]] constexpr explicit operator bool() const noexcept { return id != 0; }

  friend constexpr bool operator==(RemoteAddr a, RemoteAddr b) noexcept {
    return a.id == b.id;
  }
  friend constexpr bool operator!=(RemoteAddr a, RemoteAddr b) noexcept {
    return a.id != b.id;
  }
};

inline constexpr RemoteAddr kNullAddr{};

// The cache's hard requirements (interface doc §4).
static_assert(std::is_default_constructible_v<RemoteAddr>);
static_assert(std::is_copy_constructible_v<RemoteAddr>);
static_assert(std::is_copy_assignable_v<RemoteAddr>);

// The memcpy-into-std::atomic requirements described above.
static_assert(sizeof(RemoteAddr) == sizeof(uint64_t),
              "RemoteAddr must stay 8 bytes: it lives inside a std::atomic that "
              "vector_sfra relocates with raw memcpy");
static_assert(alignof(RemoteAddr) == alignof(uint64_t));
static_assert(std::is_trivially_copyable_v<RemoteAddr>,
              "RemoteAddr is memcpy'd by vector_sfra");
static_assert(std::atomic<RemoteAddr>::is_always_lock_free,
              "std::atomic<RemoteAddr> must be lock-free, or vector_sfra's "
              "memcpy would be relocating an embedded lock");

// A default-constructed value must be the null address, since that is the only
// signal the cache gives for "no known remote counterpart".
static_assert(RemoteAddr{}.isNull());
static_assert(!RemoteAddr{1}.isNull());

}  // namespace ds

template <>
struct std::hash<ds::RemoteAddr> {
  size_t operator()(ds::RemoteAddr a) const noexcept {
    return std::hash<uint64_t>{}(a.id);
  }
};
