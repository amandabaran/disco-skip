#pragma once

#include <array>

/// An array that pads entries out to avoid false sharing.
/// Entries are atomic.
template <typename T, size_t CAP> class padded_array {
  // This value represents how far away from each other entries need to be in
  // bytes to guarantee a lack of false sharing. A cache line is 64 bytes, but
  // according to [mfs], I may need to pad to 256 bytes to "play it safe with
  // adjacent-sector prefetch, etc."
  static constexpr size_t PADDING_BYTES = 256;

  // As above, but expressed in multiples of T's size instead of bytes.
  static constexpr size_t PADDING_T = PADDING_BYTES / sizeof(T);

  // The size (as multiple of T) of the underlying array to pad properly.
  static constexpr size_t TRUE_CAP = CAP * PADDING_T;

  // The underlying array.
  std::array<std::atomic<T>, TRUE_CAP> array;

public:
  padded_array() {
    // Make sure PADDING_BYTES is an even multiple of T's size.
    static_assert(PADDING_BYTES % sizeof(T) == 0);
  }

  /// Atomically load the entry at logical index i.
  [[nodiscard]] T load(size_t const i) const {
    size_t const idx = i * PADDING_T;
    return array[idx].load();
  }

  /// Atomically write the entry at logical index i.
  void store(size_t const i, T const &t) {
    size_t const idx = i * PADDING_T;
    array[idx].store(t);
  }

  /// Perform a CAS on the entry at logical index i.
  [[nodiscard]] bool cas(size_t const i, T &expected, const T desired) {
    size_t const idx = i * PADDING_T;
    return array[idx].compare_exchange_strong(expected, desired);
  }
};
