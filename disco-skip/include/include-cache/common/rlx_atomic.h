#pragma once

#include <atomic>

/// rlx_atomic: Relaxed Atomic
/// An atomic which is always accessed by memory_order_relaxed.
///  Syntactic sugar for primitives accessed by sequence locks.
template <typename T> struct rlx_atomic {
  static constexpr std::memory_order relaxed = std::memory_order_relaxed;

  // The contained atomic T.
  std::atomic<T> t;

  // Constructors as std::atomic
  rlx_atomic() noexcept = default;
  // Making this one-argument constructor implicit like in the std::atomic
  // struct is very much intentional.
  // cppcheck-suppress noExplicitConstructor
  constexpr explicit rlx_atomic(T desired) noexcept : t(desired) {}
  rlx_atomic(const rlx_atomic &) = delete;

  /// Operator = performs an implicit relaxed atomic store.
  T operator=(T desired) noexcept {
    t.store(desired, relaxed);
    return desired;
  }

  rlx_atomic &operator=(const rlx_atomic) noexcept = delete;

  /// An explicit call to .store() also performs a relaxed store.
  void store(T desired) noexcept { t.store(desired, relaxed); }

  /// Operator T performs an implicit relaxed atomic load.
  operator T() const noexcept { return t.load(relaxed); }

  /// An explicit call to .load() also performs a relaxed load.
  [[nodiscard]] T load() const noexcept { return t.load(relaxed); }

  // [mfs] We need some comparisons on types that rlx_atomic can't see, so we
  // put them here and let them fail when they're invalid (which should be
  // never)

  bool operator<(const T other) const { return t.load(relaxed) < other; }
  bool operator>(const T other) const { return t.load(relaxed) > other; }
  bool operator==(const T other) const { return t.load(relaxed) == other; }
  bool operator!=(const T other) const { return t.load(relaxed) != other; }
};

/// Template specialization for size_t, offering atomic operators
template <> struct rlx_atomic<size_t> {
  static constexpr std::memory_order relaxed = std::memory_order_relaxed;

  // The contained atomic size_t.
  std::atomic<size_t> t;

  // Constructors as std::atomic
  rlx_atomic() noexcept = default;
  constexpr explicit rlx_atomic(size_t desired) noexcept : t(desired) {}
  rlx_atomic(const rlx_atomic &) = delete;

  /// Operator = performs an implicit relaxed atomic store.
  size_t operator=(size_t desired) noexcept {
    t.store(desired, relaxed);
    return desired;
  }

  rlx_atomic &operator=(const rlx_atomic) noexcept = delete;

  /// An explicit call to .store() also performs a relaxed store.
  void store(size_t desired) noexcept { t.store(desired, relaxed); }

  /// Operator size_t performs an implicit relaxed atomic load.
  operator size_t() const noexcept { return t.load(relaxed); }

  /// An explicit call to .load() also performs a relaxed load.
  [[nodiscard]] size_t load() const noexcept { return t.load(relaxed); }

  size_t operator+=(size_t rhs) noexcept { return t.fetch_add(rhs, relaxed); }
  size_t operator-=(size_t rhs) noexcept { return t.fetch_sub(rhs, relaxed); }
  size_t operator&=(size_t rhs) noexcept { return t.fetch_and(rhs, relaxed); }
  size_t operator|=(size_t rhs) noexcept { return t.fetch_or(rhs, relaxed); }
  size_t operator^=(size_t rhs) noexcept { return t.fetch_xor(rhs, relaxed); }
  size_t operator++() noexcept { return t.fetch_add(1, relaxed) + 1; }
  size_t operator++(int) noexcept { return t.fetch_add(1, relaxed); }
  size_t operator--() noexcept { return t.fetch_sub(1, relaxed) - 1; }
  size_t operator--(int) noexcept { return t.fetch_sub(1, relaxed); }
};

/// Template specialization for pointer, allowing -> operator
template <typename T> struct rlx_atomic<T *> {
  static constexpr std::memory_order relaxed = std::memory_order_relaxed;

  // The contained atomic T*.
  std::atomic<T *> t;

  // Constructors as std::atomic
  rlx_atomic() noexcept = default;
  constexpr explicit rlx_atomic(T *desired) noexcept : t(desired) {}
  rlx_atomic(const rlx_atomic &) = delete;

  /// Operator = performs an implicit relaxed atomic store.
  T *operator=(T *desired) noexcept {
    t.store(desired, relaxed);
    return desired;
  }

  rlx_atomic &operator=(const rlx_atomic) noexcept = delete;

  /// An explicit call to .store() also performs a relaxed store.
  void store(T *desired) noexcept { t.store(desired, relaxed); }

  /// Operator T* performs an implicit relaxed atomic load.
  operator T *() const noexcept { return t.load(relaxed); }

  /// An explicit call to .load() also performs a relaxed load.
  [[nodiscard]] T *load() const noexcept { return t.load(relaxed); }

  /// Overload -> operator to perform implicit relaxed load.
  T *operator->() const { return t.load(relaxed); }
};