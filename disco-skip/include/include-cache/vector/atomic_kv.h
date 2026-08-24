#pragma once

/// A key and value type packed into a struct for locality.
///
/// @param ENTRY      The type representing a non-atomic key/value pair.
/// @param K_WRAPPER  A wrapper around K. Either std::atomic or rlx_atomic.
/// @param V_WRAPPER  A wrapper around V. Either std::atomic or rlx_atomic.
template <typename ENTRY, template <typename> typename K_WRAPPER,
          template <typename> typename V_WRAPPER>
struct atomic_kv {
  using K = typename ENTRY::KEY_TYPE;
  using V = typename ENTRY::VAL_TYPE;
  static constexpr K EMPTY = ENTRY::EMPTY;

  K_WRAPPER<K> key;
  V_WRAPPER<V> val;

  atomic_kv() = default;
  explicit atomic_kv(ENTRY const &entry) { operator=(entry); }
  atomic_kv(atomic_kv const &other) { operator=(other); }

  void swap(atomic_kv &other) {
    ENTRY const entry = unwrap();
    operator=(other);
    other = entry;
  }

  /// Take the victim's key by atomically exchanging it with EMPTY,
  /// and then copy the victim's value.
  void rob(atomic_kv &victim) {
    assert(victim.key.load() != EMPTY);
    key = victim.key.exchange(EMPTY);
    val = victim.val.load();
  }

  atomic_kv &operator=(atomic_kv const &other) {
    val = other.val.load();
    key = other.key.load();
    return *this;
  }

  atomic_kv &operator=(ENTRY const &entry) {
    // NB: MV_ASR requires that we write val before key,
    // and other implementations do not care.
    val = entry.get_v();
    key = entry.get_k();
    return *this;
  }

  atomic_kv &operator=(const std::pair<K, V> &pair) {
    // NB: MV_ASR requires that we write val before key,
    // and other implementations do not care.
    val = pair.second;
    key = pair.first;
    return *this;
  }

  ENTRY unwrap() { return ENTRY(key.load(), val.load()); }
};

template <typename ENTRY, template <typename> typename KW,
          template <typename> typename VW>
void swap(atomic_kv<ENTRY, KW, VW> &a1, atomic_kv<ENTRY, KW, VW> &a2) {
  a1.swap(a2);
}

template <typename ENTRY, template <typename> typename KW,
          template <typename> typename VW>
bool operator>(const atomic_kv<ENTRY, KW, VW> &a1,
               const atomic_kv<ENTRY, KW, VW> &a2) {
  return a1.key > a2.key;
}

template <typename ENTRY, template <typename> typename KW,
          template <typename> typename VW>
bool operator<(const atomic_kv<ENTRY, KW, VW> &a1,
               const atomic_kv<ENTRY, KW, VW> &a2) {
  return a1.key < a2.key;
}

template <typename ENTRY, template <typename> typename KW,
          template <typename> typename VW>
bool operator>=(const atomic_kv<ENTRY, KW, VW> &a1,
                const atomic_kv<ENTRY, KW, VW> &a2) {
  return a1.key >= a2.key;
}

template <typename ENTRY, template <typename> typename KW,
          template <typename> typename VW>
bool operator<=(const atomic_kv<ENTRY, KW, VW> &a1,
                const atomic_kv<ENTRY, KW, VW> &a2) {
  return a1.key <= a2.key;
}

template <typename ENTRY, template <typename> typename KW,
          template <typename> typename VW>
bool operator==(const atomic_kv<ENTRY, KW, VW> &a1,
                const atomic_kv<ENTRY, KW, VW> &a2) {
  return a1.key == a2.key;
}
