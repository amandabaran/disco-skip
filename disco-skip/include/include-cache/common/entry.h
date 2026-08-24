#pragma once

template <typename K, typename V, K EMPTY_KEY> class entry {
  K key;
  V val;

public:
  using KEY_TYPE = K;
  using VAL_TYPE = V;

  static constexpr K EMPTY = EMPTY_KEY;

  entry() : key(EMPTY), val(static_cast<V>(0)){};
  entry(K k, V v) : key(k), val(v){};

  K &k() { return key; }
  V &v() { return val; }
  K get_k() const { return key; }
  V get_v() const { return val; }
  void k(K const &k) { key = k; };
  void v(V const &v) { val = v; }
};

template <typename K, typename V, K EMPTY>
bool operator>(const entry<K, V, EMPTY> &a, const entry<K, V, EMPTY> &b) {
  return a.get_k() > b.get_k();
}

template <typename K, typename V, K EMPTY>
bool operator>=(const entry<K, V, EMPTY> &a, const entry<K, V, EMPTY> &b) {
  return a.get_k() >= b.get_k();
}

template <typename K, typename V, K EMPTY>
bool operator<(const entry<K, V, EMPTY> &a, const entry<K, V, EMPTY> &b) {
  return a.get_k() < b.get_k();
}

template <typename K, typename V, K EMPTY>
bool operator<=(const entry<K, V, EMPTY> &a, const entry<K, V, EMPTY> &b) {
  return a.get_k() <= b.get_k();
}

template <typename K, typename V, K EMPTY>
bool operator==(const entry<K, V, EMPTY> &a, const entry<K, V, EMPTY> &b) {
  return a.get_k() == b.get_k();
}

template <typename K, typename V, K EMPTY>
bool operator!=(const entry<K, V, EMPTY> &a, const entry<K, V, EMPTY> &b) {
  return a.get_k() != b.get_k();
}
