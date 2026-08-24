#ifndef LEHMER64_H
#define LEHMER64_H

#include "splitmix64.h"

/**
 * D. H. Lehmer, Mathematical methods in large-scale computing units.
 * Proceedings of a Second Symposium on Large Scale Digital Calculating
 * Machinery;
 * Annals of the Computation Laboratory, Harvard Univ. 26 (1951), pp. 141-146.
 *
 * P L'Ecuyer,  Tables of linear congruential generators of different sizes and
 * good lattice structure. Mathematics of Computation of the American
 * Mathematical
 * Society 68.225 (1999): 249-260.
 */

// Made stateless for multi-thread use. - Matthew
inline __uint128_t lehmer64_seed(uint64_t seed) {
  return (static_cast<__uint128_t>(splitmix64_stateless(seed)) << 64) +
         splitmix64_stateless(seed + 1);
}

inline uint64_t lehmer64(__uint128_t &g_lehmer64_state) {
  g_lehmer64_state *= UINT64_C(0xda942042e4dd58b5);
  return g_lehmer64_state >> 64;
}

#endif
