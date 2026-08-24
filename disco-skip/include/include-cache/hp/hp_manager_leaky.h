#pragma once

#include <atomic>
#include <string>

#include "hp_deletable.h"

/// A class with the same interface as the hazard pointer manager class,
/// but which does nothing and simply leaks all pointers.
class hp_manager_leaky {

public:
  static void init_context() {}
  static void tear_down() {}
  static void reclaim(hp_deletable *) {}
  static void sweep_all() {}
  static void take(hp_deletable *) {}
  static void take_first(hp_deletable *) {}
  static void take_next(hp_deletable *) {}
  static void drop() {}
  static void drop_curr() {}
  static void drop_next() {}
  static void drop_all() {}
  static int reserved_by_thread() { return 0; }
  static int count_reserved() { return 0; }
  static std::string_view get_name() { return "Leaky"; }
};
