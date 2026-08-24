#pragma once

/// A parent class for classes that are to be deletable by hazard pointer
/// implementations.
class hp_deletable {
public:
  virtual ~hp_deletable() = default;
};
