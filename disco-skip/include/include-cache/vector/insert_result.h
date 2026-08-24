#pragma once

/// An enum of the possible results of an insertion operation to a multivector
/// interface class.
enum insert_result {
  /// The insertion was successful.
  SUCCESS,

  /// The insert failed because the actual version number did not match the
  /// expected one.
  VERSION_CHANGED,

  /// This vector is dead or is in some other state
  /// that does not permit further insertions.
  DEAD_VECTOR,

  /// This insertion failed due to the vector being full,
  /// and this thread must perform the split.
  MUST_SPLIT,

  /// This insertion failed due to the vector being full, and some other thread
  /// is responsible for splitting, but it is not yet complete.
  SPLIT_PENDING
};
