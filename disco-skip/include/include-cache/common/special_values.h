#pragma once

// A special value which, for several different configuration parameters,
// indicates this value should be computed automatically.
constexpr int64_t AUTOMATIC = 0;

// A special value which, if provided to a skipvector MAP as the DATA_EXP or
// IDX_EXP templated values, indicates that all nodes of that type should bear
// only a single element, hence simulating the behavior of a skip list. The
// ratio of the number of elements in that layer compared to the layer above
// should also be fixed at 2.
constexpr int64_t SKIPLIST_SIM_MODE = -1;

// A special value which, if provided to a skipvector MAP as the IDX_SIZE
// templated value, indicates that the index layer should be completely elided,
// and all no tall nodes should ever be inserted.
constexpr int64_t SKIPARRAY_SIM_MODE = -2;
