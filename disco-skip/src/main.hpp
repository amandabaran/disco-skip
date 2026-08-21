#pragma GCC diagnostic ignored "-Wignored-attributes"
#pragma GCC diagnostic ignored "-Wignored-attributes"
#pragma GCC diagnostic ignored "-Wignored-attributes"
#pragma once

#include <chrono>
#include <array>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>
#include <functional>

#include <fmt/chrono.h>
#include <xxhash.h>

#include <fmt/color.h>
#include <fmt/ranges.h>
#include <lyra/lyra.hpp>

#include <dory/shared/branching.hpp>

#include <dory/conn/rc-exchanger.hpp>
#include <dory/conn/rc.hpp>
#include <dory/ctrl/block.hpp>
#include <dory/ctrl/device.hpp>
#include <dory/memstore/store.hpp>
#include <dory/shared/units.hpp>
#include <dory/shared/pinning.hpp>

#include <dory/extern/ibverbs.hpp>

using ConnectionExchanger = dory::conn::RcConnectionExchanger<ProcId>;
