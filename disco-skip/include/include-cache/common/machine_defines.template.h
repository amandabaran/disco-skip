#pragma once

// This file defines the maximum number of threads to use in template constants,
// to speed up compilation on systems that don't need higher thread counts.

// To build this project, you must copy this file to "machine_defines.h,"
// and configure the values below appropriately for your local machine.

// The maximum number of threads that can be run at once. Typically, this should
// be set to the number of hardware threads the machine can support.
#define MAX_THREADS 4

// The total number of numa zones on this machine.
#define NUMA_ZONES 1

// The total number of threads that can be run on each core.
// (Typically, 1 for no hyperthreading, 2 for hyperthreading.)
#define THREADS_PER_CORE 1

// The number of templates to build.
// 0 is the minimum, 1 is moderate, 2 is full.
// Lower numbers are good for speeding up debug cycles.
#define TEMPLATE_INITIALIZATION 0
