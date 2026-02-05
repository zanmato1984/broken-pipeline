#pragma once

#include <cstddef>

namespace bp::schedule {

/// @brief Optional opaque context pointer for schedule-driven tasks.
///
/// Broken Pipeline's `bp::TaskContext<Traits>` exposes a typed `Traits::Context*`. The
/// schedule library uses `void*` in its concrete TaskContext, but callers often have a
/// query-level context object to thread through.
struct ScheduleContext {
  const void* context = nullptr;
};

}  // namespace bp::schedule
