#pragma once

#include <openpipeline/concepts.h>
#include <openpipeline/task/awaiter.h>

namespace openpipeline {

/**
 * @brief Per-task immutable context + scheduler factory hooks.
 *
 * A scheduler is expected to create one `TaskContext` and pass it to all task instances
 * in a `TaskGroup`. openpipeline itself does not construct these.
 *
 * - `context` is an optional user-defined pointer to query-level state (can be null).
 * - The factories provide scheduler-specific primitives for blocking and resumption.
 *
 * When a task returns `TaskStatus::Blocked(awaiter)`, the scheduler is responsible for:
 * - waiting/suspending using the awaiter
 * - rescheduling the task when resumed
 */
template <OpenPipelineTraits Traits>
struct TaskContext {
  const typename Traits::Context* context = nullptr;
  ResumerFactory<Traits> resumer_factory;
  SingleAwaiterFactory<Traits> single_awaiter_factory;
  AnyAwaiterFactory<Traits> any_awaiter_factory;
  AllAwaiterFactory<Traits> all_awaiter_factory;
};

}  // namespace openpipeline
