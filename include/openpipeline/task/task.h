#pragma once

#include <functional>
#include <string>
#include <utility>

#include <openpipeline/concepts.h>
#include <openpipeline/task/task_context.h>
#include <openpipeline/task/task_status.h>

namespace openpipeline::task {

/**
 * @brief Scheduling hint for a task instance.
 *
 * openpipeline core does not ship a scheduler, but some schedulers may use this hint to
 * choose between CPU vs IO pools or adjust priorities.
 */
struct TaskHint {
  enum class Type {
    CPU,
    IO,
  };
  Type type = Type::CPU;
};

template <OpenPipelineTraits Traits>
using TaskId = typename Traits::TaskId;

template <OpenPipelineTraits Traits>
using TaskResult = Result<Traits, TaskStatus>;

template <OpenPipelineTraits Traits>
class Task {
 public:
  /**
   * @brief Task entrypoint.
   *
   * Contract:
   * - The scheduler will call `fn(ctx, task_id)` repeatedly until it returns
   *   `Finished/Cancelled` or an error.
   * - `task_id` is the 0-based index of this task instance within its `TaskGroup`.
   * - The function must be re-entrant: it should do bounded work per call and use
   *   internal state (often indexed by `task_id`) to resume across calls.
   */
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&, TaskId<Traits>)>;

  Task(std::string name, std::string desc, Fn fn, TaskHint hint = {})
      : name_(std::move(name)),
        desc_(std::move(desc)),
        fn_(std::move(fn)),
        hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx, TaskId<Traits> task_id) const {
    return fn_(ctx, task_id);
  }

  /**
   * @brief Human-readable name/description. Purely informational.
   */
  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }
  const TaskHint& Hint() const noexcept { return hint_; }

 private:
  std::string name_;
  std::string desc_;
  Fn fn_;
  TaskHint hint_;
};

template <OpenPipelineTraits Traits>
class Continuation {
 public:
  /**
   * @brief Continuation entrypoint executed after all task instances in the group finish.
   *
   * A `TaskGroup` may have an optional `Continuation` that is guaranteed to run exactly
   * once *after* all parallel task instances have successfully finished.
   *
   * This is the canonical place for barrier work: merging thread-local state, finalizing
   * shared structures, etc.
   */
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&)>;

  Continuation(std::string name, std::string desc, Fn fn, TaskHint hint = {})
      : name_(std::move(name)),
        desc_(std::move(desc)),
        fn_(std::move(fn)),
        hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx) const { return fn_(ctx); }

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }
  const TaskHint& Hint() const noexcept { return hint_; }

 private:
  std::string name_;
  std::string desc_;
  Fn fn_;
  TaskHint hint_;
};

}  // namespace openpipeline::task
