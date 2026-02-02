#pragma once

#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <opl/concepts.h>

namespace opl {

/**
 * @brief A scheduler-owned "waker" that transitions a previously blocked task back to runnable.
 *
 * opl operators never block threads directly. Instead, when an operator cannot
 * make progress (e.g., waiting on IO or downstream backpressure), it returns
 * `OpOutput::Blocked(resumer)`.
 *
 * A scheduler turns one or more `Resumer` objects into an `Awaiter`
 * and returns `TaskStatus::Blocked(awaiter)` from the task.
 *
 * Later, some external event triggers `Resumer::Resume()` (often from a callback).
 * The scheduler observes that and re-schedules the blocked task.
 */
class Resumer {
 public:
  virtual ~Resumer() = default;
  virtual void Resume() = 0;
  virtual bool IsResumed() const = 0;
};

/**
 * @brief A scheduler-owned wait handle for one or more resumers.
 *
 * `Awaiter` is intentionally opaque to opl core. The scheduler decides how
 * to block (or suspend) until a resumer (or group of resumers) is resumed.
 *
 * For example, a synchronous scheduler may implement an awaiter using condition
 * variables, while an async scheduler may implement it using futures/coroutines.
 *
 * opl only stores `std::shared_ptr<Awaiter>` inside `TaskStatus::Blocked(...)`.
 */
class Awaiter {
 public:
  virtual ~Awaiter() = default;
};

template <OpenPipelineTraits Traits>
using ResumerFactory = std::function<Result<Traits, std::shared_ptr<Resumer>>()>;

template <OpenPipelineTraits Traits>
using AwaiterFactory =
    std::function<Result<Traits, std::shared_ptr<Awaiter>>(std::vector<std::shared_ptr<Resumer>>)>;

/**
 * @brief Per-task immutable context + scheduler factory hooks.
 *
 * A scheduler is expected to create one `TaskContext` and pass it to all task instances
 * in a `TaskGroup`. opl itself does not construct these.
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
  AwaiterFactory<Traits> awaiter_factory;
};

/**
 * @brief The control protocol between a Task and a Scheduler.
 *
 * opl tasks are **small-step and repeatable**: a scheduler repeatedly invokes
 * a task until it returns `Finished`/`Cancelled` or an error.
 *
 * - `Continue`: still running; scheduler may invoke again immediately or later.
 * - `Blocked(awaiter)`: cannot make progress; scheduler must wait/suspend on `awaiter`.
 * - `Yield`: task is about to do a long synchronous operation and requests yielding.
 *   A scheduler may migrate it to another pool/priority class.
 * - `Finished`: completed successfully.
 * - `Cancelled`: cancelled due to external reasons (often sibling failure).
 */
class TaskStatus {
 public:
  enum class Code {
    CONTINUE,
    BLOCKED,
    YIELD,
    FINISHED,
    CANCELLED,
  };

  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Blocked(std::shared_ptr<Awaiter> awaiter) {
    return TaskStatus(std::move(awaiter));
  }
  static TaskStatus Yield() { return TaskStatus(Code::YIELD); }
  static TaskStatus Finished() { return TaskStatus(Code::FINISHED); }
  static TaskStatus Cancelled() { return TaskStatus(Code::CANCELLED); }

  bool IsContinue() const noexcept { return code_ == Code::CONTINUE; }
  bool IsBlocked() const noexcept { return code_ == Code::BLOCKED; }
  bool IsYield() const noexcept { return code_ == Code::YIELD; }
  bool IsFinished() const noexcept { return code_ == Code::FINISHED; }
  bool IsCancelled() const noexcept { return code_ == Code::CANCELLED; }

  std::shared_ptr<Awaiter>& GetAwaiter() {
    assert(IsBlocked());
    return awaiter_;
  }

  const std::shared_ptr<Awaiter>& GetAwaiter() const {
    assert(IsBlocked());
    return awaiter_;
  }

  std::string ToString() const {
    switch (code_) {
      case Code::CONTINUE:
        return "CONTINUE";
      case Code::BLOCKED:
        return "BLOCKED";
      case Code::YIELD:
        return "YIELD";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
    }
    return "UNKNOWN";
  }

 private:
  explicit TaskStatus(Code code) : code_(code) {}

  explicit TaskStatus(std::shared_ptr<Awaiter> awaiter)
      : code_(Code::BLOCKED), awaiter_(std::move(awaiter)) {}

  Code code_;
  std::shared_ptr<Awaiter> awaiter_;
};

/**
 * @brief Scheduling hint for a task instance.
 *
 * opl core does not ship a scheduler, but some schedulers may use this hint to
 * choose between CPU vs IO pools or adjust priorities.
 */
struct TaskHint {
  enum class Type {
    CPU,
    IO,
  };
  Type type = Type::CPU;
};

/**
 * @brief Task instance id within a group.
 *
 * opl uses a uniform `std::size_t` task id. It is typically interpreted as a
 * lane index for operators.
 */

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
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&, TaskId)>;

  Task(std::string name, std::string desc, Fn fn, TaskHint hint = {})
      : name_(std::move(name)),
        desc_(std::move(desc)),
        fn_(std::move(fn)),
        hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx, TaskId task_id) const {
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

template <OpenPipelineTraits Traits>
class TaskGroup {
 public:
  /**
   * @brief A conceptual group of N parallel instances of the same task.
   *
   * - `num_tasks` defines the group parallelism (often equals pipeline DOP).
   * - If provided, `cont` runs exactly once after all task instances finish successfully.
   *
   * opl does not provide a scheduler, so the exact ordering/thread of
   * `cont` is scheduler-defined.
   */
  TaskGroup(std::string name, std::string desc, Task<Traits> task, std::size_t num_tasks,
            std::optional<Continuation<Traits>> cont = std::nullopt)
      : name_(std::move(name)),
        desc_(std::move(desc)),
        task_(std::move(task)),
        num_tasks_(num_tasks),
        cont_(std::move(cont)) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  const Task<Traits>& GetTask() const noexcept { return task_; }
  std::size_t NumTasks() const noexcept { return num_tasks_; }
  const std::optional<Continuation<Traits>>& GetContinuation() const noexcept {
    return cont_;
  }

 private:
  std::string name_;
  std::string desc_;
  Task<Traits> task_;
  std::size_t num_tasks_;
  std::optional<Continuation<Traits>> cont_;
};

}  // namespace opl
