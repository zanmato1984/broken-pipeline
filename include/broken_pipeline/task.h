// Copyright 2026 Rossi Sun
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cassert>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <broken_pipeline/concepts.h>

namespace bp {

/// @brief Scheduler-owned wake-up primitive for blocked tasks.
///
/// Broken Pipeline operators never block threads directly. When an operator cannot make
/// progress (for example, due to IO or downstream backpressure), it returns
/// `OpOutput::Blocked(resumer)`.
///
/// A task aggregates one or more resumers into an awaiter using
/// `TaskContext::awaiter_factory` and returns `TaskStatus::Blocked(awaiter)`.
///
/// When the awaited condition becomes true, some non-scheduler event source calls
/// `Resumer::Resume()` (for example, an IO callback or an operator-owned background
/// task). The scheduler observes resumed resumers and re-schedules the blocked task
/// instance.
class Resumer {
 public:
  virtual ~Resumer() = default;
  virtual void Resume() = 0;
  virtual bool IsResumed() const = 0;
};

/// @brief Scheduler-owned wait handle for one or more resumers.
///
/// `Awaiter` is intentionally opaque to Broken Pipeline core. The scheduler decides how
/// to wait until one or more resumers are resumed.
///
/// Examples:
/// - Synchronous scheduler: condition variables or other blocking primitives.
/// - Asynchronous scheduler: future/promise style or event-loop integration.
/// - Coroutine scheduler: a coroutine-friendly awaitable implementation.
///
/// Broken Pipeline only stores `std::shared_ptr<Awaiter>` inside
/// `TaskStatus::Blocked(...)`.
class Awaiter {
 public:
  virtual ~Awaiter() = default;
};

template <BrokenPipelineTraits Traits>
using ResumerFactory = std::function<Result<Traits, std::shared_ptr<Resumer>>()>;

template <BrokenPipelineTraits Traits>
using AwaiterFactory = std::function<Result<Traits, std::shared_ptr<Awaiter>>(
    std::vector<std::shared_ptr<Resumer>>)>;

/// @brief Per-task immutable context + scheduler factory hooks.
///
/// A scheduler is expected to create one `TaskContext` and pass it to all task instances
/// in a `TaskGroup`. Broken Pipeline does not construct these.
///
/// - `context` is an optional, type-erased pointer to query-level state (can be null).
/// - The factories provide scheduler-specific primitives for blocked waiting and wake-up.
///
/// When a task returns `TaskStatus::Blocked(awaiter)`, the scheduler is responsible for:
/// - waiting/suspending using the awaiter
/// - re-scheduling the task instance after one or more resumers are resumed
template <BrokenPipelineTraits Traits>
struct TaskContext {
  /// @brief Optional, type-erased query-level context pointer (may be null).
  const void* context = nullptr;
  ResumerFactory<Traits> resumer_factory;
  AwaiterFactory<Traits> awaiter_factory;

  template <class T>
  const T* ContextAs() const {
    return static_cast<const T*>(context);
  }

  template <class T>
  const T& ContextRef() const {
    assert(context != nullptr);
    return *static_cast<const T*>(context);
  }
};

/// @brief The control protocol between a Task and a Scheduler.
///
/// Broken Pipeline tasks are small-step and repeatable: a scheduler repeatedly invokes
/// a task until it returns `Finished`/`Cancelled` or an error.
///
/// - `Continue`: still running; scheduler may invoke again immediately or later.
/// - `Blocked(awaiter)`: cannot make progress; scheduler must wait/suspend on `awaiter`.
/// - `Yield`: task requests a yield point before continuing, typically before a long
///   synchronous step (often IO-heavy, such as spilling). A scheduler may migrate it to
///   another pool/priority class.
/// - `Finished`: completed successfully.
/// - `Cancelled`: cancelled due to external reasons (often sibling failure).
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

/// @brief Scheduling hint for a task instance.
///
/// Broken Pipeline core does not ship a scheduler, but some schedulers may use this hint
/// to choose between CPU vs IO pools or adjust priorities.
struct TaskHint {
  enum class Type {
    CPU,
    IO,
  };
  Type type = Type::CPU;
};

template <BrokenPipelineTraits Traits>
using TaskResult = Result<Traits, TaskStatus>;

/// @brief Task instance id within a group.
///
/// Broken Pipeline uses a uniform `std::size_t` task id. It is typically interpreted as a
/// lane index for operators.
template <BrokenPipelineTraits Traits>
class Task {
 public:
  /// @brief Task entrypoint.
  ///
  /// Contract:
  /// - The scheduler will call `fn(ctx, task_id)` repeatedly until it returns
  ///   `Finished/Cancelled` or an error.
  /// - `task_id` is the 0-based index of this task instance within its `TaskGroup`.
  /// - The function must be re-entrant: it should do bounded work per call and use
  ///   internal state (often indexed by `task_id`) to continue across calls.
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&, TaskId)>;

  Task(std::string name, Fn fn, TaskHint hint = {})
      : name_(std::move(name)), fn_(std::move(fn)), hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx, TaskId task_id) const {
    return fn_(ctx, task_id);
  }

  /// @brief Human-readable name. Purely informational.
  const std::string& Name() const noexcept { return name_; }
  const TaskHint& Hint() const noexcept { return hint_; }

 private:
  std::string name_;
  Fn fn_;
  TaskHint hint_;
};

template <BrokenPipelineTraits Traits>
class Continuation {
 public:
  /// @brief Continuation entrypoint executed after all task instances in the group
  /// finish.
  ///
  /// A `TaskGroup` may have an optional `Continuation` that is guaranteed to run exactly
  /// once after all parallel task instances have successfully finished.
  ///
  /// This is the canonical place for barrier work: merging thread-local state, finalizing
  /// shared structures, etc.
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&)>;

  Continuation(std::string name, Fn fn, TaskHint hint = {})
      : name_(std::move(name)), fn_(std::move(fn)), hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx) const { return fn_(ctx); }

  const std::string& Name() const noexcept { return name_; }
  const TaskHint& Hint() const noexcept { return hint_; }

 private:
  std::string name_;
  Fn fn_;
  TaskHint hint_;
};

template <BrokenPipelineTraits Traits>
class TaskGroup {
 public:
  /// @brief A conceptual group of N parallel instances of the same task.
  ///
  /// - `num_tasks` defines the group parallelism (often equals pipeline DOP).
  /// - If provided, `cont` runs exactly once after all task instances finish
  /// successfully.
  ///
  /// Broken Pipeline does not provide a scheduler, so the exact ordering/thread of
  /// `cont` is scheduler-defined.
  TaskGroup(std::string name, bp::Task<Traits> task, std::size_t num_tasks,
            std::optional<bp::Continuation<Traits>> cont = std::nullopt)
      : name_(std::move(name)),
        task_(std::move(task)),
        num_tasks_(num_tasks),
        cont_(std::move(cont)) {}

  const std::string& Name() const noexcept { return name_; }

  const bp::Task<Traits>& Task() const noexcept { return task_; }
  std::size_t NumTasks() const noexcept { return num_tasks_; }
  const std::optional<bp::Continuation<Traits>>& Continuation() const noexcept {
    return cont_;
  }

 private:
  std::string name_;
  bp::Task<Traits> task_;
  std::size_t num_tasks_;
  std::optional<bp::Continuation<Traits>> cont_;
};

}  // namespace bp
