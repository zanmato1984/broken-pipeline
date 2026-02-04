#pragma once

#include <concepts>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include <broken_pipeline/task.h>

namespace bp::schedule {

template <class Traits>
concept ArrowBrokenPipelineTraits =
    bp::BrokenPipelineTraits<Traits> && std::same_as<typename Traits::Status, arrow::Status> &&
    std::same_as<bp::Result<Traits, int>, arrow::Result<int>>;

using Status = arrow::Status;

template <class T>
using Result = arrow::Result<T>;

using ResumerPtr = std::shared_ptr<bp::Resumer>;
using Resumers = std::vector<ResumerPtr>;

class ResumersAwaiter : public bp::Awaiter {
 public:
  ~ResumersAwaiter() override = default;
  virtual const Resumers& GetResumers() const = 0;
};

using ResumerFactory = std::function<Result<ResumerPtr>()>;
using AwaiterFactory =
    std::function<Result<std::shared_ptr<bp::Awaiter>>(Resumers)>;

/// @brief Arrow-based TaskContext used by the schedule library.
///
/// Broken Pipeline core defines `bp::TaskContext<Traits>`. This schedule library is
/// Arrow-coupled and uses `arrow::Status` / `arrow::Result<T>` directly so it can be
/// built as a concrete library (without templating on Traits).
struct TaskContext {
  const void* context = nullptr;
  ResumerFactory resumer_factory;
  AwaiterFactory awaiter_factory;
};

using TaskResult = Result<bp::TaskStatus>;

class Task {
 public:
  using Fn = std::function<TaskResult(const TaskContext&, bp::TaskId)>;

  Task(std::string name, Fn fn, bp::TaskHint hint = {})
      : name_(std::move(name)), fn_(std::move(fn)), hint_(std::move(hint)) {}

  TaskResult operator()(const TaskContext& ctx, bp::TaskId task_id) const {
    return fn_(ctx, task_id);
  }

  const std::string& Name() const noexcept { return name_; }
  const bp::TaskHint& Hint() const noexcept { return hint_; }

 private:
  std::string name_;
  Fn fn_;
  bp::TaskHint hint_;
};

class Continuation {
 public:
  using Fn = std::function<TaskResult(const TaskContext&)>;

  Continuation(std::string name, Fn fn, bp::TaskHint hint = {})
      : name_(std::move(name)), fn_(std::move(fn)), hint_(std::move(hint)) {}

  TaskResult operator()(const TaskContext& ctx) const { return fn_(ctx); }

  const std::string& Name() const noexcept { return name_; }
  const bp::TaskHint& Hint() const noexcept { return hint_; }

 private:
  std::string name_;
  Fn fn_;
  bp::TaskHint hint_;
};

class TaskGroup {
 public:
  TaskGroup(std::string name, Task task, std::size_t num_tasks,
            std::optional<Continuation> cont = std::nullopt)
      : name_(std::move(name)),
        task_(std::move(task)),
        num_tasks_(num_tasks),
        cont_(std::move(cont)) {}

  const std::string& Name() const noexcept { return name_; }
  const Task& Task() const noexcept { return task_; }
  std::size_t NumTasks() const noexcept { return num_tasks_; }
  const std::optional<Continuation>& Continuation() const noexcept { return cont_; }

 private:
  std::string name_;
  bp::schedule::Task task_;
  std::size_t num_tasks_;
  std::optional<bp::schedule::Continuation> cont_;
};

struct SchedulerOptions {
  /// @brief If true, resume all blocked resumers immediately.
  ///
  /// This is a test-only option to make scripted operators progress without wiring an
  /// external event source.
  bool auto_resume_blocked = true;

  /// @brief Safety cap to prevent infinite loops in tests.
  std::size_t step_limit = 1000;
};

Status InvalidAwaiterType(const char* scheduler_name);
Status InvalidResumerType(const char* scheduler_name);

void AutoResumeBlocked(const std::shared_ptr<bp::Awaiter>& awaiter);

/// @brief Wrap a Broken Pipeline `TaskGroup` into a schedule `TaskGroup`.
///
/// This wrapper allows the schedule library to drive any Broken Pipeline tasks whose
/// `Traits::Status` / `Traits::Result<T>` are Arrow-compatible.
template <ArrowBrokenPipelineTraits Traits>
TaskGroup WrapTaskGroup(const bp::TaskGroup<Traits>& group, bp::TaskContext<Traits> task_ctx) {
  Task task(
      group.Task().Name(),
      [task_fn = group.Task(), task_ctx](const TaskContext&, bp::TaskId task_id) -> TaskResult {
        return task_fn(task_ctx, task_id);
      },
      group.Task().Hint());

  std::optional<Continuation> cont;
  if (group.Continuation().has_value()) {
    cont = Continuation(
        group.Continuation()->Name(),
        [cont_fn = *group.Continuation(), task_ctx](const TaskContext&) -> TaskResult {
          return cont_fn(task_ctx);
        },
        group.Continuation()->Hint());
  }

  return TaskGroup(group.Name(), std::move(task), group.NumTasks(), std::move(cont));
}

}  // namespace bp::schedule
