#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/task/task.h>

namespace openpipeline {

template <OpenPipelineTraits Traits>
class TaskGroup {
 public:
  using Status = openpipeline::Status<Traits>;
  using NotifyFinishFunc = std::function<Status(const TaskContext<Traits>&)>;

  /**
   * @brief A conceptual group of N parallel instances of the same task.
   *
   * - `num_tasks` defines the group parallelism (often equals pipeline DOP).
   * - If provided, `cont` runs exactly once after all task instances finish successfully.
   * - `notify` is an optional hook to stop "possibly infinite" tasks from waiting
   *   (e.g., tell a sink to stop expecting more input).
   *
   * openpipeline does not provide a scheduler, so the exact ordering/thread of
   * `notify` and `cont` is scheduler-defined.
   */
  TaskGroup(std::string name, std::string desc, Task<Traits> task, std::size_t num_tasks,
            std::optional<Continuation<Traits>> cont = std::nullopt,
            NotifyFinishFunc notify = {})
      : name_(std::move(name)),
        desc_(std::move(desc)),
        task_(std::move(task)),
        num_tasks_(num_tasks),
        cont_(std::move(cont)),
        notify_(std::move(notify)) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  const Task<Traits>& GetTask() const noexcept { return task_; }
  std::size_t NumTasks() const noexcept { return num_tasks_; }
  const std::optional<Continuation<Traits>>& GetContinuation() const noexcept {
    return cont_;
  }

  Status NotifyFinish(const TaskContext<Traits>& ctx) const {
    if (!notify_) {
      return Traits::Status::OK();
    }
    return notify_(ctx);
  }

 private:
  std::string name_;
  std::string desc_;
  Task<Traits> task_;
  std::size_t num_tasks_;
  std::optional<Continuation<Traits>> cont_;
  NotifyFinishFunc notify_;
};

template <OpenPipelineTraits Traits>
using TaskGroups = std::vector<TaskGroup<Traits>>;

}  // namespace openpipeline
