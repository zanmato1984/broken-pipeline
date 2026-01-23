#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <utility>
#include <vector>

#include <openpipeline/common/meta.h>
#include <openpipeline/concepts.h>
#include <openpipeline/task/task.h>

namespace openpipeline::task {

template <OpenPipelineTraits Traits>
class TaskGroup : public Meta {
 public:
  using Status = Result<Traits, void>;
  using NotifyFinishFunc = std::function<Status(const TaskContext<Traits>&)>;

  TaskGroup(std::string name, std::string desc, Task<Traits> task, std::size_t num_tasks,
            std::optional<Continuation<Traits>> cont = std::nullopt,
            NotifyFinishFunc notify = {})
      : Meta(std::move(name), std::move(desc)),
        task_(std::move(task)),
        num_tasks_(num_tasks),
        cont_(std::move(cont)),
        notify_(std::move(notify)) {}

  const Task<Traits>& GetTask() const noexcept { return task_; }
  std::size_t NumTasks() const noexcept { return num_tasks_; }
  const std::optional<Continuation<Traits>>& GetContinuation() const noexcept {
    return cont_;
  }

  Status NotifyFinish(const TaskContext<Traits>& ctx) const {
    if (!notify_) {
      return Traits::Ok();
    }
    return notify_(ctx);
  }

 private:
  Task<Traits> task_;
  std::size_t num_tasks_;
  std::optional<Continuation<Traits>> cont_;
  NotifyFinishFunc notify_;
};

template <OpenPipelineTraits Traits>
using TaskGroups = std::vector<TaskGroup<Traits>>;

}  // namespace openpipeline::task

