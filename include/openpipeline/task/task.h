#pragma once

#include <functional>
#include <string>
#include <utility>

#include <openpipeline/concepts.h>
#include <openpipeline/task/task_context.h>
#include <openpipeline/task/task_status.h>

namespace openpipeline::task {

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
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&, TaskId<Traits>)>;

  Task(std::string name, std::string desc, Fn fn, TaskHint hint = {})
      : name_(std::move(name)),
        desc_(std::move(desc)),
        fn_(std::move(fn)),
        hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx, TaskId<Traits> task_id) const {
    return fn_(ctx, task_id);
  }

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
