#pragma once

#include <functional>
#include <utility>

#include <openpipeline/common/meta.h>
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
class Task : public Meta {
 public:
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&, TaskId<Traits>)>;

  Task(std::string name, std::string desc, Fn fn, TaskHint hint = {})
      : Meta(std::move(name), std::move(desc)),
        fn_(std::move(fn)),
        hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx, TaskId<Traits> task_id) const {
    return fn_(ctx, task_id);
  }

  const TaskHint& Hint() const noexcept { return hint_; }

 private:
  Fn fn_;
  TaskHint hint_;
};

template <OpenPipelineTraits Traits>
class Continuation : public Meta {
 public:
  using Fn = std::function<TaskResult<Traits>(const TaskContext<Traits>&)>;

  Continuation(std::string name, std::string desc, Fn fn, TaskHint hint = {})
      : Meta(std::move(name), std::move(desc)),
        fn_(std::move(fn)),
        hint_(std::move(hint)) {}

  TaskResult<Traits> operator()(const TaskContext<Traits>& ctx) const { return fn_(ctx); }

  const TaskHint& Hint() const noexcept { return hint_; }

 private:
  Fn fn_;
  TaskHint hint_;
};

}  // namespace openpipeline::task

