#pragma once

#include "scheduler.h"
#include "sync_awaiter.h"
#include "sync_resumer.h"

#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <type_traits>
#include <vector>

namespace bp::schedule {

class NaiveParallelScheduler {
 public:
  explicit NaiveParallelScheduler(SchedulerOptions options = {})
      : options_(std::move(options)) {}

  TaskContext MakeTaskContext(const void* context = nullptr) const;

  template <ArrowBrokenPipelineTraits Traits>
  bp::TaskContext<Traits> MakeTaskContext(const typename Traits::Context* context = nullptr) const {
    bp::TaskContext<Traits> task_ctx;
    task_ctx.context = context;
    task_ctx.resumer_factory = []() -> bp::Result<Traits, std::shared_ptr<bp::Resumer>> {
      return std::make_shared<SyncResumer>();
    };
    task_ctx.awaiter_factory =
        [](std::vector<std::shared_ptr<bp::Resumer>> resumers)
            -> bp::Result<Traits, std::shared_ptr<bp::Awaiter>> {
      return SyncAwaiter::MakeAny(std::move(resumers));
    };
    return task_ctx;
  }

  struct TaskGroupHandle {
    std::future<Result<TaskStatus>> future;
    std::shared_ptr<std::mutex> statuses_mutex;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const;

  template <ArrowBrokenPipelineTraits Traits>
  TaskGroupHandle ScheduleTaskGroup(const bp::TaskGroup<Traits>& group,
                                    bp::TaskContext<Traits> task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const {
    return ScheduleTaskGroup(WrapTaskGroup(group, std::move(task_ctx)), TaskContext{}, statuses);
  }

  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group, const void* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) const;

  template <ArrowBrokenPipelineTraits Traits>
  Result<TaskStatus> ScheduleAndWait(const bp::TaskGroup<Traits>& group,
                                     const typename Traits::Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) const {
    auto task_ctx = MakeTaskContext<Traits>(context);
    auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
    return WaitTaskGroup(handle);
  }

 private:
  using ConcreteTask = std::future<Result<TaskStatus>>;

  ConcreteTask MakeTask(const Task& task, const TaskContext& task_ctx, TaskId task_id,
                        std::shared_ptr<std::mutex> statuses_mutex,
                        std::shared_ptr<std::vector<TaskStatus>> statuses) const;

  SchedulerOptions options_;
};

}  // namespace bp::schedule
