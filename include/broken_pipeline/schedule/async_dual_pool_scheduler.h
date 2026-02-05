#pragma once

#include "scheduler.h"
#include "async_awaiter.h"
#include "async_resumer.h"

#include <folly/Executor.h>
#include <folly/futures/Future.h>

#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

namespace folly {
class CPUThreadPoolExecutor;
class IOThreadPoolExecutor;
}  // namespace folly

namespace bp::schedule {

class AsyncDualPoolScheduler {
 public:
  explicit AsyncDualPoolScheduler(SchedulerOptions options = {}, std::size_t cpu_threads = 1,
                                  std::size_t io_threads = 1);

  AsyncDualPoolScheduler(folly::Executor* cpu_executor, folly::Executor* io_executor,
                         SchedulerOptions options = {});

  ~AsyncDualPoolScheduler();

  AsyncDualPoolScheduler(const AsyncDualPoolScheduler&) = delete;
  AsyncDualPoolScheduler& operator=(const AsyncDualPoolScheduler&) = delete;
  AsyncDualPoolScheduler(AsyncDualPoolScheduler&&) noexcept = default;
  AsyncDualPoolScheduler& operator=(AsyncDualPoolScheduler&&) noexcept = default;

  TaskContext MakeTaskContext(const void* context = nullptr) const;

  template <ArrowBrokenPipelineTraits Traits>
  bp::TaskContext<Traits> MakeTaskContext(const typename Traits::Context* context = nullptr) const {
    bp::TaskContext<Traits> task_ctx;
    task_ctx.context = context;
    task_ctx.resumer_factory = []() -> bp::Result<Traits, std::shared_ptr<bp::Resumer>> {
      return std::make_shared<AsyncResumer>();
    };
    task_ctx.awaiter_factory =
        [](std::vector<std::shared_ptr<bp::Resumer>> resumers)
            -> bp::Result<Traits, std::shared_ptr<bp::Awaiter>> {
      return AsyncAwaiter::MakeAny(std::move(resumers));
    };
    return task_ctx;
  }

  struct TaskGroupHandle {
    folly::SemiFuture<Result<TaskStatus>> future;
    std::shared_ptr<std::mutex> statuses_mutex;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr);

  template <ArrowBrokenPipelineTraits Traits>
  TaskGroupHandle ScheduleTaskGroup(const bp::TaskGroup<Traits>& group,
                                    bp::TaskContext<Traits> task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) {
    return ScheduleTaskGroup(WrapTaskGroup(group, std::move(task_ctx)), TaskContext{}, statuses);
  }

  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group, const void* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr);

  template <ArrowBrokenPipelineTraits Traits>
  Result<TaskStatus> ScheduleAndWait(const bp::TaskGroup<Traits>& group,
                                     const typename Traits::Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) {
    auto task_ctx = MakeTaskContext<Traits>(context);
    auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
    return WaitTaskGroup(handle);
  }

 private:
  struct TaskState;
  using TaskFuture = folly::Future<Result<TaskStatus>>;

  TaskFuture MakeTaskFuture(const Task& task, TaskContext task_ctx, TaskId task_id,
                            std::shared_ptr<std::mutex> statuses_mutex,
                            std::shared_ptr<std::vector<TaskStatus>> statuses) const;

  SchedulerOptions options_;

  std::unique_ptr<folly::CPUThreadPoolExecutor> owned_cpu_executor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> owned_io_executor_;

  folly::Executor* cpu_executor_;
  folly::Executor* io_executor_;
};

}  // namespace bp::schedule
