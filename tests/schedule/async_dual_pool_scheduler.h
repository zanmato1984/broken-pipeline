#pragma once

#include "arrow_traits.h"
#include "scheduler.h"

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

namespace bp_test::schedule {

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

  TaskContext MakeTaskContext(const Context* context = nullptr) const;

  struct TaskGroupHandle {
    folly::SemiFuture<Result<TaskStatus>> future;
    std::shared_ptr<std::mutex> statuses_mutex;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr);
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group, const Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr);

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

}  // namespace bp_test::schedule
