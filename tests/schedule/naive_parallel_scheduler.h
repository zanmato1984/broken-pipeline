#pragma once

#include "arrow_traits.h"
#include "scheduler.h"

#include <cstddef>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

namespace bp_test::schedule {

class NaiveParallelScheduler {
 public:
  explicit NaiveParallelScheduler(SchedulerOptions options = {})
      : options_(std::move(options)) {}

  TaskContext MakeTaskContext(const Context* context = nullptr) const;

  struct TaskGroupHandle {
    std::future<Result<TaskStatus>> future;
    std::shared_ptr<std::mutex> statuses_mutex;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const;
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                     const Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) const;

 private:
  using ConcreteTask = std::future<Result<TaskStatus>>;

  ConcreteTask MakeTask(const Task& task, const TaskContext& task_ctx, TaskId task_id,
                        std::shared_ptr<std::mutex> statuses_mutex,
                        std::shared_ptr<std::vector<TaskStatus>> statuses) const;

  SchedulerOptions options_;
};

}  // namespace bp_test::schedule

