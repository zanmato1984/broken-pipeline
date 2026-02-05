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

#include "async_awaiter.h"
#include "async_resumer.h"
#include "traits.h"

#include <folly/Executor.h>
#include <folly/futures/Future.h>

#include <cstddef>
#include <future>
#include <memory>
#include <vector>

namespace folly {
class CPUThreadPoolExecutor;
class IOThreadPoolExecutor;
}  // namespace folly

namespace bp::schedule {

class AsyncDualPoolScheduler {
 public:
  static constexpr std::size_t kDefaultStepLimit = 1'000'000;

  explicit AsyncDualPoolScheduler(std::size_t cpu_threads = 1, std::size_t io_threads = 1,
                                  std::size_t step_limit = kDefaultStepLimit);

  AsyncDualPoolScheduler(folly::Executor* cpu_executor, folly::Executor* io_executor,
                         std::size_t step_limit = kDefaultStepLimit);

  ~AsyncDualPoolScheduler();

  AsyncDualPoolScheduler(const AsyncDualPoolScheduler&) = delete;
  AsyncDualPoolScheduler& operator=(const AsyncDualPoolScheduler&) = delete;
  AsyncDualPoolScheduler(AsyncDualPoolScheduler&&) noexcept = default;
  AsyncDualPoolScheduler& operator=(AsyncDualPoolScheduler&&) noexcept = default;

  TaskContext MakeTaskContext(const Traits::Context* context = nullptr) const;

  struct TaskGroupHandle {
    folly::SemiFuture<Result<TaskStatus>> future;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr);

  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                     const Traits::Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr);

 private:
  struct TaskState;
  using TaskFuture = folly::Future<Result<TaskStatus>>;

  TaskFuture MakeTaskFuture(const Task& task, TaskContext task_ctx, TaskId task_id,
                            std::shared_ptr<std::vector<TaskStatus>> status_log) const;

  std::size_t step_limit_;

  std::unique_ptr<folly::CPUThreadPoolExecutor> owned_cpu_executor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> owned_io_executor_;

  folly::Executor* cpu_executor_;
  folly::Executor* io_executor_;
};

}  // namespace bp::schedule
