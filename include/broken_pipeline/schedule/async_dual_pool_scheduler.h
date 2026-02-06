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

/// @file async_dual_pool_scheduler.h
///
/// @brief Folly-based dual-pool scheduler for Broken Pipeline task groups.
///
/// This scheduler binds to Folly executors, routes CPU-bound work to a CPU pool,
/// IO-bound work to an IO pool, and integrates with `detail::CallbackResumer`/`detail::FutureAwaiter`
/// for blocked tasks.

#include "detail/callback_resumer.h"
#include "detail/future_awaiter.h"
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

/// @brief Production-oriented scheduler using separate CPU and IO pools.
///
/// Behavior overview:
/// - Uses `detail::CallbackResumer` and `detail::FutureAwaiter` to suspend and resume blocked tasks.
/// - Routes tasks to the IO pool when `TaskHint::Type::IO` is set.
/// - Treats `TaskStatus::Yield()` as a handoff point to the IO pool for a single
///   step, then resumes normal scheduling.
/// - Aggregates per-task results and optionally invokes the TaskGroup continuation.
///
/// The scheduler can either own its executors (constructed with thread counts) or
/// attach to externally managed Folly executors.
class AsyncDualPoolScheduler {
 public:
  /// @brief Construct and own CPU/IO thread pools.
  explicit AsyncDualPoolScheduler(std::size_t cpu_threads = 1, std::size_t io_threads = 1);

  /// @brief Bind to externally managed Folly executors.
  AsyncDualPoolScheduler(folly::Executor* cpu_executor, folly::Executor* io_executor);

  ~AsyncDualPoolScheduler();

  AsyncDualPoolScheduler(const AsyncDualPoolScheduler&) = delete;
  AsyncDualPoolScheduler& operator=(const AsyncDualPoolScheduler&) = delete;
  AsyncDualPoolScheduler(AsyncDualPoolScheduler&&) noexcept = default;
  AsyncDualPoolScheduler& operator=(AsyncDualPoolScheduler&&) noexcept = default;

  /// @brief Create a TaskContext configured with future-based resumers/awaiters.
  TaskContext MakeTaskContext(const void* context = nullptr) const;

  /// @brief Handle returned by ScheduleTaskGroup for later waiting or inspection.
  ///
  /// - `future` resolves to the TaskGroup's final status.
  /// - `statuses` points to an optional, accumulated per-task status trace.
  struct TaskGroupHandle {
    folly::SemiFuture<Result<TaskStatus>> future;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  /// @brief Schedule all tasks in a TaskGroup and return a handle for completion.
  ///
  /// If `statuses` is non-null, per-task status traces are appended to it.
  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr);

  /// @brief Wait for a scheduled TaskGroup to finish and return its final status.
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  /// @brief Convenience helper to schedule a TaskGroup and wait for completion.
  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group, const void* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr);

 private:
  struct TaskState;
  using TaskFuture = folly::Future<Result<TaskStatus>>;

  TaskFuture MakeTaskFuture(const Task& task, TaskContext task_ctx, TaskId task_id,
                            std::shared_ptr<std::vector<TaskStatus>> status_log) const;

  std::unique_ptr<folly::CPUThreadPoolExecutor> owned_cpu_executor_;
  std::unique_ptr<folly::IOThreadPoolExecutor> owned_io_executor_;

  folly::Executor* cpu_executor_;
  folly::Executor* io_executor_;
};

}  // namespace bp::schedule
