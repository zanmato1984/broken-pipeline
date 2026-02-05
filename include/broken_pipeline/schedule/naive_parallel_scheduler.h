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

/// @file naive_parallel_scheduler.h
///
/// @brief Simple `std::async` scheduler for Broken Pipeline task groups.
///
/// This scheduler is intentionally minimal: it uses synchronous resumers/awaiters
/// and launches each task instance via `std::async`. It is useful for tests,
/// examples, and as a reference implementation.

#include "sync_awaiter.h"
#include "sync_resumer.h"
#include "traits.h"

#include <cstddef>
#include <future>
#include <memory>
#include <vector>

namespace bp::schedule {

/// @brief Minimal scheduler that runs each Task instance in a std::async thread.
///
/// Behavior overview:
/// - Uses `SyncResumer` and `SyncAwaiter` to block when tasks are `Blocked`.
/// - Executes each task instance in its own `std::async` future.
/// - Aggregates per-task results and optionally invokes the TaskGroup continuation.
///
/// This scheduler does not provide work stealing or dedicated CPU/IO pools; it is
/// best suited to small test cases or as a reference for integrating a richer
/// executor.
class NaiveParallelScheduler {
 public:
  /// @brief Construct the scheduler (no internal state).
  NaiveParallelScheduler() = default;

  /// @brief Create a TaskContext configured with synchronous resumers/awaiters.
  TaskContext MakeTaskContext(const Traits::Context* context = nullptr) const;

  /// @brief Handle returned by ScheduleTaskGroup for later waiting or inspection.
  ///
  /// - `future` resolves to the TaskGroup's final status.
  /// - `statuses` points to an optional, accumulated per-task status trace.
  struct TaskGroupHandle {
    std::future<Result<TaskStatus>> future;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  /// @brief Schedule all tasks in a TaskGroup and return a handle for completion.
  ///
  /// If `statuses` is non-null, per-task status traces are appended to it.
  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const;

  /// @brief Wait for a scheduled TaskGroup to finish and return its final status.
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  /// @brief Convenience helper to schedule a TaskGroup and wait for completion.
  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                     const Traits::Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) const;

 private:
  using ConcreteTask = std::future<Result<TaskStatus>>;

  ConcreteTask MakeTask(const Task& task, const TaskContext& task_ctx, TaskId task_id,
                        std::shared_ptr<std::vector<TaskStatus>> status_log) const;
};

}  // namespace bp::schedule
