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

/// @file parallel_coro_scheduler.h
///
/// @brief Coroutine-based scheduler backed by a worker thread pool.
///
/// The scheduler runs each task instance as a coroutine and cooperatively
/// schedules them on a fixed-size worker pool.

#include "detail/coro_awaiter.h"
#include "detail/coro_resumer.h"
#include "traits.h"

#include <cstddef>
#include <future>
#include <memory>
#include <vector>

namespace bp::schedule {

/// @brief Coroutine scheduler that runs tasks cooperatively on a worker pool.
class ParallelCoroScheduler {
 public:
  ParallelCoroScheduler() = default;

  /// @brief Create a TaskContext configured with coroutine resumers/awaiters.
  TaskContext MakeTaskContext(const void* context = nullptr) const;

  /// @brief Handle returned by ScheduleTaskGroup for later waiting or inspection.
  struct TaskGroupHandle {
    std::future<Result<TaskStatus>> future;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  /// @brief Schedule all tasks in a TaskGroup and return a handle for completion.
  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const;

  /// @brief Wait for a scheduled TaskGroup to finish and return its final status.
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  /// @brief Convenience helper to schedule a TaskGroup and wait for completion.
  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group, const void* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) const;
};

}  // namespace bp::schedule
