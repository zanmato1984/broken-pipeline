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

/// @file sequential_coro_scheduler.h
///
/// @brief Single-threaded coroutine scheduler.
///
/// This scheduler runs all task instances cooperatively on the calling thread
/// using C++20 coroutines. No background threads are created.

#include "detail/single_thread_awaiter.h"
#include "detail/single_thread_resumer.h"
#include "traits.h"

#include <cstddef>
#include <memory>
#include <vector>

namespace bp::schedule::detail {
class SequentialCoroHandle;
}  // namespace bp::schedule::detail

namespace bp::schedule {

/// @brief Single-threaded scheduler using coroutine suspension.
class SequentialCoroScheduler {
 public:
  SequentialCoroScheduler() = default;

  /// @brief Create a TaskContext configured with single-thread resumers/awaiters.
  TaskContext MakeTaskContext(const Traits::Context* context = nullptr) const;

  /// @brief Handle returned by ScheduleTaskGroup for later waiting or inspection.
  struct TaskGroupHandle {
    std::shared_ptr<detail::SequentialCoroHandle> handle;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  /// @brief Schedule all tasks in a TaskGroup and return a handle for completion.
  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const;

  /// @brief Wait for a scheduled TaskGroup to finish and return its final status.
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  /// @brief Convenience helper to schedule a TaskGroup and wait for completion.
  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                     const Traits::Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr);
};

}  // namespace bp::schedule
