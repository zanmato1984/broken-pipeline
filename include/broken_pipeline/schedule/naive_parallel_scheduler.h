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

#include "sync_awaiter.h"
#include "sync_resumer.h"
#include "traits.h"

#include <cstddef>
#include <future>
#include <memory>
#include <vector>

namespace bp::schedule {

class NaiveParallelScheduler {
 public:
  NaiveParallelScheduler() = default;

  TaskContext MakeTaskContext(const Traits::Context* context = nullptr) const;

  struct TaskGroupHandle {
    std::future<Result<TaskStatus>> future;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const;

  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                     const Traits::Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) const;

 private:
  using ConcreteTask = std::future<Result<TaskStatus>>;

  ConcreteTask MakeTask(const Task& task, const TaskContext& task_ctx, TaskId task_id,
                        std::shared_ptr<std::vector<TaskStatus>> status_log) const;
};

}  // namespace bp::schedule
