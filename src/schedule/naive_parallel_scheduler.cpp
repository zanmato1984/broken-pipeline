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

#include <broken_pipeline/schedule/naive_parallel_scheduler.h>

#include <cassert>
#include <future>
#include <utility>

namespace bp::schedule {

TaskContext NaiveParallelScheduler::MakeTaskContext(const Traits::Context* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<detail::CallbackResumer>();
  };
  task_ctx.awaiter_factory =
      [](std::vector<std::shared_ptr<Resumer>> resumers) -> Result<std::shared_ptr<Awaiter>> {
    ARROW_ASSIGN_OR_RAISE(auto awaiter,
                          detail::ConditonalAwaiter::MakeConditonalAwaiter(/*num_readies=*/1,
                                                                           std::move(resumers)));
    return std::static_pointer_cast<Awaiter>(std::move(awaiter));
  };
  return task_ctx;
}

NaiveParallelScheduler::ConcreteTask NaiveParallelScheduler::MakeTask(
    const Task& task, const TaskContext& task_ctx, TaskId task_id,
    std::shared_ptr<std::vector<TaskStatus>> status_log) const {
  return std::async(std::launch::async,
                    [this, task, task_ctx, task_id,
                     status_log = std::move(status_log)]() -> Result<TaskStatus> {
                      Result<TaskStatus> result = TaskStatus::Continue();
                      while (result.ok() && !result->IsFinished() &&
                             !result->IsCancelled()) {
                        if (result->IsBlocked()) {
                          auto awaiter =
                              std::dynamic_pointer_cast<detail::ConditonalAwaiter>(
                                  result->GetAwaiter());
                          if (!awaiter) {
                            assert(false &&
                                   "NaiveParallelScheduler expects awaiter type detail::ConditonalAwaiter");
                            return Status::Invalid(
                                "NaiveParallelScheduler: unexpected awaiter type");
                          }
                          awaiter->Wait();
                        }

                        result = task(task_ctx, task_id);
                        if (result.ok() && status_log != nullptr) {
                          status_log->push_back(result.ValueOrDie());
                        }
                      }
                      return result;
                    });
}

NaiveParallelScheduler::TaskGroupHandle NaiveParallelScheduler::ScheduleTaskGroup(
    const TaskGroup& group, TaskContext task_ctx, std::vector<TaskStatus>* statuses) const {
  auto statuses_shared =
      statuses != nullptr ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
                          : std::make_shared<std::vector<TaskStatus>>();

  const auto& task = group.Task();
  const auto num_tasks = group.NumTasks();
  const auto& cont = group.Continuation();

  std::vector<std::shared_ptr<std::vector<TaskStatus>>> task_statuses;
  task_statuses.reserve(num_tasks);
  for (std::size_t i = 0; i < num_tasks; ++i) {
    task_statuses.push_back(std::make_shared<std::vector<TaskStatus>>());
  }

  std::vector<ConcreteTask> tasks;
  tasks.reserve(num_tasks);
  for (std::size_t i = 0; i < num_tasks; ++i) {
    tasks.push_back(MakeTask(task, task_ctx, i, task_statuses[i]));
  }

  auto fut = std::async(std::launch::async,
                        [tasks = std::move(tasks), cont, task_ctx,
                         statuses = statuses_shared,
                         task_statuses = std::move(task_statuses)]() mutable
                            -> Result<TaskStatus> {
                          bool any_cancelled = false;
                          bool has_error = false;
                          Status error_status = Status::OK();
                          for (auto& t : tasks) {
                            auto r = t.get();
                            if (!r.ok()) {
                              if (!has_error) {
                                has_error = true;
                                error_status = r.status();
                              }
                              continue;
                            }
                            if (r->IsCancelled()) {
                              any_cancelled = true;
                            }
                          }

                          if (statuses != nullptr) {
                            for (const auto& per_task : task_statuses) {
                              if (per_task != nullptr) {
                                statuses->insert(statuses->end(), per_task->begin(),
                                                 per_task->end());
                              }
                            }
                          }

                          if (has_error) {
                            return error_status;
                          }
                          if (any_cancelled) {
                            return TaskStatus::Cancelled();
                          }
                          if (cont.has_value()) {
                            return cont.value()(task_ctx);
                          }
                          return TaskStatus::Finished();
                        });

  return TaskGroupHandle{std::move(fut), std::move(statuses_shared)};
}

Result<TaskStatus> NaiveParallelScheduler::WaitTaskGroup(TaskGroupHandle& handle) const {
  return handle.future.get();
}

Result<TaskStatus> NaiveParallelScheduler::ScheduleAndWait(const TaskGroup& group,
                                                           const Traits::Context* context,
                                                           std::vector<TaskStatus>* statuses) const {
  auto task_ctx = MakeTaskContext(context);
  auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
  return WaitTaskGroup(handle);
}

}  // namespace bp::schedule
