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

#include <broken_pipeline/schedule/async_dual_pool_scheduler.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>

#include <cassert>
#include <exception>
#include <utility>

namespace bp::schedule {

struct AsyncDualPoolScheduler::TaskState {
  Task task;
  TaskContext task_ctx;
  TaskId task_id;

  std::shared_ptr<std::vector<TaskStatus>> status_log;

  Result<TaskStatus> result = TaskStatus::Continue();

  TaskState(const Task& task_, TaskContext task_ctx_, TaskId task_id_,
            std::shared_ptr<std::vector<TaskStatus>> status_log_)
      : task(task_),
        task_ctx(std::move(task_ctx_)),
        task_id(task_id_),
        status_log(std::move(status_log_)) {}
};

AsyncDualPoolScheduler::~AsyncDualPoolScheduler() = default;

AsyncDualPoolScheduler::AsyncDualPoolScheduler(std::size_t cpu_threads, std::size_t io_threads)
    : owned_cpu_executor_(std::make_unique<folly::CPUThreadPoolExecutor>(cpu_threads)),
      owned_io_executor_(std::make_unique<folly::IOThreadPoolExecutor>(io_threads)),
      cpu_executor_(owned_cpu_executor_.get()),
      io_executor_(owned_io_executor_.get()) {}

AsyncDualPoolScheduler::AsyncDualPoolScheduler(folly::Executor* cpu_executor,
                                               folly::Executor* io_executor)
    : cpu_executor_(cpu_executor),
      io_executor_(io_executor) {}

TaskContext AsyncDualPoolScheduler::MakeTaskContext(const void* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<detail::CallbackResumer>();
  };
  task_ctx.awaiter_factory =
      [](std::vector<std::shared_ptr<Resumer>> resumers) -> Result<std::shared_ptr<Awaiter>> {
    ARROW_ASSIGN_OR_RAISE(auto awaiter,
                          detail::FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1,
                                                                   std::move(resumers)));
    return std::static_pointer_cast<Awaiter>(std::move(awaiter));
  };
  return task_ctx;
}

AsyncDualPoolScheduler::TaskFuture AsyncDualPoolScheduler::MakeTaskFuture(
    const Task& task, TaskContext task_ctx, TaskId task_id,
    std::shared_ptr<std::vector<TaskStatus>> status_log) const {
  auto state =
      std::make_shared<TaskState>(task, std::move(task_ctx), task_id, std::move(status_log));

  auto pred = [state]() {
    return state->result.ok() && !state->result->IsFinished() &&
           !state->result->IsCancelled();
  };

  auto thunk = [this, state]() -> folly::Future<folly::Unit> {
    if (state->result->IsBlocked()) {
      auto awaiter =
          std::dynamic_pointer_cast<detail::FutureAwaiter>(state->result->GetAwaiter());
      if (!awaiter) {
        assert(false && "AsyncDualPoolScheduler expects awaiter type detail::FutureAwaiter");
        state->result = Status::Invalid("AsyncDualPoolScheduler: unexpected awaiter type");
        return folly::makeFuture();
      }
      return std::move(awaiter->GetFuture())
          .via(cpu_executor_)
          .thenValue([state](auto&&) { state->result = TaskStatus::Continue(); });
    }

    if (state->result->IsYield()) {
      return folly::via(io_executor_).thenValue([state](auto&&) {
        state->result = state->task(state->task_ctx, state->task_id);
        if (state->result.ok() && state->status_log != nullptr) {
          state->status_log->push_back(state->result.ValueOrDie());
        }
      });
    }

    folly::Executor* executor =
        state->task.Hint().type == TaskHint::Type::IO ? io_executor_ : cpu_executor_;
    return folly::via(executor).thenValue([state](auto&&) {
      state->result = state->task(state->task_ctx, state->task_id);
      if (state->result.ok() && state->status_log != nullptr) {
        state->status_log->push_back(state->result.ValueOrDie());
      }
    });
  };

  return folly::via(cpu_executor_).thenValue([pred, thunk, state](auto&&) {
    return folly::whileDo(pred, thunk)
        .thenValue([state](auto&&) { return std::move(state->result); });
  });
}

AsyncDualPoolScheduler::TaskGroupHandle AsyncDualPoolScheduler::ScheduleTaskGroup(
    const TaskGroup& group, TaskContext task_ctx, std::vector<TaskStatus>* statuses) {
  auto statuses_shared =
      statuses != nullptr ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
                          : std::make_shared<std::vector<TaskStatus>>();

  const Task task = group.Task();
  const auto num_tasks = group.NumTasks();
  const auto cont = group.Continuation();

  std::vector<std::shared_ptr<std::vector<TaskStatus>>> task_statuses;
  task_statuses.reserve(num_tasks);
  for (std::size_t i = 0; i < num_tasks; ++i) {
    task_statuses.push_back(std::make_shared<std::vector<TaskStatus>>());
  }

  std::vector<TaskFuture> task_futures;
  task_futures.reserve(num_tasks);
  for (std::size_t i = 0; i < num_tasks; ++i) {
    task_futures.push_back(MakeTaskFuture(task, task_ctx, i, task_statuses[i]));
  }

  auto group_future =
      folly::collectAll(std::move(task_futures))
          .via(cpu_executor_)
          .thenValue([cont, task_ctx, statuses = statuses_shared,
                      task_statuses = std::move(task_statuses)](auto&& tries)
                         -> Result<TaskStatus> {
            bool any_cancelled = false;
            bool has_error = false;
            Status error_status = Status::OK();
            for (auto&& t : tries) {
              if (!t.hasValue()) {
                if (!has_error) {
                  has_error = true;
                  error_status =
                      Status::Invalid("AsyncDualPoolScheduler: task future missing value");
                }
                continue;
              }
              auto r = t.value();
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
                  statuses->insert(statuses->end(), per_task->begin(), per_task->end());
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
          })
          .semi();

  return TaskGroupHandle{std::move(group_future), std::move(statuses_shared)};
}

Result<TaskStatus> AsyncDualPoolScheduler::WaitTaskGroup(TaskGroupHandle& handle) const {
  try {
    return std::move(handle.future).get();
  } catch (const std::exception& e) {
    return Status::Invalid(e.what());
  }
}

Result<TaskStatus> AsyncDualPoolScheduler::ScheduleAndWait(const TaskGroup& group,
                                                           const void* context,
                                                           std::vector<TaskStatus>* statuses) {
  auto task_ctx = MakeTaskContext(context);
  auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
  return WaitTaskGroup(handle);
}

}  // namespace bp::schedule
