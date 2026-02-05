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

#include <atomic>
#include <exception>
#include <utility>

namespace bp::schedule {

struct AsyncDualPoolScheduler::TaskState {
  Task task;
  TaskContext task_ctx;
  TaskId task_id;

  SchedulerOptions options;
  std::shared_ptr<std::mutex> statuses_mutex;
  std::shared_ptr<std::vector<TaskStatus>> statuses;

  std::size_t steps = 0;
  Result<TaskStatus> result = TaskStatus::Continue();

  TaskState(const Task& task_, TaskContext task_ctx_, TaskId task_id_, SchedulerOptions options_,
            std::shared_ptr<std::mutex> statuses_mutex_,
            std::shared_ptr<std::vector<TaskStatus>> statuses_)
      : task(task_),
        task_ctx(std::move(task_ctx_)),
        task_id(task_id_),
        options(std::move(options_)),
        statuses_mutex(std::move(statuses_mutex_)),
        statuses(std::move(statuses_)) {}
};

AsyncDualPoolScheduler::~AsyncDualPoolScheduler() = default;

AsyncDualPoolScheduler::AsyncDualPoolScheduler(SchedulerOptions options,
                                               std::size_t cpu_threads,
                                               std::size_t io_threads)
    : options_(std::move(options)),
      owned_cpu_executor_(std::make_unique<folly::CPUThreadPoolExecutor>(cpu_threads)),
      owned_io_executor_(std::make_unique<folly::IOThreadPoolExecutor>(io_threads)),
      cpu_executor_(owned_cpu_executor_.get()),
      io_executor_(owned_io_executor_.get()) {}

AsyncDualPoolScheduler::AsyncDualPoolScheduler(folly::Executor* cpu_executor,
                                               folly::Executor* io_executor,
                                               SchedulerOptions options)
    : options_(std::move(options)),
      cpu_executor_(cpu_executor),
      io_executor_(io_executor) {}

TaskContext AsyncDualPoolScheduler::MakeTaskContext(const Traits::Context* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<AsyncResumer>();
  };
  task_ctx.awaiter_factory = [](Resumers resumers) -> Result<std::shared_ptr<Awaiter>> {
    ARROW_ASSIGN_OR_RAISE(auto awaiter,
                          AsyncAwaiter::MakeAsyncAwaiter(/*num_readies=*/1,
                                                        std::move(resumers)));
    return std::static_pointer_cast<Awaiter>(std::move(awaiter));
  };
  return task_ctx;
}

AsyncDualPoolScheduler::TaskFuture AsyncDualPoolScheduler::MakeTaskFuture(
    const Task& task, TaskContext task_ctx, TaskId task_id,
    std::shared_ptr<std::mutex> statuses_mutex,
    std::shared_ptr<std::vector<TaskStatus>> statuses) const {
  auto state = std::make_shared<TaskState>(task, std::move(task_ctx), task_id, options_,
                                           std::move(statuses_mutex), std::move(statuses));

  auto pred = [state]() {
    return state->result.ok() && !state->result->IsFinished() &&
           !state->result->IsCancelled();
  };

  auto thunk = [this, state]() -> folly::Future<folly::Unit> {
    if (++state->steps > state->options.step_limit) {
      state->result = Status::Invalid("AsyncDualPoolScheduler: task step limit exceeded");
      return folly::makeFuture();
    }

    if (state->result->IsBlocked()) {
      if (state->options.auto_resume_blocked) {
        AutoResumeBlocked(state->result->GetAwaiter());
      }
      auto awaiter = std::dynamic_pointer_cast<AsyncAwaiter>(state->result->GetAwaiter());
      if (!awaiter) {
        state->result = InvalidAwaiterType("AsyncDualPoolScheduler");
        return folly::makeFuture();
      }
      return std::move(awaiter->GetFuture())
          .via(cpu_executor_)
          .thenValue([state](auto&&) { state->result = TaskStatus::Continue(); });
    }

    if (state->result->IsYield()) {
      return folly::via(io_executor_).thenValue([state](auto&&) {
        state->result = state->task(state->task_ctx, state->task_id);
        if (state->result.ok()) {
          std::lock_guard<std::mutex> lock(*state->statuses_mutex);
          state->statuses->push_back(state->result.ValueOrDie());
        }
      });
    }

    folly::Executor* executor =
        state->task.Hint().type == TaskHint::Type::IO ? io_executor_ : cpu_executor_;
    return folly::via(executor).thenValue([state](auto&&) {
      state->result = state->task(state->task_ctx, state->task_id);
      if (state->result.ok()) {
        std::lock_guard<std::mutex> lock(*state->statuses_mutex);
        state->statuses->push_back(state->result.ValueOrDie());
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
  auto statuses_mutex = std::make_shared<std::mutex>();
  auto statuses_shared =
      statuses != nullptr ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
                          : std::make_shared<std::vector<TaskStatus>>();

  const Task task = group.Task();
  const auto num_tasks = group.NumTasks();
  const auto cont = group.Continuation();

  std::vector<TaskFuture> task_futures;
  task_futures.reserve(num_tasks);
  for (std::size_t i = 0; i < num_tasks; ++i) {
    task_futures.push_back(MakeTaskFuture(task, task_ctx, i, statuses_mutex, statuses_shared));
  }

  auto group_future =
      folly::collectAll(std::move(task_futures))
          .via(cpu_executor_)
          .thenValue([cont, task_ctx](auto&& tries) -> Result<TaskStatus> {
            bool any_cancelled = false;
            for (auto&& t : tries) {
              if (!t.hasValue()) {
                return Status::Invalid("AsyncDualPoolScheduler: task future missing value");
              }
              auto r = t.value();
              if (!r.ok()) {
                return r.status();
              }
              if (r->IsCancelled()) {
                any_cancelled = true;
              }
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

  return TaskGroupHandle{std::move(group_future), std::move(statuses_mutex),
                         std::move(statuses_shared)};
}

Result<TaskStatus> AsyncDualPoolScheduler::WaitTaskGroup(TaskGroupHandle& handle) const {
  try {
    return std::move(handle.future).get();
  } catch (const std::exception& e) {
    return Status::Invalid(e.what());
  }
}

Result<TaskStatus> AsyncDualPoolScheduler::ScheduleAndWait(const TaskGroup& group,
                                                           const Traits::Context* context,
                                                           std::vector<TaskStatus>* statuses) {
  auto task_ctx = MakeTaskContext(context);
  auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
  return WaitTaskGroup(handle);
}

}  // namespace bp::schedule
