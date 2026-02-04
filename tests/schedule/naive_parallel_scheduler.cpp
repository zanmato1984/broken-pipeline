#include "naive_parallel_scheduler.h"

#include "sync_awaiter.h"
#include "sync_resumer.h"

#include <future>
#include <utility>

namespace bp_test::schedule {

TaskContext NaiveParallelScheduler::MakeTaskContext(const Context* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<SyncResumer>();
  };
  task_ctx.awaiter_factory = [](Resumers resumers) -> Result<std::shared_ptr<Awaiter>> {
    return SyncAwaiter::MakeAny(std::move(resumers));
  };
  return task_ctx;
}

NaiveParallelScheduler::ConcreteTask NaiveParallelScheduler::MakeTask(
    const Task& task, const TaskContext& task_ctx, TaskId task_id,
    std::shared_ptr<std::mutex> statuses_mutex,
    std::shared_ptr<std::vector<TaskStatus>> statuses) const {
  return std::async(std::launch::async,
                    [this, task, task_ctx, task_id,
                     statuses_mutex = std::move(statuses_mutex),
                     statuses = std::move(statuses)]() -> Result<TaskStatus> {
                      Result<TaskStatus> result = TaskStatus::Continue();
                      std::size_t steps = 0;
                      while (result.ok() && !result->IsFinished() &&
                             !result->IsCancelled()) {
                        if (++steps > options_.step_limit) {
                          return Status::Invalid(
                              "NaiveParallelScheduler: task step limit exceeded");
                        }

                        if (result->IsBlocked()) {
                          if (options_.auto_resume_blocked) {
                            AutoResumeBlocked(result->GetAwaiter());
                          }

                          auto awaiter =
                              std::dynamic_pointer_cast<SyncAwaiter>(result->GetAwaiter());
                          if (!awaiter) {
                            return InvalidAwaiterType("NaiveParallelScheduler");
                          }
                          awaiter->Wait();
                        }

                        result = task(task_ctx, task_id);
                        if (result.ok() && statuses != nullptr) {
                          std::lock_guard<std::mutex> lock(*statuses_mutex);
                          statuses->push_back(result.ValueOrDie());
                        }
                      }
                      return result;
                    });
}

NaiveParallelScheduler::TaskGroupHandle NaiveParallelScheduler::ScheduleTaskGroup(
    const TaskGroup& group, TaskContext task_ctx, std::vector<TaskStatus>* statuses) const {
  auto statuses_mutex = std::make_shared<std::mutex>();
  auto statuses_shared =
      statuses != nullptr ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
                          : std::make_shared<std::vector<TaskStatus>>();

  const auto& task = group.Task();
  const auto num_tasks = group.NumTasks();
  const auto& cont = group.Continuation();

  std::vector<ConcreteTask> tasks;
  tasks.reserve(num_tasks);
  for (std::size_t i = 0; i < num_tasks; ++i) {
    tasks.push_back(MakeTask(task, task_ctx, i, statuses_mutex, statuses_shared));
  }

  auto fut = std::async(std::launch::async,
                        [tasks = std::move(tasks), cont, task_ctx]() mutable
                            -> Result<TaskStatus> {
                          bool any_cancelled = false;
                          for (auto& t : tasks) {
                            auto r = t.get();
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
                        });

  return TaskGroupHandle{std::move(fut), std::move(statuses_mutex),
                         std::move(statuses_shared)};
}

Result<TaskStatus> NaiveParallelScheduler::WaitTaskGroup(TaskGroupHandle& handle) const {
  return handle.future.get();
}

Result<TaskStatus> NaiveParallelScheduler::ScheduleAndWait(const TaskGroup& group,
                                                           const Context* context,
                                                           std::vector<TaskStatus>* statuses) const {
  auto task_ctx = MakeTaskContext(context);
  auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
  return WaitTaskGroup(handle);
}

}  // namespace bp_test::schedule

