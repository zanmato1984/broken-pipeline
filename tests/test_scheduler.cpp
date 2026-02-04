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

#include "test_scheduler.h"

#include <cassert>
#include <chrono>
#include <stdexcept>

namespace bp_test::sched {

namespace {

Status InvalidAwaiterType(const char* scheduler_name) {
  return Status::Invalid(std::string(scheduler_name) + ": unexpected awaiter type");
}

Status InvalidResumerType(const char* scheduler_name) {
  return Status::Invalid(std::string(scheduler_name) + ": unexpected resumer type");
}

void WaitOnAwaiterOrError(const std::shared_ptr<Awaiter>& awaiter,
                          const char* scheduler_name) {
  if (auto* sync = dynamic_cast<SyncAwaiter*>(awaiter.get())) {
    sync->Wait();
    return;
  }
  if (auto* async = dynamic_cast<AsyncAwaiter*>(awaiter.get())) {
    async->Wait();
    return;
  }
  throw std::runtime_error(InvalidAwaiterType(scheduler_name).ToString());
}

void AutoResumeBlocked(const std::shared_ptr<Awaiter>& awaiter) {
  auto* resumers_awaiter = dynamic_cast<ResumersAwaiter*>(awaiter.get());
  if (resumers_awaiter == nullptr) {
    return;
  }
  for (const auto& resumer : resumers_awaiter->Resumers()) {
    if (resumer) {
      resumer->Resume();
    }
  }
}

}  // namespace

void CallbackResumer::Resume() {
  std::vector<Callback> callbacks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ready_.load(std::memory_order_acquire)) {
      return;
    }
    ready_.store(true, std::memory_order_release);
    callbacks = std::move(callbacks_);
    callbacks_.clear();
  }
  for (auto& cb : callbacks) {
    if (cb) {
      cb();
    }
  }
}

bool CallbackResumer::IsResumed() const {
  return ready_.load(std::memory_order_acquire);
}

void CallbackResumer::AddCallback(Callback cb) {
  bool call_now = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (ready_.load(std::memory_order_acquire)) {
      call_now = true;
    } else {
      callbacks_.push_back(std::move(cb));
    }
  }
  if (call_now && cb) {
    cb();
  }
}

SyncAwaiter::SyncAwaiter(std::size_t num_readies,
                         std::vector<std::shared_ptr<Resumer>> resumers)
    : num_readies_(num_readies), resumers_(std::move(resumers)) {}

Result<std::shared_ptr<SyncAwaiter>> SyncAwaiter::MakeSyncAwaiter(
    std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers) {
  if (resumers.empty()) {
    return Status::Invalid("SyncAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("SyncAwaiter: num_readies must be > 0");
  }

  auto awaiter = std::shared_ptr<SyncAwaiter>(new SyncAwaiter(num_readies, resumers));
  for (auto& resumer : resumers) {
    auto casted = std::dynamic_pointer_cast<SyncResumer>(resumer);
    if (casted == nullptr) {
      return InvalidResumerType("SyncAwaiter");
    }
    casted->AddCallback([awaiter]() {
      std::unique_lock<std::mutex> lock(awaiter->mutex_);
      awaiter->readies_++;
      awaiter->cv_.notify_one();
    });
  }
  return awaiter;
}

void SyncAwaiter::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (readies_ < num_readies_) {
    cv_.wait(lock);
  }
}

Result<std::shared_ptr<Awaiter>> SyncAwaiter::MakeSingle(std::shared_ptr<Resumer> resumer) {
  std::vector<std::shared_ptr<Resumer>> resumers;
  resumers.push_back(std::move(resumer));
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeSyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> SyncAwaiter::MakeAny(
    std::vector<std::shared_ptr<Resumer>> resumers) {
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeSyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> SyncAwaiter::MakeAll(
    std::vector<std::shared_ptr<Resumer>> resumers) {
  const auto num_readies = resumers.size();
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeSyncAwaiter(num_readies, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

AsyncAwaiter::AsyncAwaiter(std::size_t num_readies,
                           std::vector<std::shared_ptr<Resumer>> resumers,
                           std::shared_ptr<std::promise<void>> promise, Future future)
    : num_readies_(num_readies),
      resumers_(std::move(resumers)),
      promise_(std::move(promise)),
      future_(std::move(future)) {}

Result<std::shared_ptr<AsyncAwaiter>> AsyncAwaiter::MakeAsyncAwaiter(
    std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers) {
  if (resumers.empty()) {
    return Status::Invalid("AsyncAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("AsyncAwaiter: num_readies must be > 0");
  }

  auto promise = std::make_shared<std::promise<void>>();
  auto future = promise->get_future().share();

  auto awaiter = std::shared_ptr<AsyncAwaiter>(
      new AsyncAwaiter(num_readies, resumers, promise, std::move(future)));

  for (auto& resumer : resumers) {
    auto casted = std::dynamic_pointer_cast<AsyncResumer>(resumer);
    if (casted == nullptr) {
      return InvalidResumerType("AsyncAwaiter");
    }
    casted->AddCallback([awaiter]() { awaiter->OnResumed(); });
  }
  return awaiter;
}

void AsyncAwaiter::OnResumed() {
  std::shared_ptr<std::promise<void>> to_set;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    readies_++;
    if (readies_ >= num_readies_ && promise_ != nullptr) {
      to_set = std::exchange(promise_, nullptr);
    }
  }
  if (to_set != nullptr) {
    try {
      to_set->set_value();
    } catch (const std::future_error&) {
    }
  }
}

Result<std::shared_ptr<Awaiter>> AsyncAwaiter::MakeSingle(std::shared_ptr<Resumer> resumer) {
  std::vector<std::shared_ptr<Resumer>> resumers;
  resumers.push_back(std::move(resumer));
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeAsyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> AsyncAwaiter::MakeAny(
    std::vector<std::shared_ptr<Resumer>> resumers) {
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeAsyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> AsyncAwaiter::MakeAll(
    std::vector<std::shared_ptr<Resumer>> resumers) {
  const auto num_readies = resumers.size();
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeAsyncAwaiter(num_readies, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

TaskContext NaiveParallelScheduler::MakeTaskContext(const Context* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<SyncResumer>();
  };
  task_ctx.awaiter_factory = [](std::vector<std::shared_ptr<Resumer>> resumers)
      -> Result<std::shared_ptr<Awaiter>> { return SyncAwaiter::MakeAny(std::move(resumers)); };
  return task_ctx;
}

NaiveParallelScheduler::ConcreteTask NaiveParallelScheduler::MakeTask(
    const Task& task, const TaskContext& task_ctx, TaskId task_id,
    std::shared_ptr<std::mutex> statuses_mutex,
    std::shared_ptr<std::vector<TaskStatus>> statuses) const {
  return std::async(std::launch::async, [this, task, task_ctx, task_id,
                                        statuses_mutex = std::move(statuses_mutex),
                                        statuses = std::move(statuses)]() -> Result<TaskStatus> {
    Result<TaskStatus> result = TaskStatus::Continue();
    std::size_t steps = 0;
    while (result.ok() && !result->IsFinished() && !result->IsCancelled()) {
      if (++steps > options_.step_limit) {
        return Status::Invalid("NaiveParallelScheduler: task step limit exceeded");
      }

      if (result->IsBlocked()) {
        if (options_.auto_resume_blocked) {
          AutoResumeBlocked(result->GetAwaiter());
        }
        try {
          WaitOnAwaiterOrError(result->GetAwaiter(), "NaiveParallelScheduler");
        } catch (const std::exception& e) {
          return Status::Invalid(e.what());
        }
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
      statuses != nullptr
          ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
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

ThreadPoolExecutor::ThreadPoolExecutor(std::size_t num_threads, std::string /*thread_name*/) {
  threads_.reserve(num_threads);
  for (std::size_t i = 0; i < num_threads; ++i) {
    threads_.emplace_back([this]() { WorkerLoop(); });
  }
}

ThreadPoolExecutor::~ThreadPoolExecutor() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_ = true;
  }
  cv_.notify_all();
  for (auto& t : threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
}

void ThreadPoolExecutor::Schedule(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push_back(std::move(fn));
  }
  cv_.notify_one();
}

void ThreadPoolExecutor::WorkerLoop() {
  while (true) {
    std::function<void()> fn;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [&]() { return stop_ || !queue_.empty(); });
      if (stop_ && queue_.empty()) {
        return;
      }
      fn = std::move(queue_.front());
      queue_.pop_front();
    }
    if (fn) {
      fn();
    }
  }
}

struct AsyncDualPoolScheduler::SharedState {
  explicit SharedState(TaskGroup group, TaskContext task_ctx, SchedulerOptions options,
                       std::shared_ptr<std::mutex> statuses_mutex,
                       std::shared_ptr<std::vector<TaskStatus>> statuses)
      : group(std::move(group)),
        task_ctx(std::move(task_ctx)),
        options(std::move(options)),
        statuses_mutex(std::move(statuses_mutex)),
        statuses(std::move(statuses)) {
    const auto n = this->group.NumTasks();
    results.resize(n, TaskStatus::Continue());
    steps.resize(n, 0);
    remaining.store(n, std::memory_order_relaxed);
    group_promise = std::make_shared<std::promise<Result<TaskStatus>>>();
    group_future = group_promise->get_future();
  }

  TaskGroup group;
  TaskContext task_ctx;
  SchedulerOptions options;

  std::mutex mutex;
  std::vector<Result<TaskStatus>> results;
  std::vector<std::size_t> steps;
  std::atomic_size_t remaining{0};

  std::shared_ptr<std::promise<Result<TaskStatus>>> group_promise;
  std::future<Result<TaskStatus>> group_future;
  std::atomic_bool completed{false};

  std::shared_ptr<std::mutex> statuses_mutex;
  std::shared_ptr<std::vector<TaskStatus>> statuses;
};

AsyncDualPoolScheduler::AsyncDualPoolScheduler(SchedulerOptions options,
                                               std::size_t cpu_threads,
                                               std::size_t io_threads)
    : options_(std::move(options)),
      cpu_(cpu_threads, "bp-test-cpu"),
      io_(io_threads, "bp-test-io") {}

TaskContext AsyncDualPoolScheduler::MakeTaskContext(const Context* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<AsyncResumer>();
  };
  task_ctx.awaiter_factory = [](std::vector<std::shared_ptr<Resumer>> resumers)
      -> Result<std::shared_ptr<Awaiter>> { return AsyncAwaiter::MakeAny(std::move(resumers)); };
  return task_ctx;
}

AsyncDualPoolScheduler::TaskGroupHandle AsyncDualPoolScheduler::ScheduleTaskGroup(
    const TaskGroup& group, TaskContext task_ctx, std::vector<TaskStatus>* statuses) {
  auto statuses_mutex = std::make_shared<std::mutex>();
  auto statuses_shared =
      statuses != nullptr
          ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
          : std::make_shared<std::vector<TaskStatus>>();

  auto state = std::make_shared<SharedState>(group, std::move(task_ctx), options_,
                                             std::move(statuses_mutex),
                                             std::move(statuses_shared));

  for (std::size_t task_id = 0; task_id < state->group.NumTasks(); ++task_id) {
    ScheduleTaskLoop(state->group.Task(), state->task_ctx, task_id, state);
  }

  return TaskGroupHandle{std::move(state->group_future), state->statuses_mutex,
                         state->statuses};
}

Result<TaskStatus> AsyncDualPoolScheduler::WaitTaskGroup(TaskGroupHandle& handle) const {
  return handle.future.get();
}

Result<TaskStatus> AsyncDualPoolScheduler::ScheduleAndWait(const TaskGroup& group,
                                                           const Context* context,
                                                           std::vector<TaskStatus>* statuses) {
  auto task_ctx = MakeTaskContext(context);
  auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
  return WaitTaskGroup(handle);
}

void AsyncDualPoolScheduler::ScheduleTaskLoop(const Task& task, const TaskContext& task_ctx,
                                              TaskId task_id,
                                              std::shared_ptr<SharedState> state) {
  if (state->completed.load(std::memory_order_acquire)) {
    return;
  }
  const auto hint = task.Hint().type;
  if (hint == TaskHint::Type::IO) {
    io_.Schedule([this, task_id, state]() {
      RunOneStep(state->group.Task(), state->task_ctx, task_id, state);
    });
    return;
  }
  cpu_.Schedule([this, task_id, state]() {
    RunOneStep(state->group.Task(), state->task_ctx, task_id, state);
  });
}

void AsyncDualPoolScheduler::RunOneStep(const Task& task, const TaskContext& task_ctx,
                                        TaskId task_id,
                                        std::shared_ptr<SharedState> state) {
  if (state->completed.load(std::memory_order_acquire)) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    if (++state->steps[task_id] > state->options.step_limit) {
      state->results[task_id] =
          Status::Invalid("AsyncDualPoolScheduler: task step limit exceeded");
    }
  }

  Result<TaskStatus> result;
  {
    std::lock_guard<std::mutex> lock(state->mutex);
    result = state->results[task_id];
  }

  if (!result.ok()) {
    OnTaskCompleted(state);
    return;
  }

  if (result->IsBlocked()) {
    if (state->options.auto_resume_blocked) {
      AutoResumeBlocked(result->GetAwaiter());
    }
    try {
      WaitOnAwaiterOrError(result->GetAwaiter(), "AsyncDualPoolScheduler");
    } catch (const std::exception& e) {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->results[task_id] = Status::Invalid(e.what());
      OnTaskCompleted(state);
      return;
    }
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->results[task_id] = TaskStatus::Continue();
    }
  }

  result = task(task_ctx, task_id);

  if (result.ok()) {
    std::lock_guard<std::mutex> lock(*state->statuses_mutex);
    state->statuses->push_back(result.ValueOrDie());
  }

  {
    std::lock_guard<std::mutex> lock(state->mutex);
    state->results[task_id] = result;
  }

  if (!result.ok() || result->IsFinished() || result->IsCancelled()) {
    OnTaskCompleted(state);
    return;
  }

  if (result->IsYield()) {
    io_.Schedule([this, task_id, state]() {
      RunOneStep(state->group.Task(), state->task_ctx, task_id, state);
    });
    return;
  }

  // Continue
  ScheduleTaskLoop(task, task_ctx, task_id, state);
}

void AsyncDualPoolScheduler::OnTaskCompleted(std::shared_ptr<SharedState> state) {
  const auto left = state->remaining.fetch_sub(1, std::memory_order_acq_rel);
  if (left != 1) {
    return;
  }

  cpu_.Schedule([this, state]() {
    if (state->completed.exchange(true, std::memory_order_acq_rel)) {
      return;
    }

    std::vector<Result<TaskStatus>> results;
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      results = state->results;
    }

    for (const auto& r : results) {
      if (!r.ok()) {
        state->group_promise->set_value(r.status());
        return;
      }
      if (r->IsCancelled()) {
        state->group_promise->set_value(TaskStatus::Cancelled());
        return;
      }
    }

    if (state->group.Continuation().has_value()) {
      state->group_promise->set_value(state->group.Continuation().value()(state->task_ctx));
      return;
    }
    state->group_promise->set_value(TaskStatus::Finished());
  });
}

}  // namespace bp_test::sched
