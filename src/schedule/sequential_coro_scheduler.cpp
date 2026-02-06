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

#include <broken_pipeline/schedule/sequential_coro_scheduler.h>

#include <algorithm>
#include <cassert>
#include <condition_variable>
#include <coroutine>
#include <deque>
#include <exception>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace bp::schedule {

namespace detail {

struct TaskCoro {
  struct promise_type {
    TaskCoro get_return_object() {
      return TaskCoro{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  explicit TaskCoro(std::coroutine_handle<promise_type> handle) : handle(handle) {}

  TaskCoro(TaskCoro&& other) noexcept : handle(other.handle) { other.handle = {}; }
  TaskCoro& operator=(TaskCoro&& other) noexcept {
    if (this != &other) {
      if (handle) {
        handle.destroy();
      }
      handle = other.handle;
      other.handle = {};
    }
    return *this;
  }

  TaskCoro(const TaskCoro&) = delete;
  TaskCoro& operator=(const TaskCoro&) = delete;

  ~TaskCoro() {
    if (handle) {
      handle.destroy();
    }
  }

  std::coroutine_handle<promise_type> handle;
};

class SequentialCoroHandle;

struct QueueEntry {
  std::coroutine_handle<> handle;
  SequentialCoroHandle* owner;
};

class SequentialCoroEngine {
 public:
  struct YieldAwaiter {
    SequentialCoroEngine* engine;
    SequentialCoroHandle* owner;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept {
      engine->Enqueue({h, owner});
    }
    void await_resume() const noexcept {}
  };

  void Enqueue(QueueEntry entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    EnqueueLocked(entry);
    cv_.notify_one();
  }

  void Register(SequentialCoroHandle* handle);
  void Unregister(SequentialCoroHandle* handle);

  YieldAwaiter Yield(SequentialCoroHandle* owner) { return YieldAwaiter{this, owner}; }

  Result<TaskStatus> WaitFor(SequentialCoroHandle* target);

 private:
  void RunLoop(SequentialCoroHandle* target);
  void FinalizeGroup(SequentialCoroHandle* owner);
  void EnqueueLocked(QueueEntry entry);
  void EnsureEnqueuedLocked();

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<QueueEntry> ready_;
  std::vector<SequentialCoroHandle*> handles_;
  bool running_ = false;
  std::thread::id runner_;
};

SequentialCoroEngine& Engine() {
  static SequentialCoroEngine engine;
  return engine;
}

class SequentialCoroHandle {
 public:
  SequentialCoroHandle(Task task, TaskContext task_ctx, std::optional<Continuation> cont,
                       std::size_t num_tasks,
                       std::shared_ptr<std::vector<TaskStatus>> statuses)
      : task_(std::move(task)),
        task_ctx_(std::move(task_ctx)),
        cont_(std::move(cont)),
        num_tasks_(num_tasks) {
    state_.statuses = std::move(statuses);
    Initialize();
    Engine().Register(this);
  }

  ~SequentialCoroHandle() { Engine().Unregister(this); }

  Result<TaskStatus> Wait() { return Engine().WaitFor(this); }

 private:
  struct State {
    std::vector<Result<TaskStatus>> results;
    std::vector<TaskCoro> coros;
    std::vector<std::shared_ptr<std::vector<TaskStatus>>> task_statuses;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
    std::size_t finished = 0;
    bool completed = false;
    bool finalizing = false;
    bool enqueued = false;
    std::optional<Result<TaskStatus>> final_result;
  };

  void Initialize();

  Task task_;
  TaskContext task_ctx_;
  std::optional<Continuation> cont_;
  std::size_t num_tasks_;

  State state_;

  friend class SequentialCoroEngine;
  friend TaskCoro MakeTaskCoro(SequentialCoroHandle&, TaskId);
};

TaskCoro MakeTaskCoro(SequentialCoroHandle& owner, TaskId task_id);

}  // namespace detail

detail::TaskCoro detail::MakeTaskCoro(detail::SequentialCoroHandle& owner,
                                      TaskId task_id) {
  auto schedule = [&owner](std::coroutine_handle<> h) {
    detail::Engine().Enqueue({h, &owner});
  };

  auto& result = owner.state_.results[task_id];
  auto status_log = owner.state_.task_statuses[task_id];

  while (result.ok() && !result->IsFinished() && !result->IsCancelled()) {
    if (result->IsYield()) {
      co_await detail::Engine().Yield(&owner);
    } else if (result->IsBlocked()) {
      auto awaiter =
          std::dynamic_pointer_cast<detail::SingleThreadAwaiter>(result->GetAwaiter());
      if (!awaiter) {
        assert(false &&
               "SequentialCoroScheduler expects awaiter type detail::SingleThreadAwaiter");
        result = Status::Invalid("SequentialCoroScheduler: unexpected awaiter type");
        co_return;
      }
      co_await awaiter->Await(schedule);
    }

    result = owner.task_(owner.task_ctx_, task_id);
    if (result.ok() && status_log != nullptr) {
      status_log->push_back(result.ValueOrDie());
    }

    if (result.ok() && result->IsContinue()) {
      co_await detail::Engine().Yield(&owner);
    }
  }
}

void detail::SequentialCoroHandle::Initialize() {
  state_.results.assign(num_tasks_, TaskStatus::Continue());
  state_.task_statuses.reserve(num_tasks_);
  state_.coros.reserve(num_tasks_);
  for (TaskId i = 0; i < num_tasks_; ++i) {
    state_.task_statuses.push_back(std::make_shared<std::vector<TaskStatus>>());
  }
  for (TaskId i = 0; i < num_tasks_; ++i) {
    state_.coros.push_back(MakeTaskCoro(*this, i));
  }
}

Result<TaskStatus> detail::SequentialCoroEngine::WaitFor(detail::SequentialCoroHandle* target) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureEnqueuedLocked();
    cv_.notify_all();
    if (target->state_.completed) {
      return *target->state_.final_result;
    }
  }

  if (target->num_tasks_ == 0) {
    FinalizeGroup(target);
    std::unique_lock<std::mutex> lock(mutex_);
    return *target->state_.final_result;
  }

  bool owns_run = false;
  while (true) {
    std::unique_lock<std::mutex> lock(mutex_);
    EnsureEnqueuedLocked();
    cv_.notify_all();
    if (target->state_.completed) {
      return *target->state_.final_result;
    }
    if (!running_) {
      running_ = true;
      runner_ = std::this_thread::get_id();
      owns_run = true;
      break;
    }
    if (runner_ == std::this_thread::get_id()) {
      break;
    }
    cv_.wait(lock, [&]() { return target->state_.completed || !running_; });
  }

  RunLoop(target);

  if (owns_run) {
    std::unique_lock<std::mutex> lock(mutex_);
    running_ = false;
    runner_ = std::thread::id{};
    cv_.notify_all();
  }

  std::unique_lock<std::mutex> lock(mutex_);
  return *target->state_.final_result;
}

void detail::SequentialCoroEngine::RunLoop(detail::SequentialCoroHandle* target) {
  while (true) {
    QueueEntry entry;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      EnsureEnqueuedLocked();
      cv_.wait(lock, [&]() { return !ready_.empty() || target->state_.completed; });
      if (target->state_.completed) {
        return;
      }
      entry = ready_.front();
      ready_.pop_front();
    }

    entry.handle.resume();

    if (entry.handle.done()) {
      bool should_finalize = false;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!entry.owner->state_.completed) {
          entry.owner->state_.finished++;
          should_finalize = entry.owner->state_.finished >= entry.owner->num_tasks_;
        }
      }
      if (should_finalize) {
        FinalizeGroup(entry.owner);
      }
    }
  }
}

void detail::SequentialCoroEngine::FinalizeGroup(detail::SequentialCoroHandle* owner) {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (owner->state_.completed || owner->state_.finalizing) {
      return;
    }
    owner->state_.finalizing = true;
  }

  bool any_cancelled = false;
  bool has_error = false;
  Status error_status = Status::OK();
  for (auto& result : owner->state_.results) {
    if (!result.ok()) {
      if (!has_error) {
        has_error = true;
        error_status = result.status();
      }
      continue;
    }
    if (result->IsCancelled()) {
      any_cancelled = true;
    }
  }

  if (owner->state_.statuses != nullptr) {
    for (const auto& per_task : owner->state_.task_statuses) {
      if (per_task != nullptr) {
        owner->state_.statuses->insert(owner->state_.statuses->end(),
                                       per_task->begin(), per_task->end());
      }
    }
  }

  Result<TaskStatus> final = TaskStatus::Finished();
  if (has_error) {
    final = error_status;
  } else if (any_cancelled) {
    final = TaskStatus::Cancelled();
  } else if (owner->cont_.has_value()) {
    final = owner->cont_.value()(owner->task_ctx_);
  }

  {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!owner->state_.completed) {
      owner->state_.completed = true;
      owner->state_.final_result = std::move(final);
    }
    owner->state_.finalizing = false;
  }
  cv_.notify_all();
}

void detail::SequentialCoroEngine::EnqueueLocked(QueueEntry entry) {
  ready_.push_back(entry);
}

void detail::SequentialCoroEngine::EnsureEnqueuedLocked() {
  for (auto* handle : handles_) {
    if (handle == nullptr || handle->state_.enqueued) {
      continue;
    }
    for (auto& coro : handle->state_.coros) {
      if (coro.handle) {
        EnqueueLocked({coro.handle, handle});
      }
    }
    handle->state_.enqueued = true;
  }
}

void detail::SequentialCoroEngine::Register(detail::SequentialCoroHandle* handle) {
  if (handle == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  handles_.push_back(handle);
  if (running_) {
    EnsureEnqueuedLocked();
    cv_.notify_all();
  }
}

void detail::SequentialCoroEngine::Unregister(detail::SequentialCoroHandle* handle) {
  if (handle == nullptr) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  handles_.erase(std::remove(handles_.begin(), handles_.end(), handle),
                 handles_.end());
  ready_.erase(std::remove_if(ready_.begin(), ready_.end(),
                              [&](const QueueEntry& entry) {
                                return entry.owner == handle;
                              }),
               ready_.end());
}

TaskContext SequentialCoroScheduler::MakeTaskContext(const void* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<detail::SingleThreadResumer>();
  };
  task_ctx.awaiter_factory =
      [](std::vector<std::shared_ptr<Resumer>> resumers) -> Result<std::shared_ptr<Awaiter>> {
    ARROW_ASSIGN_OR_RAISE(auto awaiter,
                          detail::SingleThreadAwaiter::MakeSingleThreadAwaiter(
                              /*num_readies=*/1, std::move(resumers)));
    return std::static_pointer_cast<Awaiter>(std::move(awaiter));
  };
  return task_ctx;
}

SequentialCoroScheduler::TaskGroupHandle SequentialCoroScheduler::ScheduleTaskGroup(
    const TaskGroup& group, TaskContext task_ctx, std::vector<TaskStatus>* statuses) const {
  auto statuses_shared =
      statuses != nullptr ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
                          : std::make_shared<std::vector<TaskStatus>>();

  auto handle = std::make_shared<detail::SequentialCoroHandle>(
      group.Task(), std::move(task_ctx), group.Continuation(), group.NumTasks(),
      statuses_shared);

  return TaskGroupHandle{std::move(handle), std::move(statuses_shared)};
}

Result<TaskStatus> SequentialCoroScheduler::WaitTaskGroup(TaskGroupHandle& handle) const {
  if (!handle.handle) {
    return Status::Invalid("SequentialCoroScheduler: null TaskGroupHandle");
  }
  return handle.handle->Wait();
}

Result<TaskStatus> SequentialCoroScheduler::ScheduleAndWait(const TaskGroup& group,
                                                            const void* context,
                                                            std::vector<TaskStatus>* statuses) {
  auto task_ctx = MakeTaskContext(context);
  auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
  return WaitTaskGroup(handle);
}

}  // namespace bp::schedule
