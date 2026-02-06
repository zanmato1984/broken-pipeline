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

#include <broken_pipeline/schedule/parallel_coro_scheduler.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <condition_variable>
#include <coroutine>
#include <deque>
#include <exception>
#include <future>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace bp::schedule {

namespace {

class CoroReadyQueue {
 public:
  explicit CoroReadyQueue(std::size_t capacity) {
    (void)capacity;
  }

  void Enqueue(std::coroutine_handle<> h) {
    std::unique_lock<std::mutex> lock(mutex_);
    ready_.push_back(h);
    cv_.notify_one();
  }

  template <typename DonePred>
  std::coroutine_handle<> DequeueOrWait(DonePred done) {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [&]() { return !ready_.empty() || done(); });
    if (ready_.empty()) {
      return {};
    }
    auto h = ready_.front();
    ready_.pop_front();
    return h;
  }

  void NotifyAll() { cv_.notify_all(); }

  struct YieldAwaiter {
    CoroReadyQueue* queue;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) noexcept { queue->Enqueue(h); }
    void await_resume() const noexcept {}
  };

  YieldAwaiter Yield() { return YieldAwaiter{this}; }

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::coroutine_handle<>> ready_;
};

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

std::size_t DefaultWorkerCount(std::size_t num_tasks) {
  std::size_t hw = static_cast<std::size_t>(std::thread::hardware_concurrency());
  if (hw == 0) {
    hw = 4;
  }
  return std::max<std::size_t>(1, std::min(num_tasks, hw));
}

TaskCoro MakeTaskCoro(CoroReadyQueue& queue, const Task& task,
                      const TaskContext& task_ctx, TaskId task_id,
                      Result<TaskStatus>& result,
                      std::shared_ptr<std::vector<TaskStatus>> status_log) {
  auto schedule = [&queue](std::coroutine_handle<> h) { queue.Enqueue(h); };

  while (result.ok() && !result->IsFinished() && !result->IsCancelled()) {
    if (result->IsYield()) {
      co_await queue.Yield();
    } else if (result->IsBlocked()) {
      auto awaiter = std::dynamic_pointer_cast<detail::CoroAwaiter>(result->GetAwaiter());
      if (!awaiter) {
        assert(false && "ParallelCoroScheduler expects awaiter type detail::CoroAwaiter");
        result = Status::Invalid("ParallelCoroScheduler: unexpected awaiter type");
        co_return;
      }
      co_await awaiter->Await(schedule);
    }

    result = task(task_ctx, task_id);
    if (result.ok() && status_log != nullptr) {
      status_log->push_back(result.ValueOrDie());
    }
  }
}

}  // namespace

TaskContext ParallelCoroScheduler::MakeTaskContext(const Traits::Context* context) const {
  TaskContext task_ctx;
  task_ctx.context = context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<detail::CoroResumer>();
  };
  task_ctx.awaiter_factory =
      [](std::vector<std::shared_ptr<Resumer>> resumers) -> Result<std::shared_ptr<Awaiter>> {
    ARROW_ASSIGN_OR_RAISE(auto awaiter,
                          detail::CoroAwaiter::MakeCoroAwaiter(/*num_readies=*/1,
                                                              std::move(resumers)));
    return std::static_pointer_cast<Awaiter>(std::move(awaiter));
  };
  return task_ctx;
}

ParallelCoroScheduler::TaskGroupHandle ParallelCoroScheduler::ScheduleTaskGroup(
    const TaskGroup& group, TaskContext task_ctx, std::vector<TaskStatus>* statuses) const {
  auto statuses_shared =
      statuses != nullptr ? std::shared_ptr<std::vector<TaskStatus>>(statuses, [](auto*) {})
                          : std::make_shared<std::vector<TaskStatus>>();

  const auto task = group.Task();
  const auto num_tasks = group.NumTasks();
  const auto cont = group.Continuation();

  std::vector<std::shared_ptr<std::vector<TaskStatus>>> task_statuses;
  task_statuses.reserve(num_tasks);
  for (std::size_t i = 0; i < num_tasks; ++i) {
    task_statuses.push_back(std::make_shared<std::vector<TaskStatus>>());
  }

  auto fut = std::async(
      std::launch::async,
      [task, cont, num_tasks, task_ctx, statuses = statuses_shared,
       task_statuses = std::move(task_statuses)]() mutable -> Result<TaskStatus> {
        std::vector<Result<TaskStatus>> results(num_tasks, TaskStatus::Continue());
        if (num_tasks == 0) {
          if (cont.has_value()) {
            return cont.value()(task_ctx);
          }
          return TaskStatus::Finished();
        }

        CoroReadyQueue queue(num_tasks);

        std::vector<TaskCoro> coros;
        coros.reserve(num_tasks);
        for (TaskId i = 0; i < num_tasks; ++i) {
          coros.push_back(
              MakeTaskCoro(queue, task, task_ctx, i, results[i], task_statuses[i]));
          queue.Enqueue(coros.back().handle);
        }

        std::atomic<std::size_t> finished = 0;
        auto worker = [&]() {
          while (true) {
            auto h = queue.DequeueOrWait(
                [&]() { return finished.load(std::memory_order_acquire) >= num_tasks; });
            if (!h) {
              break;
            }

            h.resume();

            if (h.done()) {
              auto new_finished = finished.fetch_add(1) + 1;
              if (new_finished >= num_tasks) {
                queue.NotifyAll();
              }
            }
          }
        };

        const auto num_workers = DefaultWorkerCount(num_tasks);
        std::vector<std::jthread> threads;
        threads.reserve(num_workers);
        for (std::size_t i = 0; i < num_workers; ++i) {
          threads.emplace_back(worker);
        }

        for (auto& thread : threads) {
          thread.join();
        }

        bool any_cancelled = false;
        bool has_error = false;
        Status error_status = Status::OK();
        for (auto& result : results) {
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
      });

  return TaskGroupHandle{std::move(fut), std::move(statuses_shared)};
}

Result<TaskStatus> ParallelCoroScheduler::WaitTaskGroup(TaskGroupHandle& handle) const {
  return handle.future.get();
}

Result<TaskStatus> ParallelCoroScheduler::ScheduleAndWait(const TaskGroup& group,
                                                          const Traits::Context* context,
                                                          std::vector<TaskStatus>* statuses) const {
  auto task_ctx = MakeTaskContext(context);
  auto handle = ScheduleTaskGroup(group, std::move(task_ctx), statuses);
  return WaitTaskGroup(handle);
}

}  // namespace bp::schedule
