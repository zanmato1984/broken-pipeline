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

#include "arrow_traits.h"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace bp_test::sched {

/// @brief Awaiter that exposes its resumers for tests.
///
/// Broken Pipeline core treats Awaiter as opaque. For unit tests we often need to
/// resume blocked tasks deterministically, so our test awaiters expose the underlying
/// resumers.
class ResumersAwaiter : public Awaiter {
 public:
  ~ResumersAwaiter() override = default;
  virtual const std::vector<std::shared_ptr<Resumer>>& Resumers() const = 0;
};

/// @brief A Resumer implementation with callback registration.
///
/// Awaiters register callbacks via `AddCallback()` and get notified when the resumer is
/// resumed.
class CallbackResumer : public Resumer {
 public:
  using Callback = std::function<void()>;

  void Resume() override;
  bool IsResumed() const override;

  void AddCallback(Callback cb);

 private:
  mutable std::mutex mutex_;
  std::atomic_bool ready_{false};
  std::vector<Callback> callbacks_;
};

class SyncResumer final : public CallbackResumer {};
class AsyncResumer final : public CallbackResumer {};

class SyncAwaiter final : public ResumersAwaiter,
                          public std::enable_shared_from_this<SyncAwaiter> {
 public:
  void Wait();
  const std::vector<std::shared_ptr<Resumer>>& Resumers() const override {
    return resumers_;
  }

  static Result<std::shared_ptr<Awaiter>> MakeSingle(std::shared_ptr<Resumer> resumer);
  static Result<std::shared_ptr<Awaiter>> MakeAny(
      std::vector<std::shared_ptr<Resumer>> resumers);
  static Result<std::shared_ptr<Awaiter>> MakeAll(
      std::vector<std::shared_ptr<Resumer>> resumers);

 private:
  SyncAwaiter(std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

  static Result<std::shared_ptr<SyncAwaiter>> MakeSyncAwaiter(
      std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

  std::size_t num_readies_;
  std::vector<std::shared_ptr<Resumer>> resumers_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::size_t readies_{0};
};

class AsyncAwaiter final : public ResumersAwaiter,
                           public std::enable_shared_from_this<AsyncAwaiter> {
 public:
  using Future = std::shared_future<void>;

  Future GetFuture() const { return future_; }
  void Wait() const { future_.wait(); }
  const std::vector<std::shared_ptr<Resumer>>& Resumers() const override {
    return resumers_;
  }

  static Result<std::shared_ptr<Awaiter>> MakeSingle(std::shared_ptr<Resumer> resumer);
  static Result<std::shared_ptr<Awaiter>> MakeAny(
      std::vector<std::shared_ptr<Resumer>> resumers);
 static Result<std::shared_ptr<Awaiter>> MakeAll(
      std::vector<std::shared_ptr<Resumer>> resumers);

 private:
  AsyncAwaiter(std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers,
              std::shared_ptr<std::promise<void>> promise, Future future);

  static Result<std::shared_ptr<AsyncAwaiter>> MakeAsyncAwaiter(
      std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

  void OnResumed();

  std::size_t num_readies_;
  std::vector<std::shared_ptr<Resumer>> resumers_;

  mutable std::mutex mutex_;
  std::size_t readies_{0};
  std::shared_ptr<std::promise<void>> promise_;
  Future future_;
};

struct SchedulerOptions {
  /// @brief If true, resume all blocked resumers immediately.
  ///
  /// This is a test-only option to make scripted operators progress without wiring an
  /// external event source.
  bool auto_resume_blocked = true;

  /// @brief Safety cap to prevent infinite loops in tests.
  std::size_t step_limit = 1000;
};

class NaiveParallelScheduler {
 public:
  explicit NaiveParallelScheduler(SchedulerOptions options = {})
      : options_(std::move(options)) {}

  TaskContext MakeTaskContext(const Context* context = nullptr) const;

  struct TaskGroupHandle {
    std::future<Result<TaskStatus>> future;
    std::shared_ptr<std::mutex> statuses_mutex;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr) const;
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group,
                                     const Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr) const;

 private:
  using ConcreteTask = std::future<Result<TaskStatus>>;

  ConcreteTask MakeTask(const Task& task, const TaskContext& task_ctx, TaskId task_id,
                        std::shared_ptr<std::mutex> statuses_mutex,
                        std::shared_ptr<std::vector<TaskStatus>> statuses) const;

  SchedulerOptions options_;
};

/// @brief Simple thread pool executor used by AsyncDualPoolScheduler.
class ThreadPoolExecutor {
 public:
  explicit ThreadPoolExecutor(std::size_t num_threads,
                              std::string thread_name = std::string{});
  ThreadPoolExecutor(const ThreadPoolExecutor&) = delete;
  ThreadPoolExecutor& operator=(const ThreadPoolExecutor&) = delete;
  ThreadPoolExecutor(ThreadPoolExecutor&&) = delete;
  ThreadPoolExecutor& operator=(ThreadPoolExecutor&&) = delete;
  ~ThreadPoolExecutor();

  template <class Fn>
  auto Submit(Fn&& fn) -> std::future<std::invoke_result_t<Fn>> {
    using R = std::invoke_result_t<Fn>;
    auto task = std::make_shared<std::packaged_task<R()>>(std::forward<Fn>(fn));
    auto fut = task->get_future();
    Schedule([task]() mutable { (*task)(); });
    return fut;
  }

  void Schedule(std::function<void()> fn);

 private:
  void WorkerLoop();

  std::mutex mutex_;
  std::condition_variable cv_;
  std::deque<std::function<void()>> queue_;
  bool stop_{false};
  std::vector<std::thread> threads_;
};

class AsyncDualPoolScheduler {
 public:
  explicit AsyncDualPoolScheduler(SchedulerOptions options = {}, std::size_t cpu_threads = 1,
                                  std::size_t io_threads = 1);

  TaskContext MakeTaskContext(const Context* context = nullptr) const;

  struct TaskGroupHandle {
    std::future<Result<TaskStatus>> future;
    std::shared_ptr<std::mutex> statuses_mutex;
    std::shared_ptr<std::vector<TaskStatus>> statuses;
  };

  TaskGroupHandle ScheduleTaskGroup(const TaskGroup& group, TaskContext task_ctx,
                                    std::vector<TaskStatus>* statuses = nullptr);
  Result<TaskStatus> WaitTaskGroup(TaskGroupHandle& handle) const;

  Result<TaskStatus> ScheduleAndWait(const TaskGroup& group, const Context* context = nullptr,
                                     std::vector<TaskStatus>* statuses = nullptr);

 private:
  struct SharedState;

  void ScheduleTaskLoop(const Task& task, const TaskContext& task_ctx, TaskId task_id,
                        std::shared_ptr<SharedState> state);

  void RunOneStep(const Task& task, const TaskContext& task_ctx, TaskId task_id,
                  std::shared_ptr<SharedState> state);

  void OnTaskCompleted(std::shared_ptr<SharedState> state);

  SchedulerOptions options_;
  ThreadPoolExecutor cpu_;
  ThreadPoolExecutor io_;
};

}  // namespace bp_test::sched
