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

#include <broken_pipeline/schedule/traits.h>

#include <broken_pipeline/schedule/async_dual_pool_scheduler.h>
#include <broken_pipeline/schedule/naive_parallel_scheduler.h>

#include <arrow/testing/gtest_util.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

using namespace bp::schedule;

namespace {

constexpr std::size_t kCpuThreadPoolSize = 4;
constexpr std::size_t kIoThreadPoolSize = 2;
constexpr auto kTinySleep = std::chrono::milliseconds(10);
constexpr auto kShortSleep = std::chrono::milliseconds(50);

struct AsyncDualPoolSchedulerHolder {
  folly::CPUThreadPoolExecutor cpu_executor{kCpuThreadPoolSize};
  folly::IOThreadPoolExecutor io_executor{kIoThreadPoolSize};
  AsyncDualPoolScheduler scheduler{&cpu_executor, &io_executor};
};

struct NaiveParallelSchedulerHolder {
  NaiveParallelScheduler scheduler;
};

template <typename SchedulerHolder>
class ScheduleTest : public ::testing::Test {
 protected:
  Result<TaskStatus> ScheduleTask(Task task, std::size_t num_tasks,
                                  std::optional<Continuation> cont = std::nullopt) {
    TaskGroup task_group("ScheduleTest", std::move(task), num_tasks, std::move(cont));
    SchedulerHolder holder;
    auto task_ctx = holder.scheduler.MakeTaskContext();
    auto handle = holder.scheduler.ScheduleTaskGroup(task_group, std::move(task_ctx));
    return holder.scheduler.WaitTaskGroup(handle);
  }
};

using SchedulerTypes =
    ::testing::Types<AsyncDualPoolSchedulerHolder, NaiveParallelSchedulerHolder>;
TYPED_TEST_SUITE(ScheduleTest, SchedulerTypes);

}  // namespace

TYPED_TEST(ScheduleTest, EmptyTask) {
  Task task("Task", [](const TaskContext&, TaskId) -> Result<TaskStatus> {
    return TaskStatus::Finished();
  });

  auto result = this->ScheduleTask(std::move(task), 4);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished()) << result->ToString();
}

TYPED_TEST(ScheduleTest, EmptyTaskWithEmptyCont) {
  Task task("Task", [](const TaskContext&, TaskId) -> Result<TaskStatus> {
    return TaskStatus::Finished();
  });
  Continuation cont("Cont", [](const TaskContext&) -> Result<TaskStatus> {
    return TaskStatus::Finished();
  });

  auto result = this->ScheduleTask(std::move(task), 4, std::move(cont));
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished()) << result->ToString();
}

TYPED_TEST(ScheduleTest, ContAfterTask) {
  std::atomic<std::size_t> counter = 0, cont_saw = 0;
  constexpr std::size_t num_tasks = 32;

  Task task("Task", [&](const TaskContext&, TaskId) -> Result<TaskStatus> {
    std::this_thread::sleep_for(kTinySleep);
    counter++;
    return TaskStatus::Finished();
  });

  Continuation cont("Cont", [&](const TaskContext&) -> Result<TaskStatus> {
    cont_saw = counter.load();
    return TaskStatus::Finished();
  });

  auto result = this->ScheduleTask(std::move(task), num_tasks, std::move(cont));
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished()) << result->ToString();
  ASSERT_EQ(cont_saw, num_tasks);
}

TYPED_TEST(ScheduleTest, EndlessTaskExternalFinish) {
  std::atomic_bool finished = false;

  Task task("Task", [&](const TaskContext&, TaskId) -> Result<TaskStatus> {
    if (!finished.load()) {
      std::this_thread::sleep_for(kTinySleep);
      return TaskStatus::Continue();
    }
    return TaskStatus::Finished();
  });

  auto finish_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(kShortSleep);
    finished = true;
  });

  auto result = this->ScheduleTask(std::move(task), 4);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished()) << result->ToString();
  finish_future.get();
}

TYPED_TEST(ScheduleTest, YieldTask) {
  constexpr std::size_t num_tasks = kCpuThreadPoolSize * 4;
  std::mutex cpu_thread_ids_mutex, io_thread_ids_mutex;
  std::unordered_set<std::thread::id> cpu_thread_ids, io_thread_ids;
  std::vector<bool> task_yielded(num_tasks, false);

  Task task("YieldTask", [&](const TaskContext&, TaskId task_id) -> Result<TaskStatus> {
    std::this_thread::sleep_for(kTinySleep);
    if (!task_yielded[task_id]) {
      task_yielded[task_id] = true;
      {
        std::lock_guard<std::mutex> lock(cpu_thread_ids_mutex);
        cpu_thread_ids.insert(std::this_thread::get_id());
      }
      return TaskStatus::Yield();
    }
    {
      std::lock_guard<std::mutex> lock(io_thread_ids_mutex);
      io_thread_ids.insert(std::this_thread::get_id());
    }
    return TaskStatus::Finished();
  });

  auto result = this->ScheduleTask(std::move(task), num_tasks);
  ASSERT_OK(result);
  ASSERT_TRUE(result->IsFinished()) << result->ToString();
}

TYPED_TEST(ScheduleTest, BlockedTask) {
  constexpr std::size_t num_tasks = 32;
  std::atomic<std::size_t> counter = 0;
  std::vector<std::shared_ptr<bp::Resumer>> resumers(num_tasks);
  std::atomic<std::size_t> num_resumers_set = 0;

  Task blocked_task(
      "BlockedTask",
      [&](const TaskContext& task_ctx, TaskId task_id) -> Result<TaskStatus> {
        if (resumers[task_id] == nullptr) {
          ARROW_ASSIGN_OR_RAISE(auto resumer, task_ctx.resumer_factory());
          std::vector<std::shared_ptr<bp::Resumer>> one{resumer};
          ARROW_ASSIGN_OR_RAISE(auto awaiter, task_ctx.awaiter_factory(std::move(one)));
          resumers[task_id] = std::move(resumer);
          num_resumers_set++;
          return TaskStatus::Blocked(std::move(awaiter));
        }
        counter++;
        return TaskStatus::Finished();
      });

  Task resumer_task("ResumerTask",
                    [&](const TaskContext&, TaskId) -> Result<TaskStatus> {
                      if (num_resumers_set != num_tasks) {
                        std::this_thread::sleep_for(kTinySleep);
                        return TaskStatus::Continue();
                      }
                      std::this_thread::sleep_for(kShortSleep);
                      for (auto& resumer : resumers) {
                        if (resumer == nullptr) {
                          return Status::UnknownError("BlockedTask: missing resumer");
                        }
                        resumer->Resume();
                      }
                      return TaskStatus::Finished();
                    },
                    {TaskHint::Type::IO});

  auto blocked_task_future = std::async(std::launch::async, [&]() -> Result<TaskStatus> {
    return this->ScheduleTask(std::move(blocked_task), num_tasks);
  });

  auto resumer_task_future = std::async(std::launch::async, [&]() -> Result<TaskStatus> {
    return this->ScheduleTask(std::move(resumer_task), 1);
  });

  {
    auto result = blocked_task_future.get();
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_EQ(counter, num_tasks);
  }
  {
    auto result = resumer_task_future.get();
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_EQ(counter, num_tasks);
  }
}

TYPED_TEST(ScheduleTest, BlockedTaskResumerErrorNotify) {
  constexpr std::size_t num_tasks = 32;
  std::atomic<std::size_t> counter = 0;

  std::mutex resumers_mutex;
  std::vector<std::shared_ptr<bp::Resumer>> resumers(num_tasks);
  std::atomic<std::size_t> num_resumers_set = 0;
  std::atomic_bool resumer_task_errored = false;
  std::atomic_bool unblock_requested = false;

  Task blocked_task(
      "BlockedTask",
      [&](const TaskContext& task_ctx, TaskId task_id) -> Result<TaskStatus> {
        {
          std::lock_guard<std::mutex> lock(resumers_mutex);
          if (resumers[task_id] == nullptr) {
            ARROW_ASSIGN_OR_RAISE(auto resumer, task_ctx.resumer_factory());
            std::vector<std::shared_ptr<bp::Resumer>> one{resumer};
            ARROW_ASSIGN_OR_RAISE(auto awaiter, task_ctx.awaiter_factory(std::move(one)));
            resumers[task_id] = resumer;
            num_resumers_set++;
            if (unblock_requested.load() && !resumer->IsResumed()) {
              resumer->Resume();
            }
            return TaskStatus::Blocked(std::move(awaiter));
          }
        }
        if (!resumer_task_errored.load()) {
          return Status::UnknownError("Blocked task resumed before resumer task errored");
        }
        counter++;
        return TaskStatus::Finished();
      });

  Task resumer_task("ResumerTask",
                    [&](const TaskContext&, TaskId) -> Result<TaskStatus> {
                      if (num_resumers_set != num_tasks) {
                        std::this_thread::sleep_for(kTinySleep);
                        return TaskStatus::Continue();
                      }
                      resumer_task_errored = true;
                      return Status::UnknownError("ResumerTaskError");
                    },
                    {TaskHint::Type::IO});

  auto blocked_task_future = std::async(std::launch::async, [&]() -> Result<TaskStatus> {
    return this->ScheduleTask(std::move(blocked_task), num_tasks);
  });

  auto resumer_task_future = std::async(std::launch::async, [&]() -> Result<TaskStatus> {
    return this->ScheduleTask(std::move(resumer_task), 1);
  });

  // Unblock after the resumer task errors (emulates notify-finish behavior).
  auto unblock_future = std::async(std::launch::async, [&]() -> Status {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!resumer_task_errored.load()) {
      if (std::chrono::steady_clock::now() > deadline) {
        unblock_requested = true;
        std::vector<std::shared_ptr<bp::Resumer>> snapshot;
        {
          std::lock_guard<std::mutex> lock(resumers_mutex);
          snapshot = resumers;
        }
        for (auto& resumer : snapshot) {
          if (resumer != nullptr && !resumer->IsResumed()) {
            resumer->Resume();
          }
        }
        return Status::UnknownError("Timed out waiting for resumer task error");
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    unblock_requested = true;
    std::vector<std::shared_ptr<bp::Resumer>> snapshot;
    {
      std::lock_guard<std::mutex> lock(resumers_mutex);
      snapshot = resumers;
    }
    for (auto& resumer : snapshot) {
      if (resumer == nullptr) {
        return Status::UnknownError("Unexpected null resumer");
      }
      if (!resumer->IsResumed()) {
        resumer->Resume();
      }
    }
    return Status::OK();
  });

  ASSERT_OK(unblock_future.get());

  {
    auto result = blocked_task_future.get();
    ASSERT_OK(result);
    ASSERT_TRUE(result->IsFinished()) << result->ToString();
    ASSERT_EQ(counter, num_tasks);
  }
  {
    auto result = resumer_task_future.get();
    ASSERT_FALSE(result.ok());
    ASSERT_EQ(result.status().message(), "ResumerTaskError");
  }
}

TYPED_TEST(ScheduleTest, ErrorAndCancel) {
  constexpr std::size_t num_errors = 4;
  constexpr std::size_t num_tasks = 32;
  std::atomic<int> error_counts = -1;

  Task task("Task", [&](const TaskContext&, TaskId) -> Result<TaskStatus> {
    if (error_counts < 0) {
      std::this_thread::sleep_for(kTinySleep);
      return TaskStatus::Continue();
    }
    if (error_counts++ < static_cast<int>(num_errors)) {
      return Status::UnknownError("42");
    }
    return TaskStatus::Cancelled();
  });

  auto kickoff = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(kShortSleep);
    error_counts = 0;
  });

  auto result = this->ScheduleTask(std::move(task), num_tasks);
  ASSERT_FALSE(result.ok());
  ASSERT_EQ(result.status().message(), "42");
  kickoff.get();
}
