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

#include <broken_pipeline/schedule/detail/single_thread_awaiter.h>
#include <broken_pipeline/schedule/detail/single_thread_resumer.h>

#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <deque>
#include <exception>
#include <future>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

namespace bp::schedule::test {

namespace {

class ReadyQueue {
 public:
  void Enqueue(std::coroutine_handle<> h) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ready_.push_back(h);
    }
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

void RunUntilDone(ReadyQueue& queue, std::coroutine_handle<> root) {
  queue.Enqueue(root);
  while (!root.done()) {
    auto h = queue.DequeueOrWait([&]() { return root.done(); });
    if (!h) {
      continue;
    }
    h.resume();
  }
}

TaskCoro AwaitOnce(std::shared_ptr<detail::SingleThreadAwaiter> awaiter, ReadyQueue& queue,
                   bool& finished) {
  auto schedule = [&queue](std::coroutine_handle<> h) { queue.Enqueue(h); };
  co_await awaiter->Await(std::move(schedule));
  finished = true;
}

}  // namespace

TEST(SingleThreadResumerTest, Basic) {
  auto resumer = std::make_shared<detail::SingleThreadResumer>();
  bool cb1_called = false, cb2_called = false, cb3_called = false;

  resumer->AddCallback([&]() { cb1_called = true; });
  resumer->AddCallback([&]() { cb2_called = cb1_called; });

  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called);
  ASSERT_FALSE(cb2_called);

  resumer->Resume();

  ASSERT_TRUE(resumer->IsResumed());
  ASSERT_TRUE(cb1_called);
  ASSERT_TRUE(cb2_called);

  resumer->AddCallback([&]() { cb3_called = true; });
  ASSERT_TRUE(cb3_called);
}

TEST(SingleThreadAwaiterTest, SingleWaitFirst) {
  auto resumer = std::make_shared<detail::SingleThreadResumer>();
  std::vector<std::shared_ptr<Resumer>> resumers{resumer};
  ASSERT_OK_AND_ASSIGN(auto awaiter, detail::SingleThreadAwaiter::MakeSingleThreadAwaiter(
                                         /*num_readies=*/1, resumers));

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    ASSERT_FALSE(finished);
    resumer->Resume();
  });

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  resume_future.get();
}

TEST(SingleThreadAwaiterTest, SingleResumeFirst) {
  auto resumer = std::make_shared<detail::SingleThreadResumer>();
  resumer->Resume();
  std::vector<std::shared_ptr<Resumer>> resumers{resumer};
  ASSERT_OK_AND_ASSIGN(auto awaiter, detail::SingleThreadAwaiter::MakeSingleThreadAwaiter(
                                         /*num_readies=*/1, resumers));

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
}

TEST(SingleThreadAwaiterTest, AnyWaitFirst) {
  const std::size_t num_resumers = 1000;
  const std::size_t lucky = 42;
  std::vector<std::shared_ptr<Resumer>> resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<detail::SingleThreadResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter, detail::SingleThreadAwaiter::MakeSingleThreadAwaiter(
                                         /*num_readies=*/1, resumers));

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    resumers[lucky]->Resume();
  });

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
  for (std::size_t i = 0; i < num_resumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
  resume_future.get();
}

TEST(SingleThreadAwaiterTest, AllWaitFirst) {
  const std::size_t num_resumers = 200;
  std::vector<std::shared_ptr<Resumer>> resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<detail::SingleThreadResumer>();
  }
  ASSERT_OK_AND_ASSIGN(
      auto awaiter, detail::SingleThreadAwaiter::MakeSingleThreadAwaiter(
                        /*num_readies=*/num_resumers, resumers));

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);

  std::atomic<std::size_t> counter = 0;
  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    for (auto& resumer : resumers) {
      counter++;
      resumer->Resume();
    }
  });

  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);
  ASSERT_EQ(counter.load(), num_resumers);
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  resume_future.get();
}

TEST(SingleThreadAwaiterTest, AllResumeFirst) {
  const std::size_t num_resumers = 200;
  std::vector<std::shared_ptr<Resumer>> resumers(num_resumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<detail::SingleThreadResumer>();
    resumer->Resume();
  }
  ASSERT_OK_AND_ASSIGN(
      auto awaiter, detail::SingleThreadAwaiter::MakeSingleThreadAwaiter(
                        /*num_readies=*/num_resumers, resumers));

  ReadyQueue queue;
  bool finished = false;
  auto coro = AwaitOnce(awaiter, queue, finished);
  RunUntilDone(queue, coro.handle);
  ASSERT_TRUE(finished);

  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
}

}  // namespace bp::schedule::test
