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

#include <broken_pipeline/schedule/detail/callback_resumer.h>
#include <broken_pipeline/schedule/detail/future_awaiter.h>

#include <arrow/testing/gtest_util.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace bp::schedule::test {

namespace {

constexpr std::size_t kRaceRounds = 2000;
constexpr std::size_t kManyResumers = 256;

}  // namespace

using detail::CallbackResumer;
using detail::FutureAwaiter;

TEST(CallbackResumerFutureTest, Basic) {
  auto resumer = std::make_shared<CallbackResumer>();
  ASSERT_FALSE(resumer->IsResumed());
  resumer->Resume();
  ASSERT_TRUE(resumer->IsResumed());
}

TEST(FutureAwaiterTest, SingleWaitFirst) {
  std::shared_ptr<Resumer> resumer = std::make_shared<CallbackResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, {resumer}));

  std::atomic_bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    finished = true;
    resumer->Resume();
  });
  folly::CPUThreadPoolExecutor executor(4);
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  future.get();
}

TEST(FutureAwaiterTest, SingleResumeFirst) {
  std::shared_ptr<Resumer> resumer = std::make_shared<CallbackResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, {resumer}));

  resumer->Resume();
  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    folly::CPUThreadPoolExecutor executor(4);
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  ASSERT_TRUE(future.get());
}

TEST(FutureAwaiterTest, Race) {
  folly::CPUThreadPoolExecutor executor(4);
  for (std::size_t i = 0; i < kRaceRounds; ++i) {
    std::shared_ptr<Resumer> resumer = std::make_shared<CallbackResumer>();
    ASSERT_OK_AND_ASSIGN(auto awaiter,
                         FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, {resumer}));

    std::atomic_bool resumer_ready = false, awaiter_ready = false, kickoff = false;
    auto resume_future = std::async(std::launch::async, [&]() {
      resumer_ready = true;
      while (!kickoff) {
      }
      resumer->Resume();
    });
    auto await_future = std::async(std::launch::async, [&]() {
      awaiter_ready = true;
      while (!kickoff) {
      }
      std::move(awaiter->GetFuture()).via(&executor).wait();
      return true;
    });
    while (!resumer_ready || !awaiter_ready) {
    }
    kickoff = true;
    resume_future.get();
    ASSERT_TRUE(resumer->IsResumed());
    ASSERT_TRUE(await_future.get());
  }
}

TEST(FutureAwaiterTest, AnyWaitFirst) {
  folly::CPUThreadPoolExecutor executor(4);
  constexpr std::size_t lucky = 42;
  std::vector<std::shared_ptr<Resumer>> resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CallbackResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, resumers));

  std::atomic_bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    finished = true;
    resumers[lucky]->Resume();
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_TRUE(finished);
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
  future.get();
}

TEST(FutureAwaiterTest, AnyReentrantWait) {
  folly::CPUThreadPoolExecutor executor(4);
  constexpr std::size_t lucky = 42;
  std::vector<std::shared_ptr<Resumer>> resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CallbackResumer>();
  }

  ASSERT_OK_AND_ASSIGN(auto awaiter1,
                       FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, resumers));
  resumers[lucky]->Resume();
  std::move(awaiter1->GetFuture()).via(&executor).wait();
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }

  resumers[lucky] = std::make_shared<CallbackResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter2,
                       FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, resumers));
  resumers[lucky]->Resume();
  std::move(awaiter2->GetFuture()).via(&executor).wait();
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
}

TEST(FutureAwaiterTest, AnyResumeFirst) {
  folly::CPUThreadPoolExecutor executor(4);
  std::vector<std::shared_ptr<Resumer>> resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CallbackResumer>();
    resumer->Resume();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, resumers));

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

TEST(FutureAwaiterTest, LifeSpan) {
  folly::CPUThreadPoolExecutor executor(4);
  constexpr std::size_t lucky = 42;
  std::vector<std::shared_ptr<Resumer>> resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<CallbackResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       FutureAwaiter::MakeFutureAwaiter(/*num_readies=*/1, resumers));

  std::atomic_bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    finished = true;
    resumers[lucky]->Resume();
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  awaiter.reset();
  ASSERT_TRUE(finished);
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
      resumers[i]->Resume();
    }
  }
  future.get();
}

}  // namespace bp::schedule::test
