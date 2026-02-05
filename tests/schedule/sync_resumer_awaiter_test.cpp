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

#include <broken_pipeline/schedule/sync_awaiter.h>
#include <broken_pipeline/schedule/sync_resumer.h>

#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace sched = broken_pipeline::schedule;

namespace {

constexpr std::size_t kRaceRounds = 2000;
constexpr std::size_t kManyResumers = 256;

}  // namespace

TEST(SyncResumerTest, Basic) {
  auto resumer = std::make_shared<sched::SyncResumer>();
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

TEST(SyncResumerTest, Interleaving) {
  auto resumer = std::make_shared<sched::SyncResumer>();
  std::atomic_bool cb1_called = false, cb2_called = false, cb3_called = false;
  resumer->AddCallback([&]() { cb1_called.store(true, std::memory_order_release); });
  resumer->AddCallback([&]() {
    cb2_called.store(cb1_called.load(std::memory_order_acquire), std::memory_order_release);
  });
  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called.load(std::memory_order_acquire));
  ASSERT_FALSE(cb2_called.load(std::memory_order_acquire));
  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    resumer->Resume();
    resumer->AddCallback([&]() { cb3_called.store(true, std::memory_order_release); });
    ASSERT_TRUE(cb3_called.load(std::memory_order_acquire));
  });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!cb2_called.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() > deadline) {
      FAIL() << "Timed out waiting for callbacks";
    }
    std::this_thread::yield();
  }
  ASSERT_TRUE(cb1_called.load(std::memory_order_acquire));
  ASSERT_TRUE(cb2_called.load(std::memory_order_acquire));
  resume_future.get();
}

TEST(SyncResumerTest, Interleaving2) {
  auto resumer = std::make_shared<sched::SyncResumer>();
  std::atomic_bool cb1_called = false, cb2_called = false, cb3_called = false;
  resumer->AddCallback([&]() { cb1_called.store(true, std::memory_order_release); });
  resumer->AddCallback([&]() {
    cb2_called.store(cb1_called.load(std::memory_order_acquire), std::memory_order_release);
  });
  ASSERT_FALSE(resumer->IsResumed());
  ASSERT_FALSE(cb1_called.load(std::memory_order_acquire));
  ASSERT_FALSE(cb2_called.load(std::memory_order_acquire));
  auto resume_future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    resumer->Resume();
  });
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  while (!cb2_called.load(std::memory_order_acquire)) {
    if (std::chrono::steady_clock::now() > deadline) {
      FAIL() << "Timed out waiting for callbacks";
    }
    std::this_thread::yield();
  }
  ASSERT_TRUE(cb1_called.load(std::memory_order_acquire));
  ASSERT_TRUE(cb2_called.load(std::memory_order_acquire));
  resumer->AddCallback([&]() { cb3_called.store(true, std::memory_order_release); });
  ASSERT_TRUE(cb3_called.load(std::memory_order_acquire));
  resume_future.get();
}

TEST(SyncAwaiterTest, SingleWaitFirst) {
  sched::ResumerPtr resumer = std::make_shared<sched::SyncResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, {resumer}));

  std::atomic_bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    finished = true;
    resumer->Resume();
  });
  awaiter->Wait();
  ASSERT_TRUE(finished);
  ASSERT_TRUE(resumer->IsResumed());
  future.get();
}

TEST(SyncAwaiterTest, SingleResumeFirst) {
  sched::ResumerPtr resumer = std::make_shared<sched::SyncResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, {resumer}));

  resumer->Resume();
  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    awaiter->Wait();
    return true;
  });
  ASSERT_TRUE(future.get());
}

TEST(SyncAwaiterTest, Race) {
  for (std::size_t i = 0; i < kRaceRounds; ++i) {
    sched::ResumerPtr resumer = std::make_shared<sched::SyncResumer>();
    ASSERT_OK_AND_ASSIGN(auto awaiter,
                         sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, {resumer}));

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
      awaiter->Wait();
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

TEST(SyncAwaiterTest, AnyWaitFirst) {
  constexpr std::size_t lucky = 42;
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::SyncResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, resumers));

  std::atomic_bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    finished = true;
    resumers[lucky]->Resume();
  });
  awaiter->Wait();
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

TEST(SyncAwaiterTest, AnyReentrantWait) {
  constexpr std::size_t lucky = 42;
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::SyncResumer>();
  }

  ASSERT_OK_AND_ASSIGN(auto awaiter1,
                       sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, resumers));
  resumers[lucky]->Resume();
  awaiter1->Wait();
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }

  resumers[lucky] = std::make_shared<sched::SyncResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter2,
                       sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, resumers));
  resumers[lucky]->Resume();
  awaiter2->Wait();
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }
}

TEST(SyncAwaiterTest, AnyResumeFirst) {
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::SyncResumer>();
    resumer->Resume();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, resumers));

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    awaiter->Wait();
    return true;
  });
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  ASSERT_TRUE(future.get());
}

TEST(SyncAwaiterTest, LifeSpan) {
  constexpr std::size_t lucky = 42;
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::SyncResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter,
                       sched::SyncAwaiter::MakeSyncAwaiter(/*num_readies=*/1, resumers));

  std::atomic_bool finished = false;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    finished = true;
    resumers[lucky]->Resume();
  });
  awaiter->Wait();
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
