#include <broken_pipeline/schedule/async_awaiter.h>
#include <broken_pipeline/schedule/async_resumer.h>

#include <arrow/testing/gtest_util.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace sched = bp::schedule;

namespace {

constexpr std::size_t kRaceRounds = 2000;
constexpr std::size_t kManyResumers = 256;

std::shared_ptr<sched::AsyncAwaiter> CastAsyncAwaiter(
    const std::shared_ptr<bp::Awaiter>& awaiter) {
  return std::dynamic_pointer_cast<sched::AsyncAwaiter>(awaiter);
}

}  // namespace

TEST(AsyncResumerTest, Basic) {
  auto resumer = std::make_shared<sched::AsyncResumer>();
  ASSERT_FALSE(resumer->IsResumed());
  resumer->Resume();
  ASSERT_TRUE(resumer->IsResumed());
}

TEST(AsyncAwaiterTest, SingleWaitFirst) {
  sched::ResumerPtr resumer = std::make_shared<sched::AsyncResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeSingle(resumer));
  auto awaiter = CastAsyncAwaiter(awaiter_base);
  ASSERT_NE(awaiter, nullptr);

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

TEST(AsyncAwaiterTest, SingleResumeFirst) {
  sched::ResumerPtr resumer = std::make_shared<sched::AsyncResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeSingle(resumer));
  auto awaiter = CastAsyncAwaiter(awaiter_base);
  ASSERT_NE(awaiter, nullptr);

  resumer->Resume();
  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    folly::CPUThreadPoolExecutor executor(4);
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  ASSERT_TRUE(future.get());
}

TEST(AsyncAwaiterTest, Race) {
  folly::CPUThreadPoolExecutor executor(4);
  for (std::size_t i = 0; i < kRaceRounds; ++i) {
    sched::ResumerPtr resumer = std::make_shared<sched::AsyncResumer>();
    ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeSingle(resumer));
    auto awaiter = CastAsyncAwaiter(awaiter_base);
    ASSERT_NE(awaiter, nullptr);

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

TEST(AsyncAwaiterTest, AnyWaitFirst) {
  folly::CPUThreadPoolExecutor executor(4);
  constexpr std::size_t lucky = 42;
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::AsyncResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeAny(resumers));
  auto awaiter = CastAsyncAwaiter(awaiter_base);
  ASSERT_NE(awaiter, nullptr);

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

TEST(AsyncAwaiterTest, AnyReentrantWait) {
  folly::CPUThreadPoolExecutor executor(4);
  constexpr std::size_t lucky = 42;
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::AsyncResumer>();
  }

  ASSERT_OK_AND_ASSIGN(auto awaiter1_base, sched::AsyncAwaiter::MakeAny(resumers));
  auto awaiter1 = CastAsyncAwaiter(awaiter1_base);
  ASSERT_NE(awaiter1, nullptr);
  resumers[lucky]->Resume();
  std::move(awaiter1->GetFuture()).via(&executor).wait();
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    if (i == lucky) {
      ASSERT_TRUE(resumers[i]->IsResumed());
    } else {
      ASSERT_FALSE(resumers[i]->IsResumed());
    }
  }

  resumers[lucky] = std::make_shared<sched::AsyncResumer>();
  ASSERT_OK_AND_ASSIGN(auto awaiter2_base, sched::AsyncAwaiter::MakeAny(resumers));
  auto awaiter2 = CastAsyncAwaiter(awaiter2_base);
  ASSERT_NE(awaiter2, nullptr);
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

TEST(AsyncAwaiterTest, AnyResumeFirst) {
  folly::CPUThreadPoolExecutor executor(4);
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::AsyncResumer>();
    resumer->Resume();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeAny(resumers));
  auto awaiter = CastAsyncAwaiter(awaiter_base);
  ASSERT_NE(awaiter, nullptr);

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

TEST(AsyncAwaiterTest, LifeSpan) {
  folly::CPUThreadPoolExecutor executor(4);
  constexpr std::size_t lucky = 42;
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::AsyncResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeAny(resumers));
  auto awaiter = CastAsyncAwaiter(awaiter_base);
  ASSERT_NE(awaiter, nullptr);

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

TEST(AsyncAwaiterTest, AllWaitFirst) {
  folly::CPUThreadPoolExecutor executor(4);
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::AsyncResumer>();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeAll(resumers));
  auto awaiter = CastAsyncAwaiter(awaiter_base);
  ASSERT_NE(awaiter, nullptr);

  std::atomic<std::size_t> counter = 0;
  auto future = std::async(std::launch::async, [&]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    for (auto& resumer : resumers) {
      counter++;
      resumer->Resume();
    }
  });
  std::move(awaiter->GetFuture()).via(&executor).wait();
  ASSERT_EQ(counter.load(), kManyResumers);
  for (auto& resumer : resumers) {
    ASSERT_TRUE(resumer->IsResumed());
  }
  future.get();
}

TEST(AsyncAwaiterTest, AllResumeFirst) {
  folly::CPUThreadPoolExecutor executor(4);
  sched::Resumers resumers(kManyResumers);
  for (auto& resumer : resumers) {
    resumer = std::make_shared<sched::AsyncResumer>();
    resumer->Resume();
  }
  ASSERT_OK_AND_ASSIGN(auto awaiter_base, sched::AsyncAwaiter::MakeAll(resumers));
  auto awaiter = CastAsyncAwaiter(awaiter_base);
  ASSERT_NE(awaiter, nullptr);

  auto future = std::async(std::launch::async, [&]() -> bool {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::move(awaiter->GetFuture()).via(&executor).wait();
    return true;
  });
  for (std::size_t i = 0; i < kManyResumers; ++i) {
    ASSERT_TRUE(resumers[i]->IsResumed());
  }
  ASSERT_TRUE(future.get());
}
