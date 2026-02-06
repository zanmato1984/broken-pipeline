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

/// @file coro_awaiter.h
///
/// @brief Coroutine awaiter implementation for coroutine schedulers.
///
/// `CoroAwaiter` aggregates one or more `CoroResumer` instances and provides a
/// coroutine-friendly awaitable that schedules the continuation when enough
/// resumers have resumed.

#include "../traits.h"
#include "coro_resumer.h"

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

namespace bp::schedule::detail {

/// @brief Awaiter that resumes a coroutine once enough resumers fire.
class CoroAwaiter final : public Awaiter,
                          public std::enable_shared_from_this<CoroAwaiter> {
 public:
  using ScheduleFn = std::function<void(std::coroutine_handle<>)>;

  ~CoroAwaiter() override;

  struct Awaitable {
    std::shared_ptr<CoroAwaiter> awaiter;
    ScheduleFn schedule;

    bool await_ready() const noexcept { return awaiter->IsReady(); }

    bool await_suspend(std::coroutine_handle<> continuation) {
      return awaiter->Suspend(std::move(schedule), continuation);
    }

    void await_resume() const noexcept {}
  };

  Awaitable Await(ScheduleFn schedule) {
    return Awaitable{shared_from_this(), std::move(schedule)};
  }

  /// @brief Access the underlying resumers that feed this awaiter.
  const std::vector<std::shared_ptr<Resumer>>& GetResumers() const { return resumers_; }

  /// @brief Build a CoroAwaiter with the expected number of resumes.
  ///
  /// `resumers` must be non-empty and must all be `CoroResumer` instances. The
  /// awaiter schedules its continuation once `num_readies` resumers have resumed.
  static Result<std::shared_ptr<CoroAwaiter>> MakeCoroAwaiter(
      std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

  void NotifyReady();

 private:
  struct ContinuationRecord {
    std::coroutine_handle<> continuation;
    ScheduleFn schedule;
  };

  static ContinuationRecord* kScheduledSentinel() {
    return reinterpret_cast<ContinuationRecord*>(1);
  }

  CoroAwaiter(std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

  bool IsReady() const noexcept;
  bool Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation);

  const std::size_t num_readies_;
  std::vector<std::shared_ptr<Resumer>> resumers_;

  std::atomic<std::size_t> readies_{0};
  std::atomic<ContinuationRecord*> record_{nullptr};
};

}  // namespace bp::schedule::detail
