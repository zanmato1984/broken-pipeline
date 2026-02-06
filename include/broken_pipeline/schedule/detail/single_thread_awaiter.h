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

/// @file single_thread_awaiter.h
///
/// @brief Coroutine awaiter for the single-threaded coroutine scheduler.

#include "../traits.h"
#include "single_thread_resumer.h"

#include <coroutine>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

namespace bp::schedule::detail {

/// @brief Awaiter that schedules continuation on a single-threaded coroutine engine.
class SingleThreadAwaiter final : public Awaiter,
                                  public std::enable_shared_from_this<SingleThreadAwaiter> {
 public:
  using ScheduleFn = std::function<void(std::coroutine_handle<>)>;

  struct Awaitable {
    std::shared_ptr<SingleThreadAwaiter> awaiter;
    ScheduleFn schedule;

    bool await_ready() const noexcept { return awaiter->IsReady(); }

    bool await_suspend(std::coroutine_handle<> continuation) {
      return awaiter->Suspend(std::move(schedule), continuation);
    }

    void await_resume() const noexcept {}
  };

  explicit SingleThreadAwaiter(std::size_t num_readies,
                               std::vector<std::shared_ptr<Resumer>> resumers);

  Awaitable Await(ScheduleFn schedule) {
    return Awaitable{shared_from_this(), std::move(schedule)};
  }

  /// @brief Access the underlying resumers that feed this awaiter.
  const std::vector<std::shared_ptr<Resumer>>& GetResumers() const { return resumers_; }

  /// @brief Build a SingleThreadAwaiter with the expected number of resumes.
  static Result<std::shared_ptr<SingleThreadAwaiter>> MakeSingleThreadAwaiter(
      std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

  void NotifyReady();

 private:
  struct ContinuationRecord {
    std::coroutine_handle<> continuation;
    ScheduleFn schedule;
  };

  bool IsReady() const noexcept;
  bool Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation);

  std::size_t num_readies_;
  std::vector<std::shared_ptr<Resumer>> resumers_;

  mutable std::mutex mutex_;
  std::size_t readies_{0};
  std::optional<ContinuationRecord> record_;
  bool scheduled_{false};
};

}  // namespace bp::schedule::detail
