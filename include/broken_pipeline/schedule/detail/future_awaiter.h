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

/// @file future_awaiter.h
///
/// @brief Folly-based Awaiter implementation for Broken Pipeline schedulers.
///
/// `FutureAwaiter` aggregates one or more `CallbackResumer` instances and exposes a
/// `folly::SemiFuture` that completes when enough resumers have been resumed.

#include "callback_resumer.h"
#include "../traits.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include <folly/futures/Future.h>

namespace bp::schedule::detail {

/// @brief Awaiter that completes a Folly future once enough resumers are ready.
///
/// `FutureAwaiter` is intended for asynchronous schedulers built on Folly. It:
/// - Registers callbacks on each `CallbackResumer`.
/// - Counts resume signals until `num_readies` are observed.
/// - Fulfills an internal promise, completing the `SemiFuture`.
///
/// The scheduler typically waits on `GetFuture()` and reschedules the task when it
/// becomes ready.
class FutureAwaiter final : public Awaiter,
                            public std::enable_shared_from_this<FutureAwaiter> {
 public:
  using Future = folly::SemiFuture<folly::Unit>;

  /// @brief Access the future that becomes ready after enough resumers fire.
  ///
  /// The returned `SemiFuture` is move-only; callers typically `std::move` it once.
  Future& GetFuture() { return future_; }
  /// @brief Access the underlying resumers that feed this awaiter.
  const std::vector<std::shared_ptr<Resumer>>& GetResumers() const { return resumers_; }

  /// @brief Build a FutureAwaiter with the expected number of resumes.
  ///
  /// `resumers` must be non-empty and must all be `CallbackResumer` instances. The
  /// awaiter completes its future once `num_readies` resumers have resumed.
  static Result<std::shared_ptr<FutureAwaiter>> MakeFutureAwaiter(
      std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

 private:
  FutureAwaiter(std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers,
                std::shared_ptr<folly::Promise<folly::Unit>> promise, Future future);

  void OnResumed();

  std::size_t num_readies_;
  std::vector<std::shared_ptr<Resumer>> resumers_;

  mutable std::mutex mutex_;
  std::size_t readies_{0};
  std::shared_ptr<folly::Promise<folly::Unit>> promise_;
  Future future_;
};

}  // namespace bp::schedule::detail
