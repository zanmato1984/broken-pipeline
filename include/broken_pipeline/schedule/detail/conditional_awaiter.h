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

/// @file conditional_awaiter.h
///
/// @brief Blocking Awaiter implementation for synchronous schedulers.
///
/// `ConditionalAwaiter` aggregates one or more `CallbackResumer` instances and blocks the
/// calling thread on a condition variable until enough resumers have fired.

#include "../traits.h"
#include "callback_resumer.h"

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace bp::schedule::detail {

/// @brief Awaiter that blocks a thread until enough resumers are ready.
///
/// `ConditionalAwaiter` is intended for simple or synchronous schedulers. It:
/// - Registers callbacks on each `CallbackResumer`.
/// - Counts resume signals until `num_readies` are observed.
/// - Unblocks any thread waiting in `Wait()`.
class ConditionalAwaiter final : public Awaiter,
                                public std::enable_shared_from_this<ConditionalAwaiter> {
 public:
  /// @brief Block the calling thread until `num_readies` resumers have fired.
  void Wait();
  /// @brief Access the underlying resumers that feed this awaiter.
  const std::vector<std::shared_ptr<Resumer>>& GetResumers() const { return resumers_; }

  /// @brief Build a ConditionalAwaiter with the expected number of resumes.
  ///
  /// `resumers` must be non-empty and must all be `CallbackResumer` instances. The
  /// awaiter unblocks once `num_readies` resumers have resumed.
  static Result<std::shared_ptr<ConditionalAwaiter>> MakeConditionalAwaiter(
      std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

 private:
  ConditionalAwaiter(std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers);

  std::size_t num_readies_;
  std::vector<std::shared_ptr<Resumer>> resumers_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::size_t readies_{0};
};

}  // namespace bp::schedule::detail
