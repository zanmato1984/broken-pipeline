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

#include "async_resumer.h"
#include "traits.h"

#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include <folly/futures/Future.h>

namespace bp::schedule {

class AsyncAwaiter final : public Awaiter,
                           public std::enable_shared_from_this<AsyncAwaiter> {
 public:
  using Future = folly::SemiFuture<folly::Unit>;

  Future& GetFuture() { return future_; }
  const std::vector<std::shared_ptr<Resumer>>& GetResumers() const { return resumers_; }

  static Result<std::shared_ptr<AsyncAwaiter>> MakeAsyncAwaiter(std::size_t num_readies,
                                                                std::vector<std::shared_ptr<Resumer>>
                                                                    resumers);

 private:
  AsyncAwaiter(std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers,
               std::shared_ptr<folly::Promise<folly::Unit>> promise, Future future);

  void OnResumed();

  std::size_t num_readies_;
  std::vector<std::shared_ptr<Resumer>> resumers_;

  mutable std::mutex mutex_;
  std::size_t readies_{0};
  std::shared_ptr<folly::Promise<folly::Unit>> promise_;
  Future future_;
};

}  // namespace bp::schedule
