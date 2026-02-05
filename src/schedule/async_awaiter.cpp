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

#include <broken_pipeline/schedule/async_awaiter.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <mutex>
#include <utility>

namespace bp::schedule {

AsyncAwaiter::AsyncAwaiter(std::size_t num_readies,
                           std::vector<std::shared_ptr<Resumer>> resumers,
                           std::shared_ptr<folly::Promise<folly::Unit>> promise,
                           Future future)
    : num_readies_(num_readies),
      resumers_(std::move(resumers)),
      promise_(std::move(promise)),
      future_(std::move(future)) {}

Result<std::shared_ptr<AsyncAwaiter>> AsyncAwaiter::MakeAsyncAwaiter(std::size_t num_readies,
                                                                    std::vector<std::shared_ptr<Resumer>>
                                                                        resumers) {
  if (resumers.empty()) {
    return Status::Invalid("AsyncAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("AsyncAwaiter: num_readies must be > 0");
  }

  auto [p, f] = folly::makePromiseContract<folly::Unit>();
  auto promise = std::make_shared<folly::Promise<folly::Unit>>(std::move(p));

  auto awaiter = std::shared_ptr<AsyncAwaiter>(new AsyncAwaiter(
      num_readies, std::move(resumers), std::move(promise), std::move(f)));

  for (auto& resumer : awaiter->resumers_) {
    auto casted = std::dynamic_pointer_cast<AsyncResumer>(resumer);
    if (casted == nullptr) {
      assert(false && "AsyncAwaiter expects resumer type AsyncResumer");
      return Status::Invalid("AsyncAwaiter: unexpected resumer type");
    }
    casted->AddCallback([awaiter]() { awaiter->OnResumed(); });
  }

  return awaiter;
}

void AsyncAwaiter::OnResumed() {
  std::shared_ptr<folly::Promise<folly::Unit>> to_set;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    readies_++;
    if (readies_ >= num_readies_ && promise_ != nullptr) {
      to_set = std::exchange(promise_, nullptr);
    }
  }

  if (to_set != nullptr) {
    try {
      to_set->setValue();
    } catch (const std::exception&) {
    }
  }
}

}  // namespace bp::schedule
