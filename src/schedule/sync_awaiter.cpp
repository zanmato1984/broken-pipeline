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

#include <cassert>

namespace bp::schedule {

SyncAwaiter::SyncAwaiter(std::size_t num_readies,
                         std::vector<std::shared_ptr<Resumer>> resumers)
    : num_readies_(num_readies), resumers_(std::move(resumers)) {}

Result<std::shared_ptr<SyncAwaiter>> SyncAwaiter::MakeSyncAwaiter(std::size_t num_readies,
                                                                  std::vector<std::shared_ptr<Resumer>>
                                                                      resumers) {
  if (resumers.empty()) {
    return Status::Invalid("SyncAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("SyncAwaiter: num_readies must be > 0");
  }

  auto awaiter =
      std::shared_ptr<SyncAwaiter>(new SyncAwaiter(num_readies, std::move(resumers)));
  for (auto& resumer : awaiter->resumers_) {
    auto casted = std::dynamic_pointer_cast<SyncResumer>(resumer);
    if (casted == nullptr) {
      assert(false && "SyncAwaiter expects resumer type SyncResumer");
      return Status::Invalid("SyncAwaiter: unexpected resumer type");
    }
    casted->AddCallback([awaiter]() {
      std::unique_lock<std::mutex> lock(awaiter->mutex_);
      awaiter->readies_++;
      awaiter->cv_.notify_one();
    });
  }
  return awaiter;
}

void SyncAwaiter::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (readies_ < num_readies_) {
    cv_.wait(lock);
  }
}

}  // namespace bp::schedule
