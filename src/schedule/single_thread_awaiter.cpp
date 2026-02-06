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

#include <broken_pipeline/schedule/detail/single_thread_awaiter.h>

#include <cassert>

namespace bp::schedule::detail {

SingleThreadAwaiter::SingleThreadAwaiter(std::size_t num_readies,
                                         std::vector<std::shared_ptr<Resumer>> resumers)
    : num_readies_(num_readies), resumers_(std::move(resumers)) {}

Result<std::shared_ptr<SingleThreadAwaiter>> SingleThreadAwaiter::MakeSingleThreadAwaiter(
    std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers) {
  if (resumers.empty()) {
    return Status::Invalid("SingleThreadAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("SingleThreadAwaiter: num_readies must be > 0");
  }

  auto awaiter = std::shared_ptr<SingleThreadAwaiter>(
      new SingleThreadAwaiter(num_readies, std::move(resumers)));
  for (auto& resumer : awaiter->resumers_) {
    auto casted = std::dynamic_pointer_cast<SingleThreadResumer>(resumer);
    if (casted == nullptr) {
      assert(false && "SingleThreadAwaiter expects resumer type SingleThreadResumer");
      return Status::Invalid("SingleThreadAwaiter: unexpected resumer type");
    }
    casted->AddCallback([awaiter]() { awaiter->NotifyReady(); });
  }
  return awaiter;
}

void SingleThreadAwaiter::NotifyReady() {
  ContinuationRecord record;
  bool should_schedule = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    ++readies_;
    if (readies_ < num_readies_) {
      return;
    }
    if (!record_.has_value() || scheduled_) {
      return;
    }
    scheduled_ = true;
    record = std::move(*record_);
    record_.reset();
    should_schedule = true;
  }
  if (should_schedule && record.continuation && record.schedule) {
    record.schedule(record.continuation);
  }
}

bool SingleThreadAwaiter::IsReady() const noexcept {
  std::lock_guard<std::mutex> lock(mutex_);
  return readies_ >= num_readies_;
}

bool SingleThreadAwaiter::Suspend(ScheduleFn schedule,
                                  std::coroutine_handle<> continuation) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (readies_ >= num_readies_) {
    return false;
  }
  assert(!record_.has_value());
  record_.emplace(ContinuationRecord{continuation, std::move(schedule)});
  return true;
}

}  // namespace bp::schedule::detail
