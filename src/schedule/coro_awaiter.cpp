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

#include <broken_pipeline/schedule/detail/coro_awaiter.h>

#include <cassert>

namespace bp::schedule::detail {

CoroAwaiter::CoroAwaiter(std::size_t num_readies,
                         std::vector<std::shared_ptr<Resumer>> resumers)
    : num_readies_(num_readies), resumers_(std::move(resumers)) {}

CoroAwaiter::~CoroAwaiter() {
  auto* rec = record_.load(std::memory_order_acquire);
  if (rec == nullptr || rec == kScheduledSentinel()) {
    return;
  }
  delete rec;
}

bool CoroAwaiter::IsReady() const noexcept {
  return readies_.load(std::memory_order_acquire) >= num_readies_;
}

bool CoroAwaiter::Suspend(ScheduleFn schedule, std::coroutine_handle<> continuation) {
  if (IsReady()) {
    return false;
  }

  auto* rec = new ContinuationRecord{continuation, std::move(schedule)};
  ContinuationRecord* expected = nullptr;
  while (!record_.compare_exchange_weak(expected, rec, std::memory_order_release,
                                        std::memory_order_acquire)) {
    if (expected == kScheduledSentinel()) {
      delete rec;
      return false;
    }
    assert(expected == nullptr);
  }
  return true;
}

void CoroAwaiter::NotifyReady() {
  const auto new_readies = readies_.fetch_add(1, std::memory_order_acq_rel) + 1;
  if (new_readies < num_readies_) {
    return;
  }

  auto* rec = record_.exchange(kScheduledSentinel(), std::memory_order_acq_rel);
  if (rec == nullptr || rec == kScheduledSentinel()) {
    return;
  }

  auto continuation = rec->continuation;
  auto schedule = std::move(rec->schedule);
  delete rec;

  if (continuation && schedule) {
    schedule(continuation);
  }
}

Result<std::shared_ptr<CoroAwaiter>> CoroAwaiter::MakeCoroAwaiter(
    std::size_t num_readies, std::vector<std::shared_ptr<Resumer>> resumers) {
  if (resumers.empty()) {
    return Status::Invalid("CoroAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("CoroAwaiter: num_readies must be > 0");
  }

  auto awaiter =
      std::shared_ptr<CoroAwaiter>(new CoroAwaiter(num_readies, std::move(resumers)));
  for (auto& resumer : awaiter->resumers_) {
    auto casted = std::dynamic_pointer_cast<CoroResumer>(resumer);
    if (casted == nullptr) {
      assert(false && "CoroAwaiter expects resumer type CoroResumer");
      return Status::Invalid("CoroAwaiter: unexpected resumer type");
    }
    casted->AddCallback([awaiter]() { awaiter->NotifyReady(); });
  }
  return awaiter;
}

}  // namespace bp::schedule::detail
