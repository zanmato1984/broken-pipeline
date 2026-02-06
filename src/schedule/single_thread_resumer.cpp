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

#include <broken_pipeline/schedule/detail/single_thread_resumer.h>

#include <utility>

namespace bp::schedule::detail {

void SingleThreadResumer::Resume() {
  std::vector<Callback> callbacks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (resumed_) {
      return;
    }
    resumed_ = true;
    callbacks.swap(callbacks_);
  }
  for (auto& cb : callbacks) {
    if (cb) {
      cb();
    }
  }
}

bool SingleThreadResumer::IsResumed() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return resumed_;
}

void SingleThreadResumer::AddCallback(Callback cb) {
  bool call_now = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (resumed_) {
      call_now = true;
    } else {
      callbacks_.push_back(std::move(cb));
    }
  }
  if (call_now && cb) {
    cb();
  }
}

}  // namespace bp::schedule::detail
