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

#include <broken_pipeline/task.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

namespace bp::schedule {

class CallbackResumer : public bp::Resumer {
 public:
  using Callback = std::function<void()>;

  void Resume() override {
    std::vector<Callback> callbacks;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (ready_.load(std::memory_order_acquire)) {
        return;
      }
      ready_.store(true, std::memory_order_release);
      callbacks = std::move(callbacks_);
      callbacks_.clear();
    }
    for (auto& cb : callbacks) {
      if (cb) {
        cb();
      }
    }
  }

  bool IsResumed() const override { return ready_.load(std::memory_order_acquire); }

  void AddCallback(Callback cb) {
    bool call_now = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (ready_.load(std::memory_order_acquire)) {
        call_now = true;
      } else {
        callbacks_.push_back(std::move(cb));
      }
    }
    if (call_now && cb) {
      cb();
    }
  }

 private:
  mutable std::mutex mutex_;
  std::atomic_bool ready_{false};
  std::vector<Callback> callbacks_;
};

class SyncResumer final : public CallbackResumer {};

}  // namespace bp::schedule
