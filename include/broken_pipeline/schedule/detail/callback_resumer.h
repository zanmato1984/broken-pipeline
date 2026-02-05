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

/// @file callback_resumer.h
///
/// @brief Resumer implementation for schedulers.
///
/// `CallbackResumer` provides a thread-safe, callback-based implementation of
/// `bp::Resumer` suitable for both blocking and async schedulers.

#include <broken_pipeline/task.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <vector>

namespace bp::schedule::detail {

/// @brief Resumer that executes callbacks exactly once on first resume.
///
/// - `Resume()` is idempotent and triggers all registered callbacks once.
/// - `AddCallback()` registers a callback to fire on resume (or runs it
///   immediately if the resumer already fired).
/// - `IsResumed()` is an atomic readiness check.
///
/// This class is the concrete resumer primitive used by schedulers.
class CallbackResumer : public bp::Resumer {
 public:
  using Callback = std::function<void()>;

  /// @brief Mark the resumer ready and invoke callbacks exactly once.
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

  /// @brief Register a callback to run when the resumer becomes ready.
  ///
  /// If the resumer has already fired, the callback is invoked immediately on the
  /// caller thread.
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

}  // namespace bp::schedule::detail
