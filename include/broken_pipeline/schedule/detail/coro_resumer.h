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

/// @file coro_resumer.h
///
/// @brief Coroutine-friendly resumer for coroutine schedulers.
///
/// `CoroResumer` is a lock-free, callback-based `bp::Resumer` designed for
/// coroutine schedulers. It supports concurrent `Resume()` and callback
/// registration without locks.

#include <broken_pipeline/task.h>

#include <atomic>
#include <functional>
#include <utility>

namespace bp::schedule::detail {

/// @brief Lock-free resumer that runs callbacks exactly once on resume.
class CoroResumer final : public bp::Resumer {
 public:
  using Callback = std::function<void()>;

  ~CoroResumer() override {
    auto* head = callbacks_.load(std::memory_order_acquire);
    if (head == kResumedSentinel()) {
      return;
    }
    while (head != nullptr) {
      auto* next = head->next;
      delete head;
      head = next;
    }
  }

  void Resume() override {
    auto* head =
        callbacks_.exchange(kResumedSentinel(), std::memory_order_acq_rel);
    if (head == kResumedSentinel()) {
      return;
    }

    CallbackNode* reversed = nullptr;
    while (head != nullptr) {
      auto* next = head->next;
      head->next = reversed;
      reversed = head;
      head = next;
    }
    head = reversed;

    while (head != nullptr) {
      auto* next = head->next;
      auto cb = std::move(head->cb);
      delete head;
      head = next;
      if (cb) {
        cb();
      }
    }
  }

  bool IsResumed() const override {
    return callbacks_.load(std::memory_order_acquire) == kResumedSentinel();
  }

  /// @brief Register a callback to run on resume (or immediately if already resumed).
  void AddCallback(Callback cb) {
    auto* head = callbacks_.load(std::memory_order_acquire);
    if (head == kResumedSentinel()) {
      if (cb) {
        cb();
      }
      return;
    }

    auto* node = new CallbackNode{std::move(cb), nullptr};
    while (head != kResumedSentinel()) {
      node->next = head;
      if (callbacks_.compare_exchange_weak(head, node, std::memory_order_release,
                                          std::memory_order_acquire)) {
        return;
      }
    }

    auto call_now = std::move(node->cb);
    delete node;
    if (call_now) {
      call_now();
    }
  }

 private:
  struct CallbackNode {
    Callback cb;
    CallbackNode* next;
  };

  static CallbackNode* kResumedSentinel() {
    return reinterpret_cast<CallbackNode*>(1);
  }

  std::atomic<CallbackNode*> callbacks_{nullptr};
};

}  // namespace bp::schedule::detail
