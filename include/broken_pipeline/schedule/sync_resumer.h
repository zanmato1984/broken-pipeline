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
