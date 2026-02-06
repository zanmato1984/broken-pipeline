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

/// @file single_thread_resumer.h
///
/// @brief Mutex-based resumer for single-threaded coroutine schedulers.

#include <broken_pipeline/task.h>

#include <functional>
#include <mutex>
#include <vector>

namespace bp::schedule::detail {

/// @brief Resumer implementation tailored for single-threaded coroutine schedulers.
class SingleThreadResumer final : public bp::Resumer {
 public:
  using Callback = std::function<void()>;

  void Resume() override;
  bool IsResumed() const override;
  void AddCallback(Callback cb);

 private:
  mutable std::mutex mutex_;
  bool resumed_{false};
  std::vector<Callback> callbacks_;
};

}  // namespace bp::schedule::detail
