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

#include "scheduler.h"
#include "sync_resumer.h"

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace bp::schedule {

class SyncAwaiter final : public ResumersAwaiter,
                          public std::enable_shared_from_this<SyncAwaiter> {
 public:
  void Wait();
  const Resumers& GetResumers() const override { return resumers_; }

  static Result<std::shared_ptr<SyncAwaiter>> MakeSyncAwaiter(std::size_t num_readies,
                                                              Resumers resumers);

 private:
  SyncAwaiter(std::size_t num_readies, Resumers resumers);

  std::size_t num_readies_;
  Resumers resumers_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::size_t readies_{0};
};

}  // namespace bp::schedule
