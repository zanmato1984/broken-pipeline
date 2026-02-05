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

#include <cstddef>
#include <memory>
#include <vector>

#include <broken_pipeline/schedule/traits.h>

namespace bp::schedule {

using Traits = bp::schedule::arrow::Traits;
using Status = bp::Status<Traits>;

template <class T>
using Result = bp::Result<Traits, T>;

using ResumerPtr = std::shared_ptr<bp::Resumer>;
using Resumers = std::vector<ResumerPtr>;

class ResumersAwaiter : public bp::Awaiter {
 public:
  ~ResumersAwaiter() override = default;
  virtual const Resumers& GetResumers() const = 0;
};

using TaskContext = bp::TaskContext<Traits>;
using Task = bp::Task<Traits>;
using Continuation = bp::Continuation<Traits>;
using TaskGroup = bp::TaskGroup<Traits>;

using TaskId = bp::TaskId;
using TaskStatus = bp::TaskStatus;
using TaskHint = bp::TaskHint;

struct SchedulerOptions {
  /// @brief If true, resume all blocked resumers immediately.
  ///
  /// This is a test-only option to make scripted operators progress without wiring an
  /// external event source.
  bool auto_resume_blocked = true;

  /// @brief Safety cap to prevent infinite loops in tests.
  std::size_t step_limit = 1000;
};

Status InvalidAwaiterType(const char* scheduler_name);
Status InvalidResumerType(const char* scheduler_name);

void AutoResumeBlocked(const std::shared_ptr<bp::Awaiter>& awaiter);

}  // namespace bp::schedule
