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

#include <broken_pipeline/schedule/scheduler.h>

namespace bp::schedule {

Status InvalidAwaiterType(const char* scheduler_name) {
  return Status::Invalid(std::string(scheduler_name) + ": unexpected awaiter type");
}

Status InvalidResumerType(const char* scheduler_name) {
  return Status::Invalid(std::string(scheduler_name) + ": unexpected resumer type");
}

void AutoResumeBlocked(const std::shared_ptr<bp::Awaiter>& awaiter) {
  auto* resumers_awaiter = dynamic_cast<ResumersAwaiter*>(awaiter.get());
  if (resumers_awaiter == nullptr) {
    return;
  }
  for (const auto& resumer : resumers_awaiter->GetResumers()) {
    if (resumer) {
      resumer->Resume();
    }
  }
}

}  // namespace bp::schedule
