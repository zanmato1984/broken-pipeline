#pragma once

#include "arrow_traits.h"

#include <cstddef>
#include <memory>
#include <string>
#include <vector>

namespace bp_test::schedule {

using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;

class ResumersAwaiter : public Awaiter {
 public:
  ~ResumersAwaiter() override = default;
  virtual const Resumers& GetResumers() const = 0;
};

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

void AutoResumeBlocked(const std::shared_ptr<Awaiter>& awaiter);

}  // namespace bp_test::schedule
