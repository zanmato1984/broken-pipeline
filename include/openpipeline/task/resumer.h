#pragma once

#include <memory>
#include <vector>

namespace openpipeline::task {

/**
 * @brief A scheduler-owned "waker" that transitions a previously blocked task back to runnable.
 *
 * openpipeline operators never block threads directly. Instead, when an operator cannot
 * make progress (e.g., waiting on IO or downstream backpressure), it returns
 * `op::OpOutput::Blocked(resumer)`.
 *
 * A scheduler turns one or more `Resumer` objects into an `Awaiter` (single/any/all)
 * and returns `TaskStatus::Blocked(awaiter)` from the task.
 *
 * Later, some external event triggers `Resumer::Resume()` (often from a callback).
 * The scheduler observes that and re-schedules the blocked task.
 */
class Resumer {
 public:
  virtual ~Resumer() = default;
  virtual void Resume() = 0;
  virtual bool IsResumed() const = 0;
};

using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;

}  // namespace openpipeline::task
