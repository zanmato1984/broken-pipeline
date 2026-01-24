#pragma once

#include <cassert>
#include <string>
#include <utility>

#include <openpipeline/task/awaiter.h>

namespace openpipeline::task {

/**
 * @brief The control protocol between a Task and a Scheduler.
 *
 * openpipeline tasks are **small-step and repeatable**: a scheduler repeatedly invokes
 * a task until it returns `Finished`/`Cancelled` or an error.
 *
 * - `Continue`: still running; scheduler may invoke again immediately or later.
 * - `Blocked(awaiter)`: cannot make progress; scheduler must wait/suspend on `awaiter`.
 * - `Yield`: task is about to do a long synchronous operation and requests yielding.
 *   A scheduler may migrate it to another pool/priority class.
 * - `Finished`: completed successfully.
 * - `Cancelled`: cancelled due to external reasons (often sibling failure).
 */
class TaskStatus {
 public:
  enum class Code {
    CONTINUE,
    BLOCKED,
    YIELD,
    FINISHED,
    CANCELLED,
  };

  static TaskStatus Continue() { return TaskStatus(Code::CONTINUE); }
  static TaskStatus Blocked(AwaiterPtr awaiter) { return TaskStatus(std::move(awaiter)); }
  static TaskStatus Yield() { return TaskStatus(Code::YIELD); }
  static TaskStatus Finished() { return TaskStatus(Code::FINISHED); }
  static TaskStatus Cancelled() { return TaskStatus(Code::CANCELLED); }

  bool IsContinue() const noexcept { return code_ == Code::CONTINUE; }
  bool IsBlocked() const noexcept { return code_ == Code::BLOCKED; }
  bool IsYield() const noexcept { return code_ == Code::YIELD; }
  bool IsFinished() const noexcept { return code_ == Code::FINISHED; }
  bool IsCancelled() const noexcept { return code_ == Code::CANCELLED; }

  AwaiterPtr& GetAwaiter() {
    assert(IsBlocked());
    return awaiter_;
  }

  const AwaiterPtr& GetAwaiter() const {
    assert(IsBlocked());
    return awaiter_;
  }

  std::string ToString() const {
    switch (code_) {
      case Code::CONTINUE:
        return "CONTINUE";
      case Code::BLOCKED:
        return "BLOCKED";
      case Code::YIELD:
        return "YIELD";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
    }
    return "UNKNOWN";
  }

 private:
  explicit TaskStatus(Code code) : code_(code) {}

  explicit TaskStatus(AwaiterPtr awaiter)
      : code_(Code::BLOCKED), awaiter_(std::move(awaiter)) {}

  Code code_;
  AwaiterPtr awaiter_;
};

}  // namespace openpipeline::task
