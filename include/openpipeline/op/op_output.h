#pragma once

#include <cassert>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include <openpipeline/concepts.h>
#include <openpipeline/task/resumer.h>

namespace openpipeline::op {

/**
 * @brief Output/control signal returned by operator callbacks to the pipeline runtime.
 *
 * `OpOutput` is the small protocol between operator implementations and a pipeline
 * driver (e.g., `pipeline::detail::PipelineTask`).
 *
 * It mixes two kinds of information:
 * - flow control ("need more input", "have more internal output")
 * - execution control ("blocked", "yield", "finished", "cancelled")
 *
 * The most important distinction is:
 * - `PIPE_SINK_NEEDS_MORE`: the operator needs more *upstream input* before it can
 *   produce more output.
 * - `SOURCE_PIPE_HAS_MORE(batch)`: the operator produced a batch but still has more
 *   *internal output pending*, so the driver should resume the same operator again
 *   (typically by calling it with `std::nullopt` input).
 */
template <OpenPipelineTraits Traits>
class OpOutput {
 public:
  enum class Code {
    /**
     * @brief Downstream needs more input; no output was produced.
     *
     * For a `Pipe`, this means: stop pushing forward and go back to the source to
     * fetch more input.
     * For a `Sink`, this is the "normal" successful consume result.
     */
    PIPE_SINK_NEEDS_MORE,
    /**
     * @brief Produced exactly one output batch and does not require immediate resumption.
     */
    PIPE_EVEN,
    /**
     * @brief Produced one output batch and has more output pending internally.
     *
     * The driver should resume the same operator before pulling a new input batch.
     */
    SOURCE_PIPE_HAS_MORE,
    /**
     * @brief Cannot make progress until `Resumer::Resume()` is triggered.
     */
    BLOCKED,
    /**
     * @brief Yield to the scheduler before continuing.
     *
     * A driver may propagate this as `TaskStatus::Yield()` to let the scheduler
     * migrate execution to another pool or priority class.
     */
    PIPE_YIELD,
    /**
     * @brief Resume handshake for a previously yielded operator.
     */
    PIPE_YIELD_BACK,
    /**
     * @brief Stream finished; may carry a final output batch.
     */
    FINISHED,
    /**
     * @brief Cancelled (usually due to error in a sibling task).
     */
    CANCELLED,
  };

  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::PIPE_SINK_NEEDS_MORE); }
  static OpOutput PipeEven(typename Traits::Batch batch) {
    return OpOutput(Code::PIPE_EVEN, std::move(batch));
  }
  static OpOutput SourcePipeHasMore(typename Traits::Batch batch) {
    return OpOutput(Code::SOURCE_PIPE_HAS_MORE, std::move(batch));
  }
  static OpOutput Blocked(task::ResumerPtr resumer) { return OpOutput(std::move(resumer)); }
  static OpOutput PipeYield() { return OpOutput(Code::PIPE_YIELD); }
  static OpOutput PipeYieldBack() { return OpOutput(Code::PIPE_YIELD_BACK); }
  static OpOutput Finished(std::optional<typename Traits::Batch> batch = std::nullopt) {
    return OpOutput(Code::FINISHED, std::move(batch));
  }
  static OpOutput Cancelled() { return OpOutput(Code::CANCELLED); }

  bool IsPipeSinkNeedsMore() const noexcept { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() const noexcept { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const noexcept { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsBlocked() const noexcept { return code_ == Code::BLOCKED; }
  bool IsPipeYield() const noexcept { return code_ == Code::PIPE_YIELD; }
  bool IsPipeYieldBack() const noexcept { return code_ == Code::PIPE_YIELD_BACK; }
  bool IsFinished() const noexcept { return code_ == Code::FINISHED; }
  bool IsCancelled() const noexcept { return code_ == Code::CANCELLED; }

  std::optional<typename Traits::Batch>& GetBatch() {
    assert(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<typename Traits::Batch>>(payload_);
  }

  const std::optional<typename Traits::Batch>& GetBatch() const {
    assert(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<typename Traits::Batch>>(payload_);
  }

  task::ResumerPtr& GetResumer() {
    assert(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  const task::ResumerPtr& GetResumer() const {
    assert(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  std::string ToString() const {
    switch (code_) {
      case Code::PIPE_SINK_NEEDS_MORE:
        return "PIPE_SINK_NEEDS_MORE";
      case Code::PIPE_EVEN:
        return "PIPE_EVEN";
      case Code::SOURCE_PIPE_HAS_MORE:
        return "SOURCE_PIPE_HAS_MORE";
      case Code::BLOCKED:
        return "BLOCKED";
      case Code::PIPE_YIELD:
        return "PIPE_YIELD";
      case Code::PIPE_YIELD_BACK:
        return "PIPE_YIELD_BACK";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
    }
    return "UNKNOWN";
  }

 private:
  explicit OpOutput(Code code, std::optional<typename Traits::Batch> batch = std::nullopt)
      : code_(code), payload_(std::move(batch)) {}

  explicit OpOutput(task::ResumerPtr resumer)
      : code_(Code::BLOCKED), payload_(std::move(resumer)) {}

  Code code_;
  std::variant<task::ResumerPtr, std::optional<typename Traits::Batch>> payload_;
};

}  // namespace openpipeline::op
