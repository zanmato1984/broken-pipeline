#pragma once

#include <cassert>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <broken_pipeline/concepts.h>

namespace bp {

class Resumer;

template <BrokenPipelineTraits Traits>
struct TaskContext;

template <BrokenPipelineTraits Traits>
class TaskGroup;

/// @brief Output/control signal returned by operator callbacks to the pipeline runtime.
///
/// `OpOutput` is the small protocol between operator implementations and a pipeline
/// driver (e.g., `PipeExec`).
///
/// It mixes two kinds of information:
/// - flow control ("need more input", "have more internal output")
/// - execution control ("blocked", "yield", "finished", "cancelled")
///
/// The most important distinction is:
/// - `PIPE_SINK_NEEDS_MORE`: the operator needs more *upstream input* before it can
///   produce more output.
/// - `SOURCE_PIPE_HAS_MORE(batch)`: the operator produced a batch but still has more
///   *internal output pending*, so the driver should resume the same operator again
///   (typically by calling it with `std::nullopt` input).
template <BrokenPipelineTraits Traits>
class OpOutput {
 public:
  enum class Code {
    /// @brief Downstream needs more input; no output was produced.
    ///
    /// For a `Pipe`, this means: stop pushing forward and go back to the source to
    /// fetch more input.
    /// For a `Sink`, this is the "normal" successful consume result.
    PIPE_SINK_NEEDS_MORE,
    /// @brief Produced exactly one output batch and does not require immediate resumption.
    PIPE_EVEN,
    /// @brief Produced one output batch and has more output pending internally.
    ///
    /// The driver should resume the same operator before pulling a new input batch.
    SOURCE_PIPE_HAS_MORE,
    /// @brief Cannot make progress until `Resumer::Resume()` is triggered.
    BLOCKED,
    /// @brief Yield to the scheduler before continuing.
    ///
    /// A driver may propagate this as `TaskStatus::Yield()` to let the scheduler
    /// migrate execution to another pool or priority class.
    PIPE_YIELD,
    /// @brief Resume handshake for a previously yielded operator.
    PIPE_YIELD_BACK,
    /// @brief Stream finished; may carry a final output batch.
    FINISHED,
    /// @brief Cancelled (usually due to error in a sibling task).
    CANCELLED,
  };

  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::PIPE_SINK_NEEDS_MORE); }
  static OpOutput PipeEven(typename Traits::Batch batch) {
    return OpOutput(Code::PIPE_EVEN, std::move(batch));
  }
  static OpOutput SourcePipeHasMore(typename Traits::Batch batch) {
    return OpOutput(Code::SOURCE_PIPE_HAS_MORE, std::move(batch));
  }
  static OpOutput Blocked(std::shared_ptr<Resumer> resumer) {
    return OpOutput(std::move(resumer));
  }
  static OpOutput PipeYield() { return OpOutput(Code::PIPE_YIELD); }
  static OpOutput PipeYieldBack() { return OpOutput(Code::PIPE_YIELD_BACK); }
  static OpOutput Finished(std::optional<typename Traits::Batch> batch = std::nullopt) {
    return OpOutput(Code::FINISHED, std::move(batch));
  }
  static OpOutput Cancelled() { return OpOutput(Code::CANCELLED); }

  bool IsPipeSinkNeedsMore() const noexcept {
    return code_ == Code::PIPE_SINK_NEEDS_MORE;
  }
  bool IsPipeEven() const noexcept { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const noexcept {
    return code_ == Code::SOURCE_PIPE_HAS_MORE;
  }
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

  std::shared_ptr<Resumer>& GetResumer() {
    assert(IsBlocked());
    return std::get<std::shared_ptr<Resumer>>(payload_);
  }

  const std::shared_ptr<Resumer>& GetResumer() const {
    assert(IsBlocked());
    return std::get<std::shared_ptr<Resumer>>(payload_);
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

  explicit OpOutput(std::shared_ptr<Resumer> resumer)
      : code_(Code::BLOCKED), payload_(std::move(resumer)) {}

  Code code_;
  std::variant<std::shared_ptr<Resumer>, std::optional<typename Traits::Batch>> payload_;
};

/// @brief Result type for operator callbacks.
///
/// Operators return `Traits::Result<OpOutput<Traits>>`.
template <BrokenPipelineTraits Traits>
using OpResult = Result<Traits, OpOutput<Traits>>;

/// @brief Source callback signature.
///
/// The pipeline driver repeatedly calls `Source(ctx, thread_id)` to pull batches.
///
/// A source typically returns:
/// - `Finished()` when it has no more data
/// - `SourcePipeHasMore(batch)` for more data
/// - `Blocked(resumer)` if it needs to wait for an external event (async IO /
/// backpressure)
template <BrokenPipelineTraits Traits>
using PipelineSource =
    std::function<OpResult<Traits>(const TaskContext<Traits>&, ThreadId)>;

/// @brief Pipe/Sink callback signature.
///
/// The `input` parameter is `std::optional<Batch>`:
/// - When `input` has a value, it is a new upstream batch.
/// - When `input` is `std::nullopt`, the driver is resuming the operator to emit more
///   output from internal state (e.g., after `SOURCE_PIPE_HAS_MORE`, after Blocked, or
///   after Yield).
template <BrokenPipelineTraits Traits>
using PipelinePipe = std::function<OpResult<Traits>(
    const TaskContext<Traits>&, ThreadId, std::optional<typename Traits::Batch>)>;

/// @brief Drain callback signature.
///
/// Drains are called only after the upstream source is exhausted (`Finished()` observed).
/// They allow operators to flush tail output that can only be produced at end-of-stream.
///
/// An operator that does not need draining should return an empty `std::function{}`.
template <BrokenPipelineTraits Traits>
using PipelineDrain =
    std::function<OpResult<Traits>(const TaskContext<Traits>&, ThreadId)>;

template <BrokenPipelineTraits Traits>
using PipelineSink = PipelinePipe<Traits>;

/// @brief Source operator interface.
///
/// Lifecycle hooks:
/// - `Frontend()`: stage work before the source is run (e.g., start scan, open files).
/// - `Backend()`: optional extra stage work after the pipeline stage is done.
///
/// broken_pipeline does not impose a specific driver/scheduler for these hooks; the host
/// orchestration decides ordering.
template <BrokenPipelineTraits Traits>
class SourceOp {
 public:
  explicit SourceOp(std::string name = {}) : name_(std::move(name)) {}
  virtual ~SourceOp() = default;

  const std::string& Name() const noexcept { return name_; }

  virtual PipelineSource<Traits> Source() = 0;
  virtual std::vector<TaskGroup<Traits>> Frontend() = 0;
  virtual std::optional<TaskGroup<Traits>> Backend() = 0;

 private:
  std::string name_;
};

/// @brief Pipe operator interface (transform operator).
///
/// A pipe participates in the main streaming path via `Pipe()`, may optionally implement
/// `Drain()` for tail output, and may optionally introduce a new stage via
/// `ImplicitSource()`.
///
/// `ImplicitSource()` is the hook used to split a pipeline into multiple sub-pipeline
/// stages (via host orchestration):
/// - returning `nullptr` means "no split here"
/// - returning a source means "downstream operators become a new sub-pipeline stage rooted
/// at this implicit source"
template <BrokenPipelineTraits Traits>
class PipeOp {
 public:
  explicit PipeOp(std::string name = {}) : name_(std::move(name)) {}
  virtual ~PipeOp() = default;

  const std::string& Name() const noexcept { return name_; }

  virtual PipelinePipe<Traits> Pipe() = 0;
  virtual PipelineDrain<Traits> Drain() = 0;  // empty std::function means “no drain”
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;

 private:
  std::string name_;
};

/// @brief Sink operator interface.
///
/// The sink consumes the output stream. Most sinks return `PIPE_SINK_NEEDS_MORE` after
/// successfully consuming a batch, and may return `BLOCKED(resumer)` for backpressure.
///
/// Like pipes, sinks also expose lifecycle hooks (`Frontend/Backend`) and an optional
/// `ImplicitSource()` hook which can be used by higher-level orchestration to chain a sink
/// output into a subsequent pipeline.
template <BrokenPipelineTraits Traits>
class SinkOp {
 public:
  explicit SinkOp(std::string name = {}) : name_(std::move(name)) {}
  virtual ~SinkOp() = default;

  const std::string& Name() const noexcept { return name_; }

  virtual PipelineSink<Traits> Sink() = 0;
  virtual std::vector<TaskGroup<Traits>> Frontend() = 0;
  virtual std::optional<TaskGroup<Traits>> Backend() = 0;
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;

 private:
  std::string name_;
};

}  // namespace bp
