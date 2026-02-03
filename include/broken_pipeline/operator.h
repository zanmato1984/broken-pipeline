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
/// - flow control ("needs more input", "produced output", "has more pending output")
/// - execution control ("blocked", "yield", "finished", "cancelled")
///
/// Flow control is defined in terms of the push-style callbacks:
/// - A `Pipe` or `Sink` callback is invoked with an upstream `input` batch (or `nullopt`
/// for
///   re-entry).
/// - A `Source` callback is invoked by the driver to produce a batch.
///
/// Two key flow-control codes are:
/// - `PIPE_SINK_NEEDS_MORE`: the operator consumed input (or advanced internal state) and
///   did not produce an output batch. The driver may continue by providing more upstream
///   input when available.
/// - `SOURCE_PIPE_HAS_MORE(batch)`: the operator produced one output batch and requires a
///   follow-up re-entry (typically with `input=nullopt`) to emit more pending output
///   before it can accept another upstream input batch.
///
/// Terminology note:
/// - Re-entering an operator (invoking it again, often with `input=nullopt`) is distinct
///   from `Resumer::Resume()`, which is the wake-up mechanism used only for
///   `OpOutput::Blocked(resumer)`.
template <BrokenPipelineTraits Traits>
class OpOutput {
 public:
  enum class Code {
    /// @brief Downstream needs more input; no output was produced.
    ///
    /// For a `Pipe`, this means: stop pushing downstream and continue by obtaining
    /// another
    /// upstream batch (for example, by invoking the upstream source or upstream pipe in
    /// the
    /// driver's state machine).
    /// For a `Sink`, this is the "normal" successful consume result.
    PIPE_SINK_NEEDS_MORE,
    /// @brief Produced one output batch and does not require a follow-up re-entry.
    ///
    /// The operator can accept another upstream input batch immediately.
    PIPE_EVEN,
    /// @brief Produced one output batch and has more output pending internally.
    ///
    /// The driver should re-enter the same operator (input=nullopt) before providing it
    /// another upstream input batch.
    SOURCE_PIPE_HAS_MORE,
    /// @brief Cannot make progress until `Resumer::Resume()` is triggered.
    BLOCKED,
    /// @brief Yield to the scheduler before continuing.
    ///
    /// A driver may propagate this as `TaskStatus::Yield()` to let the scheduler
    /// migrate execution to another pool or priority class.
    PIPE_YIELD,
    /// @brief Yield-back handshake signal for a previously yielded operator.
    ///
    /// A common interpretation is: the yield-required long synchronous step (often
    /// IO-heavy,
    /// such as spilling) has completed, and subsequent work should be scheduled back to a
    /// CPU-oriented pool.
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
/// The pipeline driver repeatedly calls `Source(ctx, thread_id)` to obtain output
/// batches.
///
/// A source typically returns:
/// - `SourcePipeHasMore(batch)` to produce one batch
/// - `Finished()` when it has no more data
/// - `Finished(batch)` to signal end-of-stream while also producing a final batch
/// - `Blocked(resumer)` if it needs to wait for an external event (async IO /
/// backpressure)
template <BrokenPipelineTraits Traits>
using PipelineSource =
    std::function<OpResult<Traits>(const TaskContext<Traits>&, ThreadId)>;

/// @brief Pipe/Sink callback signature.
///
/// The `input` parameter is `std::optional<Batch>`:
/// - When `input` has a value, it is a new upstream batch.
/// - When `input` is `std::nullopt`, the driver is re-entering the operator to continue
///   from internal state (for example, after `SOURCE_PIPE_HAS_MORE`, after a blocked wait
///   completes, or as part of a yield handshake).
///
/// Terminology note:
/// - Re-entering an operator is distinct from `Resumer::Resume()`, which is the wake-up
///   mechanism used only for `OpOutput::Blocked(resumer)`.
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
/// Frontend/Backend task groups:
/// - `Frontend()` returns a list of task groups the host schedules before running the
///   pipelinexe(s) that invoke `Source()`.
/// - `Backend()` returns an optional task group the host typically schedules ahead as an
///   IO-readiness task (for example, synchronous IO reading/dequeueing from a queue, or
///   asynchronous IO waiting on callbacks/signals).
///
/// These task groups allow a host to split a source into multiple computation stages
/// (for example, CPU-heavy preparation in frontend vs IO-heavy readiness in backend) and
/// route them using `TaskHint`.
///
/// Broken Pipeline does not impose a specific scheduler or ordering for these task
/// groups; the host orchestration decides.
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
/// `ImplicitSource()` is the hook used by `bp::Compile` to split a pipeline into multiple
/// pipelinexes (stages):
/// - returning `nullptr` means "no split here"
/// - returning a source means "downstream operators become a new stage rooted at this
///   implicit source"
///
/// Example:
/// - A right outer join hash probe may need a post-probe scan phase of the build-side
/// hash
///   table to emit unmatched rows. Representing that scan phase as an implicit source
///   makes it a separate downstream pipelinexe.
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
/// Frontend/Backend task groups:
/// - `Frontend()` returns a list of task groups the host schedules strictly after the
///   pipelinexe(s) feeding this sink have finished.
/// - `Backend()` returns an optional task group the host typically schedules ahead as an
///   IO-readiness task (for example, synchronous IO reading/dequeueing from a queue, or
///   asynchronous IO waiting on callbacks/signals).
///
/// These task groups allow a host to split a sink into multiple computation stages (for
/// example, IO-heavy readiness in backend, the main streaming consumption in `Sink()`,
/// and post-stream CPU-heavy finalization in frontend) and route them using `TaskHint`.
///
/// `ImplicitSource()` is a host-orchestration hook for chaining a sink's output into a
/// subsequent pipeline stage. `bp::Compile` does not use sink implicit sources.
///
/// Example:
/// - An aggregation implemented as a sink can use `ImplicitSource()` to provide a source
///   that emits group-by results into a subsequent pipeline stage.
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
