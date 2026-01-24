#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/op/op_output.h>
#include <openpipeline/task/task_context.h>
#include <openpipeline/task/task_group.h>

namespace openpipeline::op {

/**
 * @brief Result type for operator callbacks.
 *
 * Operators return `Traits::Result<OpOutput<Traits>>`.
 */
template <OpenPipelineTraits Traits>
using OpResult = Result<Traits, OpOutput<Traits>>;

/**
 * @brief Source callback signature.
 *
 * The pipeline driver repeatedly calls `Source(ctx, thread_id)` to pull batches.
 *
 * A source typically returns:
 * - `Finished()` when it has no more data
 * - `SourcePipeHasMore(batch)` for more data
 * - `Blocked(resumer)` if it needs to wait for an external event (async IO / backpressure)
 */
template <OpenPipelineTraits Traits>
using PipelineSource =
    std::function<OpResult<Traits>(const task::TaskContext<Traits>&, typename Traits::ThreadId)>;

/**
 * @brief Pipe/Sink callback signature.
 *
 * The `input` parameter is `std::optional<Batch>`:
 * - When `input` has a value, it is a new upstream batch.
 * - When `input` is `std::nullopt`, the driver is resuming the operator to emit more
 *   output from internal state (e.g., after `SOURCE_PIPE_HAS_MORE`, after Blocked, or
 *   after Yield).
 */
template <OpenPipelineTraits Traits>
using PipelinePipe = std::function<OpResult<Traits>(const task::TaskContext<Traits>&,
                                                    typename Traits::ThreadId,
                                                    std::optional<typename Traits::Batch>)>;

/**
 * @brief Drain callback signature.
 *
 * Drains are called only after the upstream source is exhausted (`Finished()` observed).
 * They allow operators to flush tail output that can only be produced at end-of-stream.
 *
 * An operator that does not need draining should return an empty `std::function{}`.
 */
template <OpenPipelineTraits Traits>
using PipelineDrain =
    std::function<OpResult<Traits>(const task::TaskContext<Traits>&, typename Traits::ThreadId)>;

template <OpenPipelineTraits Traits>
using PipelineSink = PipelinePipe<Traits>;

/**
 * @brief Source operator interface.
 *
 * Lifecycle hooks:
 * - `Frontend()`: stage work before the source is run (e.g., start scan, open files).
 * - `Backend()`: optional extra stage work after the pipeline stage is done.
 *
 * openpipeline does not impose a specific driver/scheduler for these hooks; helpers like
 * `pipeline::CompileTaskGroups` decide ordering.
 */
template <OpenPipelineTraits Traits>
class SourceOp {
 public:
  SourceOp(std::string name = {}, std::string desc = {})
      : name_(std::move(name)), desc_(std::move(desc)) {}
  virtual ~SourceOp() = default;

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  virtual PipelineSource<Traits> Source() = 0;
  virtual task::TaskGroups<Traits> Frontend() = 0;
  virtual std::optional<task::TaskGroup<Traits>> Backend() = 0;

 private:
  std::string name_;
  std::string desc_;
};

/**
 * @brief Pipe operator interface (transform operator).
 *
 * A pipe participates in the main streaming path via `Pipe()`, may optionally implement
 * `Drain()` for tail output, and may optionally introduce a new stage via `ImplicitSource()`.
 *
 * `ImplicitSource()` is the hook used to split a logical pipeline into multiple physical
 * pipeline stages (see `pipeline::CompileTaskGroups`):
 * - returning `nullptr` means "no split here"
 * - returning a source means "downstream operators become a new physical stage rooted at
 *   this implicit source"
 */
template <OpenPipelineTraits Traits>
class PipeOp {
 public:
  PipeOp(std::string name = {}, std::string desc = {})
      : name_(std::move(name)), desc_(std::move(desc)) {}
  virtual ~PipeOp() = default;

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  virtual PipelinePipe<Traits> Pipe() = 0;
  virtual PipelineDrain<Traits> Drain() = 0;  // empty std::function means “no drain”
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;

 private:
  std::string name_;
  std::string desc_;
};

/**
 * @brief Sink operator interface.
 *
 * The sink consumes the output stream. Most sinks return `PIPE_SINK_NEEDS_MORE` after
 * successfully consuming a batch, and may return `BLOCKED(resumer)` for backpressure.
 *
 * Like pipes, sinks also expose lifecycle hooks (`Frontend/Backend`) and an optional
 * `ImplicitSource()` hook which can be used by higher-level orchestration to chain a sink
 * output into a subsequent pipeline.
 */
template <OpenPipelineTraits Traits>
class SinkOp {
 public:
  SinkOp(std::string name = {}, std::string desc = {})
      : name_(std::move(name)), desc_(std::move(desc)) {}
  virtual ~SinkOp() = default;

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  virtual PipelineSink<Traits> Sink() = 0;
  virtual task::TaskGroups<Traits> Frontend() = 0;
  virtual std::optional<task::TaskGroup<Traits>> Backend() = 0;
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;

 private:
  std::string name_;
  std::string desc_;
};

}  // namespace openpipeline::op
