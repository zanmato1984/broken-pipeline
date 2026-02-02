#pragma once

/**
 * @file arrow_traits.h
 *
 * @brief Apache Arrow-backed `opl` Traits + convenience aliases.
 *
 * This header is optional: it provides a ready-to-use `Traits` surface for
 * integrating `opl` with Apache Arrow (`arrow::Status` / `arrow::Result<T>`).
 *
 * It mirrors the alias style used in the `examples/opl-arrow` demo, but is
 * parameterized over the batch type so tests and downstream projects can reuse it.
 */

#include <variant>

#include <arrow/result.h>
#include <arrow/status.h>

#include <opl/opl.h>

namespace opl_arrow {

/**
 * @brief Arrow-backed Traits (parameterized batch and context).
 *
 * @tparam BatchT   Stream/batch type (e.g. `std::shared_ptr<arrow::Array>`,
 *                  `std::shared_ptr<arrow::RecordBatch>`, or a lightweight test type).
 * @tparam ContextT Optional query-level context type.
 */
template <class BatchT, class ContextT = std::monostate>
struct Traits {
  using Batch = BatchT;
  using Context = ContextT;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

/**
 * @brief Convenience alias bundle for a given (Batch, Context) pair.
 *
 * This is the same "plumbing" as the example `arrow_traits.h`, but generic.
 */
template <class BatchT, class ContextT = std::monostate>
struct Aliases {
  using Traits = opl_arrow::Traits<BatchT, ContextT>;

  using Batch = typename Traits::Batch;
  using Status = typename Traits::Status;

  template <class T>
  using Result = typename Traits::template Result<T>;

  using TaskContext = opl::TaskContext<Traits>;
  using TaskGroup = opl::TaskGroup<Traits>;

  using Task = opl::Task<Traits>;
  using Continuation = opl::Continuation<Traits>;
  using TaskId = opl::TaskId;
  using ThreadId = opl::ThreadId;
  using TaskStatus = opl::TaskStatus;
  using TaskHint = opl::TaskHint;

  using OpOutput = opl::OpOutput<Traits>;
  using OpResult = opl::OpResult<Traits>;
  using PipelineSource = opl::PipelineSource<Traits>;
  using PipelineDrain = opl::PipelineDrain<Traits>;
  using PipelinePipe = opl::PipelinePipe<Traits>;
  using PipelineSink = opl::PipelineSink<Traits>;

  using SourceOp = opl::SourceOp<Traits>;
  using PipeOp = opl::PipeOp<Traits>;
  using SinkOp = opl::SinkOp<Traits>;

  using Pipeline = opl::Pipeline<Traits>;
  using PipelineChannel = typename Pipeline::Channel;
};

}  // namespace opl_arrow

