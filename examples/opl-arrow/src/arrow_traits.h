#pragma once

/**
 * @file arrow_traits.h
 *
 * @brief Example `openpipeline` Traits implementation backed by Apache Arrow.
 *
 * openpipeline does not define its own Status/Result type. Instead, all openpipeline APIs
 * are parameterized by `Traits::Status` and `Traits::Result<T>`.
 *
 * In this example:
 * - `Status` maps to `arrow::Status`
 * - `Result<T>` maps to `arrow::Result<T>`
 * - `Batch` maps to `std::shared_ptr<arrow::RecordBatch>`
 */

#include <memory>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <openpipeline/openpipeline.h>

namespace opl_arrow {

struct Context {
  const char* query_name = "opl-arrow-demo";
};

struct Traits {
  using Batch = std::shared_ptr<arrow::RecordBatch>;
  using Context = opl_arrow::Context;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

// Convenience aliases for the demo so other headers don't have to repeat the plumbing.
using Batch = Traits::Batch;
using Status = Traits::Status;

template <class T>
using Result = Traits::template Result<T>;

using TaskContext = openpipeline::TaskContext<Traits>;
using TaskGroup = openpipeline::TaskGroup<Traits>;
using TaskGroups = openpipeline::TaskGroups<Traits>;

using Task = openpipeline::Task<Traits>;
using TaskId = openpipeline::TaskId;
using ThreadId = openpipeline::ThreadId;
using TaskStatus = openpipeline::TaskStatus;
using TaskHint = openpipeline::TaskHint;

using ResumerPtr = openpipeline::ResumerPtr;
using Resumers = openpipeline::Resumers;
using AwaiterPtr = openpipeline::AwaiterPtr;

using OpOutput = openpipeline::OpOutput<Traits>;
using OpResult = openpipeline::OpResult<Traits>;
using PipelineSource = openpipeline::PipelineSource<Traits>;
using PipelineDrain = openpipeline::PipelineDrain<Traits>;
using PipelinePipe = openpipeline::PipelinePipe<Traits>;
using PipelineSink = openpipeline::PipelineSink<Traits>;

using SourceOp = openpipeline::SourceOp<Traits>;
using PipeOp = openpipeline::PipeOp<Traits>;
using SinkOp = openpipeline::SinkOp<Traits>;

using Pipeline = openpipeline::Pipeline<Traits>;
using PipelineChannel = Pipeline::Channel;

}  // namespace opl_arrow
