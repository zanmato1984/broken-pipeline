#pragma once

/**
 * @file arrow_traits.h
 *
 * @brief Example `broken_pipeline` Traits implementation backed by Apache Arrow.
 *
 * broken_pipeline does not define its own Status/Result type. Instead, all broken_pipeline APIs
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

#include <broken_pipeline/broken_pipeline.h>

namespace broken_pipeline_arrow {

struct Context {
  const char* query_name = "broken-pipeline-arrow";
};

struct Traits {
  using Batch = std::shared_ptr<arrow::RecordBatch>;
  using Context = broken_pipeline_arrow::Context;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

// Convenience aliases for the example so other headers don't have to repeat the plumbing.
using Batch = Traits::Batch;
using Status = Traits::Status;

template <class T>
using Result = Traits::template Result<T>;

using TaskContext = broken_pipeline::TaskContext<Traits>;
using TaskGroup = broken_pipeline::TaskGroup<Traits>;

using Task = broken_pipeline::Task<Traits>;
using TaskId = broken_pipeline::TaskId;
using ThreadId = broken_pipeline::ThreadId;
using TaskStatus = broken_pipeline::TaskStatus;
using TaskHint = broken_pipeline::TaskHint;

using OpOutput = broken_pipeline::OpOutput<Traits>;
using OpResult = broken_pipeline::OpResult<Traits>;
using PipelineSource = broken_pipeline::PipelineSource<Traits>;
using PipelineDrain = broken_pipeline::PipelineDrain<Traits>;
using PipelinePipe = broken_pipeline::PipelinePipe<Traits>;
using PipelineSink = broken_pipeline::PipelineSink<Traits>;

using SourceOp = broken_pipeline::SourceOp<Traits>;
using PipeOp = broken_pipeline::PipeOp<Traits>;
using SinkOp = broken_pipeline::SinkOp<Traits>;

using Pipeline = broken_pipeline::Pipeline<Traits>;
using PipelineChannel = Pipeline::Channel;

}  // namespace broken_pipeline_arrow
