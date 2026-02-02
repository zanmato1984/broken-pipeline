#pragma once

/**
 * @file arrow_traits.h
 *
 * @brief `opl` Traits for unit tests (Arrow Status/Result).
 *
 * opl does not define its own Status/Result type. Instead, all opl APIs are parameterized
 * by `Traits::Status` and `Traits::Result<T>`.
 *
 * In this test Traits:
 * - `Status` maps to `arrow::Status`
 * - `Result<T>` maps to `arrow::Result<T>`
 * - `Batch` maps to `int`
 */

#include <memory>

#include <arrow/result.h>
#include <arrow/status.h>

#include <opl/opl.h>

namespace opl_test {

struct Context {
  const char* query_name = "opl-tests";
};

struct Traits {
  using Batch = int;
  using Context = opl_test::Context;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

// Convenience aliases for tests so callers don't have to repeat the plumbing.
using Batch = Traits::Batch;
using Status = Traits::Status;

template <class T>
using Result = Traits::template Result<T>;

using TaskContext = opl::TaskContext<Traits>;
using TaskGroup = opl::TaskGroup<Traits>;

using Task = opl::Task<Traits>;
using TaskId = opl::TaskId;
using ThreadId = opl::ThreadId;
using TaskStatus = opl::TaskStatus;
using TaskHint = opl::TaskHint;
using Resumer = opl::Resumer;
using Awaiter = opl::Awaiter;

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
using PipelineChannel = Pipeline::Channel;

}  // namespace opl_test
