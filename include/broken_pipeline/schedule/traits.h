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

/// @file traits.h
///
/// @brief Broken Pipeline Traits implementation backed by Apache Arrow.
///
/// Broken Pipeline core is traits-based and does not define its own Status/Result/Batch
/// types. This header provides the project's unified Arrow binding:
/// - `Status` maps to `arrow::Status`
/// - `Result<T>` maps to `arrow::Result<T>`
/// - `Batch` maps to `std::shared_ptr<arrow::RecordBatch>`

#include <memory>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

#include <broken_pipeline/broken_pipeline.h>

namespace bp::schedule {

struct Context {
  const char* query_name = "broken-pipeline";
};

struct Traits {
  using Batch = std::shared_ptr<::arrow::RecordBatch>;
  using Context = bp::schedule::Context;
  using Status = ::arrow::Status;

  template <class T>
  using Result = ::arrow::Result<T>;
};

// Convenience aliases so callers don't have to repeat the plumbing.
using Batch = Traits::Batch;
using Status = Traits::Status;

template <class T>
using Result = Traits::template Result<T>;

using TaskContext = bp::TaskContext<Traits>;
using TaskGroup = bp::TaskGroup<Traits>;

using Task = bp::Task<Traits>;
using Continuation = bp::Continuation<Traits>;
using TaskId = bp::TaskId;
using ThreadId = bp::ThreadId;
using TaskStatus = bp::TaskStatus;
using TaskHint = bp::TaskHint;
using Resumer = bp::Resumer;
using Awaiter = bp::Awaiter;

using OpOutput = bp::OpOutput<Traits>;
using OpResult = bp::OpResult<Traits>;
using PipelineSource = bp::PipelineSource<Traits>;
using PipelineDrain = bp::PipelineDrain<Traits>;
using PipelinePipe = bp::PipelinePipe<Traits>;
using PipelineSink = bp::PipelineSink<Traits>;

using SourceOp = bp::SourceOp<Traits>;
using PipeOp = bp::PipeOp<Traits>;
using SinkOp = bp::SinkOp<Traits>;

using Pipeline = bp::Pipeline<Traits>;
using PipelineChannel = Pipeline::Channel;

using bp::Compile;

}  // namespace bp::schedule
