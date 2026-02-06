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
/// @brief Schedule convenience aliases for the Arrow-backed core Traits.
///
/// The Arrow-backed Traits live in `broken_pipeline/traits/arrow.h`. This header
/// re-exports them into the `bp::schedule` namespace alongside the schedule helpers.

#include <broken_pipeline/broken_pipeline.h>
#include <broken_pipeline/traits/arrow.h>

namespace bp::schedule {

/// @brief Arrow-backed Traits binding for Broken Pipeline.
using Traits = bp::traits::arrow::Traits;

// Convenience aliases so callers don't have to repeat the plumbing.
using Batch = Traits::Batch;
using Status = Traits::Status;
template <class T>
using Result = Traits::template Result<T>;

/// @brief Task context alias for Arrow-backed Traits.
using TaskContext = bp::TaskContext<Traits>;
/// @brief Task group alias for Arrow-backed Traits.
using TaskGroup = bp::TaskGroup<Traits>;

/// @brief Task alias for Arrow-backed Traits.
using Task = bp::Task<Traits>;
/// @brief Continuation alias for Arrow-backed Traits.
using Continuation = bp::Continuation<Traits>;
/// @brief TaskId alias.
using TaskId = bp::TaskId;
/// @brief ThreadId alias.
using ThreadId = bp::ThreadId;
/// @brief TaskStatus alias.
using TaskStatus = bp::TaskStatus;
/// @brief TaskHint alias.
using TaskHint = bp::TaskHint;
/// @brief Resumer alias.
using Resumer = bp::Resumer;
/// @brief Awaiter alias.
using Awaiter = bp::Awaiter;

/// @brief Operator output alias for Arrow-backed Traits.
using OpOutput = bp::OpOutput<Traits>;
/// @brief Operator result alias for Arrow-backed Traits.
using OpResult = bp::OpResult<Traits>;
/// @brief Pipeline source alias.
using PipelineSource = bp::PipelineSource<Traits>;
/// @brief Pipeline drain alias.
using PipelineDrain = bp::PipelineDrain<Traits>;
/// @brief Pipeline pipe alias.
using PipelinePipe = bp::PipelinePipe<Traits>;
/// @brief Pipeline sink alias.
using PipelineSink = bp::PipelineSink<Traits>;

/// @brief Source operator alias.
using SourceOp = bp::SourceOp<Traits>;
/// @brief Pipe operator alias.
using PipeOp = bp::PipeOp<Traits>;
/// @brief Sink operator alias.
using SinkOp = bp::SinkOp<Traits>;

/// @brief Pipeline alias.
using Pipeline = bp::Pipeline<Traits>;
/// @brief Pipeline channel alias.
using PipelineChannel = Pipeline::Channel;

/// @brief Compile helper alias for Arrow-backed Traits.
using bp::Compile;

}  // namespace bp::schedule
