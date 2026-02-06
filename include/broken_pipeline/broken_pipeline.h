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

/// @file broken_pipeline.h
///
/// @brief Umbrella header for Broken Pipeline.
///
/// Broken Pipeline is a header-only C++20 library that defines protocols for building
/// batch-at-a-time execution pipelines that can be driven without blocking threads.
///
/// The library is traits-based: you provide a `Traits` type (see
/// `bp::PipelineBreaker`) that defines `Traits::Batch` and an Arrow-like
/// `Traits::Status` / `Traits::Result<T>` transport.
///
/// The core execution model is:
/// - Operator callbacks are small-step and re-entrant; the reference runtime (`PipeExec`)
///   runs them as an explicit state machine (no blocking waits inside the library).
/// - Operators integrate with a host scheduler via `OpOutput`:
///   - `OpOutput::Blocked(resumer)` for event-driven waiting (async IO / backpressure)
///   - `OpOutput::PipeYield()` / `OpOutput::PipeYieldBack()` as a two-phase scheduling
///   point
///     around long synchronous work (often IO-heavy, such as spilling)
/// - Tasks return `TaskStatus` and use scheduler-provided `Resumer`/`Awaiter` primitives
/// to
///   represent blocked waiting. These primitives can be implemented with synchronous
///   primitives (for example, condition variables), asynchronous primitives (for example,
///   future/promise style), or coroutines.
///
/// Additional staging:
/// - `SourceOp::Frontend()` / `SourceOp::Backend()` and `SinkOp::Frontend()` /
///   `SinkOp::Backend()` allow a host to split operator work into separate task groups
///   and route them using `TaskHint` (for example, CPU vs IO pools).
///
/// Broken Pipeline provides:
/// - Task protocol: `Task`, `TaskGroup`, `Continuation`, `TaskStatus`, `TaskContext`,
///   `Resumer`/`Awaiter`
/// - Operator protocol: `SourceOp` / `PipeOp` / `SinkOp` and `OpOutput`
/// - Pipeline graph: `Pipeline`
/// - Reference compilation/runtime: `Compile(pipeline, dop)`, `PipelineExec`,
/// `Pipelinexe`,
///   `PipeExec`
///
/// Broken Pipeline does not provide:
/// - A scheduler/executor implementation or thread pool
/// - Concrete operators or concrete data types
/// - An async runtime dependency
///
/// Notes:
/// - Core public symbols live in namespace `bp`; optional adapters live under
///   `bp::traits::*`.
/// - `Compile` splits pipelines only on `PipeOp::ImplicitSource()`.
/// `SinkOp::ImplicitSource()`
///   is provided for host orchestration and is not used by `Compile`.

#include <broken_pipeline/concepts.h>
#include <broken_pipeline/result.h>
#include <broken_pipeline/types.h>

#include <broken_pipeline/operator.h>
#include <broken_pipeline/pipeline.h>
#include <broken_pipeline/task.h>

#include <broken_pipeline/pipeline_exec.h>
