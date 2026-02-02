#pragma once

/**
 * @file broken_pipeline.h
 *
 * @brief Public umbrella header for the core broken_pipeline protocol.
 *
 * broken_pipeline is a **header-only, purely generic (traits-based)** set of interfaces and
 * small protocol data structures for building a query/execution engine in the style of
 * Ara:
 *
 * - Operators expose a small-step, re-entrant, batch-at-a-time interface.
 * - Pipelines are driven as explicit state machines (not via blocking threads).
 * - Execution is expressed in terms of `Task` / `TaskGroup` and a small `TaskStatus`
 *   protocol (Continue/Blocked/Yield/Finished/Cancelled).
 *
 * What broken_pipeline **does not** provide:
 * - No concrete operators.
 * - No scheduler/executor implementation.
 * - No async/future library dependency.
 * - No dependency on Arrow (or any other data structure library).
 *
 * The design goal is to let you plug in your own:
 * - batch type (your `Traits::Batch`)
 * - status/result types (your `Traits::Status` and `Traits::Result<T>`)
 * - scheduler primitives (your `Resumer`/`Awaiter` implementations + factories)
 *
 * Public surface (via this umbrella header):
 * - Task protocol: `Task`, `TaskGroup`, `TaskStatus`, `Resumer`/`Awaiter`, `TaskContext`
 * - Operator protocol: `SourceOp` / `PipeOp` / `SinkOp` and `OpOutput`
 * - Pipeline graph: `Pipeline`
 *
 * Helper:
 * - `#include <broken_pipeline/pipeline_exec.h>` provides `broken_pipeline::Compile(pipeline, dop)`
 *   and the compiled plan `broken_pipeline::PipelineExec` (with `PipelineExecSegment` / `PipeExec`).
 */

#include <broken_pipeline/concepts.h>

#include <broken_pipeline/operator.h>
#include <broken_pipeline/pipeline.h>
#include <broken_pipeline/task.h>

#include <broken_pipeline/pipeline_exec.h>
