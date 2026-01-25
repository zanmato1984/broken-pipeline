#pragma once

/**
 * @file opl.h
 *
 * @brief Public umbrella header for the core opl protocol.
 *
 * opl is a **header-only, purely generic (traits-based)** set of interfaces and
 * small protocol data structures for building a query/execution engine in the style of
 * Ara:
 *
 * - Operators expose a small-step, re-entrant, batch-at-a-time interface.
 * - Pipelines are driven as explicit state machines (not via blocking threads).
 * - Execution is expressed in terms of `Task` / `TaskGroup` and a small `TaskStatus`
 *   protocol (Continue/Blocked/Yield/Finished/Cancelled).
 *
 * What opl **does not** provide:
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
 * - Pipeline runtime: `PipelineExec` (include `opl/pipeline_exec.h`)
 *
 * Internal building blocks (intentionally *not* included here):
 * - `include/opl/detail/*` contains reference implementations for splitting a pipeline
 *   into sub-pipeline stages and driving a stage as a small-step task.
 */

#include <opl/concepts.h>

#include <opl/task.h>

#include <opl/op.h>
#include <opl/pipeline.h>
