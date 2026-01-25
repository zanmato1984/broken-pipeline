#pragma once

/**
 * @file openpipeline.h
 *
 * @brief Public umbrella header for the core openpipeline protocol.
 *
 * openpipeline is a **header-only, purely generic (traits-based)** set of interfaces and
 * small protocol data structures for building a query/execution engine in the style of
 * Ara:
 *
 * - Operators expose a small-step, re-entrant, batch-at-a-time interface.
 * - Pipelines are driven as explicit state machines (not via blocking threads).
 * - Execution is expressed in terms of `Task` / `TaskGroup` and a small `TaskStatus`
 *   protocol (Continue/Blocked/Yield/Finished/Cancelled).
 *
 * What openpipeline **does not** provide:
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
 * Optional helper (intentionally *not* included here):
 * - `#include <openpipeline/compile.h>` provides
 *   `openpipeline::CompileTaskGroups(...)`, which compiles a `Pipeline`
 *   into an ordered list of `TaskGroup`s by internally splitting it into *physical*
 *   stages (based on pipe implicit sources) and wrapping each stage in a generic
 *   `detail::PipelineExec` state machine.
 */

#include <openpipeline/concepts.h>

#include <openpipeline/task.h>

#include <openpipeline/op.h>
#include <openpipeline/pipeline.h>
