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
 * - error/result type (your `Traits::Result<T>`)
 * - scheduler primitives (your `Resumer`/`Awaiter` implementations + factories)
 *
 * Public surface (via this umbrella header):
 * - `openpipeline::task::*`: tasks, task groups, resumer/awaiter, task status
 * - `openpipeline::op::*`: operator interfaces and `OpOutput`
 * - `openpipeline::pipeline::Pipeline`: the pipeline graph container
 *
 * Optional helper (intentionally *not* included here):
 * - `#include <openpipeline/pipeline/compile.h>` provides
 *   `openpipeline::pipeline::CompileTaskGroups(...)`, which compiles a `Pipeline`
 *   into an ordered list of `TaskGroup`s by internally splitting it into *physical*
 *   stages (based on pipe implicit sources) and wrapping each stage in a generic
 *   `PipelineTask` state machine.
 */

#include <openpipeline/concepts.h>

#include <openpipeline/task/awaiter.h>
#include <openpipeline/task/resumer.h>
#include <openpipeline/task/task_context.h>
#include <openpipeline/task/task_group.h>
#include <openpipeline/task/task.h>
#include <openpipeline/task/task_status.h>

#include <openpipeline/op/op_output.h>
#include <openpipeline/op/op.h>
#include <openpipeline/pipeline/pipeline.h>
