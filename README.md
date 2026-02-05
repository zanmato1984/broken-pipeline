# Broken Pipeline

Broken Pipeline is a header-only C++20 library that defines protocols for building
batch-at-a-time execution pipelines that can be driven without blocking threads. It is
traits-based: you provide the batch type and your status/result transport.

This repository contains:
- Core headers under `include/broken_pipeline/`
- Unit tests under `tests/` (require Apache Arrow and GoogleTest)

## Goals

- Provide minimal interfaces between operators, a pipeline runtime, and a scheduler.
- Make operator execution small-step and resumable.
- Avoid coupling to any particular data structure library or async runtime.

## Non-goals

- Scheduler, thread pool, or async runtime implementation
- Concrete operators
- A ready-to-run engine loop that orchestrates task groups

Note: the core library stays scheduler-agnostic, but the optional
`broken_pipeline::schedule` runtime (documented below) provides ready-to-use
Arrow-based schedulers for host applications that want one.

## Public API and layout

All public symbols are in namespace bp (no sub-namespaces).

Headers:
- `include/broken_pipeline/broken_pipeline.h` (umbrella header)
- `include/broken_pipeline/concepts.h`
- `include/broken_pipeline/task.h`
- `include/broken_pipeline/operator.h`
- `include/broken_pipeline/pipeline.h`
- `include/broken_pipeline/pipeline_exec.h`

Optional schedule runtime (Apache Arrow + Folly):
- `include/broken_pipeline/schedule/traits.h`
- `include/broken_pipeline/schedule/naive_parallel_scheduler.h`
- `include/broken_pipeline/schedule/async_dual_pool_scheduler.h`
- `include/broken_pipeline/schedule/detail/conditional_awaiter.h`, `include/broken_pipeline/schedule/detail/future_awaiter.h`
- `include/broken_pipeline/schedule/detail/callback_resumer.h`

CMake target:
- `broken_pipeline` (INTERFACE)
- `broken_pipeline::schedule` (optional compiled library; see Schedule runtime)

Include everything:

```cpp
#include <broken_pipeline/broken_pipeline.h>
```

## Terminology

- Batch: `Traits::Batch`
- DOP: degree of parallelism, used as the number of task instances in a stage
- TaskId: 0..dop-1 index within a `TaskGroup` (`std::size_t`)
- ThreadId: execution lane id passed to operators (`std::size_t`); the reference runtime uses `ThreadId = TaskId`
- Channel: one source plus a linear chain of pipes feeding a shared sink
- Pipelinexe: one compiled executable stage (a set of channels sharing a sink)
- Implicit source: a SourceOp created by a pipe or sink to start a downstream stage
- Drain: a tail-output callback invoked after the upstream source is exhausted

## Traits

Broken Pipeline is parameterized by a Traits type that satisfies `bp::BrokenPipelineTraits`
(`include/broken_pipeline/concepts.h`).

Your Traits must provide:
- `using Batch = ...;` (movable)
- `using Context = ...;` (an object type; can be `std::monostate`)
- `using Status = ...;` with `Status::OK()` and `status.ok()`
- `template<class T> using Result = ...;` with an Arrow-like surface:
  - `result.ok()`, `result.status()`, `result.ValueOrDie()`
  - constructible from `T` (success) and `Status` (error)

Minimal example (no dependencies):

```cpp
#include <string>
#include <utility>
#include <variant>

struct Status {
  static Status OK() { return Status(); }
  Status() = default;
  explicit Status(std::string msg) : msg_(std::move(msg)) {}

  bool ok() const { return msg_.empty(); }
  const std::string& message() const { return msg_; }

 private:
  std::string msg_;
};

template<class T>
class Result {
 public:
  Result(T value) : data_(std::move(value)) {}
  Result(Status status) : data_(std::move(status)) {}

  bool ok() const { return std::holds_alternative<T>(data_); }
  Status status() const { return ok() ? Status::OK() : std::get<Status>(data_); }

  T& ValueOrDie() & { return std::get<T>(data_); }
  const T& ValueOrDie() const & { return std::get<T>(data_); }
  T ValueOrDie() && { return std::move(std::get<T>(data_)); }

 private:
  std::variant<T, Status> data_;
};

struct Traits {
  using Batch = MyBatch;
  using Context = MyQueryContext;
  using Status = ::Status;

  template<class T>
  using Result = ::Result<T>;
};
```

## Task protocol

The core task types are defined in `include/broken_pipeline/task.h`:
- `bp::TaskStatus`
- `bp::Task<Traits>`
- `bp::TaskGroup<Traits>`
- `bp::Continuation<Traits>`
- `bp::Resumer`, `bp::Awaiter`
- `bp::TaskContext<Traits>`

Task and continuation entrypoints return `bp::TaskResult<Traits>`, which is an alias of
`Traits::Result<bp::TaskStatus>`.

TaskStatus codes:
- Continue: the task is runnable and should be invoked again.
- Blocked(awaiter): the task cannot make progress; the scheduler must wait/suspend on awaiter.
- Yield: the task requests a yield point before continuing.
- Finished: the task completed successfully.
- Cancelled: the task was cancelled (typically due to a sibling error).

Blocked is built out of scheduler-owned primitives:
- Operators return `OpOutput::Blocked(resumer)`.
- The task groups one or more resumers into a `TaskStatus::Blocked(awaiter)` using
  `TaskContext::awaiter_factory`.
- Some non-scheduler event source calls `Resumer::Resume()` when the awaited condition is
  satisfied. This is often driven by an operator (for example, a backend task completing IO
  or emitting a batch that unblocks downstream).
- The scheduler is responsible for observing resumed resumers and rescheduling the blocked
  task instance.

Compatibility note:
- `Resumer` and `Awaiter` are intentionally opaque to Broken Pipeline and can be implemented
  with synchronous primitives (for example, condition variables), asynchronous primitives
  (for example, promise/future style), or coroutines.

## Operator protocol

The operator interfaces and OpOutput are defined in `include/broken_pipeline/operator.h`:
- `bp::SourceOp<Traits>`
- `bp::PipeOp<Traits>`
- `bp::SinkOp<Traits>`
- `bp::OpOutput<Traits>`

Operator callbacks return `bp::OpResult<Traits>`, which is an alias of
`Traits::Result<bp::OpOutput<Traits>>`.

Notation:
- `TaskContext` means `bp::TaskContext<Traits>`
- `ThreadId` means `bp::ThreadId`
- `Batch` means `typename Traits::Batch`
- `OpResult` means `bp::OpResult<Traits>`

Callback signatures:
- Source: `OpResult(const TaskContext&, ThreadId)`
- Pipe: `OpResult(const TaskContext&, ThreadId, std::optional<Batch>)`
- Drain: `OpResult(const TaskContext&, ThreadId)`
- Sink: `OpResult(const TaskContext&, ThreadId, std::optional<Batch>)`

Input conventions for Pipe and Sink:
- input has a value: a new upstream batch
- input is nullopt: re-enter the operator callback to continue (after has-more, after a
  blocked wake-up, or as part of a yield handshake)

Terminology note:
- Re-entering an operator means the pipeline runtime invokes the operator callback again
  (often with input=nullopt). This is distinct from `Resumer::Resume()`, which is the
  wake-up mechanism used for `OpOutput::Blocked(resumer)`.

OpOutput codes:
- `PIPE_SINK_NEEDS_MORE`: the operator needs more upstream input and did not produce an output batch.
- `PIPE_EVEN(batch)`: the operator produced one output batch and can accept another upstream input batch immediately.
- `SOURCE_PIPE_HAS_MORE(batch)`: the operator produced one output batch and requires a follow-up re-entry with input=nullopt to emit more pending output before it can accept another upstream input batch.
- `BLOCKED(resumer)`: the operator cannot make progress until `Resumer::Resume()` is called on the resumer.
- `PIPE_YIELD`: yield request from the operator.
- `PIPE_YIELD_BACK`: yield-back signal from a previously yielding operator, indicating the
  yield-required long synchronous step has completed and it should be scheduled back to a
  CPU-oriented pool.
- `FINISHED(optional batch)`: end-of-stream; may carry a final output batch.
- `CANCELLED`: cancellation marker used by the reference runtime.

Typical uses for `PIPE_YIELD`:
- Operators can use `PIPE_YIELD` as a cooperative scheduling point before a long synchronous
  action, such as spilling to disk on the fly. A scheduler can move the task to an IO pool
  for that work.
- On the first re-entry after yielding, the operator returns `PIPE_YIELD_BACK` to indicate
  the long synchronous step has completed and execution can migrate back to a CPU pool.
- This yield/yield-back handshake provides a natural two-phase scheduling point: run the
  yielded step in an IO pool, then schedule subsequent work back on a CPU pool.

Contracts for the reference runtime in `include/broken_pipeline/pipeline_exec.h`:
- `SourceOp::Source()` must return one of:
  - `OpOutput::SourcePipeHasMore(batch)`
  - `OpOutput::Finished(optional batch)`
  - `OpOutput::Blocked(resumer)`
  - an error Result
- `PipeOp::Pipe()` must return one of:
  - `OpOutput::PipeSinkNeedsMore()`
  - `OpOutput::PipeEven(batch)`
  - `OpOutput::SourcePipeHasMore(batch)`
  - `OpOutput::Blocked(resumer)`
  - `OpOutput::PipeYield()`, then later `OpOutput::PipeYieldBack()` on the next re-entry
    (input=nullopt)
  - an error Result
- `PipeOp::Drain()` is called only after the upstream source returned `Finished()`. It must return one of:
  - `OpOutput::SourcePipeHasMore(batch)`
  - `OpOutput::Finished(optional batch)`
  - `OpOutput::Blocked(resumer)`
  - `OpOutput::PipeYield()`, then later `OpOutput::PipeYieldBack()` on the next re-entry
    (input=nullopt)
  - an error Result
- `SinkOp::Sink()` must return one of:
  - `OpOutput::PipeSinkNeedsMore()`
  - `OpOutput::Blocked(resumer)`
  - an error Result
  The sink must not return output batches.

### Frontend/Backend task groups and TaskHint

- `SourceOp::Frontend()` returns a list of task groups the host schedules before running the
  pipelinexe(s) that invoke its `Source()` callback.
- `SinkOp::Frontend()` returns a list of task groups the host schedules strictly after the
  pipelinexe(s) feeding the sink have finished.
- `SourceOp::Backend()` and `SinkOp::Backend()` return an optional task group the host
  typically schedules before running the main stage work, often to initiate or wait on IO
  readiness (for example, synchronous IO reading/dequeueing from a queue, or asynchronous
  IO waiting on callbacks/signals).
- `TaskHint` is the scheduler hint that marks tasks as CPU vs IO.
- A typical orchestration pattern is to use frontend/backend to split work into distinct
  computation stages and use `TaskHint` so a scheduler can route CPU-bound work and IO-bound
  work to different pools.
- A common convention is to treat frontend work as primarily CPU-bound and backend work as
  primarily IO-bound, but Broken Pipeline does not enforce this split; it is defined by the
  host scheduler and operator implementations.
- Examples:
  - A hash join build can be modeled as a `SinkOp` whose `Sink()` ingests the build side and
    whose frontend builds the hash table (often in parallel at the pipeline DOP). Its backend
    can be used to start or wait on IO-heavy work (for example, preparing spill files or
    awaiting IO completion).
  - In a right outer join, a hash join probe operator may need a post-probe scan of the build
    side to output unmatched rows. That scan phase can be represented as a pipe-provided
    `ImplicitSource()`, which becomes a new downstream pipelinexe. The implicit source can
    also expose a frontend for preparatory work such as partitioning the hash table, distinct
    from its per-lane scanning work implemented in `Source()`.
  - File reads/writes and network RPCs are typically IO-bound and are good candidates for
    tasks with `TaskHint::Type::IO`.

## Pipelines and compilation

Pipeline graph type:
- `bp::Pipeline<Traits>` in `include/broken_pipeline/pipeline.h`

A Pipeline contains:
- name
- channels: each channel has a SourceOp pointer and a vector of PipeOp pointers
- a single shared SinkOp pointer

Multiple channels in one pipelinexe:
- A single pipeline stage can have multiple channels feeding the same sink. This is useful
  for union-all-like fan-in operators where either child can produce output into the same
  downstream sink.

Compilation:
- `bp::Compile(pipeline, dop)` in `include/broken_pipeline/pipeline_exec.h` produces
  `bp::PipelineExec<Traits>`.
- Compilation splits a pipeline into an ordered list of Pipelinexes when a pipe returns a
  non-null `PipeOp::ImplicitSource()`.
- Only `PipeOp::ImplicitSource()` creates stage boundaries today. `SinkOp::ImplicitSource()`
  exists for host-level orchestration but is not used by `bp::Compile`. One example is an
  aggregation implemented as a sink that uses `SinkOp::ImplicitSource()` to provide a source
  for emitting group-by results into a subsequent pipeline stage.

Lifetime:
- Pipeline stores raw pointers. Operator instances must outlive the compiled plan and any
  tasks created from it.
- Implicit sources returned from PipeOp::ImplicitSource() are owned by the compiled plan and
  kept alive by each Pipelinexe.
- Implicit sources returned from `SinkOp::ImplicitSource()` are owned by the host orchestration
  that calls it.

## PipeExec: reference small-step runtime

Each Pipelinexe exposes `Pipelinexe::PipeExec().TaskGroup()`, which runs the stage as a
`TaskGroup` with dop task instances.

Semantics:
- Each task instance uses TaskId as ThreadId (0..dop-1).
- Within a task call, the runtime performs bounded work and keeps state internally to
  continue on a later invocation.
- If all unfinished channels are blocked, the task returns `TaskStatus::Blocked(awaiter)`
  built from the channels' resumers.
- If an operator returns `PIPE_YIELD`, the task returns `TaskStatus::Yield()`. On the next
  invocation, the runtime re-enters the yielding operator (input=nullopt) and expects
  `PIPE_YIELD_BACK`.
- If any operator returns an error Result, the plan is cancelled. The call that observes
  the error returns that error; subsequent calls return `TaskStatus::Cancelled()`.

Concurrency requirements:
- A scheduler must not execute the same Task instance concurrently.
- Different TaskIds within the same TaskGroup may run concurrently. Operator implementations
  must be safe under that assumption, especially for shared objects such as the sink.

## Orchestration

PipelineExec only provides building blocks. A host scheduler/executor is responsible for:
- Creating a `TaskContext` with `resumer_factory` and `awaiter_factory`.
- Scheduling frontend/backend task groups (source frontend before pipelinexes, sink frontend
  after pipelinexes, and backends as IO-readiness tasks typically scheduled ahead).
- Executing each Pipelinexe PipeExec task group with the desired scheduling policy.

## Schedule runtime (optional)

`broken_pipeline::schedule` is an optional compiled library that specializes Broken
Pipeline with Apache Arrow and provides schedulers that host projects can use directly.
It is designed for applications that want a production-ready scheduler without building
their own Traits binding or resumer/awaiter primitives.

What it provides:
- Arrow binding (`bp::schedule::Traits`, `bp::schedule::Batch`, `bp::schedule::Status`,
  `bp::schedule::Result<T>`).
- Scheduler primitives: `detail::CallbackResumer`, `detail::ConditionalAwaiter`, and `detail::FutureAwaiter`.
- Scheduler implementations:
  - `AsyncDualPoolScheduler`: Folly-based dual CPU/IO pools; routes work using
    `TaskHint::Type::IO` and honors `TaskStatus::Yield` as an IO handoff point.
  - `NaiveParallelScheduler`: a minimal `std::async` scheduler intended for tests and
    reference usage.

Minimal usage:

```cpp
#include <broken_pipeline/schedule/async_dual_pool_scheduler.h>
#include <broken_pipeline/schedule/traits.h>

bp::schedule::Context ctx{.query_name = "demo"};
bp::schedule::AsyncDualPoolScheduler scheduler(8, 2);

bp::schedule::TaskGroup group = /* from PipelineExec / Pipelinexe */;
auto status = scheduler.ScheduleAndWait(group, &ctx);
```

Build and link:
- Configure with `-DBROKEN_PIPELINE_BUILD_SCHEDULE=ON`.
- Link against `broken_pipeline::schedule` (which pulls in Arrow, Folly, and glog).

## Build

The core library is header-only. The optional `broken_pipeline::schedule` runtime is a compiled
library.

Build the interface target:

```bash
cmake -S . -B build
cmake --build build
```

Build the schedule runtime:

```bash
cmake -S . -B build -DBROKEN_PIPELINE_BUILD_SCHEDULE=ON
cmake --build build
```

Build and run tests (requires Arrow C++ CMake package, Folly, glog, and GoogleTest):

```bash
cmake --preset tests
cmake --build --preset tests
ctest --preset tests
```

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.
