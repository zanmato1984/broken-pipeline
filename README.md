# Broken Pipeline

Broken Pipeline is a header-only C++20 library that defines protocols for building
batch-at-a-time execution pipelines that can be driven without blocking threads. It is
traits-based: you provide the batch type and your status/result transport.

This repository contains:
- Core headers under `include/broken_pipeline/`
- Unit tests under `tests/` (require Apache Arrow and GoogleTest)
- An Arrow adoption example under `examples/broken_pipeline_arrow/`

## Goals

- Provide minimal interfaces between operators, a pipeline driver, and a scheduler.
- Make operator execution small-step and resumable.
- Avoid coupling to any particular data structure library or async runtime.

## Non-goals

- Scheduler, thread pool, or async runtime implementation
- Concrete operators
- A ready-to-run engine loop that orchestrates task groups

## Public API and layout

All public symbols are in namespace bp (no sub-namespaces).

Headers:
- `include/broken_pipeline/broken_pipeline.h` (umbrella header)
- `include/broken_pipeline/concepts.h`
- `include/broken_pipeline/task.h`
- `include/broken_pipeline/operator.h`
- `include/broken_pipeline/pipeline.h`
- `include/broken_pipeline/pipeline_exec.h`

CMake target:
- `broken_pipeline` (INTERFACE)

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
- Implicit source: a SourceOp created by a pipe to start a downstream stage
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
- The scheduler is responsible for calling `Resumer::Resume()` from an external event and
  rescheduling the blocked task instance.

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
- input is nullopt: resume the operator (after has-more, after blocked, or after yield)

OpOutput codes:
- `PIPE_SINK_NEEDS_MORE`: the operator consumed input and needs more upstream input.
- `PIPE_EVEN(batch)`: the operator produced one output batch and does not require immediate resumption.
- `SOURCE_PIPE_HAS_MORE(batch)`: the operator produced one output batch and must be resumed with nullopt before pulling new input.
- `BLOCKED(resumer)`: the operator cannot make progress until resumer is resumed.
- `PIPE_YIELD`: yield request from the operator.
- `PIPE_YIELD_BACK`: resume acknowledgement from a previously yielding operator.
- `FINISHED(optional batch)`: end-of-stream; may carry a final output batch.
- `CANCELLED`: cancellation marker used by the reference runtime.

Contracts for the reference driver in `include/broken_pipeline/pipeline_exec.h`:
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
  - `OpOutput::PipeYield()`, then later `OpOutput::PipeYieldBack()` when resumed
  - an error Result
- `PipeOp::Drain()` is called only after the upstream source returned `Finished()`. It must return one of:
  - `OpOutput::SourcePipeHasMore(batch)`
  - `OpOutput::Finished(optional batch)`
  - `OpOutput::Blocked(resumer)`
  - `OpOutput::PipeYield()`, then later `OpOutput::PipeYieldBack()` when resumed
  - an error Result
- `SinkOp::Sink()` must return one of:
  - `OpOutput::PipeSinkNeedsMore()`
  - `OpOutput::Blocked(resumer)`
  - an error Result
  The sink must not return output batches.

## Pipelines and compilation

Pipeline graph type:
- `bp::Pipeline<Traits>` in `include/broken_pipeline/pipeline.h`

A Pipeline contains:
- name
- channels: each channel has a SourceOp pointer and a vector of PipeOp pointers
- a single shared SinkOp pointer

Compilation:
- `bp::Compile(pipeline, dop)` in `include/broken_pipeline/pipeline_exec.h` produces
  `bp::PipelineExec<Traits>`.
- Compilation splits a pipeline into an ordered list of Pipelinexes when a pipe returns a
  non-null `PipeOp::ImplicitSource()`.
- Only `PipeOp::ImplicitSource()` creates stage boundaries today. `SinkOp::ImplicitSource()`
  exists for host-level orchestration but is not used by `bp::Compile`.

Lifetime:
- Pipeline stores raw pointers. Operator instances must outlive the compiled plan and any
  tasks created from it.
- Implicit sources returned from PipeOp::ImplicitSource() are owned by the compiled plan and
  kept alive by each Pipelinexe.

## PipeExec: reference small-step runtime

Each Pipelinexe exposes `Pipelinexe::PipeExec().TaskGroup()`, which runs the stage as a
`TaskGroup` with dop task instances.

Semantics:
- Each task instance uses TaskId as ThreadId (0..dop-1).
- Within a task call, the driver performs bounded work and keeps state internally to
  resume later.
- If all unfinished channels are blocked, the task returns `TaskStatus::Blocked(awaiter)`
  built from the channels' resumers.
- If an operator returns `PIPE_YIELD`, the task returns `TaskStatus::Yield()`. On the next
  invocation, the driver resumes the yielding operator with nullopt and expects
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
- Choosing an ordering for `SourceOp` and `SinkOp` Frontend/Backend task groups.
- Executing each Pipelinexe PipeExec task group with the desired scheduling policy.

The Arrow example in `examples/broken_pipeline_arrow` demonstrates one possible single-threaded
driver and shows how to map Traits to Arrow's Status and Result types.

## Build

This repo is header-only.

Build the interface target:

```bash
cmake -S . -B build
cmake --build build
```

Build and run tests (requires Arrow and GoogleTest):

```bash
cmake -S . -B build -DBROKEN_PIPELINE_BUILD_TESTS=ON -DBROKEN_PIPELINE_ARROW_PROVIDER=system
cmake --build build
ctest --test-dir build
```

`BROKEN_PIPELINE_ARROW_PROVIDER`:
- system: use an installed Arrow CMake package (Arrow, ArrowCompute, ArrowTesting)
- bundled: fetch and build Arrow 22.0.0 as part of the build

## Examples

- `examples/broken_pipeline_arrow`: Apache Arrow adoption example (requires Arrow C++ CMake package)

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.
