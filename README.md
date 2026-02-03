# broken-pipeline

Header-only protocol/interfaces extracted from Ara’s execution model.

broken-pipeline is a **C++20**, **traits-based**, **data-structure-agnostic** set of building
blocks for implementing a batch-at-a-time execution engine:

- Operators expose small-step, re-entrant callbacks that a pipeline driver can resume.
- Pipelines are driven as explicit state machines (instead of blocking threads).
- Execution is expressed as `Task` / `TaskGroup`, with `TaskStatus` as the contract to a
  scheduler.

What this project provides:
- Generic `Task` / `TaskGroup` protocol (`Continue/Blocked/Yield/Finished/Cancelled`)
- Generic `Resumer` / `Awaiter` interfaces and factories (scheduler-owned)
- Generic operator interfaces (`SourceOp` / `PipeOp` / `SinkOp`) and `OpOutput`
- `Pipeline`

What this project intentionally does **not** provide:
- No scheduler implementations (sync/async), no thread pools, no futures library dependency
- No concrete operators (no Arrow, no joins/aggregates, etc.)
- No “engine loop” that runs task groups; you plug in your own scheduler/executor

## Requirements

- C++20
- CMake >= 3.20

## The Layered Model (Operator → Pipeline → Task → Scheduler)

broken-pipeline deliberately separates concerns:

All public types live directly in `namespace bp` (no sub-namespaces); headers are
organized as a small set of top-level files:

1) **Operator protocol** (`include/broken_pipeline/operator.h`)
- “How do I transform/consume/produce batches?”
- Exposes a small set of flow-control signals (`OpOutput`).

2) **Pipeline driver** (`include/broken_pipeline/pipeline.h` + `include/broken_pipeline/pipeline_exec.h`)
- “How do I wire Source/Pipe/Sink together and resume them correctly?”
- Encodes “drain after upstream finished”, “resume an operator that has more internal
  output”, “propagate blocked/yield” as a state machine.

3) **Task protocol** (`include/broken_pipeline/task.h`)
- “How do I package execution into schedulable units?”
- Exposes `TaskStatus` (`Continue/Blocked/Yield/Finished/Cancelled`) for scheduler control.

4) **Scheduler (yours)**
- “Where/when do tasks run? How do we wait? How do we handle Yield?”
- broken-pipeline provides the semantic hooks (`Resumer`/`Awaiter`) but does not implement
  any scheduling policy or concurrency runtime.

This architecture is what lets broken-pipeline be both:
- highly generic (no Arrow / no Folly / no concrete data types)
- scheduler-agnostic (sync or async, cooperative or preemptive)

## Key Protocols

### TaskStatus (Task → Scheduler)

Defined in `include/broken_pipeline/task.h`:

- `Continue`: task is still running; scheduler may invoke again.
- `Blocked(awaiter)`: task cannot proceed until awaited condition is resumed.
- `Yield`: task requests yielding before a long synchronous operation.
- `Finished`: task is done successfully.
- `Cancelled`: task is cancelled (often due to sibling error).

### OpOutput (Operator → Pipeline driver)

Defined in `include/broken_pipeline/operator.h`:

The most important distinction:
- `PIPE_SINK_NEEDS_MORE`: operator needs more upstream input.
- `SOURCE_PIPE_HAS_MORE(batch)`: operator produced a batch but still has more internal
  output; pipeline driver should resume the same operator again (usually with `nullopt`).

Other notable signals:
- `BLOCKED(resumer)`: cannot progress (IO/backpressure/resource). A scheduler turns
  resumers into an awaiter and returns `TaskStatus::Blocked(...)`.
- `PIPE_YIELD` / `PIPE_YIELD_BACK`: explicit yield handshake to enable schedulers to
  migrate execution.
- `FINISHED(optional<batch>)`: end-of-stream with optional final batch.

## Usage

broken-pipeline is fully generic via a user-defined `Traits` type (checked by C++20 concepts).

Your `Traits` must define:
- `using Batch = ...;` (movable)
- `using Context = ...;` (any type, can be `std::monostate`)
- `using Status = ...;` (Arrow-style status type)
- `template<class T> using Result = ...;` (Arrow-style result type)

Required surface:
- `Status::OK()` and `status.ok()`
- `result.ok()`, `result.status()`, and `result.ValueOrDie()` (lvalue/const/rvalue)
- `Result<T>(T)` for success and `Result<T>(Status)` for error

broken-pipeline defines `bp::TaskId` and `bp::ThreadId` as `std::size_t`,
so you do not provide id types in your `Traits`.

Then include:

```cpp
#include <broken_pipeline/broken_pipeline.h>
```

## Minimal Traits Example (no dependencies)

Below is a small Arrow-shaped `Status`/`Result<T>` built on `std::variant` just to satisfy
the concept.

```cpp
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

## Operator Skeleton

All operator interfaces live in `namespace bp` (defined in `include/broken_pipeline/operator.h`):
- `SourceOp<Traits>` → `Source()`, `Frontend()`, `Backend()`
- `PipeOp<Traits>` → `Pipe()`, `Drain()`, `ImplicitSource()`
- `SinkOp<Traits>` → `Sink()`, `Frontend()`, `Backend()`, `ImplicitSource()`

Key conventions:
- Operators are re-entrant and may be called repeatedly.
- `ThreadId` is a “lane id”; operators usually keep per-lane state indexed by thread id.
- `Pipe/Sink` take `std::optional<Batch>`:
  - `Batch` means “new upstream input”
  - `nullopt` means “resume internal output / continue after blocked/yield”

## Driving a Pipeline

broken-pipeline ships a small reference "compiler" that splits a `Pipeline` into executable stages, but
the host is still responsible for orchestration and scheduling.

Reference implementations live in:
- `include/broken_pipeline/pipeline_exec.h`: `bp::Compile(pipeline, dop)` compiles a
  `Pipeline` into a single `bp::PipelineExec` (with an ordered list of
  Pipelinexes (`Pipelinexe`, "pipelinexe"))
- `include/broken_pipeline/pipeline_exec.h`: `Pipelinexe` / `PipeExec` small-step runtime for each pipelinexe

## Notes on Lifetime and Threading

- `Pipeline` stores raw pointers to operators; you own the operator lifetimes.
- A scheduler must not execute the same task instance concurrently.
- Different `TaskId` instances within a `TaskGroup` may run concurrently (that is the
  point of `dop`).

## Build

This repo is header-only; the CMake target is an INTERFACE library:

```bash
cmake -S . -B build
cmake --build build
```

## Examples

- `examples/broken_pipeline_arrow`: Apache Arrow based adoption example (requires Arrow C++ CMake package).

## License

Licensed under the Apache License, Version 2.0. See `LICENSE`.
