# openpipeline

Header-only protocol/interfaces extracted from Ara’s execution model.

openpipeline is a **C++20**, **traits-based**, **data-structure-agnostic** set of building
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
- Optional helper: `CompileTaskGroups` (internally splits into physical stages via implicit sources)

What this project intentionally does **not** provide:
- No scheduler implementations (sync/async), no thread pools, no futures library dependency
- No concrete operators (no Arrow, no joins/aggregates, etc.)
- No “engine loop” that runs task groups; you plug in your own scheduler/executor

## Requirements

- C++20
- CMake >= 3.20

## The Layered Model (Operator → Pipeline → Task → Scheduler)

openpipeline deliberately separates concerns:

All public types live directly in `namespace openpipeline` (no sub-namespaces); headers are
organized by directory:

1) **Operator protocol** (`include/openpipeline/op/*`)
- “How do I transform/consume/produce batches?”
- Exposes a small set of flow-control signals (`OpOutput`).

2) **Pipeline driver** (`include/openpipeline/pipeline/*` + internal `include/openpipeline/pipeline/detail/*`)
- “How do I wire Source/Pipe/Sink together and resume them correctly?”
- Encodes “drain after upstream finished”, “resume an operator that has more internal
  output”, “propagate blocked/yield” as a state machine.

3) **Task protocol** (`include/openpipeline/task/*`)
- “How do I package execution into schedulable units?”
- Exposes `TaskStatus` (`Continue/Blocked/Yield/Finished/Cancelled`) for scheduler control.

4) **Scheduler (yours)**
- “Where/when do tasks run? How do we wait? How do we handle Yield?”
- openpipeline provides the semantic hooks (`Resumer`/`Awaiter`) but does not implement
  any scheduling policy or concurrency runtime.

This architecture is what lets openpipeline be both:
- highly generic (no Arrow / no Folly / no concrete data types)
- scheduler-agnostic (sync or async, cooperative or preemptive)

## Key Protocols

### TaskStatus (Task → Scheduler)

Defined in `include/openpipeline/task/task_status.h`:

- `Continue`: task is still running; scheduler may invoke again.
- `Blocked(awaiter)`: task cannot proceed until awaited condition is resumed.
- `Yield`: task requests yielding before a long synchronous operation.
- `Finished`: task is done successfully.
- `Cancelled`: task is cancelled (often due to sibling error).

### OpOutput (Operator → Pipeline driver)

Defined in `include/openpipeline/op/op_output.h`:

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

`openpipeline` is fully generic via a user-defined `Traits` type (checked by C++20 concepts).

Your `Traits` must define:
- `using Batch = ...;` (movable)
- `using Context = ...;` (any type, can be `std::monostate`)
- `template<class T> using Result = ...;` (your result type)
- `static Result<void> Ok();`
- `template<class T> static Result<T> Ok(T value);`
- `template<class T> static bool IsOk(const Result<T>&);`
- `template<class T> static T& Value(Result<T>&);`
- `template<class T> static const T& Value(const Result<T>&);`
- `template<class T> static T Take(Result<T>&&);`
- `template<class U, class T> static Result<U> ErrorFrom(const Result<T>&);`

openpipeline defines `openpipeline::TaskId` and `openpipeline::ThreadId` as `std::size_t`,
so you do not provide id types in your `Traits`.

Then include:

```cpp
#include <openpipeline/openpipeline.h>
```

To compile a `Pipeline` into runnable `TaskGroup`s (optional helper):

```cpp
#include <openpipeline/pipeline/compile.h>
```

## Minimal Traits Example (no dependencies)

Below is a small “result” implementation built on `std::variant` just to satisfy the
concept. In real code you can map this to `std::expected`-like types.

```cpp
struct Error { std::string msg; };

template<class T>
struct Result { std::variant<T, Error> v; };

template<>
struct Result<void> { std::variant<std::monostate, Error> v; };

struct Traits {
  using Batch = MyBatch;
  using Context = MyQueryContext;

  template<class T>
  using Result = ::Result<T>;

  static Result<void> Ok() { return {std::monostate{}}; }

  template<class T>
  static Result<T> Ok(T value) { return {std::move(value)}; }

  template<class T>
  static bool IsOk(const Result<T>& r) { return std::holds_alternative<T>(r.v); }

  template<class T>
  static T& Value(Result<T>& r) { return std::get<T>(r.v); }

  template<class T>
  static const T& Value(const Result<T>& r) { return std::get<T>(r.v); }

  template<class T>
  static T Take(Result<T>&& r) { return std::move(std::get<T>(r.v)); }

  template<class U, class T>
  static Result<U> ErrorFrom(const Result<T>& r) { return {std::get<Error>(r.v)}; }
};
```

## Operator Skeleton

All operator interfaces live in `namespace openpipeline` (headers under `include/openpipeline/op/*`):
- `SourceOp<Traits>` → `Source()`, `Frontend()`, `Backend()`
- `PipeOp<Traits>` → `Pipe()`, `Drain()`, `ImplicitSource()`
- `SinkOp<Traits>` → `Sink()`, `Frontend()`, `Backend()`, `ImplicitSource()`

Key conventions:
- Operators are re-entrant and may be called repeatedly.
- `ThreadId` is a “lane id”; operators usually keep per-lane state indexed by thread id.
- `Pipe/Sink` take `std::optional<Batch>`:
  - `Batch` means “new upstream input”
  - `nullopt` means “resume internal output / continue after blocked/yield”

## Compiling a Pipeline into TaskGroups

Include:

```cpp
#include <openpipeline/pipeline/compile.h>
```

Then:

```cpp
openpipeline::Pipeline<Traits> pipeline(
  "P",
  { {source0, {pipe0, pipe1}}, {source1, {pipe0, pipe1}} },  // channels
  sink
);

auto groups = openpipeline::CompileTaskGroups<Traits>(pipeline, /*dop=*/8);
```

The returned `groups` are ordered. A typical driver/scheduler would execute them in order
and stop on the first error. openpipeline does not provide this scheduler.

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

- `examples/opl-arrow`: Apache Arrow based adoption example (requires Arrow C++ CMake package).
