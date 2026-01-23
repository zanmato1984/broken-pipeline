# openpipeline

Header-only protocol/interfaces extracted from Ara’s execution model.

What this project provides:
- Generic `Task` / `TaskGroup` protocol (`Continue/Blocked/Yield/Finished/Cancelled`)
- Generic `Resumer` / `Awaiter` interfaces and factories (scheduler-owned)
- Generic pipeline operator interfaces (`SourceOp` / `PipeOp` / `SinkOp`) and `OpOutput`
- Generic `LogicalPipeline` / `PhysicalPipeline` and `CompilePipeline` (implicit-source splitting)

What this project intentionally does **not** provide:
- No scheduler implementations (sync/async), no thread pools, no futures library dependency
- No concrete operators (no Arrow, no joins/aggregates, etc.)

## Requirements

- C++20
- CMake >= 3.20

## Usage

`openpipeline` is fully generic via a user-defined `Traits` type (checked by C++20 concepts).

Your `Traits` must define:
- `using TaskId = ...;` (unsigned integral)
- `using ThreadId = ...;` (unsigned integral)
- `using Batch = ...;` (movable)
- `using QueryContext = ...;` (any type, can be `std::monostate`)
- `template<class T> using Result = ...;` (your result type)
- `static Result<void> Ok();`
- `template<class T> static Result<T> Ok(T value);`

Then include:

```cpp
#include <openpipeline/openpipeline.h>
```

