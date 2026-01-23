# openpipeline

Header-only protocol/interfaces extracted from Ara’s execution model.

What this project provides:
- Generic `Task` / `TaskGroup` protocol (`Continue/Blocked/Yield/Finished/Cancelled`)
- Generic `Resumer` / `Awaiter` interfaces and factories (scheduler-owned)
- Generic operator interfaces (`SourceOp` / `PipeOp` / `SinkOp`) and `OpOutput`
- `LogicalPipeline` + `CompileTaskGroups` (internally splits into physical stages via implicit sources)

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
- `template<class T> static bool IsOk(const Result<T>&);`
- `template<class T> static T& Value(Result<T>&);`
- `template<class T> static const T& Value(const Result<T>&);`
- `template<class T> static T Take(Result<T>&&);`
- `template<class U, class T> static Result<U> ErrorFrom(const Result<T>&);`

Then include:

```cpp
#include <openpipeline/openpipeline.h>
```

To compile a `LogicalPipeline` into runnable `TaskGroup`s:

```cpp
#include <openpipeline/pipeline/compile.h>
```
