# System Arrow Example

This example is a small host project that uses `find_package(Arrow)` to link
against a system-installed Apache Arrow, enables the `broken_pipeline::schedule`
runtime, and runs a tiny pipeline that emits a single row with value 42, passes
it through, and prints it.

## What it shows

- Using a system-installed Apache Arrow via `find_package(Arrow)`.
- Enabling and linking the `broken_pipeline::schedule` runtime.
- Orchestrating a compiled pipeline and running it with the
  `NaiveParallelScheduler`.

## Requirements

- CMake 3.20+
- A C++20 compiler
- Apache Arrow C++ installed with its CMake package available
- Folly and glog available to CMake (system install or vcpkg). The schedule
  runtime links both.

## Build

From this directory:

```bash
cmake -S . -B build \
  -DCMAKE_PREFIX_PATH="/path/to/arrow;/path/to/folly;/path/to/glog"
cmake --build build
```

If Arrow is installed in a custom location, set `Arrow_DIR` or include its
prefix in `CMAKE_PREFIX_PATH`.

## Run

```bash
./build/system-arrow
```

Expected output:

```
Output value: 42
```

## Notes

- The example source lives in `examples/arrow-common/main.cpp` and is shared with
  the bundled Arrow example.
