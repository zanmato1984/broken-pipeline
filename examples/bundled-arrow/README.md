# Bundled Arrow Example

This example is a small host project that bundles Apache Arrow via CMake
`FetchContent`, enables the `broken_pipeline::schedule` runtime, and runs a tiny
pipeline that emits a single row with value 42, passes it through, and prints it.

## What it shows

- Fetching Apache Arrow from source ("bundled" dependency).
- Enabling and linking the `broken_pipeline::schedule` runtime.
- Orchestrating a compiled pipeline and running it with the
  `NaiveParallelScheduler`.

## Requirements

- CMake 3.20+
- A C++20 compiler
- Folly and glog available to CMake (system install or vcpkg). The schedule
  runtime links both.

## Build

From this directory:

```bash
cmake -S . -B build \
  -DCMAKE_PREFIX_PATH="/path/to/folly;/path/to/glog"
cmake --build build
```

This example always downloads and builds Arrow in-tree. If you change
`ARROW_VERSION`, delete `build/_deps/arrow-src` and `build/_deps/arrow-build`
to force a rebuild.

## Run

```bash
./build/bundled-arrow
```

Expected output:

```
Output value: 42
```

## Notes

- The example source lives in `examples/arrow-common/main.cpp` and is shared with
  the system Arrow example.
- The example sets `dop=1` so operator state can be stored directly on the
  operator instances. Increase DOP only after making the operators thread-safe.
- Update the Arrow URL/version in `examples/bundled-arrow/CMakeLists.txt` to
  match the release you want to bundle.
