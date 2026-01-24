# opl-arrow

Standalone example project showing how to adopt `openpipeline` in an Apache Arrow based
codebase.

This example demonstrates:
- Declaring a `Traits` type that aliases Arrow's `arrow::Status` and `arrow::Result<T>`.
- Using an Arrow-native batch type (`std::shared_ptr<arrow::RecordBatch>`).
- Compiling a `Pipeline` into `TaskGroup`s via `openpipeline::CompileTaskGroups`.

## Build

This project depends on the Arrow C++ CMake package.

From the `openpipeline` repo root:

```bash
cmake -S examples/opl-arrow -B build/opl-arrow
cmake --build build/opl-arrow
./build/opl-arrow/opl-arrow-demo
```

If Arrow is installed in a non-default prefix, set `CMAKE_PREFIX_PATH`:

```bash
cmake -S examples/opl-arrow -B build/opl-arrow -DCMAKE_PREFIX_PATH=/path/to/arrow
```
