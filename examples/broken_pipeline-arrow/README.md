# broken_pipeline-arrow

Standalone example project showing how to adopt `broken_pipeline` in an Apache Arrow based
codebase.

This example demonstrates:
- Declaring a `Traits` type that aliases Arrow's `arrow::Status` and `arrow::Result<T>`.
- Using an Arrow-native batch type (`std::shared_ptr<arrow::RecordBatch>`).
- Splitting a `Pipeline` into stages and building `TaskGroup`s via `broken_pipeline/pipeline_exec.h`.

## Build

This project depends on the Arrow C++ CMake package.

From the repo root:

```bash
cmake -S examples/broken_pipeline-arrow -B build/broken_pipeline-arrow
cmake --build build/broken_pipeline-arrow
./build/broken_pipeline-arrow/broken_pipeline-arrow-demo
```

If Arrow is installed in a non-default prefix, set `CMAKE_PREFIX_PATH`:

```bash
cmake -S examples/broken_pipeline-arrow -B build/broken_pipeline-arrow -DCMAKE_PREFIX_PATH=/path/to/arrow
```
