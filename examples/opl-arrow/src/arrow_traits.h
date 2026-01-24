#pragma once

/**
 * @file arrow_traits.h
 *
 * @brief Example `openpipeline` Traits implementation backed by Apache Arrow.
 *
 * openpipeline does not define its own Status/Result type. Instead, all openpipeline APIs
 * are parameterized by `Traits::Status` and `Traits::Result<T>`.
 *
 * In this example:
 * - `Status` maps to `arrow::Status`
 * - `Result<T>` maps to `arrow::Result<T>`
 * - `Batch` maps to `std::shared_ptr<arrow::RecordBatch>`
 */

#include <memory>

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>

namespace opl_arrow {

struct Context {
  const char* query_name = "opl-arrow-demo";
};

struct Traits {
  using Batch = std::shared_ptr<arrow::RecordBatch>;
  using Context = opl_arrow::Context;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

}  // namespace opl_arrow
