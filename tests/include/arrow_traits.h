#pragma once

/**
 * @file arrow_traits.h
 *
 * @brief Reusable `opl` Traits implementation backed by Apache Arrow, for unit tests.
 *
 * opl does not define its own Status/Result type. Instead, all opl APIs are parameterized
 * by `Traits::Status` and `Traits::Result<T>`.
 *
 * In this test Traits:
 * - `Status` maps to `arrow::Status`
 * - `Result<T>` maps to `arrow::Result<T>`
 * - `Batch` is user-defined (template parameter)
 */

#include <memory>
#include <variant>

#include <arrow/result.h>
#include <arrow/status.h>

#include <opl/opl.h>

namespace opl_test {

template <class BatchT, class ContextT = std::monostate>
struct ArrowTraits {
  using Batch = BatchT;
  using Context = ContextT;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

}  // namespace opl_test
