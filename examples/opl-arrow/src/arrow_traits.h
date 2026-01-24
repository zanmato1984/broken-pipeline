#pragma once

/**
 * @file arrow_traits.h
 *
 * @brief Example `openpipeline` Traits implementation backed by Apache Arrow.
 *
 * This shows "Option B" result handling: openpipeline does not define its own Status/Result
 * type. Instead, all openpipeline APIs are parameterized by `Traits::Result<T>` and use
 * a small adapter surface (`Ok/IsOk/Value/Take/ErrorFrom`).
 *
 * In this example:
 * - `Result<T>` maps to `arrow::Result<T>`
 * - `Result<void>` maps to `arrow::Status`
 * - `Batch` maps to `std::shared_ptr<arrow::RecordBatch>`
 */

#include <type_traits>
#include <utility>

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

  template <class T>
  struct ResultT {
    using type = arrow::Result<T>;
  };

  template <>
  struct ResultT<void> {
    using type = arrow::Status;
  };

  template <class T>
  using Result = typename ResultT<T>::type;

  static Result<void> Ok() { return arrow::Status::OK(); }

  template <class T>
  static Result<T> Ok(T value) {
    return arrow::Result<T>(std::move(value));
  }

  template <class T>
  static bool IsOk(const Result<T>& r) {
    return r.ok();
  }

  template <class T>
  static T& Value(Result<T>& r) {
    return r.ValueOrDie();
  }

  template <class T>
  static const T& Value(const Result<T>& r) {
    return r.ValueOrDie();
  }

  template <class T>
  static T Take(Result<T>&& r) {
    return std::move(r).ValueOrDie();
  }

  template <class T>
  static arrow::Status StatusOf(const arrow::Result<T>& r) {
    return r.status();
  }

  static arrow::Status StatusOf(const arrow::Status& s) { return s; }

  template <class U, class T>
  static Result<U> ErrorFrom(const Result<T>& r) {
    auto st = StatusOf(r);
    if constexpr (std::is_void_v<U>) {
      return st;
    } else {
      return arrow::Result<U>(st);
    }
  }
};

}  // namespace opl_arrow
