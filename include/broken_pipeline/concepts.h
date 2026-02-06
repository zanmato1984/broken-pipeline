// Copyright 2026 Rossi Sun
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <concepts>
#include <utility>

#include <broken_pipeline/result.h>

namespace bp {

/// @brief Concept defining the required "Traits" surface (PipelineBreaker) for Broken Pipeline.
///
/// You provide a `Traits` type to parametrize Broken Pipeline over:
/// - The batch type (`Batch`)
/// - Your error/result transport (`Status` + `Result<T>`)
///
/// Query-level context is carried separately as a type-erased pointer in
/// `bp::TaskContext<Traits>`.
///
/// Broken Pipeline expects an Arrow-like API (zero-overhead when using Arrow directly):
///
/// - `Traits::Status`:
///   - `static Status OK()`
///   - `bool ok() const`
/// - `Traits::Result<T>`:
///   - `bool ok() const`
///   - `Status status() const`
///   - `T& ValueOrDie() &`
///   - `const T& ValueOrDie() const &`
///   - `T ValueOrDie() &&`
///   - constructible from `T` (success)
///   - constructible from `Status` (error)
///
/// Broken Pipeline does not define its own Status/Result type.
///
/// Typical mapping to Apache Arrow:
/// - `Status` = `arrow::Status`
/// - `Result<T>` = `arrow::Result<T>`
///
/// For other transports (e.g. `std::expected`), provide a thin wrapper type that exposes
/// an equivalent surface (`OK()/ok()/status()/ValueOrDie()`).
template <class T>
concept PipelineBreaker =
    requires {
      typename T::Batch;
      typename T::Status;
      typename Result<T, int>;
    } && std::movable<typename T::Batch> &&
    requires {
      { T::Status::OK() } -> std::same_as<typename T::Status>;
    } &&
    requires(const typename T::Status& status) {
      { status.ok() } -> std::same_as<bool>;
    } && std::constructible_from<Result<T, int>, int> &&
    std::constructible_from<Result<T, int>, typename T::Status> &&
    requires(Result<T, int> result, const Result<T, int>& cresult) {
      { result.ok() } -> std::same_as<bool>;
      { cresult.ok() } -> std::same_as<bool>;
      { result.status() } -> std::convertible_to<typename T::Status>;
      { cresult.status() } -> std::convertible_to<typename T::Status>;
      { result.ValueOrDie() } -> std::same_as<int&>;
      { cresult.ValueOrDie() } -> std::same_as<const int&>;
      { std::move(result).ValueOrDie() } -> std::convertible_to<int>;
    };

}  // namespace bp
