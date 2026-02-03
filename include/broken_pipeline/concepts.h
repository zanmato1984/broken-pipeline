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
#include <cstddef>
#include <type_traits>
#include <utility>

namespace bp {

/// @brief Task instance id within a `TaskGroup`.
///
/// broken_pipeline intentionally keeps ids simple and uniform: task instances are indexed
/// 0..N-1 within their group.
using TaskId = std::size_t;

/// @brief Execution lane id.
///
/// Many operator implementations keep per-lane state indexed by `ThreadId`. In the
/// reference runtime (`PipeExec`), a task instance typically uses
/// `TaskId` as its `ThreadId`.
using ThreadId = std::size_t;

/// @brief Alias helper for `Traits::Result<T>`.
///
/// broken_pipeline never assumes a particular error type or transport. Instead, all APIs
/// return `Traits::Result<T>` and rely on an Arrow-like result surface:
/// - `result.ok()`
/// - `result.status()`
/// - `result.ValueOrDie()`
template <class Traits, class T>
using Result = typename Traits::template Result<T>;

/// @brief Status type (success-or-error) used by broken_pipeline.
///
/// This is `Traits::Status` (Arrow-style, separate from `Result<T>`).
template <class Traits>
using Status = typename Traits::Status;

/// @brief Concept defining the required "Traits" surface for broken_pipeline.
///
/// You provide a `Traits` type to parametrize broken_pipeline over:
/// - The batch type (`Batch`)
/// - An optional query-level context type (`Context`)
/// - Your error/result transport (`Status` + `Result<T>`)
///
/// broken_pipeline expects an Arrow-like API (zero-overhead when using Arrow directly):
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
/// This is still "Option B": broken_pipeline does not define its own Status/Result type.
///
/// Typical mapping to Apache Arrow:
/// - `Status` = `arrow::Status`
/// - `Result<T>` = `arrow::Result<T>`
///
/// For other transports (e.g. `std::expected`), provide a thin wrapper type that exposes
/// an equivalent surface (`OK()/ok()/status()/ValueOrDie()`).
template <class Traits>
concept BrokenPipelineTraits =
    requires {
      typename Traits::Batch;
      typename Traits::Context;
      typename Traits::Status;
      typename Result<Traits, int>;
    } && std::movable<typename Traits::Batch> &&
    std::is_object_v<typename Traits::Context> &&
    requires {
      { Traits::Status::OK() } -> std::same_as<typename Traits::Status>;
    } &&
    requires(const typename Traits::Status& status) {
      { status.ok() } -> std::same_as<bool>;
    } && std::constructible_from<Result<Traits, int>, int> &&
    std::constructible_from<Result<Traits, int>, typename Traits::Status> &&
    requires(Result<Traits, int> result, const Result<Traits, int>& cresult) {
      { result.ok() } -> std::same_as<bool>;
      { cresult.ok() } -> std::same_as<bool>;
      { result.status() } -> std::convertible_to<typename Traits::Status>;
      { cresult.status() } -> std::convertible_to<typename Traits::Status>;
      { result.ValueOrDie() } -> std::same_as<int&>;
      { cresult.ValueOrDie() } -> std::same_as<const int&>;
      { std::move(result).ValueOrDie() } -> std::convertible_to<int>;
    };

}  // namespace bp
