#pragma once

#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace openpipeline {

/**
 * @brief Task instance id within a `TaskGroup`.
 *
 * openpipeline intentionally keeps ids simple and uniform: task instances are indexed
 * 0..N-1 within their group.
 */
using TaskId = std::size_t;

/**
 * @brief Logical execution lane id.
 *
 * Many operator implementations keep per-lane state indexed by `ThreadId`. In the
 * reference runtime (`PipelineTask`), a task instance typically uses
 * `TaskId` as its `ThreadId`.
 */
using ThreadId = std::size_t;

/**
 * @brief Alias helper for `Traits::Result<T>`.
 *
 * openpipeline never assumes a particular error type or transport. Instead, all APIs
 * return `Traits::Result<T>` and rely on an Arrow-like result surface:
 * - `result.ok()`
 * - `result.status()`
 * - `result.ValueOrDie()`
 */
template <class Traits, class T>
using Result = typename Traits::template Result<T>;

/**
 * @brief Status type (success-or-error) used by openpipeline.
 *
 * This is `Traits::Status` (Arrow-style, separate from `Result<T>`).
 */
template <class Traits>
using Status = typename Traits::Status;

/**
 * @brief Concept defining the required "Traits" surface for openpipeline.
 *
 * You provide a `Traits` type to parametrize openpipeline over:
 * - The batch type (`Batch`)
 * - An optional query-level context type (`Context`)
 * - Your error/result transport (`Status` + `Result<T>`)
 *
 * openpipeline expects an Arrow-like API (zero-overhead when using Arrow directly):
 *
 * - `Traits::Status`:
 *   - `static Status OK()`
 *   - `bool ok() const`
 * - `Traits::Result<T>`:
 *   - `bool ok() const`
 *   - `Status status() const`
 *   - `T& ValueOrDie() &`
 *   - `const T& ValueOrDie() const &`
 *   - `T ValueOrDie() &&`
 *   - constructible from `T` (success)
 *   - constructible from `Status` (error)
 *
 * This is still "Option B": openpipeline does not define its own Status/Result type.
 *
 * Typical mapping to Apache Arrow:
 * - `Status` = `arrow::Status`
 * - `Result<T>` = `arrow::Result<T>`
 *
 * For other transports (e.g. `std::expected`), provide a thin wrapper type that exposes
 * an equivalent surface (`OK()/ok()/status()/ValueOrDie()`).
 */
template <class Traits>
concept OpenPipelineTraits =
    requires {
      typename Traits::Batch;
      typename Traits::Context;
      typename Traits::Status;
      typename Result<Traits, int>;
    } &&
    std::movable<typename Traits::Batch> &&
    std::is_object_v<typename Traits::Context> &&
    requires {
      { Traits::Status::OK() } -> std::same_as<typename Traits::Status>;
    } &&
    requires(const typename Traits::Status& status) {
      { status.ok() } -> std::same_as<bool>;
    } &&
    std::constructible_from<Result<Traits, int>, int> &&
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

}  // namespace openpipeline
