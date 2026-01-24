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
 * return `Traits::Result<T>` and rely on a small adapter surface (see `OpenPipelineTraits`)
 * to inspect and move values/errors.
 */
template <class Traits, class T>
using Result = typename Traits::template Result<T>;

/**
 * @brief Status type (success-or-error) used by openpipeline.
 *
 * This is just `Traits::Result<void>`.
 */
template <class Traits>
using Status = Result<Traits, void>;

/**
 * @brief Concept defining the required "Traits" surface for openpipeline.
 *
 * You provide a `Traits` type to parametrize openpipeline over:
 * - The batch type (`Batch`)
 * - An optional query-level context type (`Context`)
 * - Your error/result transport (`Result<T>`)
 *
 * The required static functions are intentionally minimal and are used only to:
 * - construct success values (`Ok`)
 * - test success vs error (`IsOk`)
 * - access/take the success value (`Value` / `Take`)
 * - rewrap an error into another `Result<U>` (`ErrorFrom`)
 *
 * This is "Option B": openpipeline does not define its own Status/Result type.
 *
 * Typical mapping to `std::expected<T, E>`:
 * - `Result<T>` = `std::expected<T, E>`
 * - `Ok(value)` returns `std::expected<T, E>(std::in_place, value)`
 * - `IsOk(r)` -> `r.has_value()`
 * - `Value(r)` -> `r.value()`
 * - `Take(std::move(r))` -> `std::move(r).value()`
 * - `ErrorFrom<U>(r)` -> `std::unexpected(r.error())`
 */
template <class Traits>
concept OpenPipelineTraits =
    requires {
      typename Traits::Batch;
      typename Traits::Context;
      typename Result<Traits, void>;
    } &&
    std::movable<typename Traits::Batch> &&
    std::is_object_v<typename Traits::Context> &&
    requires {
      { Traits::Ok() } -> std::same_as<Status<Traits>>;
      { Traits::Ok(std::declval<int>()) } -> std::same_as<Result<Traits, int>>;
      { Traits::IsOk(std::declval<const Result<Traits, int>&>()) } -> std::same_as<bool>;
      { Traits::Value(std::declval<Result<Traits, int>&>()) } -> std::same_as<int&>;
      { Traits::Value(std::declval<const Result<Traits, int>&>()) } ->
          std::same_as<const int&>;
      { Traits::Take(std::declval<Result<Traits, int>&&>()) } -> std::same_as<int>;
      { Traits::template ErrorFrom<int>(std::declval<const Result<Traits, void>&>()) } ->
          std::same_as<Result<Traits, int>>;
      { Traits::template ErrorFrom<void>(std::declval<const Result<Traits, int>&>()) } ->
          std::same_as<Result<Traits, void>>;
    };

}  // namespace openpipeline
