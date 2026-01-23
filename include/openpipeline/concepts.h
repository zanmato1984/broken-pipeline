#pragma once

#include <concepts>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace openpipeline {

template <class Traits, class T>
using Result = typename Traits::template Result<T>;

template <class Traits>
using Status = Result<Traits, void>;

template <class Traits>
concept OpenPipelineTraits =
    requires {
      typename Traits::TaskId;
      typename Traits::ThreadId;
      typename Traits::Batch;
      typename Traits::QueryContext;
      typename Result<Traits, void>;
    } &&
    std::unsigned_integral<typename Traits::TaskId> &&
    std::unsigned_integral<typename Traits::ThreadId> &&
    std::movable<typename Traits::Batch> &&
    std::is_object_v<typename Traits::QueryContext> &&
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
