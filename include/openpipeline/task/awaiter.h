#pragma once

#include <functional>
#include <memory>

#include <openpipeline/concepts.h>
#include <openpipeline/task/resumer.h>

namespace openpipeline {

/**
 * @brief A scheduler-owned wait handle for one or more resumers.
 *
 * `Awaiter` is intentionally opaque to openpipeline core. The scheduler decides how
 * to block (or suspend) until a resumer (or group of resumers) is resumed.
 *
 * For example, a synchronous scheduler may implement an awaiter using condition
 * variables, while an async scheduler may implement it using futures/coroutines.
 *
 * openpipeline only stores `AwaiterPtr` inside `TaskStatus::Blocked(...)`.
 */
class Awaiter {
 public:
  virtual ~Awaiter() = default;
};

using AwaiterPtr = std::shared_ptr<Awaiter>;

template <OpenPipelineTraits Traits>
using ResumerFactory = std::function<Result<Traits, ResumerPtr>()>;

template <OpenPipelineTraits Traits>
using SingleAwaiterFactory = std::function<Result<Traits, AwaiterPtr>(ResumerPtr)>;

template <OpenPipelineTraits Traits>
using AnyAwaiterFactory = std::function<Result<Traits, AwaiterPtr>(Resumers)>;

template <OpenPipelineTraits Traits>
using AllAwaiterFactory = std::function<Result<Traits, AwaiterPtr>(Resumers)>;

}  // namespace openpipeline
