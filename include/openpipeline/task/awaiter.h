#pragma once

#include <functional>
#include <memory>

#include <openpipeline/concepts.h>
#include <openpipeline/task/resumer.h>

namespace openpipeline::task {

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

}  // namespace openpipeline::task

