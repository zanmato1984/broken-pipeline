#pragma once

#include <openpipeline/concepts.h>
#include <openpipeline/task/awaiter.h>

namespace openpipeline::task {

template <OpenPipelineTraits Traits>
struct TaskContext {
  const typename Traits::QueryContext* query_ctx = nullptr;
  ResumerFactory<Traits> resumer_factory;
  SingleAwaiterFactory<Traits> single_awaiter_factory;
  AnyAwaiterFactory<Traits> any_awaiter_factory;
  AllAwaiterFactory<Traits> all_awaiter_factory;
};

}  // namespace openpipeline::task

