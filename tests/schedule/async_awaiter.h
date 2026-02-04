#pragma once

#include "async_resumer.h"
#include "scheduler.h"

#include <folly/futures/Future.h>

namespace bp_test::schedule {

class AsyncAwaiter final : public ResumersAwaiter,
                           public std::enable_shared_from_this<AsyncAwaiter> {
 public:
  using Future = folly::SemiFuture<folly::Unit>;

  Future& GetFuture() { return future_; }
  const Resumers& GetResumers() const override { return resumers_; }

  static Result<std::shared_ptr<Awaiter>> MakeSingle(ResumerPtr resumer);
  static Result<std::shared_ptr<Awaiter>> MakeAny(Resumers resumers);
  static Result<std::shared_ptr<Awaiter>> MakeAll(Resumers resumers);

 private:
  AsyncAwaiter(std::size_t num_readies, Resumers resumers,
              std::shared_ptr<folly::Promise<folly::Unit>> promise, Future future);

  static Result<std::shared_ptr<AsyncAwaiter>> MakeAsyncAwaiter(std::size_t num_readies,
                                                                Resumers resumers);

  void OnResumed();

  std::size_t num_readies_;
  Resumers resumers_;

  mutable std::mutex mutex_;
  std::size_t readies_{0};
  std::shared_ptr<folly::Promise<folly::Unit>> promise_;
  Future future_;
};

}  // namespace bp_test::schedule
