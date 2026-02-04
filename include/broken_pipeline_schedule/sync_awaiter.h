#pragma once

#include "scheduler.h"
#include "sync_resumer.h"

#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

namespace bp::schedule {

class SyncAwaiter final : public ResumersAwaiter,
                          public std::enable_shared_from_this<SyncAwaiter> {
 public:
  void Wait();
  const Resumers& GetResumers() const override { return resumers_; }

  static Result<std::shared_ptr<Awaiter>> MakeSingle(ResumerPtr resumer);
  static Result<std::shared_ptr<Awaiter>> MakeAny(Resumers resumers);
  static Result<std::shared_ptr<Awaiter>> MakeAll(Resumers resumers);

 private:
  SyncAwaiter(std::size_t num_readies, Resumers resumers);

  static Result<std::shared_ptr<SyncAwaiter>> MakeSyncAwaiter(std::size_t num_readies,
                                                              Resumers resumers);

  std::size_t num_readies_;
  Resumers resumers_;

  std::mutex mutex_;
  std::condition_variable cv_;
  std::size_t readies_{0};
};

}  // namespace bp::schedule
