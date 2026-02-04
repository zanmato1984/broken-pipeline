#include "sync_awaiter.h"

namespace bp_test::schedule {

SyncAwaiter::SyncAwaiter(std::size_t num_readies, Resumers resumers)
    : num_readies_(num_readies), resumers_(std::move(resumers)) {}

Result<std::shared_ptr<SyncAwaiter>> SyncAwaiter::MakeSyncAwaiter(std::size_t num_readies,
                                                                  Resumers resumers) {
  if (resumers.empty()) {
    return Status::Invalid("SyncAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("SyncAwaiter: num_readies must be > 0");
  }

  auto awaiter = std::shared_ptr<SyncAwaiter>(new SyncAwaiter(num_readies, resumers));
  for (auto& resumer : resumers) {
    auto casted = std::dynamic_pointer_cast<SyncResumer>(resumer);
    if (casted == nullptr) {
      return InvalidResumerType("SyncAwaiter");
    }
    casted->AddCallback([awaiter]() {
      std::unique_lock<std::mutex> lock(awaiter->mutex_);
      awaiter->readies_++;
      awaiter->cv_.notify_one();
    });
  }
  return awaiter;
}

void SyncAwaiter::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (readies_ < num_readies_) {
    cv_.wait(lock);
  }
}

Result<std::shared_ptr<Awaiter>> SyncAwaiter::MakeSingle(ResumerPtr resumer) {
  Resumers resumers;
  resumers.push_back(std::move(resumer));
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeSyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> SyncAwaiter::MakeAny(Resumers resumers) {
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeSyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> SyncAwaiter::MakeAll(Resumers resumers) {
  const auto num_readies = resumers.size();
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeSyncAwaiter(num_readies, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

}  // namespace bp_test::schedule
