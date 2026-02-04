#include <broken_pipeline_schedule/async_awaiter.h>

#include <cstddef>
#include <future>
#include <mutex>
#include <utility>

namespace bp::schedule {

AsyncAwaiter::AsyncAwaiter(std::size_t num_readies, Resumers resumers,
                           std::shared_ptr<folly::Promise<folly::Unit>> promise,
                           Future future)
    : num_readies_(num_readies),
      resumers_(std::move(resumers)),
      promise_(std::move(promise)),
      future_(std::move(future)) {}

Result<std::shared_ptr<AsyncAwaiter>> AsyncAwaiter::MakeAsyncAwaiter(std::size_t num_readies,
                                                                    Resumers resumers) {
  if (resumers.empty()) {
    return Status::Invalid("AsyncAwaiter: empty resumers");
  }
  if (num_readies == 0) {
    return Status::Invalid("AsyncAwaiter: num_readies must be > 0");
  }

  auto [p, f] = folly::makePromiseContract<folly::Unit>();
  auto promise = std::make_shared<folly::Promise<folly::Unit>>(std::move(p));

  auto awaiter = std::shared_ptr<AsyncAwaiter>(
      new AsyncAwaiter(num_readies, resumers, std::move(promise), std::move(f)));

  for (auto& resumer : resumers) {
    auto casted = std::dynamic_pointer_cast<AsyncResumer>(resumer);
    if (casted == nullptr) {
      return InvalidResumerType("AsyncAwaiter");
    }
    casted->AddCallback([awaiter]() { awaiter->OnResumed(); });
  }

  return awaiter;
}

void AsyncAwaiter::OnResumed() {
  std::shared_ptr<folly::Promise<folly::Unit>> to_set;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    readies_++;
    if (readies_ >= num_readies_ && promise_ != nullptr) {
      to_set = std::exchange(promise_, nullptr);
    }
  }

  if (to_set != nullptr) {
    try {
      to_set->setValue();
    } catch (const std::exception&) {
    }
  }
}

Result<std::shared_ptr<Awaiter>> AsyncAwaiter::MakeSingle(ResumerPtr resumer) {
  Resumers resumers;
  resumers.push_back(std::move(resumer));
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeAsyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> AsyncAwaiter::MakeAny(Resumers resumers) {
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeAsyncAwaiter(1, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

Result<std::shared_ptr<Awaiter>> AsyncAwaiter::MakeAll(Resumers resumers) {
  const auto num_readies = resumers.size();
  ARROW_ASSIGN_OR_RAISE(auto awaiter, MakeAsyncAwaiter(num_readies, std::move(resumers)));
  return std::static_pointer_cast<Awaiter>(std::move(awaiter));
}

}  // namespace bp::schedule
