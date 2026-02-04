#include <broken_pipeline_schedule/scheduler.h>

namespace bp::schedule {

Status InvalidAwaiterType(const char* scheduler_name) {
  return Status::Invalid(std::string(scheduler_name) + ": unexpected awaiter type");
}

Status InvalidResumerType(const char* scheduler_name) {
  return Status::Invalid(std::string(scheduler_name) + ": unexpected resumer type");
}

void AutoResumeBlocked(const std::shared_ptr<bp::Awaiter>& awaiter) {
  auto* resumers_awaiter = dynamic_cast<ResumersAwaiter*>(awaiter.get());
  if (resumers_awaiter == nullptr) {
    return;
  }
  for (const auto& resumer : resumers_awaiter->GetResumers()) {
    if (resumer) {
      resumer->Resume();
    }
  }
}

}  // namespace bp::schedule
