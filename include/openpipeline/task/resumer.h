#pragma once

#include <memory>
#include <vector>

namespace openpipeline::task {

class Resumer {
 public:
  virtual ~Resumer() = default;
  virtual void Resume() = 0;
  virtual bool IsResumed() const = 0;
};

using ResumerPtr = std::shared_ptr<Resumer>;
using Resumers = std::vector<ResumerPtr>;

}  // namespace openpipeline::task

