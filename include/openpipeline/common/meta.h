#pragma once

#include <string>
#include <utility>

namespace openpipeline {

class Meta {
 public:
  Meta(std::string name = {}, std::string desc = {})
      : name_(std::move(name)), desc_(std::move(desc)) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

 private:
  std::string name_;
  std::string desc_;
};

}  // namespace openpipeline

