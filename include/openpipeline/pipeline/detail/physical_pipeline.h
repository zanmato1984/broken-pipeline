#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/op/op.h>

namespace openpipeline::pipeline::detail {

template <OpenPipelineTraits Traits>
class PhysicalPipeline {
 public:
  struct Channel {
    op::SourceOp<Traits>* source_op;
    std::vector<op::PipeOp<Traits>*> pipe_ops;
    op::SinkOp<Traits>* sink_op;
  };

  PhysicalPipeline(std::string name, std::vector<Channel> channels,
                   std::vector<std::unique_ptr<op::SourceOp<Traits>>> implicit_sources)
      : name_(std::move(name)),
        desc_(Explain(channels)),
        channels_(std::move(channels)),
        implicit_sources_(std::move(implicit_sources)) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  const std::vector<Channel>& Channels() const noexcept { return channels_; }
  const std::vector<std::unique_ptr<op::SourceOp<Traits>>>& ImplicitSources() const noexcept {
    return implicit_sources_;
  }

 private:
  static std::string Explain(const std::vector<Channel>& channels) {
    std::stringstream ss;
    for (std::size_t i = 0; i < channels.size(); ++i) {
      if (i > 0) {
        ss << '\n';
      }
      ss << "Channel" << i << ": " << channels[i].source_op->Name() << " -> ";
      for (std::size_t j = 0; j < channels[i].pipe_ops.size(); ++j) {
        ss << channels[i].pipe_ops[j]->Name() << " -> ";
      }
      ss << channels[i].sink_op->Name();
    }
    return ss.str();
  }

  std::string name_;
  std::string desc_;
  std::vector<Channel> channels_;
  std::vector<std::unique_ptr<op::SourceOp<Traits>>> implicit_sources_;
};

template <OpenPipelineTraits Traits>
using PhysicalPipelines = std::vector<PhysicalPipeline<Traits>>;

}  // namespace openpipeline::pipeline::detail

