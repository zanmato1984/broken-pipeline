#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/common/meta.h>
#include <openpipeline/concepts.h>
#include <openpipeline/pipeline/op.h>

namespace openpipeline::pipeline {

template <OpenPipelineTraits Traits>
class PhysicalPipeline : public Meta {
 public:
  struct Channel {
    SourceOp<Traits>* source_op;
    std::vector<PipeOp<Traits>*> pipe_ops;
    SinkOp<Traits>* sink_op;
  };

  PhysicalPipeline(std::string name, std::vector<Channel> channels,
                   std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources)
      : Meta(std::move(name), Explain(channels)),
        channels_(std::move(channels)),
        implicit_sources_(std::move(implicit_sources)) {}

  const std::vector<Channel>& Channels() const noexcept { return channels_; }
  const std::vector<std::unique_ptr<SourceOp<Traits>>>& ImplicitSources() const noexcept {
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

  std::vector<Channel> channels_;
  std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_;
};

template <OpenPipelineTraits Traits>
using PhysicalPipelines = std::vector<PhysicalPipeline<Traits>>;

}  // namespace openpipeline::pipeline

