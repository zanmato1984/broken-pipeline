#pragma once

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/common/meta.h>
#include <openpipeline/concepts.h>
#include <openpipeline/pipeline/op.h>

namespace openpipeline::pipeline {

template <OpenPipelineTraits Traits>
class LogicalPipeline : public Meta {
 public:
  struct Channel {
    SourceOp<Traits>* source_op;
    std::vector<PipeOp<Traits>*> pipe_ops;
  };

  LogicalPipeline(std::string name, std::vector<Channel> channels, SinkOp<Traits>* sink_op)
      : Meta(std::move(name), Explain(channels, sink_op)),
        channels_(std::move(channels)),
        sink_op_(sink_op) {}

  const std::vector<Channel>& Channels() const noexcept { return channels_; }
  SinkOp<Traits>* Sink() const noexcept { return sink_op_; }

 private:
  static std::string Explain(const std::vector<Channel>& channels, SinkOp<Traits>* sink_op) {
    std::stringstream ss;
    for (std::size_t i = 0; i < channels.size(); ++i) {
      if (i > 0) {
        ss << '\n';
      }
      ss << "Channel" << i << ": " << channels[i].source_op->Name() << " -> ";
      for (std::size_t j = 0; j < channels[i].pipe_ops.size(); ++j) {
        ss << channels[i].pipe_ops[j]->Name() << " -> ";
      }
      ss << sink_op->Name();
    }
    return ss.str();
  }

  std::vector<Channel> channels_;
  SinkOp<Traits>* sink_op_;
};

}  // namespace openpipeline::pipeline

