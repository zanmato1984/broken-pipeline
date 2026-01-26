#pragma once

#include <cstddef>
#include <string>
#include <utility>
#include <vector>

#include <opl/concepts.h>

namespace opl {

template <OpenPipelineTraits Traits>
class SourceOp;

template <OpenPipelineTraits Traits>
class PipeOp;

template <OpenPipelineTraits Traits>
class SinkOp;

/**
 * @brief A pipeline graph: (one or more channels) -> (shared sink).
 *
 * A pipeline contains:
 * - `Channel`: a `SourceOp` plus a linear chain of `PipeOp`s
 * - A single shared `SinkOp`
 *
 * Notes:
 * - The pipeline stores raw pointers to operators. Operator lifetime is owned by you and
 *   must outlive any compilation/execution that uses the pipeline.
 * - A pipeline may be split into multiple stages if any `PipeOp` returns a
 *   non-null `ImplicitSource()`.
 */
template <OpenPipelineTraits Traits>
class Pipeline {
 public:
  struct Channel {
    SourceOp<Traits>* source_op;
    std::vector<PipeOp<Traits>*> pipe_ops;
  };

  Pipeline(std::string name, std::vector<Channel> channels, SinkOp<Traits>* sink_op)
      : name_(std::move(name)),
        desc_(Explain(channels, sink_op)),
        channels_(std::move(channels)),
        sink_op_(sink_op) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  const std::vector<Channel>& Channels() const noexcept { return channels_; }
 SinkOp<Traits>* Sink() const noexcept { return sink_op_; }

 private:
  static std::string Explain(const std::vector<Channel>& channels, SinkOp<Traits>* sink_op) {
    std::string out;
    for (std::size_t i = 0; i < channels.size(); ++i) {
      if (i > 0) {
        out.push_back('\n');
      }
      out += "Channel";
      out += std::to_string(i);
      out += ": ";
      out += channels[i].source_op->Name();
      out += " -> ";
      for (std::size_t j = 0; j < channels[i].pipe_ops.size(); ++j) {
        out += channels[i].pipe_ops[j]->Name();
        out += " -> ";
      }
      out += sink_op->Name();
    }
    return out;
  }

  std::string name_;
  std::string desc_;
  std::vector<Channel> channels_;
  SinkOp<Traits>* sink_op_;
};

}  // namespace opl
