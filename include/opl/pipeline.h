#pragma once

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
      : name_(std::move(name)), channels_(std::move(channels)), sink_op_(sink_op) {}

  const std::string& Name() const noexcept { return name_; }

  const std::vector<Channel>& Channels() const noexcept { return channels_; }
  SinkOp<Traits>* Sink() const noexcept { return sink_op_; }

 private:
  std::string name_;
  std::vector<Channel> channels_;
  SinkOp<Traits>* sink_op_;
};

}  // namespace opl
