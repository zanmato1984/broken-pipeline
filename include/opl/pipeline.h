#pragma once

#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include <opl/concepts.h>
#include <opl/op.h>

namespace opl {

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
 * - A pipeline may be split into multiple sub-pipeline stages if any `PipeOp` returns a
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

  std::string name_;
  std::string desc_;
  std::vector<Channel> channels_;
  SinkOp<Traits>* sink_op_;
};

}  // namespace opl

namespace opl::detail {

/**
 * @brief Internal representation of a sub-pipeline stage.
 *
 * A sub-pipeline is a subset of a pipeline after splitting on
 * `PipeOp::ImplicitSource()` boundaries.
 */
template <OpenPipelineTraits Traits>
class SubPipeline {
 public:
  struct Channel {
    SourceOp<Traits>* source_op;
    std::vector<PipeOp<Traits>*> pipe_ops;
    SinkOp<Traits>* sink_op;
  };

  SubPipeline(std::string name, std::vector<Channel> channels,
              std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources)
      : name_(std::move(name)),
        desc_(Explain(channels)),
        channels_(std::move(channels)),
        implicit_sources_(std::move(implicit_sources)) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

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

  std::string name_;
  std::string desc_;
  std::vector<Channel> channels_;
  std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_;
};

}  // namespace opl::detail

namespace opl {

/**
 * @brief Internal stage representation exposed for host orchestration.
 *
 * A `SubPipeline` is produced by stage splitting (e.g. via `include/opl/detail/*`) and can
 * be driven using `opl::PipelineExec`.
 */
template <OpenPipelineTraits Traits>
using SubPipeline = detail::SubPipeline<Traits>;

}  // namespace opl
