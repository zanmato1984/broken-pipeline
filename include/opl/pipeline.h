#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <opl/concepts.h>
#include <opl/op.h>

namespace opl {

template <OpenPipelineTraits Traits>
class PipelineExec;

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

  std::vector<PipelineExec<Traits>> Compile(std::size_t dop) const;

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

#include <opl/pipeline_exec.h>

namespace opl {

/**
 * @brief Internal compiler that splits a `Pipeline` into `PipelineExec` stages.
 *
 * Splitting rule (current):
 * - Only pipe implicit sources (`PipeOp::ImplicitSource()`) create stage boundaries.
 * - When a pipe provides an implicit source, the downstream pipe chain becomes a new
 *   channel rooted at that implicit source in a later stage.
 */
template <OpenPipelineTraits Traits>
class PipelineCompiler {
 public:
  PipelineCompiler(const Pipeline<Traits>& pipeline, std::size_t dop)
      : pipeline_(pipeline), dop_(dop) {}

  std::vector<PipelineExec<Traits>> Compile() && {
    ExtractTopology();
    SortTopology();
    return BuildPipelineExecs();
  }

 private:
  void ExtractTopology() {
    std::unordered_map<PipeOp<Traits>*, SourceOp<Traits>*> pipe_source_map;

    for (auto& channel : pipeline_.Channels()) {
      std::size_t id = 0;
      topology_.emplace(channel.source_op,
                        std::pair<std::size_t, typename Pipeline<Traits>::Channel>{id++,
                                                                                   channel});
      sources_keep_order_.push_back(channel.source_op);

      for (std::size_t i = 0; i < channel.pipe_ops.size(); ++i) {
        auto* pipe = channel.pipe_ops[i];

        if (pipe_source_map.count(pipe) == 0) {
          if (auto implicit_source_up = pipe->ImplicitSource(); implicit_source_up) {
            auto* implicit_source = implicit_source_up.get();
            pipe_source_map.emplace(pipe, implicit_source);

            typename Pipeline<Traits>::Channel new_channel{
                implicit_source,
                std::vector<PipeOp<Traits>*>(channel.pipe_ops.begin() + i + 1,
                                             channel.pipe_ops.end())};

            topology_.emplace(
                implicit_source,
                std::pair<std::size_t, typename Pipeline<Traits>::Channel>{
                    id++, std::move(new_channel)});
            sources_keep_order_.push_back(implicit_source);
            implicit_sources_keepalive_.emplace(implicit_source,
                                                std::move(implicit_source_up));
          }
        } else {
          auto* implicit_source = pipe_source_map[pipe];
          if (topology_[implicit_source].first < id) {
            topology_[implicit_source].first = id++;
          }
        }
      }
    }
  }

  void SortTopology() {
    for (auto* source : sources_keep_order_) {
      auto& stage_info = topology_[source];
      if (implicit_sources_keepalive_.count(source) > 0) {
        stages_[stage_info.first].first.push_back(
            std::move(implicit_sources_keepalive_[source]));
      }
      stages_[stage_info.first].second.push_back(std::move(stage_info.second));
    }
  }

  std::vector<PipelineExec<Traits>> BuildPipelineExecs() {
    std::vector<PipelineExec<Traits>> pipeline_execs;

    if (stages_.empty()) {
      return pipeline_execs;
    }

    const std::size_t last_stage_id = stages_.rbegin()->first;
    pipeline_execs.reserve(stages_.size());

    for (auto& [id, stage_info] : stages_) {
      auto sources_keepalive = std::move(stage_info.first);
      auto pipeline_channels = std::move(stage_info.second);

      std::vector<typename PipelineExec<Traits>::Channel> stage_channels(
          pipeline_channels.size());
      std::transform(
          pipeline_channels.begin(),
          pipeline_channels.end(),
          stage_channels.begin(),
          [&](auto& channel) -> typename PipelineExec<Traits>::Channel {
            return {channel.source_op, std::move(channel.pipe_ops), pipeline_.Sink()};
          });

      auto name = "PipelineExec" + std::to_string(id) + "(" + pipeline_.Name() + ")";
      const bool include_sink_task_groups = (id == last_stage_id);
      pipeline_execs.emplace_back(std::move(name),
                                  std::move(stage_channels),
                                  std::move(sources_keepalive),
                                  dop_,
                                  include_sink_task_groups);
    }

    return pipeline_execs;
  }

  const Pipeline<Traits>& pipeline_;
  std::size_t dop_;

  std::unordered_map<SourceOp<Traits>*, std::pair<std::size_t, typename Pipeline<Traits>::Channel>>
      topology_;
  std::vector<SourceOp<Traits>*> sources_keep_order_;
  std::unordered_map<SourceOp<Traits>*, std::unique_ptr<SourceOp<Traits>>>
      implicit_sources_keepalive_;

  std::map<std::size_t,
           std::pair<std::vector<std::unique_ptr<SourceOp<Traits>>>,
                     std::vector<typename Pipeline<Traits>::Channel>>>
      stages_;
};

template <OpenPipelineTraits Traits>
std::vector<PipelineExec<Traits>> CompileSubPipelines(const Pipeline<Traits>& pipeline,
                                                      std::size_t dop) {
  return PipelineCompiler<Traits>(pipeline, dop).Compile();
}

template <OpenPipelineTraits Traits>
std::vector<PipelineExec<Traits>> Pipeline<Traits>::Compile(std::size_t dop) const {
  return PipelineCompiler<Traits>(*this, dop).Compile();
}

}  // namespace opl
