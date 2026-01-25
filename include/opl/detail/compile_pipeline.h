#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <opl/concepts.h>
#include <opl/detail/sub_pipeline.h>
#include <opl/pipeline.h>

namespace opl::detail {

/**
 * @brief Internal compiler that splits a `Pipeline` into `SubPipeline` stages.
 *
 * Splitting rule (current):
 * - Only pipe implicit sources (`PipeOp::ImplicitSource()`) create stage boundaries.
 * - When a pipe provides an implicit source, the downstream pipe chain becomes a new
 *   channel rooted at that implicit source in a later sub-pipeline stage.
 *
 * This is intentionally internal because opl’s public surface is protocol-first.
 * Users typically consume it via `CompileTaskGroups`.
 */
template <OpenPipelineTraits Traits>
class PipelineCompiler {
 public:
  explicit PipelineCompiler(const Pipeline<Traits>& pipeline) : pipeline_(pipeline) {}

  SubPipelines<Traits> Compile() && {
    ExtractTopology();
    SortTopology();
    return BuildSubPipelines();
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
                std::pair<std::size_t, typename Pipeline<Traits>::Channel>{id++,
                                                                            std::move(new_channel)});
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
        sub_pipelines_[stage_info.first].first.push_back(
            std::move(implicit_sources_keepalive_[source]));
      }
      sub_pipelines_[stage_info.first].second.push_back(std::move(stage_info.second));
    }
  }

  SubPipelines<Traits> BuildSubPipelines() {
    SubPipelines<Traits> sub_pipelines;

    for (auto& [id, stage_info] : sub_pipelines_) {
      auto sources_keepalive = std::move(stage_info.first);
      auto pipeline_channels = std::move(stage_info.second);

      std::vector<typename SubPipeline<Traits>::Channel> stage_channels(
          pipeline_channels.size());
      std::transform(
          pipeline_channels.begin(), pipeline_channels.end(), stage_channels.begin(),
          [&](auto& channel) -> typename SubPipeline<Traits>::Channel {
            return {channel.source_op, std::move(channel.pipe_ops), pipeline_.Sink()};
          });

      auto name = "SubPipeline" + std::to_string(id) + "(" + pipeline_.Name() + ")";
      sub_pipelines.emplace_back(std::move(name), std::move(stage_channels),
                                 std::move(sources_keepalive));
    }

    return sub_pipelines;
  }

  const Pipeline<Traits>& pipeline_;

  std::unordered_map<SourceOp<Traits>*, std::pair<std::size_t, typename Pipeline<Traits>::Channel>>
      topology_;
  std::vector<SourceOp<Traits>*> sources_keep_order_;
  std::unordered_map<SourceOp<Traits>*, std::unique_ptr<SourceOp<Traits>>>
      implicit_sources_keepalive_;

  std::map<std::size_t,
           std::pair<std::vector<std::unique_ptr<SourceOp<Traits>>>,
                     std::vector<typename Pipeline<Traits>::Channel>>>
      sub_pipelines_;
};

template <OpenPipelineTraits Traits>
SubPipelines<Traits> CompileSubPipelines(const Pipeline<Traits>& pipeline) {
  return PipelineCompiler<Traits>(pipeline).Compile();
}

}  // namespace opl::detail
