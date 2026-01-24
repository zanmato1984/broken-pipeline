#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/pipeline/detail/physical_pipeline.h>
#include <openpipeline/pipeline/logical_pipeline.h>

namespace openpipeline::pipeline::detail {

/**
 * @brief Internal compiler that splits a `LogicalPipeline` into `PhysicalPipeline` stages.
 *
 * Splitting rule (current):
 * - Only pipe implicit sources (`PipeOp::ImplicitSource()`) create stage boundaries.
 * - When a pipe provides an implicit source, the downstream pipe chain becomes a new
 *   channel rooted at that implicit source in a later physical stage.
 *
 * This is intentionally internal because openpipeline’s public surface is protocol-first.
 * Users typically consume it via `pipeline::CompileTaskGroups`.
 */
template <OpenPipelineTraits Traits>
class PipelineCompiler {
 public:
  explicit PipelineCompiler(const pipeline::LogicalPipeline<Traits>& logical_pipeline)
      : logical_pipeline_(logical_pipeline) {}

  PhysicalPipelines<Traits> Compile() && {
    ExtractTopology();
    SortTopology();
    return BuildPhysicalPipelines();
  }

 private:
  void ExtractTopology() {
    std::unordered_map<op::PipeOp<Traits>*, op::SourceOp<Traits>*> pipe_source_map;

    for (auto& channel : logical_pipeline_.Channels()) {
      std::size_t id = 0;
      topology_.emplace(channel.source_op,
                        std::pair<std::size_t, typename pipeline::LogicalPipeline<Traits>::Channel>{
                            id++, channel});
      sources_keep_order_.push_back(channel.source_op);

      for (std::size_t i = 0; i < channel.pipe_ops.size(); ++i) {
        auto* pipe = channel.pipe_ops[i];

        if (pipe_source_map.count(pipe) == 0) {
          if (auto implicit_source_up = pipe->ImplicitSource(); implicit_source_up) {
            auto* implicit_source = implicit_source_up.get();
            pipe_source_map.emplace(pipe, implicit_source);

            typename pipeline::LogicalPipeline<Traits>::Channel new_channel{
                implicit_source,
                std::vector<op::PipeOp<Traits>*>(channel.pipe_ops.begin() + i + 1,
                                                 channel.pipe_ops.end())};

            topology_.emplace(
                implicit_source,
                std::pair<std::size_t, typename pipeline::LogicalPipeline<Traits>::Channel>{
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
      auto& physical_info = topology_[source];
      if (implicit_sources_keepalive_.count(source) > 0) {
        physical_pipelines_[physical_info.first].first.push_back(
            std::move(implicit_sources_keepalive_[source]));
      }
      physical_pipelines_[physical_info.first].second.push_back(
          std::move(physical_info.second));
    }
  }

  PhysicalPipelines<Traits> BuildPhysicalPipelines() {
    PhysicalPipelines<Traits> physical_pipelines;

    for (auto& [id, physical_info] : physical_pipelines_) {
      auto sources_keepalive = std::move(physical_info.first);
      auto logical_channels = std::move(physical_info.second);

      std::vector<typename PhysicalPipeline<Traits>::Channel> physical_channels(
          logical_channels.size());
      std::transform(
          logical_channels.begin(), logical_channels.end(), physical_channels.begin(),
          [&](auto& channel) -> typename PhysicalPipeline<Traits>::Channel {
            return {channel.source_op, std::move(channel.pipe_ops), logical_pipeline_.Sink()};
          });

      auto name = "PhysicalPipeline" + std::to_string(id) + "(" + logical_pipeline_.Name() +
                  ")";
      physical_pipelines.emplace_back(std::move(name), std::move(physical_channels),
                                      std::move(sources_keepalive));
    }

    return physical_pipelines;
  }

  const pipeline::LogicalPipeline<Traits>& logical_pipeline_;

  std::unordered_map<op::SourceOp<Traits>*,
                     std::pair<std::size_t, typename pipeline::LogicalPipeline<Traits>::Channel>>
      topology_;
  std::vector<op::SourceOp<Traits>*> sources_keep_order_;
  std::unordered_map<op::SourceOp<Traits>*, std::unique_ptr<op::SourceOp<Traits>>>
      implicit_sources_keepalive_;

  std::map<std::size_t,
           std::pair<std::vector<std::unique_ptr<op::SourceOp<Traits>>>,
                     std::vector<typename pipeline::LogicalPipeline<Traits>::Channel>>>
      physical_pipelines_;
};

template <OpenPipelineTraits Traits>
PhysicalPipelines<Traits> CompilePhysicalPipelines(
    const pipeline::LogicalPipeline<Traits>& logical_pipeline) {
  return PipelineCompiler<Traits>(logical_pipeline).Compile();
}

}  // namespace openpipeline::pipeline::detail
