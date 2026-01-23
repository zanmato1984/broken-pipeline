#pragma once

#include <cstddef>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/pipeline/detail/compile_pipeline.h>
#include <openpipeline/pipeline/detail/pipeline_task.h>
#include <openpipeline/pipeline/logical_pipeline.h>
#include <openpipeline/task/task_group.h>

namespace openpipeline::pipeline {

template <OpenPipelineTraits Traits>
task::TaskGroups<Traits> CompileTaskGroups(const LogicalPipeline<Traits>& logical_pipeline,
                                           std::size_t dop) {
  auto physical_pipelines = detail::CompilePhysicalPipelines<Traits>(logical_pipeline);

  task::TaskGroups<Traits> task_groups;
  task_groups.reserve(physical_pipelines.size() + 1);

  for (auto& physical : physical_pipelines) {
    // Run source frontends for this stage before executing the stage pipeline task.
    for (auto& ch : physical.Channels()) {
      auto fe = ch.source_op->Frontend();
      task_groups.insert(task_groups.end(),
                         std::make_move_iterator(fe.begin()),
                         std::make_move_iterator(fe.end()));
    }

    auto pipeline_sp =
        std::make_shared<detail::PhysicalPipeline<Traits>>(std::move(physical));
    auto pipeline_task =
        std::make_shared<detail::PipelineTask<Traits>>(std::move(pipeline_sp), dop);

    task::Task<Traits> task(
        pipeline_task->Name(), pipeline_task->Desc(),
        [pipeline_task](const task::TaskContext<Traits>& ctx,
                        typename Traits::TaskId task_id) {
          return (*pipeline_task)(ctx, task_id);
        });

    task_groups.emplace_back(pipeline_task->Name(), pipeline_task->Desc(), std::move(task),
                             dop);
  }

  // Run sink frontend(s) once after all physical pipeline stages complete.
  {
    auto fe = logical_pipeline.Sink()->Frontend();
    task_groups.insert(task_groups.end(),
                       std::make_move_iterator(fe.begin()),
                       std::make_move_iterator(fe.end()));
  }

  return task_groups;
}

}  // namespace openpipeline::pipeline
