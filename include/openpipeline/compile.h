#pragma once

#include <cstddef>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/detail/compile_pipeline.h>
#include <openpipeline/detail/pipeline_task.h>
#include <openpipeline/pipeline.h>
#include <openpipeline/task.h>

namespace openpipeline {

/**
 * @brief Compile a `Pipeline` into an ordered list of `TaskGroup`s.
 *
 * This is an optional helper that provides a *generic pipeline runtime*:
 * - It internally splits the pipeline into one or more *physical* stages using
 *   `PipeOp::ImplicitSource()` (implemented in internal `detail/` headers).
 * - Each physical stage is wrapped into a `PipelineTask`, which is a
 *   state machine driving `Source/Pipe/Drain/Sink` step-by-step and mapping operator
 *   signals (`OpOutput`) into task signals (`TaskStatus`).
 *
 * Output ordering:
 * - For each physical stage:
 *   1) append all `SourceOp::Frontend()` task groups for that stage's sources
 *   2) append one `TaskGroup` that runs the stage `PipelineTask` with parallelism = `dop`
 * - After all physical stages, append `SinkOp::Frontend()` task groups once.
 *
 * What this helper does *not* do:
 * - It does not execute anything; you must provide a scheduler to run the returned task
 *   groups.
 * - It does not attempt to parallelize stages or schedule a stage DAG; it simply returns
 *   a linear list whose order preserves the implicit-source stage dependencies.
 *
 * @param pipeline A pipeline referencing user-owned operator instances.
 * @param dop Parallelism for the generated stage `TaskGroup`s.
 */
template <OpenPipelineTraits Traits>
TaskGroups<Traits> CompileTaskGroups(const Pipeline<Traits>& pipeline, std::size_t dop) {
  auto physical_pipelines = CompilePhysicalPipelines<Traits>(pipeline);

  TaskGroups<Traits> task_groups;
  task_groups.reserve(physical_pipelines.size() + 1);

  for (auto& physical : physical_pipelines) {
    // Run source frontends for this stage before executing the stage pipeline task.
    for (auto& ch : physical.Channels()) {
      auto fe = ch.source_op->Frontend();
      task_groups.insert(task_groups.end(),
                         std::make_move_iterator(fe.begin()),
                         std::make_move_iterator(fe.end()));
    }

    auto pipeline_sp = std::make_shared<PhysicalPipeline<Traits>>(std::move(physical));
    auto pipeline_task =
        std::make_shared<PipelineTask<Traits>>(std::move(pipeline_sp), dop);

    Task<Traits> task(
        pipeline_task->Name(), pipeline_task->Desc(),
        [pipeline_task](const TaskContext<Traits>& ctx, TaskId task_id) {
          return (*pipeline_task)(ctx, task_id);
        });

    task_groups.emplace_back(pipeline_task->Name(), pipeline_task->Desc(), std::move(task),
                             dop);
  }

  // Run sink frontend(s) once after all physical pipeline stages complete.
  {
    auto fe = pipeline.Sink()->Frontend();
    task_groups.insert(task_groups.end(),
                       std::make_move_iterator(fe.begin()),
                       std::make_move_iterator(fe.end()));
  }

  return task_groups;
}

}  // namespace openpipeline
