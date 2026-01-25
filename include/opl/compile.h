#pragma once

#include <cstddef>
#include <iterator>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <opl/concepts.h>
#include <opl/detail/compile_pipeline.h>
#include <opl/detail/pipeline_exec.h>
#include <opl/pipeline.h>
#include <opl/task.h>

namespace opl {

/**
 * @brief Compile a `Pipeline` into an ordered list of `TaskGroup`s.
 *
 * This is an optional helper that provides a *generic pipeline runtime*:
 * - It internally splits the pipeline into one or more sub-pipeline stages using
 *   `PipeOp::ImplicitSource()` (implemented in internal `detail/` headers).
 * - Each sub-pipeline stage is wrapped into a `detail::PipelineExec`, which is a
 *   state machine driving `Source/Pipe/Drain/Sink` step-by-step and mapping operator
 *   signals (`OpOutput`) into task signals (`TaskStatus`).
 *
 * Output ordering:
 * - For each sub-pipeline stage:
 *   1) append all `SourceOp::Frontend()` task groups for that stage's sources
 *   2) append one `TaskGroup` that runs the stage `detail::PipelineExec` with parallelism =
 *      `dop`
 * - After all sub-pipeline stages, append `SinkOp::Frontend()` task groups once.
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
  auto sub_pipelines = detail::CompileSubPipelines<Traits>(pipeline);

  TaskGroups<Traits> task_groups;
  task_groups.reserve(sub_pipelines.size() + 1);

  for (auto& sub : sub_pipelines) {
    // Run source frontends for this stage before executing the stage pipeline task.
    for (auto& ch : sub.Channels()) {
      auto fe = ch.source_op->Frontend();
      task_groups.insert(task_groups.end(),
                         std::make_move_iterator(fe.begin()),
                         std::make_move_iterator(fe.end()));
    }

    auto pipeline_sp = std::make_shared<detail::SubPipeline<Traits>>(std::move(sub));
    auto pipeline_exec =
        std::make_shared<detail::PipelineExec<Traits>>(std::move(pipeline_sp), dop);

    Task<Traits> task(
        pipeline_exec->Name(), pipeline_exec->Desc(),
        [pipeline_exec](const TaskContext<Traits>& ctx, TaskId task_id) {
          return (*pipeline_exec)(ctx, task_id);
        });

    task_groups.emplace_back(pipeline_exec->Name(), pipeline_exec->Desc(), std::move(task),
                             dop);
  }

  // Run sink frontend(s) once after all sub-pipeline stages complete.
  {
    auto fe = pipeline.Sink()->Frontend();
    task_groups.insert(task_groups.end(),
                       std::make_move_iterator(fe.begin()),
                       std::make_move_iterator(fe.end()));
  }

  return task_groups;
}

}  // namespace opl
