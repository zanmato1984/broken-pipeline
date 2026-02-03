#include <cstddef>
#include <iostream>
#include <memory>
#include <vector>

#include <arrow/status.h>

#include <broken_pipeline/broken_pipeline.h>
#include <broken_pipeline/pipeline_exec.h>

#include "arrow_op.h"
#include "arrow_traits.h"

namespace {

std::vector<broken_pipeline_arrow::TaskGroup> CompileTaskGroups(
    const broken_pipeline_arrow::Pipeline& pipeline, std::size_t dop) {
  auto exec = bp::Compile(pipeline, dop);

  std::vector<broken_pipeline_arrow::TaskGroup> task_groups;
  task_groups.reserve(exec.Pipelinexes().size() * 2 + 3);

  for (const auto& pipelinexe : exec.Pipelinexes()) {
    const auto source_execs = pipelinexe.SourceExecs();
    for (const auto& source : source_execs) {
      for (const auto& tg : source.frontend) {
        task_groups.push_back(tg);
      }
    }

    task_groups.push_back(pipelinexe.PipeExec().TaskGroup());
  }

  for (const auto& tg : exec.Sink().frontend) {
    task_groups.push_back(tg);
  }

  for (const auto& pipelinexe : exec.Pipelinexes()) {
    const auto source_execs = pipelinexe.SourceExecs();
    for (const auto& source : source_execs) {
      if (source.backend.has_value()) {
        task_groups.push_back(*source.backend);
      }
    }
  }

  if (exec.Sink().backend.has_value()) {
    task_groups.push_back(*exec.Sink().backend);
  }

  return task_groups;
}

broken_pipeline_arrow::Status RunTaskGroup(const broken_pipeline_arrow::TaskGroup& group,
                                           const broken_pipeline_arrow::TaskContext& task_ctx) {
  std::vector<bool> done(group.NumTasks(), false);
  std::size_t done_count = 0;

  while (done_count < done.size()) {
    for (broken_pipeline_arrow::TaskId task_id = 0; task_id < done.size(); ++task_id) {
      if (done[task_id]) {
        continue;
      }

      auto status_r = group.GetTask()(task_ctx, task_id);
      if (!status_r.ok()) {
        return status_r.status();
      }

      const auto& ts = status_r.ValueOrDie();
      if (ts.IsContinue() || ts.IsYield()) {
        continue;
      }

      if (ts.IsBlocked()) {
        return arrow::Status::NotImplemented("Example scheduler does not support Blocked()");
      }

      if (ts.IsFinished() || ts.IsCancelled()) {
        done[task_id] = true;
        ++done_count;
      }
    }
  }

  if (group.GetContinuation().has_value()) {
    auto cont = *group.GetContinuation();
    for (;;) {
      auto cont_r = cont(task_ctx);
      if (!cont_r.ok()) {
        return cont_r.status();
      }
      const auto& ts = cont_r.ValueOrDie();
      if (ts.IsContinue() || ts.IsYield()) {
        continue;
      }
      if (ts.IsBlocked()) {
        return arrow::Status::NotImplemented("Example scheduler does not support Blocked()");
      }
      if (ts.IsFinished() || ts.IsCancelled()) {
        break;
      }
    }
  }

  return broken_pipeline_arrow::Status::OK();
}

broken_pipeline_arrow::Status RunTaskGroups(
    const std::vector<broken_pipeline_arrow::TaskGroup>& groups,
    const broken_pipeline_arrow::TaskContext& task_ctx) {
  for (const auto& group : groups) {
    auto st = RunTaskGroup(group, task_ctx);
    if (!st.ok()) {
      return st;
    }
  }
  return broken_pipeline_arrow::Status::OK();
}

}  // namespace

int main() {
  constexpr std::size_t dop = 2;

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (int i = 0; i < 3; ++i) {
    auto rb_r =
        broken_pipeline_arrow::MakeInt32Batch(schema, /*start=*/i * 10, /*length=*/5);
    if (!rb_r.ok()) {
      std::cerr << "MakeInt32Batch failed: " << rb_r.status().ToString() << "\n";
      return 1;
    }
    batches.push_back(*rb_r);
  }

  broken_pipeline_arrow::BatchesSource source(std::move(batches));
  broken_pipeline_arrow::PassThroughPipe pipe;
  broken_pipeline_arrow::DelayLastBatchPipe drain_pipe(dop);
  broken_pipeline_arrow::RowCountSink sink;

  broken_pipeline_arrow::Pipeline pipeline(
      "P", {broken_pipeline_arrow::PipelineChannel{&source, {&pipe, &drain_pipe}}},
      &sink);

  auto groups = CompileTaskGroups(pipeline, dop);

  broken_pipeline_arrow::Context context;
  broken_pipeline_arrow::TaskContext task_ctx;
  task_ctx.context = &context;
  task_ctx.resumer_factory =
      []() -> broken_pipeline_arrow::Result<std::shared_ptr<bp::Resumer>> {
    return broken_pipeline_arrow::Result<std::shared_ptr<bp::Resumer>>(
        arrow::Status::NotImplemented("resumer_factory not used in example"));
  };
  task_ctx.awaiter_factory =
      [](std::vector<std::shared_ptr<bp::Resumer>>)
          -> broken_pipeline_arrow::Result<std::shared_ptr<bp::Awaiter>> {
    return broken_pipeline_arrow::Result<std::shared_ptr<bp::Awaiter>>(
        arrow::Status::NotImplemented("awaiter_factory not used in example"));
  };

  auto st = RunTaskGroups(groups, task_ctx);
  if (!st.ok()) {
    std::cerr << "Execution failed: " << st.ToString() << "\n";
    return 1;
  }

  std::cout << "total_rows=" << sink.TotalRows()
            << " source_frontend=" << (source.FrontendFinished() ? "yes" : "no")
            << " source_backend=" << (source.BackendFinished() ? "yes" : "no")
            << " sink_frontend=" << (sink.FrontendFinished() ? "yes" : "no")
            << " sink_backend=" << (sink.BackendFinished() ? "yes" : "no")
            << " drained_batches=" << drain_pipe.DrainedBatches() << "\n";
  return 0;
}
