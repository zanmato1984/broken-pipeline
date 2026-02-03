// Copyright 2026 Rossi Sun
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <cstddef>
#include <iostream>
#include <memory>
#include <vector>

#include <arrow/status.h>
#include <broken_pipeline/broken_pipeline.h>

#include "arrow_op.h"
#include "arrow_traits.h"

namespace bp_arrow {
namespace {

std::vector<TaskGroup> CompileTaskGroups(const Pipeline& pipeline, std::size_t dop) {
  auto exec = Compile(pipeline, dop);

  std::vector<TaskGroup> task_groups;
  task_groups.reserve(exec.Pipelinexes().size() * 3 + 3);

  // A typical orchestration schedules backend (IO-readiness) tasks ahead, then runs the
  // streaming pipelinexes, then runs sink frontend (post-stream) tasks.
  if (exec.Sink().backend.has_value()) {
    task_groups.push_back(*exec.Sink().backend);
  }

  for (const auto& pipelinexe : exec.Pipelinexes()) {
    const auto source_execs = pipelinexe.SourceExecs();

    for (const auto& source : source_execs) {
      if (source.backend.has_value()) {
        task_groups.push_back(*source.backend);
      }
    }

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

  return task_groups;
}

Status RunTaskGroup(const TaskGroup& group, const TaskContext& task_ctx) {
  std::vector<bool> done(group.NumTasks(), false);
  std::size_t done_count = 0;

  while (done_count < done.size()) {
    for (TaskId task_id = 0; task_id < done.size(); ++task_id) {
      if (done[task_id]) {
        continue;
      }

      auto status_r = group.Task()(task_ctx, task_id);
      if (!status_r.ok()) {
        return status_r.status();
      }

      const auto& ts = status_r.ValueOrDie();
      if (ts.IsContinue() || ts.IsYield()) {
        continue;
      }

      if (ts.IsBlocked()) {
        return Status::NotImplemented("Example scheduler does not support Blocked()");
      }

      if (ts.IsFinished() || ts.IsCancelled()) {
        done[task_id] = true;
        ++done_count;
      }
    }
  }

  if (group.Continuation().has_value()) {
    auto cont = *group.Continuation();
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
        return Status::NotImplemented("Example scheduler does not support Blocked()");
      }
      if (ts.IsFinished() || ts.IsCancelled()) {
        break;
      }
    }
  }

  return Status::OK();
}

Status RunTaskGroups(const std::vector<TaskGroup>& groups, const TaskContext& task_ctx) {
  for (const auto& group : groups) {
    auto st = RunTaskGroup(group, task_ctx);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
}

}  // namespace
}  // namespace bp_arrow

int main() {
  using namespace bp_arrow;

  constexpr std::size_t dop = 2;

  auto schema = arrow::schema({arrow::field("x", arrow::int32())});

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (int i = 0; i < 3; ++i) {
    auto rb_r = MakeInt32Batch(schema, /*start=*/i * 10, /*length=*/5);
    if (!rb_r.ok()) {
      std::cerr << "MakeInt32Batch failed: " << rb_r.status().ToString() << "\n";
      return 1;
    }
    batches.push_back(*rb_r);
  }

  BatchesSource source(std::move(batches));
  PassThroughPipe pipe;
  DelayLastBatchPipe drain_pipe(dop);
  RowCountSink sink;

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe, &drain_pipe}}}, &sink);

  auto groups = CompileTaskGroups(pipeline, dop);

  Context context;
  TaskContext task_ctx;
  task_ctx.context = &context;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return Result<std::shared_ptr<Resumer>>(
        Status::NotImplemented("resumer_factory not used in example"));
  };
  task_ctx.awaiter_factory =
      [](std::vector<std::shared_ptr<Resumer>>) -> Result<std::shared_ptr<Awaiter>> {
    return Result<std::shared_ptr<Awaiter>>(
        Status::NotImplemented("awaiter_factory not used in example"));
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
