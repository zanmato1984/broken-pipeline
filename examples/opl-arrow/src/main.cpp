#include <cstddef>
#include <iostream>
#include <optional>
#include <vector>

#include <arrow/status.h>

#include <openpipeline/openpipeline.h>
#include <openpipeline/pipeline/compile.h>

#include "arrow_traits.h"
#include "demo_ops.h"

namespace {

using Traits = opl_arrow::Traits;

arrow::Status RunTaskGroup(const openpipeline::task::TaskGroup<Traits>& group,
                           const openpipeline::task::TaskContext<Traits>& task_ctx) {
  std::vector<bool> done(group.NumTasks(), false);
  std::size_t done_count = 0;

  while (done_count < done.size()) {
    for (openpipeline::TaskId task_id = 0; task_id < done.size(); ++task_id) {
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
        return arrow::Status::NotImplemented("Demo scheduler does not support Blocked()");
      }

      if (ts.IsFinished() || ts.IsCancelled()) {
        done[task_id] = true;
        ++done_count;
      }
    }
  }

  auto notify_st = group.NotifyFinish(task_ctx);
  if (!notify_st.ok()) {
    return notify_st;
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
        return arrow::Status::NotImplemented("Demo scheduler does not support Blocked()");
      }
      if (ts.IsFinished() || ts.IsCancelled()) {
        break;
      }
    }
  }

  return arrow::Status::OK();
}

arrow::Status RunTaskGroups(const openpipeline::task::TaskGroups<Traits>& groups,
                            const openpipeline::task::TaskContext<Traits>& task_ctx) {
  for (const auto& group : groups) {
    auto st = RunTaskGroup(group, task_ctx);
    if (!st.ok()) {
      return st;
    }
  }
  return arrow::Status::OK();
}

}  // namespace

int main() {
  auto schema = arrow::schema({arrow::field("x", arrow::int32())});

  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (int i = 0; i < 3; ++i) {
    auto rb_r = opl_arrow::MakeInt32Batch(schema, /*start=*/i * 10, /*length=*/5);
    if (!rb_r.ok()) {
      std::cerr << "MakeInt32Batch failed: " << rb_r.status().ToString() << "\n";
      return 1;
    }
    batches.push_back(*rb_r);
  }

  opl_arrow::BatchesSource source(std::move(batches));
  opl_arrow::PassThroughPipe pipe;
  opl_arrow::RowCountSink sink;

  openpipeline::pipeline::LogicalPipeline<Traits> logical(
      "P", {openpipeline::pipeline::LogicalPipeline<Traits>::Channel{&source, {&pipe}}},
      &sink);

  const std::size_t dop = 2;
  auto groups = openpipeline::pipeline::CompileTaskGroups<Traits>(logical, dop);

  opl_arrow::Context context;
  openpipeline::task::TaskContext<Traits> task_ctx;
  task_ctx.context = &context;
  task_ctx.resumer_factory = []() -> Traits::Result<openpipeline::task::ResumerPtr> {
    return arrow::Result<openpipeline::task::ResumerPtr>(
        arrow::Status::NotImplemented("resumer_factory not used in demo"));
  };
  task_ctx.single_awaiter_factory =
      [](openpipeline::task::ResumerPtr) -> Traits::Result<openpipeline::task::AwaiterPtr> {
    return arrow::Result<openpipeline::task::AwaiterPtr>(
        arrow::Status::NotImplemented("single_awaiter_factory not used in demo"));
  };
  task_ctx.any_awaiter_factory =
      [](openpipeline::task::Resumers) -> Traits::Result<openpipeline::task::AwaiterPtr> {
    return arrow::Result<openpipeline::task::AwaiterPtr>(
        arrow::Status::NotImplemented("any_awaiter_factory not used in demo"));
  };
  task_ctx.all_awaiter_factory =
      [](openpipeline::task::Resumers) -> Traits::Result<openpipeline::task::AwaiterPtr> {
    return arrow::Result<openpipeline::task::AwaiterPtr>(
        arrow::Status::NotImplemented("all_awaiter_factory not used in demo"));
  };

  auto st = RunTaskGroups(groups, task_ctx);
  if (!st.ok()) {
    std::cerr << "Execution failed: " << st.ToString() << "\n";
    return 1;
  }

  std::cout << "total_rows=" << sink.TotalRows() << "\n";
  return 0;
}
