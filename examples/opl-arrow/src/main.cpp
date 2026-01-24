#include <cstddef>
#include <iostream>
#include <optional>
#include <vector>

#include <arrow/status.h>

#include <openpipeline/openpipeline.h>
#include <openpipeline/compile.h>

#include "arrow_traits.h"
#include "arrow_op.h"

namespace {

using Traits = opl_arrow::Traits;
template <class T>
using Result = Traits::template Result<T>;
using Status = Traits::Status;

using TaskContext = openpipeline::TaskContext<Traits>;
using TaskGroup = openpipeline::TaskGroup<Traits>;
using TaskGroups = openpipeline::TaskGroups<Traits>;

Status RunTaskGroup(const TaskGroup& group, const TaskContext& task_ctx) {
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

  return Status::OK();
}

Status RunTaskGroups(const TaskGroups& groups, const TaskContext& task_ctx) {
  for (const auto& group : groups) {
    auto st = RunTaskGroup(group, task_ctx);
    if (!st.ok()) {
      return st;
    }
  }
  return Status::OK();
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

  openpipeline::Pipeline<Traits> pipeline(
      "P", {openpipeline::Pipeline<Traits>::Channel{&source, {&pipe}}}, &sink);

  const std::size_t dop = 2;
  auto groups = openpipeline::CompileTaskGroups<Traits>(pipeline, dop);

  opl_arrow::Context context;
  TaskContext task_ctx;
  task_ctx.context = &context;
  task_ctx.resumer_factory = []() -> Result<openpipeline::ResumerPtr> {
    return arrow::Result<openpipeline::ResumerPtr>(
        arrow::Status::NotImplemented("resumer_factory not used in demo"));
  };
  task_ctx.single_awaiter_factory =
      [](openpipeline::ResumerPtr) -> Result<openpipeline::AwaiterPtr> {
    return arrow::Result<openpipeline::AwaiterPtr>(
        arrow::Status::NotImplemented("single_awaiter_factory not used in demo"));
  };
  task_ctx.any_awaiter_factory =
      [](openpipeline::Resumers) -> Result<openpipeline::AwaiterPtr> {
    return arrow::Result<openpipeline::AwaiterPtr>(
        arrow::Status::NotImplemented("any_awaiter_factory not used in demo"));
  };
  task_ctx.all_awaiter_factory =
      [](openpipeline::Resumers) -> Result<openpipeline::AwaiterPtr> {
    return arrow::Result<openpipeline::AwaiterPtr>(
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
