#include <cstddef>
#include <iostream>
#include <memory>
#include <optional>
#include <vector>

#include <arrow/status.h>

#include <opl/opl.h>
#include <opl/compile.h>

#include "arrow_traits.h"
#include "arrow_op.h"

namespace {
using opl::CompileTaskGroups;

opl_arrow::Status RunTaskGroup(const opl_arrow::TaskGroup& group,
                              const opl_arrow::TaskContext& task_ctx) {
  std::vector<bool> done(group.NumTasks(), false);
  std::size_t done_count = 0;

  while (done_count < done.size()) {
    for (opl_arrow::TaskId task_id = 0; task_id < done.size(); ++task_id) {
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

  return opl_arrow::Status::OK();
}

opl_arrow::Status RunTaskGroups(const opl_arrow::TaskGroups& groups,
                               const opl_arrow::TaskContext& task_ctx) {
  for (const auto& group : groups) {
    auto st = RunTaskGroup(group, task_ctx);
    if (!st.ok()) {
      return st;
    }
  }
  return opl_arrow::Status::OK();
}

}  // namespace

int main() {
  constexpr std::size_t dop = 2;

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
  opl_arrow::DelayLastBatchPipe drain_pipe(dop);
  opl_arrow::RowCountSink sink;

  opl_arrow::Pipeline pipeline("P",
                               {opl_arrow::PipelineChannel{&source, {&pipe, &drain_pipe}}},
                               &sink);

  auto groups = CompileTaskGroups(pipeline, dop);

  opl_arrow::Context context;
  opl_arrow::TaskContext task_ctx;
  task_ctx.context = &context;
  task_ctx.resumer_factory = []() -> opl_arrow::Result<std::shared_ptr<opl::Resumer>> {
    return opl_arrow::Result<std::shared_ptr<opl::Resumer>>(
        arrow::Status::NotImplemented("resumer_factory not used in demo"));
  };
  task_ctx.single_awaiter_factory =
      [](std::shared_ptr<opl::Resumer>) -> opl_arrow::Result<std::shared_ptr<opl::Awaiter>> {
    return opl_arrow::Result<std::shared_ptr<opl::Awaiter>>(
        arrow::Status::NotImplemented("single_awaiter_factory not used in demo"));
  };
  task_ctx.any_awaiter_factory =
      [](std::vector<std::shared_ptr<opl::Resumer>>)
          -> opl_arrow::Result<std::shared_ptr<opl::Awaiter>> {
    return opl_arrow::Result<std::shared_ptr<opl::Awaiter>>(
        arrow::Status::NotImplemented("any_awaiter_factory not used in demo"));
  };
  task_ctx.all_awaiter_factory =
      [](std::vector<std::shared_ptr<opl::Resumer>>)
          -> opl_arrow::Result<std::shared_ptr<opl::Awaiter>> {
    return opl_arrow::Result<std::shared_ptr<opl::Awaiter>>(
        arrow::Status::NotImplemented("all_awaiter_factory not used in demo"));
  };

  auto st = RunTaskGroups(groups, task_ctx);
  if (!st.ok()) {
    std::cerr << "Execution failed: " << st.ToString() << "\n";
    return 1;
  }

  if (auto be = source.Backend(); be.has_value()) {
    auto be_st = RunTaskGroup(*be, task_ctx);
    if (!be_st.ok()) {
      std::cerr << "Source backend failed: " << be_st.ToString() << "\n";
      return 1;
    }
  }
  if (auto be = sink.Backend(); be.has_value()) {
    auto be_st = RunTaskGroup(*be, task_ctx);
    if (!be_st.ok()) {
      std::cerr << "Sink backend failed: " << be_st.ToString() << "\n";
      return 1;
    }
  }

  std::cout << "total_rows=" << sink.TotalRows() << " source_frontend="
            << (source.FrontendFinished() ? "yes" : "no") << " source_backend="
            << (source.BackendFinished() ? "yes" : "no") << " sink_frontend="
            << (sink.FrontendFinished() ? "yes" : "no") << " sink_backend="
            << (sink.BackendFinished() ? "yes" : "no") << " drained_batches="
            << drain_pipe.DrainedBatches() << "\n";
  return 0;
}
