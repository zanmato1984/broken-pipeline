#include <arrow_traits.h>

#include <opl/pipeline_exec.h>

#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace opl {

namespace {

using O = ::opl_arrow::Aliases<int>;
using SmokeTraits = O::Traits;

static_assert(OpenPipelineTraits<SmokeTraits>);

class CountingSource final : public O::SourceOp {
 public:
  CountingSource(std::size_t dop, int n)
      : O::SourceOp("CountingSource"), next_(dop, 1), n_(n) {}

  O::PipelineSource Source() override {
    return [this](const O::TaskContext&, O::ThreadId thread_id) -> O::OpResult {
      auto& next = next_[thread_id];
      if (next <= n_) {
        return O::OpOutput::SourcePipeHasMore(next++);
      }
      return O::OpOutput::Finished();
    };
  }

  std::vector<O::TaskGroup> Frontend() override { return {}; }

  std::optional<O::TaskGroup> Backend() override { return std::nullopt; }

 private:
  std::vector<int> next_;
  int n_;
};

class MultiplyPipe final : public O::PipeOp {
 public:
  explicit MultiplyPipe(int factor) : O::PipeOp("MultiplyPipe"), factor_(factor) {}

  O::PipelinePipe Pipe() override {
    return [this](const O::TaskContext&, O::ThreadId,
                  std::optional<O::Batch> input) -> O::OpResult {
      if (!input.has_value()) {
        return O::OpOutput::PipeSinkNeedsMore();
      }
      return O::OpOutput::PipeEven(*input * factor_);
    };
  }

  O::PipelineDrain Drain() override { return {}; }

  std::unique_ptr<O::SourceOp> ImplicitSource() override { return nullptr; }

 private:
  int factor_;
};

class SumSink final : public O::SinkOp {
 public:
  explicit SumSink(std::size_t dop) : O::SinkOp("SumSink"), sums_(dop, 0) {}

  O::PipelineSink Sink() override {
    return [this](const O::TaskContext&, O::ThreadId thread_id,
                  std::optional<O::Batch> input) -> O::OpResult {
      if (input.has_value()) {
        sums_[thread_id] += *input;
      }
      return O::OpOutput::PipeSinkNeedsMore();
    };
  }

  std::vector<O::TaskGroup> Frontend() override { return {}; }

  std::optional<O::TaskGroup> Backend() override { return std::nullopt; }

  std::unique_ptr<O::SourceOp> ImplicitSource() override { return nullptr; }

  int Total() const {
    int total = 0;
    for (int v : sums_) {
      total += v;
    }
    return total;
  }

 private:
  std::vector<int> sums_;
};

arrow::Status RunTaskGroup(const O::TaskGroup& group, const O::TaskContext& task_ctx) {
  std::vector<bool> done(group.NumTasks(), false);
  std::size_t done_count = 0;

  while (done_count < done.size()) {
    for (O::TaskId task_id = 0; task_id < done.size(); ++task_id) {
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
        return arrow::Status::NotImplemented("Blocked() not used in smoke test");
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
        return arrow::Status::NotImplemented("Blocked() not used in smoke test");
      }
      if (ts.IsFinished() || ts.IsCancelled()) {
        break;
      }
    }
  }

  return arrow::Status::OK();
}

}  // namespace

TEST(OplPipelineExecSmokeTest, RunsSingleStagePipeline) {
  constexpr std::size_t dop = 1;

  CountingSource source(dop, /*n=*/3);
  MultiplyPipe pipe(/*factor=*/2);
  SumSink sink(dop);

  O::Pipeline pipeline("P", {O::PipelineChannel{&source, {&pipe}}}, &sink);

  auto exec = Compile(pipeline, dop);
  ASSERT_EQ(exec.Segments().size(), 1);

  O::TaskContext task_ctx;
  task_ctx.context = nullptr;
  task_ctx.resumer_factory = []() -> O::Result<std::shared_ptr<Resumer>> {
    return arrow::Status::NotImplemented("resumer_factory not used in smoke test");
  };
  task_ctx.awaiter_factory = [](std::vector<std::shared_ptr<Resumer>>)
      -> O::Result<std::shared_ptr<Awaiter>> {
    return arrow::Status::NotImplemented("awaiter_factory not used in smoke test");
  };

  ASSERT_OK(RunTaskGroup(exec.Segments()[0].PipeExec().TaskGroup(), task_ctx));
  ASSERT_EQ(sink.Total(), 12);
}

}  // namespace opl
