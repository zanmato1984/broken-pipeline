#include <opl/pipeline_exec.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>
#include <optional>
#include <variant>
#include <vector>

namespace opl {

namespace {

struct SmokeTraits {
  using Batch = int;
  using Context = std::monostate;
  using Status = arrow::Status;

  template <class T>
  using Result = arrow::Result<T>;
};

static_assert(OpenPipelineTraits<SmokeTraits>);

class CountingSource final : public SourceOp<SmokeTraits> {
 public:
  CountingSource(std::size_t dop, int n)
      : SourceOp("CountingSource"), next_(dop, 1), n_(n) {}

  PipelineSource<SmokeTraits> Source() override {
    return [this](const TaskContext<SmokeTraits>&,
                  ThreadId thread_id) -> OpResult<SmokeTraits> {
      auto& next = next_[thread_id];
      if (next <= n_) {
        return OpResult<SmokeTraits>(OpOutput<SmokeTraits>::SourcePipeHasMore(next++));
      }
      return OpResult<SmokeTraits>(OpOutput<SmokeTraits>::Finished());
    };
  }

  std::vector<TaskGroup<SmokeTraits>> Frontend() override { return {}; }

  std::optional<TaskGroup<SmokeTraits>> Backend() override { return std::nullopt; }

 private:
  std::vector<int> next_;
  int n_;
};

class MultiplyPipe final : public PipeOp<SmokeTraits> {
 public:
  explicit MultiplyPipe(int factor) : PipeOp("MultiplyPipe"), factor_(factor) {}

  PipelinePipe<SmokeTraits> Pipe() override {
    return [this](const TaskContext<SmokeTraits>&, ThreadId,
                  std::optional<SmokeTraits::Batch> input) -> OpResult<SmokeTraits> {
      if (!input.has_value()) {
        return OpResult<SmokeTraits>(OpOutput<SmokeTraits>::PipeSinkNeedsMore());
      }
      return OpResult<SmokeTraits>(OpOutput<SmokeTraits>::PipeEven(*input * factor_));
    };
  }

  PipelineDrain<SmokeTraits> Drain() override { return {}; }

  std::unique_ptr<SourceOp<SmokeTraits>> ImplicitSource() override { return nullptr; }

 private:
  int factor_;
};

class SumSink final : public SinkOp<SmokeTraits> {
 public:
  explicit SumSink(std::size_t dop) : SinkOp("SumSink"), sums_(dop, 0) {}

  PipelineSink<SmokeTraits> Sink() override {
    return [this](const TaskContext<SmokeTraits>&, ThreadId thread_id,
                  std::optional<SmokeTraits::Batch> input) -> OpResult<SmokeTraits> {
      if (input.has_value()) {
        sums_[thread_id] += *input;
      }
      return OpResult<SmokeTraits>(OpOutput<SmokeTraits>::PipeSinkNeedsMore());
    };
  }

  std::vector<TaskGroup<SmokeTraits>> Frontend() override { return {}; }

  std::optional<TaskGroup<SmokeTraits>> Backend() override { return std::nullopt; }

  std::unique_ptr<SourceOp<SmokeTraits>> ImplicitSource() override { return nullptr; }

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

arrow::Status RunTaskGroup(const TaskGroup<SmokeTraits>& group,
                           const TaskContext<SmokeTraits>& task_ctx) {
  std::vector<bool> done(group.NumTasks(), false);
  std::size_t done_count = 0;

  while (done_count < done.size()) {
    for (TaskId task_id = 0; task_id < done.size(); ++task_id) {
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

  Pipeline<SmokeTraits> pipeline("P", {Pipeline<SmokeTraits>::Channel{&source, {&pipe}}},
                                 &sink);

  auto exec = Compile(pipeline, dop);
  ASSERT_EQ(exec.Segments().size(), 1);

  TaskContext<SmokeTraits> task_ctx;
  task_ctx.context = nullptr;
  task_ctx.resumer_factory = []() -> SmokeTraits::Result<std::shared_ptr<Resumer>> {
    return arrow::Status::NotImplemented("resumer_factory not used in smoke test");
  };
  task_ctx.awaiter_factory = [](std::vector<std::shared_ptr<Resumer>>)
      -> SmokeTraits::Result<std::shared_ptr<Awaiter>> {
    return arrow::Status::NotImplemented("awaiter_factory not used in smoke test");
  };

  ASSERT_OK(RunTaskGroup(exec.Segments()[0].PipeExec().TaskGroup(), task_ctx));
  ASSERT_EQ(sink.Total(), 12);
}

}  // namespace opl
