#include "arrow_traits.h"

#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace broken_pipeline_test {

namespace {

class TestResumer final : public Resumer {
 public:
  void Resume() override { resumed_.store(true); }
  bool IsResumed() const override { return resumed_.load(); }

 private:
  std::atomic_bool resumed_{false};
};

class TestAwaiter final : public Awaiter {
 public:
  explicit TestAwaiter(std::vector<std::shared_ptr<Resumer>> resumers)
      : resumers_(std::move(resumers)) {}

  const std::vector<std::shared_ptr<Resumer>>& Resumers() const { return resumers_; }

 private:
  std::vector<std::shared_ptr<Resumer>> resumers_;
};

struct Trace {
  std::string op;
  std::string method;
  std::optional<Batch> input;
  std::string output;

  bool operator==(const Trace& other) const {
    return op == other.op && method == other.method && input == other.input &&
           output == other.output;
  }

  friend void PrintTo(const Trace& trace, std::ostream* os) {
    *os << trace.op << "::" << trace.method << "(";
    if (trace.input.has_value()) {
      *os << *trace.input;
    } else {
      *os << "null";
    }
    *os << ") -> " << trace.output;
  }
};

struct Step {
  enum class Kind {
    OUTPUT,
    BLOCKED,
    ERROR,
  };

  Kind kind = Kind::OUTPUT;
  std::optional<std::optional<Batch>> expected_input;
  std::optional<OpOutput> output;
  std::optional<Status> error;
};

Step OutputStep(OpOutput out, std::optional<std::optional<Batch>> expected_input = {}) {
  Step s;
  s.kind = Step::Kind::OUTPUT;
  s.expected_input = std::move(expected_input);
  s.output = std::move(out);
  return s;
}

Step BlockedStep(std::optional<std::optional<Batch>> expected_input = {}) {
  Step s;
  s.kind = Step::Kind::BLOCKED;
  s.expected_input = std::move(expected_input);
  return s;
}

Step ErrorStep(Status status, std::optional<std::optional<Batch>> expected_input = {}) {
  Step s;
  s.kind = Step::Kind::ERROR;
  s.expected_input = std::move(expected_input);
  s.error = std::move(status);
  return s;
}

class ScriptedSource final : public SourceOp {
 public:
  ScriptedSource(std::string name, std::vector<std::vector<Step>> steps,
                 std::vector<Trace>* traces)
      : SourceOp(std::move(name)),
        steps_(std::move(steps)),
        pcs_(steps_.size(), 0),
        traces_(traces) {}

  PipelineSource Source() override {
    return [this](const TaskContext& task_ctx, ThreadId tid) -> OpResult {
      auto step = NextStep(tid);
      if (step.kind == Step::Kind::ERROR) {
        traces_->push_back(Trace{Name(), "Source", std::nullopt, step.error->ToString()});
        return *step.error;
      }
      if (step.kind == Step::Kind::BLOCKED) {
        auto resumer_r = task_ctx.resumer_factory();
        if (!resumer_r.ok()) {
          traces_->push_back(
              Trace{Name(), "Source", std::nullopt, resumer_r.status().ToString()});
          return resumer_r.status();
        }
        auto resumer = std::move(resumer_r).ValueOrDie();
        auto out = OpOutput::Blocked(std::move(resumer));
        traces_->push_back(Trace{Name(), "Source", std::nullopt, out.ToString()});
        return out;
      }

      auto out = std::move(*step.output);
      traces_->push_back(Trace{Name(), "Source", std::nullopt, out.ToString()});
      return out;
    };
  }

  std::vector<TaskGroup> Frontend() override { return {}; }
  std::optional<TaskGroup> Backend() override { return std::nullopt; }

 private:
  Step NextStep(ThreadId tid) {
    if (tid >= steps_.size()) {
      ADD_FAILURE() << "ScriptedSource thread_id out of range";
      return ErrorStep(Status::Invalid("ScriptedSource thread_id out of range"));
    }
    if (pcs_[tid] >= steps_[tid].size()) {
      ADD_FAILURE() << "ScriptedSource script exhausted";
      return ErrorStep(Status::Invalid("ScriptedSource script exhausted"));
    }
    return steps_[tid][pcs_[tid]++];
  }

  std::vector<std::vector<Step>> steps_;
  std::vector<std::size_t> pcs_;
  std::vector<Trace>* traces_;
};

class ScriptedPipe final : public PipeOp {
 public:
  ScriptedPipe(std::string name, std::vector<std::vector<Step>> pipe_steps,
               std::vector<std::vector<Step>> drain_steps, std::vector<Trace>* traces)
      : PipeOp(std::move(name)),
        pipe_steps_(std::move(pipe_steps)),
        drain_steps_(std::move(drain_steps)),
        pipe_pcs_(pipe_steps_.size(), 0),
        drain_pcs_(drain_steps_.size(), 0),
        traces_(traces) {}

  PipelinePipe Pipe() override {
    return [this](const TaskContext& task_ctx, ThreadId tid,
                  std::optional<Batch> input) -> OpResult {
      auto step = NextPipeStep(tid, input);
      return ExecuteStep(task_ctx, tid, "Pipe", std::move(step), std::move(input));
    };
  }

  PipelineDrain Drain() override {
    if (drain_steps_.empty()) {
      return {};
    }
    return [this](const TaskContext& task_ctx, ThreadId tid) -> OpResult {
      auto step = NextDrainStep(tid);
      return ExecuteStep(task_ctx, tid, "Drain", std::move(step), std::nullopt);
    };
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

 private:
  Step NextPipeStep(ThreadId tid, const std::optional<Batch>& input) {
    if (tid >= pipe_steps_.size()) {
      ADD_FAILURE() << "ScriptedPipe thread_id out of range";
      return ErrorStep(Status::Invalid("ScriptedPipe thread_id out of range"));
    }
    if (pipe_pcs_[tid] >= pipe_steps_[tid].size()) {
      ADD_FAILURE() << "ScriptedPipe pipe script exhausted";
      return ErrorStep(Status::Invalid("ScriptedPipe pipe script exhausted"));
    }
    Step step = pipe_steps_[tid][pipe_pcs_[tid]++];
    CheckExpectedInput(step, input);
    return step;
  }

  Step NextDrainStep(ThreadId tid) {
    if (tid >= drain_steps_.size()) {
      ADD_FAILURE() << "ScriptedPipe thread_id out of range (drain)";
      return ErrorStep(Status::Invalid("ScriptedPipe thread_id out of range"));
    }
    if (drain_pcs_[tid] >= drain_steps_[tid].size()) {
      ADD_FAILURE() << "ScriptedPipe drain script exhausted";
      return ErrorStep(Status::Invalid("ScriptedPipe drain script exhausted"));
    }
    return drain_steps_[tid][drain_pcs_[tid]++];
  }

  static void CheckExpectedInput(const Step& step, const std::optional<Batch>& input) {
    if (!step.expected_input.has_value()) {
      return;
    }
    ASSERT_EQ(input.has_value(), step.expected_input->has_value());
    if (input.has_value()) {
      ASSERT_EQ(*input, **step.expected_input);
    }
  }

  OpResult ExecuteStep(const TaskContext& task_ctx, ThreadId tid, std::string method,
                       Step step, std::optional<Batch> input) {
    if (step.kind == Step::Kind::ERROR) {
      traces_->push_back(
          Trace{Name(), std::move(method), std::move(input), step.error->ToString()});
      return *step.error;
    }

    if (step.kind == Step::Kind::BLOCKED) {
      auto resumer_r = task_ctx.resumer_factory();
      if (!resumer_r.ok()) {
        traces_->push_back(Trace{Name(), std::move(method), std::move(input),
                                 resumer_r.status().ToString()});
        return resumer_r.status();
      }
      auto resumer = std::move(resumer_r).ValueOrDie();
      auto out = OpOutput::Blocked(std::move(resumer));
      traces_->push_back(
          Trace{Name(), std::move(method), std::move(input), out.ToString()});
      return out;
    }

    auto out = std::move(*step.output);
    traces_->push_back(
        Trace{Name(), std::move(method), std::move(input), out.ToString()});
    return out;
  }

  std::vector<std::vector<Step>> pipe_steps_;
  std::vector<std::vector<Step>> drain_steps_;
  std::vector<std::size_t> pipe_pcs_;
  std::vector<std::size_t> drain_pcs_;
  std::vector<Trace>* traces_;
};

class ScriptedSink final : public SinkOp {
 public:
  ScriptedSink(std::string name, std::vector<std::vector<Step>> steps,
               std::vector<Trace>* traces)
      : SinkOp(std::move(name)),
        steps_(std::move(steps)),
        pcs_(steps_.size(), 0),
        traces_(traces) {}

  PipelineSink Sink() override {
    return [this](const TaskContext& task_ctx, ThreadId tid,
                  std::optional<Batch> input) -> OpResult {
      auto step = NextStep(tid, input);
      if (step.kind == Step::Kind::ERROR) {
        traces_->push_back(
            Trace{Name(), "Sink", std::move(input), step.error->ToString()});
        return *step.error;
      }
      if (step.kind == Step::Kind::BLOCKED) {
        auto resumer_r = task_ctx.resumer_factory();
        if (!resumer_r.ok()) {
          traces_->push_back(
              Trace{Name(), "Sink", std::move(input), resumer_r.status().ToString()});
          return resumer_r.status();
        }
        auto resumer = std::move(resumer_r).ValueOrDie();
        auto out = OpOutput::Blocked(std::move(resumer));
        traces_->push_back(Trace{Name(), "Sink", std::move(input), out.ToString()});
        return out;
      }

      auto out = std::move(*step.output);
      traces_->push_back(Trace{Name(), "Sink", std::move(input), out.ToString()});
      return out;
    };
  }

  std::vector<TaskGroup> Frontend() override { return {}; }
  std::optional<TaskGroup> Backend() override { return std::nullopt; }
  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

 private:
  Step NextStep(ThreadId tid, const std::optional<Batch>& input) {
    if (tid >= steps_.size()) {
      ADD_FAILURE() << "ScriptedSink thread_id out of range";
      return ErrorStep(Status::Invalid("ScriptedSink thread_id out of range"));
    }
    if (pcs_[tid] >= steps_[tid].size()) {
      ADD_FAILURE() << "ScriptedSink script exhausted";
      return ErrorStep(Status::Invalid("ScriptedSink script exhausted"));
    }
    Step step = steps_[tid][pcs_[tid]++];
    if (step.expected_input.has_value()) {
      EXPECT_EQ(input.has_value(), step.expected_input->has_value());
      if (input.has_value()) {
        EXPECT_EQ(*input, **step.expected_input);
      }
    }
    return step;
  }

  std::vector<std::vector<Step>> steps_;
  std::vector<std::size_t> pcs_;
  std::vector<Trace>* traces_;
};

TaskContext MakeTaskContext() {
  TaskContext task_ctx;
  task_ctx.context = nullptr;
  task_ctx.resumer_factory = []() -> Result<std::shared_ptr<Resumer>> {
    return std::make_shared<TestResumer>();
  };
  task_ctx.awaiter_factory = [](std::vector<std::shared_ptr<Resumer>> resumers)
      -> Result<std::shared_ptr<Awaiter>> {
    return std::make_shared<TestAwaiter>(std::move(resumers));
  };
  return task_ctx;
}

Status RunSingleTaskToDone(const TaskGroup& group, const TaskContext& task_ctx,
                           std::vector<TaskStatus>* statuses = nullptr) {
  bool done = false;
  std::size_t steps = 0;
  while (!done) {
    if (steps++ >= 1000u) {
      ADD_FAILURE() << "RunSingleTaskToDone exceeded step limit";
      return Status::Invalid("RunSingleTaskToDone exceeded step limit");
    }

    auto status_r = group.GetTask()(task_ctx, /*task_id=*/0);
    if (!status_r.ok()) {
      return status_r.status();
    }
    const auto& ts = status_r.ValueOrDie();
    if (statuses) {
      statuses->push_back(ts);
    }

    if (ts.IsContinue() || ts.IsYield()) {
      continue;
    }
    if (ts.IsFinished() || ts.IsCancelled()) {
      done = true;
      continue;
    }
    if (ts.IsBlocked()) {
      auto* awaiter = dynamic_cast<TestAwaiter*>(ts.GetAwaiter().get());
      if (awaiter == nullptr) {
        ADD_FAILURE() << "unexpected awaiter type";
        return Status::Invalid("unexpected awaiter type");
      }
      for (auto& resumer : awaiter->Resumers()) {
        if (resumer == nullptr) {
          ADD_FAILURE() << "null resumer";
          return Status::Invalid("null resumer");
        }
        resumer->Resume();
      }
      continue;
    }
  }
  return Status::OK();
}

}  // namespace

TEST(BrokenPipelinePipeExecTest, EmptySourceFinishesWithoutCallingSink) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{OutputStep(OpOutput::Finished())}}, &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Segments().size(), 1);

  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));
  ASSERT_FALSE(statuses.empty());
  EXPECT_TRUE(statuses.back().IsFinished());

  ASSERT_EQ(traces.size(), 1);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
}

TEST(BrokenPipelinePipeExecTest, OnePass) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished())}},
                        &traces);

  ScriptedSink sink("Sink",
                    {{OutputStep(OpOutput::PipeSinkNeedsMore(),
                                 std::optional<std::optional<Batch>>(Batch{1}))}},
                    &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));

  ASSERT_GE(statuses.size(), 2u);
  EXPECT_TRUE(statuses[0].IsContinue());
  EXPECT_TRUE(statuses.back().IsFinished());

  ASSERT_EQ(traces.size(), 3);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Sink", "Sink", Batch{1}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[2], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
}

TEST(BrokenPipelinePipeExecTest, PipeNeedsMoreGoesBackToSource) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished(std::optional<Batch>(2)))}},
                        &traces);

  ScriptedPipe pipe("Pipe",
                    {{OutputStep(OpOutput::PipeSinkNeedsMore(),
                                 std::optional<std::optional<Batch>>(Batch{1})),
                      OutputStep(OpOutput::PipeEven(/*batch=*/2),
                                 std::optional<std::optional<Batch>>(Batch{2}))}},
                    /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink",
                    {{OutputStep(OpOutput::PipeSinkNeedsMore(),
                                 std::optional<std::optional<Batch>>(Batch{2}))}},
                    &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 5);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[2], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[3], (Trace{"Pipe", "Pipe", Batch{2}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[4], (Trace{"Sink", "Sink", Batch{2}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, PipeHasMoreResumesPipeBeforeSource) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished())}},
                        &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/10),
                   std::optional<std::optional<Batch>>(Batch{1})),
        OutputStep(OpOutput::PipeEven(/*batch=*/11),
                   std::optional<std::optional<Batch>>(std::optional<Batch>{}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink",
                    {{OutputStep(OpOutput::PipeSinkNeedsMore(),
                                 std::optional<std::optional<Batch>>(Batch{10})),
                      OutputStep(OpOutput::PipeSinkNeedsMore(),
                                 std::optional<std::optional<Batch>>(Batch{11}))}},
                    &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 6);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", Batch{10}, "PIPE_SINK_NEEDS_MORE"}));

  // Pipe resumes (input=null) before source finishes.
  EXPECT_EQ(traces[3], (Trace{"Pipe", "Pipe", std::nullopt, "PIPE_EVEN"}));
  EXPECT_EQ(traces[4], (Trace{"Sink", "Sink", Batch{11}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[5], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
}

TEST(BrokenPipelinePipeExecTest, PipeYieldHandshake) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished())}},
                        &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{OutputStep(OpOutput::PipeYield(), std::optional<std::optional<Batch>>(Batch{1})),
        OutputStep(OpOutput::PipeYieldBack(),
                   std::optional<std::optional<Batch>>(std::optional<Batch>{})),
        OutputStep(OpOutput::PipeSinkNeedsMore(),
                   std::optional<std::optional<Batch>>(std::optional<Batch>{}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r.ok());
  ASSERT_TRUE(status_r->IsYield());

  // Resume after yield: yield back + continue running.
  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));

  ASSERT_FALSE(traces.empty());
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "PIPE_YIELD"}));
  EXPECT_EQ(traces[2], (Trace{"Pipe", "Pipe", std::nullopt, "PIPE_YIELD_BACK"}));
}

TEST(BrokenPipelinePipeExecTest, PipeBlockedResumesWithNullInput) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished())}},
                        &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{BlockedStep(std::optional<std::optional<Batch>>(Batch{1})),
        OutputStep(OpOutput::PipeSinkNeedsMore(),
                   std::optional<std::optional<Batch>>(std::optional<Batch>{}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r.ok());
  ASSERT_TRUE(status_r->IsBlocked());

  // Calling again without resuming stays blocked and does not re-invoke Pipe().
  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsBlocked());

  // Resume and complete.
  auto* awaiter = dynamic_cast<TestAwaiter*>(status_r->GetAwaiter().get());
  ASSERT_NE(awaiter, nullptr);
  for (auto& resumer : awaiter->Resumers()) {
    resumer->Resume();
  }
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_GE(traces.size(), 2u);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "BLOCKED"}));
}

TEST(BrokenPipelinePipeExecTest, SinkBackpressureResumesWithNullInput) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished())}},
                        &traces);

  ScriptedSink sink(
      "Sink",
      {{BlockedStep(std::optional<std::optional<Batch>>(Batch{1})),
        BlockedStep(std::optional<std::optional<Batch>>(std::optional<Batch>{})),
        OutputStep(OpOutput::PipeSinkNeedsMore(),
                   std::optional<std::optional<Batch>>(std::optional<Batch>{}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  // First call blocks in sink.
  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r.ok());
  ASSERT_TRUE(status_r->IsBlocked());

  auto* awaiter = dynamic_cast<TestAwaiter*>(status_r->GetAwaiter().get());
  ASSERT_NE(awaiter, nullptr);
  for (auto& resumer : awaiter->Resumers()) {
    resumer->Resume();
  }

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_GE(traces.size(), 2u);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Sink", "Sink", Batch{1}, "BLOCKED"}));

  // Sink resumes with input=null (backpressure resume path).
  ASSERT_GE(traces.size(), 3u);
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", std::nullopt, "BLOCKED"}));
}

TEST(BrokenPipelinePipeExecTest, DrainProducesTailOutput) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{OutputStep(OpOutput::Finished())}}, &traces);

  ScriptedPipe pipe("Pipe",
                    /*pipe_steps=*/{{}},
                    /*drain_steps=*/
                    {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                      OutputStep(OpOutput::Finished(std::optional<Batch>(2)))}},
                    &traces);

  ScriptedSink sink("Sink",
                    {{OutputStep(OpOutput::PipeSinkNeedsMore(),
                                 std::optional<std::optional<Batch>>(Batch{1})),
                      OutputStep(OpOutput::PipeSinkNeedsMore(),
                                 std::optional<std::optional<Batch>>(Batch{2}))}},
                    &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 5);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Drain", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", Batch{1}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[3], (Trace{"Pipe", "Drain", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[4], (Trace{"Sink", "Sink", Batch{2}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, MultiChannelAllBlockedReturnsTaskBlocked) {
  std::vector<Trace> traces;

  ScriptedSource source1("Source1", {{BlockedStep()}}, &traces);
  ScriptedSource source2("Source2", {{BlockedStep()}}, &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source1, {}}, PipelineChannel{&source2, {}}},
                    &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r.ok());
  ASSERT_TRUE(status_r->IsBlocked());

  auto* awaiter = dynamic_cast<TestAwaiter*>(status_r->GetAwaiter().get());
  ASSERT_NE(awaiter, nullptr);
  ASSERT_EQ(awaiter->Resumers().size(), 2);
}

TEST(BrokenPipelinePipeExecTest, ErrorCancelsSubsequentCalls) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{ErrorStep(Status::UnknownError("boom"))}}, &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Segments()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_FALSE(status_r.ok());
  ASSERT_EQ(status_r.status().message(), "boom");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

}  // namespace broken_pipeline_test
