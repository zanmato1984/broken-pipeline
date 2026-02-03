#include "arrow_traits.h"

#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace bp_test {

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

std::optional<std::optional<Batch>> ExpectInput(std::optional<Batch> input) {
  return std::optional<std::optional<Batch>>(std::move(input));
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
               std::vector<std::vector<Step>> drain_steps, std::vector<Trace>* traces,
               std::unique_ptr<SourceOp> implicit_source = nullptr)
      : PipeOp(std::move(name)),
        pipe_steps_(std::move(pipe_steps)),
        drain_steps_(std::move(drain_steps)),
        pipe_pcs_(pipe_steps_.size(), 0),
        drain_pcs_(drain_steps_.size(), 0),
        traces_(traces),
        implicit_source_(std::move(implicit_source)) {}

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

  std::unique_ptr<SourceOp> ImplicitSource() override {
    return std::move(implicit_source_);
  }

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
  std::unique_ptr<SourceOp> implicit_source_;
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
  ASSERT_EQ(exec.Pipelinexes().size(), 1);

  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));
  ASSERT_FALSE(statuses.empty());
  EXPECT_TRUE(statuses.back().IsFinished());

  ASSERT_EQ(traces.size(), 1);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
}

TEST(BrokenPipelinePipeExecTest, EmptySourceNotReady) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{BlockedStep(), OutputStep(OpOutput::Finished())}},
                        &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 1);

  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r.ok());
  ASSERT_TRUE(status_r->IsBlocked());
  ASSERT_EQ(traces.size(), 1);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "BLOCKED"}));

  // Calling again without resuming stays blocked and does not re-invoke Source().
  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsBlocked());
  ASSERT_EQ(traces.size(), 1);

  auto* awaiter = dynamic_cast<TestAwaiter*>(status_r->GetAwaiter().get());
  ASSERT_NE(awaiter, nullptr);
  for (auto& resumer : awaiter->Resumers()) {
    resumer->Resume();
  }

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));
  ASSERT_EQ(traces.size(), 2);
  EXPECT_EQ(traces[1], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
}

TEST(BrokenPipelinePipeExecTest, TwoSourceOneNotReady) {
  std::vector<Trace> traces;

  ScriptedSource source1("Source1",
                         {{BlockedStep(), OutputStep(OpOutput::Finished())}}, &traces);
  ScriptedSource source2("Source2", {{OutputStep(OpOutput::Finished())}}, &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source1, {}}, PipelineChannel{&source2, {}}},
                    &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 1);

  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  // First run can make progress on channel 2 even if channel 1 is blocked.
  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r.ok());
  ASSERT_TRUE(status_r->IsContinue());

  ASSERT_EQ(traces.size(), 2);
  EXPECT_EQ(traces[0], (Trace{"Source1", "Source", std::nullopt, "BLOCKED"}));
  EXPECT_EQ(traces[1], (Trace{"Source2", "Source", std::nullopt, "FINISHED"}));

  // Now only channel 1 is unfinished and blocked -> task blocks.
  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsBlocked());

  auto* awaiter = dynamic_cast<TestAwaiter*>(status_r2->GetAwaiter().get());
  ASSERT_NE(awaiter, nullptr);
  ASSERT_EQ(awaiter->Resumers().size(), 1);
  awaiter->Resumers()[0]->Resume();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 3);
  EXPECT_EQ(traces[2], (Trace{"Source1", "Source", std::nullopt, "FINISHED"}));
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
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

TEST(BrokenPipelinePipeExecTest, OnePassDirectFinish) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::Finished(std::optional<Batch>(1)))}}, &traces);

  ScriptedSink sink("Sink",
                    {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{1}))}},
                    &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));

  ASSERT_GE(statuses.size(), 2u);
  EXPECT_TRUE(statuses[0].IsContinue());
  EXPECT_TRUE(statuses.back().IsFinished());

  ASSERT_EQ(traces.size(), 2);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[1], (Trace{"Sink", "Sink", Batch{1}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, OnePassWithPipe) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished())}},
                        &traces);

  ScriptedPipe pipe("Pipe", {{OutputStep(OpOutput::PipeEven(/*batch=*/10),
                                         ExpectInput(Batch{1}))}},
                    /*drain_steps=*/{}, &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 4);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", Batch{10}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[3], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
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

TEST(BrokenPipelinePipeExecTest, PipeAsyncSpill) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
                          OutputStep(OpOutput::Finished())}},
                        &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{BlockedStep(ExpectInput(Batch{1})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(std::nullopt))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 4);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "BLOCKED"}));
  EXPECT_EQ(traces[2], (Trace{"Pipe", "Pipe", std::nullopt, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[3], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 5);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Drain", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", Batch{1}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[3], (Trace{"Pipe", "Drain", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[4], (Trace{"Sink", "Sink", Batch{2}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, Drain) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::Finished(std::optional<Batch>(2)))}},
      &traces);

  ScriptedPipe pipe(
      "Pipe",
      /*pipe_steps=*/{{OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(Batch{1})),
                      OutputStep(OpOutput::PipeEven(/*batch=*/20), ExpectInput(Batch{2}))}},
      /*drain_steps=*/
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/30)),
        OutputStep(OpOutput::PipeYield()),
        OutputStep(OpOutput::PipeYieldBack()),
        OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/31)),
        BlockedStep(),
        OutputStep(OpOutput::Finished(std::optional<Batch>(32)))}},
      &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{20})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{30})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{31})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{32}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));

  // Draining includes a yield handshake.
  ASSERT_FALSE(statuses.empty());
  EXPECT_TRUE(std::any_of(statuses.begin(), statuses.end(),
                          [](const TaskStatus& s) { return s.IsYield(); }));

  ASSERT_EQ(traces.size(), 15);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", Batch{10}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[3], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[4], (Trace{"Pipe", "Pipe", Batch{2}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[5], (Trace{"Sink", "Sink", Batch{20}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[6], (Trace{"Pipe", "Drain", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[7], (Trace{"Sink", "Sink", Batch{30}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[8], (Trace{"Pipe", "Drain", std::nullopt, "PIPE_YIELD"}));
  EXPECT_EQ(traces[9], (Trace{"Pipe", "Drain", std::nullopt, "PIPE_YIELD_BACK"}));
  EXPECT_EQ(traces[10], (Trace{"Pipe", "Drain", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[11], (Trace{"Sink", "Sink", Batch{31}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[12], (Trace{"Pipe", "Drain", std::nullopt, "BLOCKED"}));
  EXPECT_EQ(traces[13], (Trace{"Pipe", "Drain", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[14], (Trace{"Sink", "Sink", Batch{32}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, ImplicitSource) {
  std::vector<Trace> traces;

  auto implicit_source_up = std::make_unique<ScriptedSource>(
      "ImplicitSource",
      std::vector<std::vector<Step>>{
          {OutputStep(OpOutput::Finished(std::optional<Batch>(2)))}},
      &traces);
  auto* implicit_source = implicit_source_up.get();

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::Finished(std::optional<Batch>(1)))}}, &traces);

  ScriptedPipe pipe("Pipe", {{OutputStep(OpOutput::PipeEven(/*batch=*/10),
                                         ExpectInput(Batch{1}))}},
                    /*drain_steps=*/{}, &traces, std::move(implicit_source_up));

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{2}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 2);
  ASSERT_EQ(exec.Pipelinexes()[1].Channels().size(), 1);
  ASSERT_EQ(exec.Pipelinexes()[1].Channels()[0].source_op, implicit_source);

  auto task_ctx = MakeTaskContext();
  ASSERT_OK(RunSingleTaskToDone(exec.Pipelinexes()[0].PipeExec().TaskGroup(), task_ctx));
  ASSERT_OK(RunSingleTaskToDone(exec.Pipelinexes()[1].PipeExec().TaskGroup(), task_ctx));

  ASSERT_EQ(traces.size(), 5);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", Batch{10}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[3], (Trace{"ImplicitSource", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[4], (Trace{"Sink", "Sink", Batch{2}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, Backpressure) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::Finished(std::optional<Batch>(2)))}},
      &traces);

  ScriptedPipe pipe(
      "Pipe",
      /*pipe_steps=*/{{OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(Batch{1})),
                      OutputStep(OpOutput::PipeEven(/*batch=*/20), ExpectInput(Batch{2}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink(
      "Sink",
      {{BlockedStep(ExpectInput(Batch{10})),
        BlockedStep(ExpectInput(std::nullopt)),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(std::nullopt)),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{20}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 8);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe", "Pipe", Batch{1}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[2], (Trace{"Sink", "Sink", Batch{10}, "BLOCKED"}));
  EXPECT_EQ(traces[3], (Trace{"Sink", "Sink", std::nullopt, "BLOCKED"}));
  EXPECT_EQ(traces[4], (Trace{"Sink", "Sink", std::nullopt, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[5], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[6], (Trace{"Pipe", "Pipe", Batch{2}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[7], (Trace{"Sink", "Sink", Batch{20}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, MultiPipe) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/2)),
        OutputStep(OpOutput::Finished(std::optional<Batch>(3)))}},
      &traces);

  ScriptedPipe pipe1(
      "Pipe1",
      /*pipe_steps=*/
      {{OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(Batch{1})),
        OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/11), ExpectInput(Batch{2})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(std::nullopt)),
        OutputStep(OpOutput::PipeEven(/*batch=*/13), ExpectInput(Batch{3}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedPipe pipe2(
      "Pipe2",
      /*pipe_steps=*/
      {{OutputStep(OpOutput::PipeYield(), ExpectInput(Batch{10})),
        OutputStep(OpOutput::PipeYieldBack(), ExpectInput(std::nullopt)),
        OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/100), ExpectInput(std::nullopt)),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(std::nullopt)),
        OutputStep(OpOutput::PipeEven(/*batch=*/200), ExpectInput(Batch{11})),
        OutputStep(OpOutput::PipeEven(/*batch=*/300), ExpectInput(Batch{13}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{100})),
        BlockedStep(ExpectInput(Batch{200})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(std::nullopt)),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{300}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe1, &pipe2}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));
  EXPECT_TRUE(std::any_of(statuses.begin(), statuses.end(),
                          [](const TaskStatus& s) { return s.IsYield(); }));

  ASSERT_EQ(traces.size(), 17);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe1", "Pipe", Batch{1}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[2], (Trace{"Pipe2", "Pipe", Batch{10}, "PIPE_YIELD"}));
  EXPECT_EQ(traces[3], (Trace{"Pipe2", "Pipe", std::nullopt, "PIPE_YIELD_BACK"}));
  EXPECT_EQ(traces[4], (Trace{"Pipe2", "Pipe", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[5], (Trace{"Sink", "Sink", Batch{100}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[6], (Trace{"Pipe2", "Pipe", std::nullopt, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[7], (Trace{"Source", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[8], (Trace{"Pipe1", "Pipe", Batch{2}, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[9], (Trace{"Pipe2", "Pipe", Batch{11}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[10], (Trace{"Sink", "Sink", Batch{200}, "BLOCKED"}));
  EXPECT_EQ(traces[11], (Trace{"Sink", "Sink", std::nullopt, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[12], (Trace{"Pipe1", "Pipe", std::nullopt, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[13], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[14], (Trace{"Pipe1", "Pipe", Batch{3}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[15], (Trace{"Pipe2", "Pipe", Batch{13}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[16], (Trace{"Sink", "Sink", Batch{300}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, MultiDrain) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{OutputStep(OpOutput::Finished())}}, &traces);

  ScriptedPipe pipe1(
      "Pipe1", /*pipe_steps=*/{{}},
      /*drain_steps=*/
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::Finished())}},
      &traces);

  ScriptedPipe pipe2(
      "Pipe2",
      /*pipe_steps=*/
      {{OutputStep(OpOutput::PipeYield(), ExpectInput(Batch{1})),
        OutputStep(OpOutput::PipeYieldBack(), ExpectInput(std::nullopt)),
        OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(std::nullopt))}},
      /*drain_steps=*/
      {{BlockedStep(), OutputStep(OpOutput::Finished(std::optional<Batch>(2)))}},
      &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{2}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe1, &pipe2}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  std::vector<TaskStatus> statuses;
  ASSERT_OK(RunSingleTaskToDone(group, task_ctx, &statuses));
  EXPECT_TRUE(std::any_of(statuses.begin(), statuses.end(),
                          [](const TaskStatus& s) { return s.IsYield(); }));

  ASSERT_EQ(traces.size(), 10);
  EXPECT_EQ(traces[0], (Trace{"Source", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[1], (Trace{"Pipe1", "Drain", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[2], (Trace{"Pipe2", "Pipe", Batch{1}, "PIPE_YIELD"}));
  EXPECT_EQ(traces[3], (Trace{"Pipe2", "Pipe", std::nullopt, "PIPE_YIELD_BACK"}));
  EXPECT_EQ(traces[4], (Trace{"Pipe2", "Pipe", std::nullopt, "PIPE_EVEN"}));
  EXPECT_EQ(traces[5], (Trace{"Sink", "Sink", Batch{10}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[6], (Trace{"Pipe1", "Drain", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[7], (Trace{"Pipe2", "Drain", std::nullopt, "BLOCKED"}));
  EXPECT_EQ(traces[8], (Trace{"Pipe2", "Drain", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[9], (Trace{"Sink", "Sink", Batch{2}, "PIPE_SINK_NEEDS_MORE"}));
}

TEST(BrokenPipelinePipeExecTest, MultiChannel) {
  std::vector<Trace> traces;

  ScriptedSource source1(
      "Source1",
      {{BlockedStep(), OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::Finished())}},
      &traces);
  ScriptedPipe pipe1("Pipe1", {{OutputStep(OpOutput::PipeEven(/*batch=*/10),
                                           ExpectInput(Batch{1}))}},
                     /*drain_steps=*/{}, &traces);

  ScriptedSource source2(
      "Source2",
      {{BlockedStep(), OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/2)),
        OutputStep(OpOutput::Finished())}},
      &traces);
  ScriptedPipe pipe2(
      "Pipe2",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/20), ExpectInput(Batch{2})),
        OutputStep(OpOutput::PipeEven(/*batch=*/21), ExpectInput(std::nullopt))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{20})),
        OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{21}))}},
      &traces);

  Pipeline pipeline("P",
                    {PipelineChannel{&source1, {&pipe1}},
                     PipelineChannel{&source2, {&pipe2}}},
                    &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  // Both channels start blocked.
  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r.ok());
  ASSERT_TRUE(status_r->IsBlocked());

  auto* awaiter = dynamic_cast<TestAwaiter*>(status_r->GetAwaiter().get());
  ASSERT_NE(awaiter, nullptr);
  ASSERT_EQ(awaiter->Resumers().size(), 2);

  // Resume only channel 0 first (leave channel 1 blocked).
  awaiter->Resumers()[0]->Resume();

  // Channel 0 can now run and reach sink.
  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsContinue());

  // Finish channel 0; task still continues because channel 1 is blocked.
  auto status_r3 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r3.ok());
  ASSERT_TRUE(status_r3->IsContinue());

  // Now the only unfinished channel is blocked -> task blocks with a single resumer.
  auto status_r4 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r4.ok());
  ASSERT_TRUE(status_r4->IsBlocked());
  auto* awaiter2 = dynamic_cast<TestAwaiter*>(status_r4->GetAwaiter().get());
  ASSERT_NE(awaiter2, nullptr);
  ASSERT_EQ(awaiter2->Resumers().size(), 1);
  awaiter2->Resumers()[0]->Resume();

  ASSERT_OK(RunSingleTaskToDone(group, task_ctx));

  ASSERT_EQ(traces.size(), 12);
  EXPECT_EQ(traces[0], (Trace{"Source1", "Source", std::nullopt, "BLOCKED"}));
  EXPECT_EQ(traces[1], (Trace{"Source2", "Source", std::nullopt, "BLOCKED"}));
  EXPECT_EQ(traces[2], (Trace{"Source1", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[3], (Trace{"Pipe1", "Pipe", Batch{1}, "PIPE_EVEN"}));
  EXPECT_EQ(traces[4], (Trace{"Sink", "Sink", Batch{10}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[5], (Trace{"Source1", "Source", std::nullopt, "FINISHED"}));
  EXPECT_EQ(traces[6], (Trace{"Source2", "Source", std::nullopt, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[7], (Trace{"Pipe2", "Pipe", Batch{2}, "SOURCE_PIPE_HAS_MORE"}));
  EXPECT_EQ(traces[8], (Trace{"Sink", "Sink", Batch{20}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[9], (Trace{"Pipe2", "Pipe", std::nullopt, "PIPE_EVEN"}));
  EXPECT_EQ(traces[10], (Trace{"Sink", "Sink", Batch{21}, "PIPE_SINK_NEEDS_MORE"}));
  EXPECT_EQ(traces[11], (Trace{"Source2", "Source", std::nullopt, "FINISHED"}));
}

TEST(BrokenPipelinePipeExecTest, MultiChannelAllBlockedReturnsTaskBlocked) {
  std::vector<Trace> traces;

  ScriptedSource source1("Source1", {{BlockedStep()}}, &traces);
  ScriptedSource source2("Source2", {{BlockedStep()}}, &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source1, {}}, PipelineChannel{&source2, {}}},
                    &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
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
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto status_r = group.GetTask()(task_ctx, 0);
  ASSERT_FALSE(status_r.ok());
  ASSERT_EQ(status_r.status().message(), "boom");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, DirectSourceError) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{ErrorStep(Status::UnknownError("42"))}}, &traces);
  ScriptedPipe pipe("Pipe", {{}} /*unused*/, /*drain_steps=*/{}, &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, SourceErrorAfterBlocked) {
  std::vector<Trace> traces;

  ScriptedSource source("Source",
                        {{BlockedStep(), ErrorStep(Status::UnknownError("42"))}},
                        &traces);
  ScriptedPipe pipe("Pipe", {{}} /*unused*/, /*drain_steps=*/{}, &traces);
  ScriptedSink sink("Sink", {{}}, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, SourceError) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        ErrorStep(Status::UnknownError("42"))}},
      &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{1}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, PipeError) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1))}}, &traces);

  ScriptedPipe pipe(
      "Pipe", {{ErrorStep(Status::UnknownError("42"), ExpectInput(Batch{1}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, PipeErrorAfterEven) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/2))}},
      &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(Batch{1})),
        ErrorStep(Status::UnknownError("42"), ExpectInput(Batch{2}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, PipeErrorAfterNeedsMore) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/2))}},
      &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{1})),
        ErrorStep(Status::UnknownError("42"), ExpectInput(Batch{2}))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, PipeErrorAfterHasMore) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1))}}, &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/10), ExpectInput(Batch{1})),
        ErrorStep(Status::UnknownError("42"), ExpectInput(std::nullopt))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, PipeErrorAfterYieldBack) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1))}}, &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{OutputStep(OpOutput::PipeYield(), ExpectInput(Batch{1})),
        OutputStep(OpOutput::PipeYieldBack(), ExpectInput(std::nullopt)),
        ErrorStep(Status::UnknownError("42"), ExpectInput(std::nullopt))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, PipeErrorAfterBlocked) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1))}}, &traces);

  ScriptedPipe pipe(
      "Pipe",
      {{BlockedStep(ExpectInput(Batch{1})),
        ErrorStep(Status::UnknownError("42"), ExpectInput(std::nullopt))}},
      /*drain_steps=*/{}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, DrainError) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{OutputStep(OpOutput::Finished())}}, &traces);

  ScriptedPipe pipe("Pipe", /*pipe_steps=*/{{}},
                    {{ErrorStep(Status::UnknownError("42"))}}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, DrainErrorAfterHasMore) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{OutputStep(OpOutput::Finished())}}, &traces);

  ScriptedPipe pipe(
      "Pipe", /*pipe_steps=*/{{}},
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        ErrorStep(Status::UnknownError("42"))}},
      &traces);

  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{1}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, DrainErrorAfterYieldBack) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{OutputStep(OpOutput::Finished())}}, &traces);

  ScriptedPipe pipe(
      "Pipe", /*pipe_steps=*/{{}},
      {{OutputStep(OpOutput::PipeYield()), OutputStep(OpOutput::PipeYieldBack()),
        ErrorStep(Status::UnknownError("42"))}},
      &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, DrainErrorAfterBlocked) {
  std::vector<Trace> traces;

  ScriptedSource source("Source", {{OutputStep(OpOutput::Finished())}}, &traces);

  ScriptedPipe pipe(
      "Pipe", /*pipe_steps=*/{{}},
      {{BlockedStep(), ErrorStep(Status::UnknownError("42"))}}, &traces);

  ScriptedSink sink("Sink", {{}} /*unused*/, &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, SinkError) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1))}}, &traces);
  ScriptedPipe pipe(
      "Pipe", {{OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(Batch{1}))}},
      /*drain_steps=*/{}, &traces);
  ScriptedSink sink(
      "Sink", {{ErrorStep(Status::UnknownError("42"), ExpectInput(Batch{10}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, SinkErrorAfterNeedsMore) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source",
      {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1)),
        OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/2))}},
      &traces);
  ScriptedPipe pipe(
      "Pipe",
      {{OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(Batch{1})),
        OutputStep(OpOutput::PipeEven(/*batch=*/20), ExpectInput(Batch{2}))}},
      /*drain_steps=*/{}, &traces);
  ScriptedSink sink(
      "Sink",
      {{OutputStep(OpOutput::PipeSinkNeedsMore(), ExpectInput(Batch{10})),
        ErrorStep(Status::UnknownError("42"), ExpectInput(Batch{20}))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

TEST(BrokenPipelinePipeExecTest, SinkErrorAfterBlocked) {
  std::vector<Trace> traces;

  ScriptedSource source(
      "Source", {{OutputStep(OpOutput::SourcePipeHasMore(/*batch=*/1))}}, &traces);
  ScriptedPipe pipe(
      "Pipe", {{OutputStep(OpOutput::PipeEven(/*batch=*/10), ExpectInput(Batch{1}))}},
      /*drain_steps=*/{}, &traces);
  ScriptedSink sink(
      "Sink",
      {{BlockedStep(ExpectInput(Batch{10})),
        ErrorStep(Status::UnknownError("42"), ExpectInput(std::nullopt))}},
      &traces);

  Pipeline pipeline("P", {PipelineChannel{&source, {&pipe}}}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  auto group = exec.Pipelinexes()[0].PipeExec().TaskGroup();
  auto task_ctx = MakeTaskContext();

  auto st = RunSingleTaskToDone(group, task_ctx);
  ASSERT_FALSE(st.ok());
  ASSERT_TRUE(st.IsUnknownError());
  ASSERT_EQ(st.message(), "42");

  auto status_r2 = group.GetTask()(task_ctx, 0);
  ASSERT_TRUE(status_r2.ok());
  ASSERT_TRUE(status_r2->IsCancelled());
}

}  // namespace bp_test
