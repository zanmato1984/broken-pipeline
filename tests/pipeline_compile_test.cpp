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

#include <broken_pipeline/schedule/traits.h>

#include <broken_pipeline/pipeline_exec.h>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace bp_test {

using namespace broken_pipeline::schedule;

namespace {

class FooSource final : public SourceOp {
 public:
  explicit FooSource(std::string name = "FooSource") : SourceOp(std::move(name)) {}

  PipelineSource Source() override {
    return [](const TaskContext&, ThreadId) -> OpResult { return OpOutput::Finished(); };
  }

  std::vector<TaskGroup> Frontend() override { return {}; }

  std::optional<TaskGroup> Backend() override { return std::nullopt; }
};

class FooPipe final : public PipeOp {
 public:
  FooPipe(std::string name = "FooPipe", PipelineDrain drain = {},
          std::unique_ptr<SourceOp> implicit_source = nullptr)
      : PipeOp(std::move(name)),
        drain_(std::move(drain)),
        implicit_source_(std::move(implicit_source)) {}

  PipelinePipe Pipe() override {
    return [](const TaskContext&, ThreadId, std::optional<Batch>) -> OpResult {
      return OpOutput::PipeSinkNeedsMore();
    };
  }

  PipelineDrain Drain() override { return drain_; }

  std::unique_ptr<SourceOp> ImplicitSource() override {
    return std::move(implicit_source_);
  }

 private:
  PipelineDrain drain_;
  std::unique_ptr<SourceOp> implicit_source_;
};

class FooSink final : public SinkOp {
 public:
  explicit FooSink(std::string name = "FooSink") : SinkOp(std::move(name)) {}

  PipelineSink Sink() override {
    return [](const TaskContext&, ThreadId, std::optional<Batch>) -> OpResult {
      return OpOutput::PipeSinkNeedsMore();
    };
  }

  std::vector<TaskGroup> Frontend() override { return {}; }

  std::optional<TaskGroup> Backend() override { return std::nullopt; }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
};

}  // namespace

TEST(BrokenPipelinePipelineCompileTest, EmptyPipeline) {
  FooSink sink;
  Pipeline pipeline("EmptyPipeline", {}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_TRUE(exec.Pipelinexes().empty());
}

TEST(BrokenPipelinePipelineCompileTest, SingleChannelPipeline) {
  FooSource source;
  FooPipe pipe;
  FooSink sink;

  Pipeline pipeline("SingleChannelPipeline", {PipelineChannel{&source, {&pipe}}}, &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 1);

  const auto& seg0 = exec.Pipelinexes()[0];
  ASSERT_EQ(seg0.Channels().size(), 1);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  const auto& ch0 = seg0.Channels()[0];
  ASSERT_EQ(ch0.source_op, &source);
  ASSERT_EQ(ch0.pipe_ops.size(), 1);
  ASSERT_EQ(ch0.pipe_ops[0], &pipe);
}

TEST(BrokenPipelinePipelineCompileTest, DoubleChannelPipeline) {
  FooSource source1, source2;
  FooPipe pipe;
  FooSink sink;

  Pipeline pipeline(
      "DoubleChannelPipeline",
      {PipelineChannel{&source1, {&pipe}}, PipelineChannel{&source2, {&pipe}}}, &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 1);

  const auto& seg0 = exec.Pipelinexes()[0];
  ASSERT_EQ(seg0.Channels().size(), 2);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg0.Channels()[0].source_op, &source1);
  ASSERT_EQ(seg0.Channels()[1].source_op, &source2);
}

TEST(BrokenPipelinePipelineCompileTest, DoublePhysicalPipeline) {
  FooSource source;
  auto implicit_source_up = std::make_unique<FooSource>("ImplicitSource");
  auto* implicit_source = implicit_source_up.get();
  FooPipe pipe("FooPipe", {}, std::move(implicit_source_up));
  FooSink sink;

  Pipeline pipeline("DoublePhysicalPipeline", {PipelineChannel{&source, {&pipe}}}, &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 2);

  const auto& seg0 = exec.Pipelinexes()[0];
  const auto& seg1 = exec.Pipelinexes()[1];

  ASSERT_EQ(seg0.Channels().size(), 1);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg1.Channels().size(), 1);
  ASSERT_EQ(seg1.NumImplicitSources(), 1);
  ASSERT_EQ(seg1.Channels()[0].source_op, implicit_source);
  ASSERT_TRUE(seg1.Channels()[0].pipe_ops.empty());
}

TEST(BrokenPipelinePipelineCompileTest, DoublePhysicalDoubleChannelPipeline) {
  FooSource source1, source2;

  auto implicit_source1_up = std::make_unique<FooSource>("ImplicitSource1");
  auto implicit_source2_up = std::make_unique<FooSource>("ImplicitSource2");
  auto* implicit_source1 = implicit_source1_up.get();
  auto* implicit_source2 = implicit_source2_up.get();

  FooPipe pipe1("Pipe1", {}, std::move(implicit_source1_up));
  FooPipe pipe2("Pipe2", {}, std::move(implicit_source2_up));
  FooSink sink;

  Pipeline pipeline(
      "DoublePhysicalDoubleChannelPipeline",
      {PipelineChannel{&source1, {&pipe1}}, PipelineChannel{&source2, {&pipe2}}}, &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 2);

  const auto& seg0 = exec.Pipelinexes()[0];
  const auto& seg1 = exec.Pipelinexes()[1];

  ASSERT_EQ(seg0.Channels().size(), 2);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg1.Channels().size(), 2);
  ASSERT_EQ(seg1.NumImplicitSources(), 2);
  ASSERT_EQ(seg1.Channels()[0].source_op, implicit_source1);
  ASSERT_EQ(seg1.Channels()[1].source_op, implicit_source2);
}

TEST(BrokenPipelinePipelineCompileTest, TripplePhysicalPipeline) {
  FooSource source1, source2;

  auto implicit_source1_up = std::make_unique<FooSource>("ImplicitSource1");
  auto implicit_source2_up = std::make_unique<FooSource>("ImplicitSource2");
  auto implicit_source3_up = std::make_unique<FooSource>("ImplicitSource3");
  auto* implicit_source1 = implicit_source1_up.get();
  auto* implicit_source2 = implicit_source2_up.get();
  auto* implicit_source3 = implicit_source3_up.get();

  FooPipe pipe1("Pipe1", {}, std::move(implicit_source1_up));
  FooPipe pipe2("Pipe2", {}, std::move(implicit_source2_up));
  FooPipe pipe3("Pipe3", {}, std::move(implicit_source3_up));
  FooSink sink;

  Pipeline pipeline("TripplePhysicalPipeline",
                    {PipelineChannel{&source1, {&pipe1, &pipe3}},
                     PipelineChannel{&source2, {&pipe2, &pipe3}}},
                    &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 3);

  const auto& seg0 = exec.Pipelinexes()[0];
  const auto& seg1 = exec.Pipelinexes()[1];
  const auto& seg2 = exec.Pipelinexes()[2];

  ASSERT_EQ(seg0.Channels().size(), 2);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg1.Channels().size(), 2);
  ASSERT_EQ(seg1.NumImplicitSources(), 2);
  ASSERT_EQ(seg1.Channels()[0].source_op, implicit_source1);
  ASSERT_EQ(seg1.Channels()[1].source_op, implicit_source2);
  ASSERT_EQ(seg1.Channels()[0].pipe_ops.size(), 1);
  ASSERT_EQ(seg1.Channels()[0].pipe_ops[0], &pipe3);
  ASSERT_EQ(seg1.Channels()[1].pipe_ops.size(), 1);
  ASSERT_EQ(seg1.Channels()[1].pipe_ops[0], &pipe3);

  ASSERT_EQ(seg2.Channels().size(), 1);
  ASSERT_EQ(seg2.NumImplicitSources(), 1);
  ASSERT_EQ(seg2.Channels()[0].source_op, implicit_source3);
  ASSERT_TRUE(seg2.Channels()[0].pipe_ops.empty());
}

TEST(BrokenPipelinePipelineCompileTest, OddQuadroStagePipeline) {
  FooSource source1, source2, source3, source4;

  auto implicit_source1_up = std::make_unique<FooSource>("ImplicitSource1");
  auto implicit_source2_up = std::make_unique<FooSource>("ImplicitSource2");
  auto implicit_source3_up = std::make_unique<FooSource>("ImplicitSource3");
  auto implicit_source4_up = std::make_unique<FooSource>("ImplicitSource4");
  auto* implicit_source1 = implicit_source1_up.get();
  auto* implicit_source2 = implicit_source2_up.get();
  auto* implicit_source3 = implicit_source3_up.get();
  auto* implicit_source4 = implicit_source4_up.get();

  FooPipe pipe1("Pipe1", {}, std::move(implicit_source1_up));
  FooPipe pipe2("Pipe2", {}, std::move(implicit_source2_up));
  FooPipe pipe3("Pipe3", {}, std::move(implicit_source3_up));
  FooPipe pipe4("Pipe4", {}, std::move(implicit_source4_up));
  FooSink sink;

  Pipeline pipeline(
      "OddQuadroStagePipeline",
      {PipelineChannel{&source1, {&pipe1, &pipe2, &pipe4}},
       PipelineChannel{&source2, {&pipe2, &pipe4}},
       PipelineChannel{&source3, {&pipe3, &pipe4}}, PipelineChannel{&source4, {&pipe4}}},
      &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Pipelinexes().size(), 4);

  const auto& seg0 = exec.Pipelinexes()[0];
  const auto& seg1 = exec.Pipelinexes()[1];
  const auto& seg2 = exec.Pipelinexes()[2];
  const auto& seg3 = exec.Pipelinexes()[3];

  ASSERT_EQ(seg0.Channels().size(), 4);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg1.Channels().size(), 2);
  ASSERT_EQ(seg1.NumImplicitSources(), 2);
  ASSERT_EQ(seg1.Channels()[0].source_op, implicit_source1);
  ASSERT_EQ(seg1.Channels()[1].source_op, implicit_source3);

  ASSERT_EQ(seg2.Channels().size(), 1);
  ASSERT_EQ(seg2.NumImplicitSources(), 1);
  ASSERT_EQ(seg2.Channels()[0].source_op, implicit_source2);

  ASSERT_EQ(seg3.Channels().size(), 1);
  ASSERT_EQ(seg3.NumImplicitSources(), 1);
  ASSERT_EQ(seg3.Channels()[0].source_op, implicit_source4);
}

}  // namespace bp_test
