#include "arrow_traits.h"

#include <opl/pipeline_exec.h>

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace opl {

namespace {

class FooSource final : public opl_test::SourceOp {
 public:
  explicit FooSource(std::string name = "FooSource") : SourceOp(std::move(name)) {}

  opl_test::PipelineSource Source() override {
    return [](const opl_test::TaskContext&, opl_test::ThreadId) -> opl_test::OpResult {
      return opl_test::OpOutput::Finished();
    };
  }

  std::vector<opl_test::TaskGroup> Frontend() override { return {}; }

  std::optional<opl_test::TaskGroup> Backend() override { return std::nullopt; }
};

class FooPipe final : public opl_test::PipeOp {
 public:
  FooPipe(std::string name = "FooPipe", opl_test::PipelineDrain drain = {},
          std::unique_ptr<opl_test::SourceOp> implicit_source = nullptr)
      : PipeOp(std::move(name)),
        drain_(std::move(drain)),
        implicit_source_(std::move(implicit_source)) {}

  opl_test::PipelinePipe Pipe() override {
    return [](const opl_test::TaskContext&, opl_test::ThreadId,
              std::optional<opl_test::Batch>) -> opl_test::OpResult {
      return opl_test::OpOutput::PipeSinkNeedsMore();
    };
  }

  opl_test::PipelineDrain Drain() override { return drain_; }

  std::unique_ptr<opl_test::SourceOp> ImplicitSource() override {
    return std::move(implicit_source_);
  }

 private:
  opl_test::PipelineDrain drain_;
  std::unique_ptr<opl_test::SourceOp> implicit_source_;
};

class FooSink final : public opl_test::SinkOp {
 public:
  explicit FooSink(std::string name = "FooSink") : SinkOp(std::move(name)) {}

  opl_test::PipelineSink Sink() override {
    return [](const opl_test::TaskContext&, opl_test::ThreadId,
              std::optional<opl_test::Batch>) -> opl_test::OpResult {
      return opl_test::OpOutput::PipeSinkNeedsMore();
    };
  }

  std::vector<opl_test::TaskGroup> Frontend() override { return {}; }

  std::optional<opl_test::TaskGroup> Backend() override { return std::nullopt; }

  std::unique_ptr<opl_test::SourceOp> ImplicitSource() override { return nullptr; }
};

}  // namespace

TEST(OplPipelineCompileTest, EmptyPipeline) {
  FooSink sink;
  opl_test::Pipeline pipeline("EmptyPipeline", {}, &sink);
  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_TRUE(exec.Segments().empty());
}

TEST(OplPipelineCompileTest, SingleChannelPipeline) {
  FooSource source;
  FooPipe pipe;
  FooSink sink;

  opl_test::Pipeline pipeline(
      "SingleChannelPipeline",
      {opl_test::PipelineChannel{&source, {&pipe}}},
      &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Segments().size(), 1);

  const auto& seg0 = exec.Segments()[0];
  ASSERT_EQ(seg0.Channels().size(), 1);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  const auto& ch0 = seg0.Channels()[0];
  ASSERT_EQ(ch0.source_op, &source);
  ASSERT_EQ(ch0.pipe_ops.size(), 1);
  ASSERT_EQ(ch0.pipe_ops[0], &pipe);
}

TEST(OplPipelineCompileTest, DoubleChannelPipeline) {
  FooSource source1, source2;
  FooPipe pipe;
  FooSink sink;

  opl_test::Pipeline pipeline(
      "DoubleChannelPipeline",
      {opl_test::PipelineChannel{&source1, {&pipe}},
       opl_test::PipelineChannel{&source2, {&pipe}}},
      &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Segments().size(), 1);

  const auto& seg0 = exec.Segments()[0];
  ASSERT_EQ(seg0.Channels().size(), 2);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg0.Channels()[0].source_op, &source1);
  ASSERT_EQ(seg0.Channels()[1].source_op, &source2);
}

TEST(OplPipelineCompileTest, DoublePhysicalPipeline) {
  FooSource source;
  auto implicit_source_up = std::make_unique<FooSource>("ImplicitSource");
  auto* implicit_source = implicit_source_up.get();
  FooPipe pipe("FooPipe", {}, std::move(implicit_source_up));
  FooSink sink;

  opl_test::Pipeline pipeline("DoublePhysicalPipeline",
                              {opl_test::PipelineChannel{&source, {&pipe}}}, &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Segments().size(), 2);

  const auto& seg0 = exec.Segments()[0];
  const auto& seg1 = exec.Segments()[1];

  ASSERT_EQ(seg0.Channels().size(), 1);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg1.Channels().size(), 1);
  ASSERT_EQ(seg1.NumImplicitSources(), 1);
  ASSERT_EQ(seg1.Channels()[0].source_op, implicit_source);
  ASSERT_TRUE(seg1.Channels()[0].pipe_ops.empty());
}

TEST(OplPipelineCompileTest, DoublePhysicalDoubleChannelPipeline) {
  FooSource source1, source2;

  auto implicit_source1_up = std::make_unique<FooSource>("ImplicitSource1");
  auto implicit_source2_up = std::make_unique<FooSource>("ImplicitSource2");
  auto* implicit_source1 = implicit_source1_up.get();
  auto* implicit_source2 = implicit_source2_up.get();

  FooPipe pipe1("Pipe1", {}, std::move(implicit_source1_up));
  FooPipe pipe2("Pipe2", {}, std::move(implicit_source2_up));
  FooSink sink;

  opl_test::Pipeline pipeline(
      "DoublePhysicalDoubleChannelPipeline",
      {opl_test::PipelineChannel{&source1, {&pipe1}},
       opl_test::PipelineChannel{&source2, {&pipe2}}},
      &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Segments().size(), 2);

  const auto& seg0 = exec.Segments()[0];
  const auto& seg1 = exec.Segments()[1];

  ASSERT_EQ(seg0.Channels().size(), 2);
  ASSERT_EQ(seg0.NumImplicitSources(), 0);

  ASSERT_EQ(seg1.Channels().size(), 2);
  ASSERT_EQ(seg1.NumImplicitSources(), 2);
  ASSERT_EQ(seg1.Channels()[0].source_op, implicit_source1);
  ASSERT_EQ(seg1.Channels()[1].source_op, implicit_source2);
}

TEST(OplPipelineCompileTest, TripplePhysicalPipeline) {
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

  opl_test::Pipeline pipeline(
      "TripplePhysicalPipeline",
      {opl_test::PipelineChannel{&source1, {&pipe1, &pipe3}},
       opl_test::PipelineChannel{&source2, {&pipe2, &pipe3}}},
      &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Segments().size(), 3);

  const auto& seg0 = exec.Segments()[0];
  const auto& seg1 = exec.Segments()[1];
  const auto& seg2 = exec.Segments()[2];

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

TEST(OplPipelineCompileTest, OddQuadroStagePipeline) {
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

  opl_test::Pipeline pipeline(
      "OddQuadroStagePipeline",
      {opl_test::PipelineChannel{&source1, {&pipe1, &pipe2, &pipe4}},
       opl_test::PipelineChannel{&source2, {&pipe2, &pipe4}},
       opl_test::PipelineChannel{&source3, {&pipe3, &pipe4}},
       opl_test::PipelineChannel{&source4, {&pipe4}}},
      &sink);

  auto exec = Compile(pipeline, /*dop=*/1);
  ASSERT_EQ(exec.Segments().size(), 4);

  const auto& seg0 = exec.Segments()[0];
  const auto& seg1 = exec.Segments()[1];
  const auto& seg2 = exec.Segments()[2];
  const auto& seg3 = exec.Segments()[3];

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

}  // namespace opl

