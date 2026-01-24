#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arrow/array/builder_primitive.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/schema.h>
#include <arrow/status.h>
#include <arrow/type.h>

#include <openpipeline/openpipeline.h>

#include "arrow_traits.h"

namespace opl_arrow {

using Batch = Traits::Batch;
using TaskContext = openpipeline::TaskContext<Traits>;
using OpOutput = openpipeline::OpOutput<Traits>;
using OpResult = openpipeline::OpResult<Traits>;
using PipelineSource = openpipeline::PipelineSource<Traits>;
using PipelineDrain = openpipeline::PipelineDrain<Traits>;
using PipelinePipe = openpipeline::PipelinePipe<Traits>;
using PipelineSink = openpipeline::PipelineSink<Traits>;

inline arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeInt32Batch(
    const std::shared_ptr<arrow::Schema>& schema, std::int32_t start, std::int32_t length) {
  arrow::Int32Builder b;
  for (std::int32_t i = 0; i < length; ++i) {
    auto st = b.Append(start + i);
    if (!st.ok()) {
      return st;
    }
  }

  std::shared_ptr<arrow::Array> arr;
  auto st = b.Finish(&arr);
  if (!st.ok()) {
    return st;
  }

  return arrow::RecordBatch::Make(schema, arr->length(), {std::move(arr)});
}

class BatchesSource final : public openpipeline::SourceOp<Traits> {
 public:
  explicit BatchesSource(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
      : SourceOp("BatchesSource", "Emit a fixed list of RecordBatches"),
        batches_(std::move(batches)),
        next_(0) {}

  PipelineSource Source() override {
    return [this](const TaskContext& /*ctx*/, openpipeline::ThreadId /*thread_id*/) -> OpResult {
      const std::size_t idx = next_.fetch_add(1);
      if (idx >= batches_.size()) {
        return OpResult(OpOutput::Finished());
      }
      return OpResult(OpOutput::SourcePipeHasMore(batches_[idx]));
    };
  }

  openpipeline::TaskGroups<Traits> Frontend() override { return {}; }

  std::optional<openpipeline::TaskGroup<Traits>> Backend() override {
    return std::nullopt;
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::atomic<std::size_t> next_;
};

class PassThroughPipe final : public openpipeline::PipeOp<Traits> {
 public:
  PassThroughPipe() : PipeOp("PassThroughPipe", "Forward input batches unchanged") {}

  PipelinePipe Pipe() override {
    return [](const TaskContext& /*ctx*/, openpipeline::ThreadId /*thread_id*/,
              std::optional<Batch> input) -> OpResult {
      if (!input.has_value()) {
        return OpResult(OpOutput::PipeSinkNeedsMore());
      }
      return OpResult(OpOutput::PipeEven(std::move(*input)));
    };
  }

  PipelineDrain Drain() override { return {}; }

  std::unique_ptr<openpipeline::SourceOp<Traits>> ImplicitSource() override {
    return nullptr;
  }
};

class RowCountSink final : public openpipeline::SinkOp<Traits> {
 public:
  RowCountSink() : SinkOp("RowCountSink", "Count rows across all batches") {}

  PipelineSink Sink() override {
    return [this](const TaskContext& /*ctx*/, openpipeline::ThreadId /*thread_id*/,
                  std::optional<Batch> input) -> OpResult {
      if (input.has_value() && *input) {
        total_rows_.fetch_add(static_cast<std::size_t>((*input)->num_rows()));
      }
      return OpResult(OpOutput::PipeSinkNeedsMore());
    };
  }

  openpipeline::TaskGroups<Traits> Frontend() override { return {}; }

  std::optional<openpipeline::TaskGroup<Traits>> Backend() override {
    return std::nullopt;
  }

  std::unique_ptr<openpipeline::SourceOp<Traits>> ImplicitSource() override {
    return nullptr;
  }

  std::size_t TotalRows() const { return total_rows_.load(); }

 private:
  std::atomic<std::size_t> total_rows_{0};
};

}  // namespace opl_arrow
