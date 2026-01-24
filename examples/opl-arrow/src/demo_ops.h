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

class BatchesSource final : public openpipeline::op::SourceOp<Traits> {
 public:
  explicit BatchesSource(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
      : SourceOp("BatchesSource", "Emit a fixed list of RecordBatches"),
        batches_(std::move(batches)),
        next_(0) {}

  openpipeline::op::PipelineSource<Traits> Source() override {
    return [this](const openpipeline::task::TaskContext<Traits>& /*ctx*/,
                  openpipeline::ThreadId /*thread_id*/) -> openpipeline::op::OpResult<Traits> {
      const std::size_t idx = next_.fetch_add(1);
      if (idx >= batches_.size()) {
        return Traits::Ok(openpipeline::op::OpOutput<Traits>::Finished());
      }
      return Traits::Ok(openpipeline::op::OpOutput<Traits>::SourcePipeHasMore(batches_[idx]));
    };
  }

  openpipeline::task::TaskGroups<Traits> Frontend() override { return {}; }

  std::optional<openpipeline::task::TaskGroup<Traits>> Backend() override {
    return std::nullopt;
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::atomic<std::size_t> next_;
};

class PassThroughPipe final : public openpipeline::op::PipeOp<Traits> {
 public:
  PassThroughPipe() : PipeOp("PassThroughPipe", "Forward input batches unchanged") {}

  openpipeline::op::PipelinePipe<Traits> Pipe() override {
    return [](const openpipeline::task::TaskContext<Traits>& /*ctx*/,
              openpipeline::ThreadId /*thread_id*/,
              std::optional<Traits::Batch> input) -> openpipeline::op::OpResult<Traits> {
      if (!input.has_value()) {
        return Traits::Ok(openpipeline::op::OpOutput<Traits>::PipeSinkNeedsMore());
      }
      return Traits::Ok(openpipeline::op::OpOutput<Traits>::PipeEven(std::move(*input)));
    };
  }

  openpipeline::op::PipelineDrain<Traits> Drain() override { return {}; }

  std::unique_ptr<openpipeline::op::SourceOp<Traits>> ImplicitSource() override {
    return nullptr;
  }
};

class RowCountSink final : public openpipeline::op::SinkOp<Traits> {
 public:
  RowCountSink() : SinkOp("RowCountSink", "Count rows across all batches") {}

  openpipeline::op::PipelineSink<Traits> Sink() override {
    return [this](const openpipeline::task::TaskContext<Traits>& /*ctx*/,
                  openpipeline::ThreadId /*thread_id*/,
                  std::optional<Traits::Batch> input) -> openpipeline::op::OpResult<Traits> {
      if (input.has_value() && *input) {
        total_rows_.fetch_add(static_cast<std::size_t>((*input)->num_rows()));
      }
      return Traits::Ok(openpipeline::op::OpOutput<Traits>::PipeSinkNeedsMore());
    };
  }

  openpipeline::task::TaskGroups<Traits> Frontend() override { return {}; }

  std::optional<openpipeline::task::TaskGroup<Traits>> Backend() override {
    return std::nullopt;
  }

  std::unique_ptr<openpipeline::op::SourceOp<Traits>> ImplicitSource() override {
    return nullptr;
  }

  std::size_t TotalRows() const { return total_rows_.load(); }

 private:
  std::atomic<std::size_t> total_rows_{0};
};

}  // namespace opl_arrow
