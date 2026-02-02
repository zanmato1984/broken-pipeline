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

#include "arrow_traits.h"

namespace opl_arrow {

inline Result<Batch> MakeInt32Batch(const std::shared_ptr<arrow::Schema>& schema,
                                    std::int32_t start, std::int32_t length) {
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

class BatchesSource final : public SourceOp {
 public:
  explicit BatchesSource(std::vector<std::shared_ptr<arrow::RecordBatch>> batches)
      : SourceOp("BatchesSource"), batches_(std::move(batches)), next_(0) {}

  PipelineSource Source() override {
    return [this](const TaskContext& /*ctx*/, ThreadId /*thread_id*/) -> OpResult {
      if (!frontend_finished_.load()) {
        return OpResult(
            arrow::Status::Invalid("BatchesSource Frontend() must run before Source()"));
      }

      const std::size_t idx = next_.fetch_add(1);
      if (idx >= batches_.size()) {
        return OpResult(OpOutput::Finished());
      }
      return OpResult(OpOutput::SourcePipeHasMore(batches_[idx]));
    };
  }

  std::vector<TaskGroup> Frontend() override {
    frontend_started_.store(false);
    frontend_finished_.store(false);

    Task task(
        "BatchesSource.Frontend",
        [this](const TaskContext& /*ctx*/, TaskId /*task_id*/) -> Result<TaskStatus> {
          // Demonstrate TaskStatus::Yield() and TaskHint::IO for IO-heavy work.
          if (!frontend_started_.exchange(true)) {
            return TaskStatus::Yield();
          }
          frontend_finished_.store(true);
          return TaskStatus::Finished();
        },
        TaskHint{TaskHint::Type::IO});

    TaskGroup group("BatchesSource.Frontend", std::move(task), /*num_tasks=*/1);
    return {std::move(group)};
  }

  std::optional<TaskGroup> Backend() override {
    backend_started_.store(false);
    backend_finished_.store(false);

    Task task(
        "BatchesSource.Backend",
        [this](const TaskContext& /*ctx*/, TaskId /*task_id*/) -> Result<TaskStatus> {
          if (!backend_started_.exchange(true)) {
            return TaskStatus::Yield();
          }
          backend_finished_.store(true);
          return TaskStatus::Finished();
        },
        TaskHint{TaskHint::Type::IO});

    return TaskGroup("BatchesSource.Backend", std::move(task), /*num_tasks=*/1);
  }

  bool FrontendFinished() const { return frontend_finished_.load(); }
  bool BackendFinished() const { return backend_finished_.load(); }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
  std::atomic<std::size_t> next_;

  std::atomic_bool frontend_started_{false};
  std::atomic_bool frontend_finished_{false};
  std::atomic_bool backend_started_{false};
  std::atomic_bool backend_finished_{false};
};

class PassThroughPipe final : public PipeOp {
 public:
  PassThroughPipe() : PipeOp("PassThroughPipe") {}

  PipelinePipe Pipe() override {
    return [](const TaskContext& /*ctx*/, ThreadId /*thread_id*/,
              std::optional<Batch> input) -> OpResult {
      if (!input.has_value()) {
        return OpResult(OpOutput::PipeSinkNeedsMore());
      }
      return OpResult(OpOutput::PipeEven(std::move(*input)));
    };
  }

  PipelineDrain Drain() override { return {}; }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
};

// A pipe that uses Drain() to flush the last buffered batch at end-of-stream.
// This demonstrates the "tail output" interface: the pipeline driver calls Drain() only
// after observing Finished() from the upstream source.
class DelayLastBatchPipe final : public PipeOp {
 public:
  explicit DelayLastBatchPipe(std::size_t dop)
      : PipeOp("DelayLastBatchPipe"), pending_(dop) {}

  PipelinePipe Pipe() override {
    return [this](const TaskContext& /*ctx*/, ThreadId thread_id,
                  std::optional<Batch> input) -> OpResult {
      if (!input.has_value()) {
        return OpResult(OpOutput::PipeSinkNeedsMore());
      }

      auto& pending = pending_[thread_id];
      if (pending.has_value()) {
        auto out = std::move(*pending);
        pending = std::move(*input);
        return OpResult(OpOutput::PipeEven(std::move(out)));
      }

      pending = std::move(*input);
      return OpResult(OpOutput::PipeSinkNeedsMore());
    };
  }

  PipelineDrain Drain() override {
    return [this](const TaskContext& /*ctx*/, ThreadId thread_id) -> OpResult {
      auto& pending = pending_[thread_id];
      if (!pending.has_value()) {
        return OpResult(OpOutput::Finished());
      }

      drained_batches_.fetch_add(1);
      auto out = std::move(*pending);
      pending.reset();
      return OpResult(OpOutput::Finished(std::optional<Batch>(std::move(out))));
    };
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

  std::size_t DrainedBatches() const { return drained_batches_.load(); }

 private:
  std::vector<std::optional<Batch>> pending_;
  std::atomic<std::size_t> drained_batches_{0};
};

class RowCountSink final : public SinkOp {
 public:
  RowCountSink() : SinkOp("RowCountSink") {}

  PipelineSink Sink() override {
    return [this](const TaskContext& /*ctx*/, ThreadId /*thread_id*/,
                  std::optional<Batch> input) -> OpResult {
      if (input.has_value() && *input) {
        total_rows_.fetch_add(static_cast<std::size_t>((*input)->num_rows()));
      }
      return OpResult(OpOutput::PipeSinkNeedsMore());
    };
  }

  std::vector<TaskGroup> Frontend() override {
    frontend_started_.store(false);
    frontend_finished_.store(false);

    Task task(
        "RowCountSink.Frontend",
        [this](const TaskContext& /*ctx*/, TaskId /*task_id*/) -> Result<TaskStatus> {
          if (!frontend_started_.exchange(true)) {
            return TaskStatus::Yield();
          }
          frontend_finished_.store(true);
          return TaskStatus::Finished();
        },
        TaskHint{TaskHint::Type::IO});

    TaskGroup group("RowCountSink.Frontend", std::move(task), /*num_tasks=*/1);
    return {std::move(group)};
  }

  std::optional<TaskGroup> Backend() override {
    backend_started_.store(false);
    backend_finished_.store(false);

    Task task(
        "RowCountSink.Backend",
        [this](const TaskContext& /*ctx*/, TaskId /*task_id*/) -> Result<TaskStatus> {
          if (!backend_started_.exchange(true)) {
            return TaskStatus::Yield();
          }
          backend_finished_.store(true);
          return TaskStatus::Finished();
        },
        TaskHint{TaskHint::Type::IO});

    return TaskGroup("RowCountSink.Backend", std::move(task), /*num_tasks=*/1);
  }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

  std::size_t TotalRows() const { return total_rows_.load(); }
  bool FrontendFinished() const { return frontend_finished_.load(); }
  bool BackendFinished() const { return backend_finished_.load(); }

 private:
  std::atomic<std::size_t> total_rows_{0};
  std::atomic_bool frontend_started_{false};
  std::atomic_bool frontend_finished_{false};
  std::atomic_bool backend_started_{false};
  std::atomic_bool backend_finished_{false};
};

}  // namespace opl_arrow
