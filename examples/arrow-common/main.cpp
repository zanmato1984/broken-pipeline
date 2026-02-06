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

#include <arrow/api.h>

#include <broken_pipeline/schedule/naive_parallel_scheduler.h>
#include <broken_pipeline/schedule/traits.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>

#ifndef EXAMPLE_PIPELINE_NAME
#define EXAMPLE_PIPELINE_NAME "arrow-example"
#endif

namespace example {

using bp::schedule::Batch;
using bp::schedule::OpOutput;
using bp::schedule::OpResult;
using bp::schedule::PipeOp;
using bp::schedule::Pipeline;
using bp::schedule::PipelineChannel;
using bp::schedule::PipelineDrain;
using bp::schedule::PipelinePipe;
using bp::schedule::PipelineSink;
using bp::schedule::PipelineSource;
using bp::schedule::SinkOp;
using bp::schedule::SourceOp;
using bp::schedule::Status;
using bp::schedule::TaskContext;
using bp::schedule::TaskGroup;
using bp::schedule::ThreadId;
using bp::schedule::NaiveParallelScheduler;
using Traits = bp::schedule::Traits;
using PipelineExec = bp::PipelineExec<Traits>;

bp::schedule::Result<Batch> MakeSingleRowBatch(const std::shared_ptr<arrow::Schema>& schema,
                                               int32_t value) {
  arrow::Int32Builder builder;
  auto status = builder.Append(value);
  if (!status.ok()) {
    return status;
  }
  std::shared_ptr<arrow::Array> array;
  status = builder.Finish(&array);
  if (!status.ok()) {
    return status;
  }
  return arrow::RecordBatch::Make(schema, 1, {std::move(array)});
}

class SingleRowSource final : public SourceOp {
 public:
  explicit SingleRowSource(Batch batch)
      : SourceOp("SingleRowSource"), batch_(std::move(batch)) {}

  PipelineSource Source() override {
    return [this](const TaskContext&, ThreadId) -> OpResult {
      if (done_) {
        return OpOutput::Finished();
      }
      done_ = true;
      return OpOutput::Finished(std::move(batch_));
    };
  }

  std::vector<TaskGroup> Frontend() override { return {}; }

  std::optional<TaskGroup> Backend() override { return std::nullopt; }

 private:
  Batch batch_;
  bool done_ = false;
};

class PassthroughPipe final : public PipeOp {
 public:
  PassthroughPipe() : PipeOp("PassthroughPipe") {}

  PipelinePipe Pipe() override {
    return [](const TaskContext&, ThreadId, std::optional<Batch> input) -> OpResult {
      if (!input.has_value()) {
        return OpOutput::PipeSinkNeedsMore();
      }
      return OpOutput::PipeEven(std::move(*input));
    };
  }

  PipelineDrain Drain() override { return {}; }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }
};

class PrintSink final : public SinkOp {
 public:
  PrintSink() : SinkOp("PrintSink") {}

  PipelineSink Sink() override {
    return [this](const TaskContext&, ThreadId, std::optional<Batch> input) -> OpResult {
      if (!input.has_value()) {
        return OpOutput::PipeSinkNeedsMore();
      }

      const auto& batch = *input;
      if (batch->num_columns() != 1) {
        return Status::Invalid("PrintSink expects one column");
      }
      if (batch->num_rows() != 1) {
        return Status::Invalid("PrintSink expects one row");
      }
      if (batch->column(0)->type_id() != arrow::Type::INT32) {
        return Status::Invalid("PrintSink expects int32 column");
      }

      auto values = std::static_pointer_cast<arrow::Int32Array>(batch->column(0));
      if (values->IsNull(0)) {
        return Status::Invalid("PrintSink expects non-null value");
      }
      value_ = values->Value(0);
      std::cout << "Output value: " << *value_ << "\n";
      return OpOutput::PipeSinkNeedsMore();
    };
  }

  std::vector<TaskGroup> Frontend() override { return {}; }

  std::optional<TaskGroup> Backend() override { return std::nullopt; }

  std::unique_ptr<SourceOp> ImplicitSource() override { return nullptr; }

  std::optional<int32_t> value() const { return value_; }

 private:
  std::optional<int32_t> value_;
};

Status RunTaskGroup(const NaiveParallelScheduler& scheduler, const TaskGroup& group,
                    const void* context) {
  auto result = scheduler.ScheduleAndWait(group, context);
  if (!result.ok()) {
    return result.status();
  }
  if (result->IsCancelled()) {
    return Status::Cancelled("TaskGroup cancelled");
  }
  if (!result->IsFinished()) {
    return Status::Invalid("Unexpected TaskGroup status: " + result->ToString());
  }
  return Status::OK();
}

Status RunPipeline(const PipelineExec& exec, NaiveParallelScheduler& scheduler,
                   const void* context) {
  if (exec.Sink().backend.has_value()) {
    auto status = RunTaskGroup(scheduler, *exec.Sink().backend, context);
    if (!status.ok()) {
      return status;
    }
  }

  for (const auto& pipelinexe : exec.Pipelinexes()) {
    auto source_execs = pipelinexe.SourceExecs();
    for (auto& src_exec : source_execs) {
      for (const auto& group : src_exec.frontend) {
        auto status = RunTaskGroup(scheduler, group, context);
        if (!status.ok()) {
          return status;
        }
      }

      if (src_exec.backend.has_value()) {
        auto status = RunTaskGroup(scheduler, *src_exec.backend, context);
        if (!status.ok()) {
          return status;
        }
      }
    }

    auto group = pipelinexe.PipeExec().TaskGroup();
    auto status = RunTaskGroup(scheduler, group, context);
    if (!status.ok()) {
      return status;
    }
  }

  for (const auto& group : exec.Sink().frontend) {
    auto status = RunTaskGroup(scheduler, group, context);
    if (!status.ok()) {
      return status;
    }
  }

  return Status::OK();
}

}  // namespace example

int main() {
  using namespace example;

  auto schema = arrow::schema({arrow::field("value", arrow::int32())});
  auto batch_r = MakeSingleRowBatch(schema, 42);
  if (!batch_r.ok()) {
    std::cerr << batch_r.status().ToString() << "\n";
    return 1;
  }

  SingleRowSource source(batch_r.ValueOrDie());
  PassthroughPipe pipe;
  PrintSink sink;

  Pipeline pipeline(EXAMPLE_PIPELINE_NAME, {PipelineChannel{&source, {&pipe}}}, &sink);

  std::size_t dop = 1;
  auto exec = bp::schedule::Compile(pipeline, dop);

  NaiveParallelScheduler scheduler;
  int context = 0;
  auto status = RunPipeline(exec, scheduler, &context);
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }

  return 0;
}
