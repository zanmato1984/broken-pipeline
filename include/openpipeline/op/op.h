#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/op/op_output.h>
#include <openpipeline/task/task_context.h>
#include <openpipeline/task/task_group.h>

namespace openpipeline::op {

template <OpenPipelineTraits Traits>
using OpResult = Result<Traits, OpOutput<Traits>>;

template <OpenPipelineTraits Traits>
using PipelineSource =
    std::function<OpResult<Traits>(const task::TaskContext<Traits>&, typename Traits::ThreadId)>;

template <OpenPipelineTraits Traits>
using PipelinePipe = std::function<OpResult<Traits>(const task::TaskContext<Traits>&,
                                                    typename Traits::ThreadId,
                                                    std::optional<typename Traits::Batch>)>;

template <OpenPipelineTraits Traits>
using PipelineDrain =
    std::function<OpResult<Traits>(const task::TaskContext<Traits>&, typename Traits::ThreadId)>;

template <OpenPipelineTraits Traits>
using PipelineSink = PipelinePipe<Traits>;

template <OpenPipelineTraits Traits>
class SourceOp {
 public:
  SourceOp(std::string name = {}, std::string desc = {})
      : name_(std::move(name)), desc_(std::move(desc)) {}
  virtual ~SourceOp() = default;

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  virtual PipelineSource<Traits> Source() = 0;
  virtual task::TaskGroups<Traits> Frontend() = 0;
  virtual std::optional<task::TaskGroup<Traits>> Backend() = 0;

 private:
  std::string name_;
  std::string desc_;
};

template <OpenPipelineTraits Traits>
class PipeOp {
 public:
  PipeOp(std::string name = {}, std::string desc = {})
      : name_(std::move(name)), desc_(std::move(desc)) {}
  virtual ~PipeOp() = default;

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  virtual PipelinePipe<Traits> Pipe() = 0;
  virtual PipelineDrain<Traits> Drain() = 0;  // empty std::function means “no drain”
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;

 private:
  std::string name_;
  std::string desc_;
};

template <OpenPipelineTraits Traits>
class SinkOp {
 public:
  SinkOp(std::string name = {}, std::string desc = {})
      : name_(std::move(name)), desc_(std::move(desc)) {}
  virtual ~SinkOp() = default;

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  virtual PipelineSink<Traits> Sink() = 0;
  virtual task::TaskGroups<Traits> Frontend() = 0;
  virtual std::optional<task::TaskGroup<Traits>> Backend() = 0;
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;

 private:
  std::string name_;
  std::string desc_;
};

}  // namespace openpipeline::op
