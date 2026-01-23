#pragma once

#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include <openpipeline/common/meta.h>
#include <openpipeline/concepts.h>
#include <openpipeline/pipeline/op_output.h>
#include <openpipeline/task/task_context.h>
#include <openpipeline/task/task_group.h>

namespace openpipeline::pipeline {

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
class SourceOp : public Meta {
 public:
  using Meta::Meta;
  virtual ~SourceOp() = default;

  virtual PipelineSource<Traits> Source() = 0;
  virtual task::TaskGroups<Traits> Frontend() = 0;
  virtual std::optional<task::TaskGroup<Traits>> Backend() = 0;
};

template <OpenPipelineTraits Traits>
class PipeOp : public Meta {
 public:
  using Meta::Meta;
  virtual ~PipeOp() = default;

  virtual PipelinePipe<Traits> Pipe() = 0;
  virtual PipelineDrain<Traits> Drain() = 0;  // empty std::function means “no drain”
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;
};

template <OpenPipelineTraits Traits>
class SinkOp : public Meta {
 public:
  using Meta::Meta;
  virtual ~SinkOp() = default;

  virtual PipelineSink<Traits> Sink() = 0;
  virtual task::TaskGroups<Traits> Frontend() = 0;
  virtual std::optional<task::TaskGroup<Traits>> Backend() = 0;
  virtual std::unique_ptr<SourceOp<Traits>> ImplicitSource() = 0;
};

}  // namespace openpipeline::pipeline

