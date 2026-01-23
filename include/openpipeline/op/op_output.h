#pragma once

#include <cassert>
#include <optional>
#include <string>
#include <utility>
#include <variant>

#include <openpipeline/concepts.h>
#include <openpipeline/task/resumer.h>

namespace openpipeline::op {

template <OpenPipelineTraits Traits>
class OpOutput {
 public:
  enum class Code {
    PIPE_SINK_NEEDS_MORE,
    PIPE_EVEN,
    SOURCE_PIPE_HAS_MORE,
    BLOCKED,
    PIPE_YIELD,
    PIPE_YIELD_BACK,
    FINISHED,
    CANCELLED,
  };

  static OpOutput PipeSinkNeedsMore() { return OpOutput(Code::PIPE_SINK_NEEDS_MORE); }
  static OpOutput PipeEven(typename Traits::Batch batch) {
    return OpOutput(Code::PIPE_EVEN, std::move(batch));
  }
  static OpOutput SourcePipeHasMore(typename Traits::Batch batch) {
    return OpOutput(Code::SOURCE_PIPE_HAS_MORE, std::move(batch));
  }
  static OpOutput Blocked(task::ResumerPtr resumer) { return OpOutput(std::move(resumer)); }
  static OpOutput PipeYield() { return OpOutput(Code::PIPE_YIELD); }
  static OpOutput PipeYieldBack() { return OpOutput(Code::PIPE_YIELD_BACK); }
  static OpOutput Finished(std::optional<typename Traits::Batch> batch = std::nullopt) {
    return OpOutput(Code::FINISHED, std::move(batch));
  }
  static OpOutput Cancelled() { return OpOutput(Code::CANCELLED); }

  bool IsPipeSinkNeedsMore() const noexcept { return code_ == Code::PIPE_SINK_NEEDS_MORE; }
  bool IsPipeEven() const noexcept { return code_ == Code::PIPE_EVEN; }
  bool IsSourcePipeHasMore() const noexcept { return code_ == Code::SOURCE_PIPE_HAS_MORE; }
  bool IsBlocked() const noexcept { return code_ == Code::BLOCKED; }
  bool IsPipeYield() const noexcept { return code_ == Code::PIPE_YIELD; }
  bool IsPipeYieldBack() const noexcept { return code_ == Code::PIPE_YIELD_BACK; }
  bool IsFinished() const noexcept { return code_ == Code::FINISHED; }
  bool IsCancelled() const noexcept { return code_ == Code::CANCELLED; }

  std::optional<typename Traits::Batch>& GetBatch() {
    assert(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<typename Traits::Batch>>(payload_);
  }

  const std::optional<typename Traits::Batch>& GetBatch() const {
    assert(IsPipeEven() || IsSourcePipeHasMore() || IsFinished());
    return std::get<std::optional<typename Traits::Batch>>(payload_);
  }

  task::ResumerPtr& GetResumer() {
    assert(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  const task::ResumerPtr& GetResumer() const {
    assert(IsBlocked());
    return std::get<task::ResumerPtr>(payload_);
  }

  std::string ToString() const {
    switch (code_) {
      case Code::PIPE_SINK_NEEDS_MORE:
        return "PIPE_SINK_NEEDS_MORE";
      case Code::PIPE_EVEN:
        return "PIPE_EVEN";
      case Code::SOURCE_PIPE_HAS_MORE:
        return "SOURCE_PIPE_HAS_MORE";
      case Code::BLOCKED:
        return "BLOCKED";
      case Code::PIPE_YIELD:
        return "PIPE_YIELD";
      case Code::PIPE_YIELD_BACK:
        return "PIPE_YIELD_BACK";
      case Code::FINISHED:
        return "FINISHED";
      case Code::CANCELLED:
        return "CANCELLED";
    }
    return "UNKNOWN";
  }

 private:
  explicit OpOutput(Code code, std::optional<typename Traits::Batch> batch = std::nullopt)
      : code_(code), payload_(std::move(batch)) {}

  explicit OpOutput(task::ResumerPtr resumer)
      : code_(Code::BLOCKED), payload_(std::move(resumer)) {}

  Code code_;
  std::variant<task::ResumerPtr, std::optional<typename Traits::Batch>> payload_;
};

}  // namespace openpipeline::op
