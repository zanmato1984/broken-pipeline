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

#pragma once

#include <string>
#include <utility>
#include <vector>

#include <broken_pipeline/concepts.h>

namespace bp {

template <BrokenPipelineTraits Traits>
class SourceOp;

template <BrokenPipelineTraits Traits>
class PipeOp;

template <BrokenPipelineTraits Traits>
class SinkOp;

/// @brief A pipeline graph: (one or more channels) -> (shared sink).
///
/// A pipeline contains:
/// - `Channel`: a `SourceOp` plus a linear chain of `PipeOp`s
/// - A single shared `SinkOp`
///
/// Notes:
/// - The pipeline stores raw pointers to operators. Operator lifetime is owned by you and
///   must outlive any compilation/execution that uses the pipeline.
/// - A pipeline may be split into multiple stages if any `PipeOp` returns a
///   non-null `ImplicitSource()`.
template <BrokenPipelineTraits Traits>
class Pipeline {
 public:
  struct Channel {
    SourceOp<Traits>* source_op;
    std::vector<PipeOp<Traits>*> pipe_ops;
  };

  Pipeline(std::string name, std::vector<Channel> channels, SinkOp<Traits>* sink_op)
      : name_(std::move(name)), channels_(std::move(channels)), sink_op_(sink_op) {}

  const std::string& Name() const noexcept { return name_; }

  const std::vector<Channel>& Channels() const noexcept { return channels_; }
  SinkOp<Traits>* Sink() const noexcept { return sink_op_; }

 private:
  std::string name_;
  std::vector<Channel> channels_;
  SinkOp<Traits>* sink_op_;
};

}  // namespace bp
