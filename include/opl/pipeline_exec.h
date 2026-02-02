#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <opl/concepts.h>
#include <opl/operator.h>
#include <opl/pipeline.h>
#include <opl/task.h>

namespace opl {

template <OpenPipelineTraits Traits>
struct SourceExec {
  std::vector<TaskGroup<Traits>> frontend;
  std::optional<TaskGroup<Traits>> backend;
};

template <OpenPipelineTraits Traits>
struct SinkExec {
  std::vector<TaskGroup<Traits>> frontend;
  std::optional<TaskGroup<Traits>> backend;
};

/**
 * @brief Encapsulation of the task group that runs a pipeline segment.
 */
template <OpenPipelineTraits Traits>
class PipeExec {
 public:
  PipeExec(const std::vector<typename Pipeline<Traits>::Channel>& channels,
           SinkOp<Traits>* sink_op,
           std::size_t dop)
      : dop_(dop), runtime_(std::make_shared<Runtime>(channels, sink_op, dop_)) {}

  opl::TaskGroup<Traits> TaskGroup() const {
    Task<Traits> task(
        std::string{},
        [runtime = runtime_](const TaskContext<Traits>& ctx, TaskId task_id) {
          return (*runtime)(ctx, task_id);
        });
    return opl::TaskGroup<Traits>(std::string{}, std::move(task), dop_);
  }

 private:
  class Runtime {
   public:
    class Channel {
     public:
      Channel(const typename Pipeline<Traits>::Channel& channel,
              SinkOp<Traits>* sink_op,
              std::size_t channel_id,
              std::size_t dop,
              std::atomic_bool& cancelled)
          : channel_id_(channel_id),
            source_(channel.source_op->Source()),
            pipes_(channel.pipe_ops.size()),
            sink_(sink_op->Sink()),
            thread_locals_(dop),
            cancelled_(cancelled) {
        for (std::size_t i = 0; i < channel.pipe_ops.size(); ++i) {
          pipes_[i] = {channel.pipe_ops[i]->Pipe(), channel.pipe_ops[i]->Drain()};
        }

        std::vector<std::size_t> drains;
        for (std::size_t i = 0; i < pipes_.size(); ++i) {
          if (pipes_[i].second) {
            drains.push_back(i);
          }
        }

        for (std::size_t i = 0; i < dop; ++i) {
          thread_locals_[i].drains = drains;
        }
      }

      OpResult<Traits> operator()(const TaskContext<Traits>& task_ctx, ThreadId thread_id) {
        if (cancelled_.load()) {
          return OpResult<Traits>(OpOutput<Traits>::Cancelled());
        }

        auto& tl = thread_locals_[thread_id];

        if (tl.sinking) {
          tl.sinking = false;
          return Sink(task_ctx, thread_id, std::nullopt);
        }

        if (!tl.pipe_stack.empty()) {
          const auto pipe_id = tl.pipe_stack.back();
          tl.pipe_stack.pop_back();
          return Pipe(task_ctx, thread_id, pipe_id, std::nullopt);
        }

        if (!tl.source_done) {
          auto src_r = source_(task_ctx, thread_id);
          if (!src_r.ok()) {
            cancelled_.store(true);
            return src_r;
          }
          auto& src_out = src_r.ValueOrDie();
          if (src_out.IsBlocked()) {
            return src_r;
          }
          if (src_out.IsFinished()) {
            tl.source_done = true;
            if (src_out.GetBatch().has_value()) {
              return Pipe(task_ctx, thread_id, 0, std::move(src_out.GetBatch()));
            }
          } else {
            assert(src_out.IsSourcePipeHasMore());
            assert(src_out.GetBatch().has_value());
            return Pipe(task_ctx, thread_id, 0, std::move(src_out.GetBatch()));
          }
        }

        if (tl.draining >= tl.drains.size()) {
          return OpResult<Traits>(OpOutput<Traits>::Finished());
        }

        for (; tl.draining < tl.drains.size(); ++tl.draining) {
          const auto drain_id = tl.drains[tl.draining];
          auto& drain_fn = pipes_[drain_id].second;
          assert(static_cast<bool>(drain_fn));

          auto drain_r = drain_fn(task_ctx, thread_id);
          if (!drain_r.ok()) {
            cancelled_.store(true);
            return drain_r;
          }

          auto& drain_out = drain_r.ValueOrDie();

          if (tl.yield) {
            assert(drain_out.IsPipeYieldBack());
            tl.yield = false;
            return OpResult<Traits>(OpOutput<Traits>::PipeYieldBack());
          }

          if (drain_out.IsPipeYield()) {
            assert(!tl.yield);
            tl.yield = true;
            return OpResult<Traits>(OpOutput<Traits>::PipeYield());
          }

          if (drain_out.IsBlocked()) {
            return drain_r;
          }

          assert(drain_out.IsSourcePipeHasMore() || drain_out.IsFinished());
          if (drain_out.GetBatch().has_value()) {
            if (drain_out.IsFinished()) {
              ++tl.draining;
            }
            return Pipe(task_ctx, thread_id, drain_id + 1, std::move(drain_out.GetBatch()));
          }
        }

        return OpResult<Traits>(OpOutput<Traits>::Finished());
      }

     private:
      OpResult<Traits> Pipe(const TaskContext<Traits>& task_ctx,
                            ThreadId thread_id,
                            std::size_t pipe_id,
                            std::optional<typename Traits::Batch> input) {
        auto& tl = thread_locals_[thread_id];

        for (std::size_t i = pipe_id; i < pipes_.size(); ++i) {
          auto pipe_r = pipes_[i].first(task_ctx, thread_id, std::move(input));
          if (!pipe_r.ok()) {
            cancelled_.store(true);
            return pipe_r;
          }

          auto& out = pipe_r.ValueOrDie();

          if (tl.yield) {
            assert(out.IsPipeYieldBack());
            tl.pipe_stack.push_back(i);
            tl.yield = false;
            return OpResult<Traits>(OpOutput<Traits>::PipeYieldBack());
          }

          if (out.IsPipeYield()) {
            assert(!tl.yield);
            tl.pipe_stack.push_back(i);
            tl.yield = true;
            return OpResult<Traits>(OpOutput<Traits>::PipeYield());
          }

          if (out.IsBlocked()) {
            tl.pipe_stack.push_back(i);
            return pipe_r;
          }

          assert(out.IsPipeSinkNeedsMore() || out.IsPipeEven() || out.IsSourcePipeHasMore());

          if (out.IsPipeEven() || out.IsSourcePipeHasMore()) {
            if (out.IsSourcePipeHasMore()) {
              tl.pipe_stack.push_back(i);
            }
            assert(out.GetBatch().has_value());
            input = std::move(out.GetBatch());
            continue;
          }

          return OpResult<Traits>(OpOutput<Traits>::PipeSinkNeedsMore());
        }

        return Sink(task_ctx, thread_id, std::move(input));
      }

      OpResult<Traits> Sink(const TaskContext<Traits>& task_ctx,
                            ThreadId thread_id,
                            std::optional<typename Traits::Batch> input) {
        auto sink_r = sink_(task_ctx, thread_id, std::move(input));
        if (!sink_r.ok()) {
          cancelled_.store(true);
          return sink_r;
        }
        auto& out = sink_r.ValueOrDie();
        assert(out.IsPipeSinkNeedsMore() || out.IsBlocked());
        if (out.IsBlocked()) {
          thread_locals_[thread_id].sinking = true;
        }
        return sink_r;
      }

      std::size_t channel_id_;

      PipelineSource<Traits> source_;
      std::vector<std::pair<PipelinePipe<Traits>, PipelineDrain<Traits>>> pipes_;
      PipelineSink<Traits> sink_;

      struct ThreadLocal {
        bool sinking = false;
        std::vector<std::size_t> pipe_stack;
        bool source_done = false;
        std::vector<std::size_t> drains;
        std::size_t draining = 0;
        bool yield = false;
      };
      std::vector<ThreadLocal> thread_locals_;

      std::atomic_bool& cancelled_;
    };

    Runtime(const std::vector<typename Pipeline<Traits>::Channel>& channels,
            SinkOp<Traits>* sink_op,
            std::size_t dop)
        : cancelled_(false) {
      channels_.reserve(channels.size());
      for (std::size_t i = 0; i < channels.size(); ++i) {
        channels_.emplace_back(channels[i], sink_op, i, dop, cancelled_);
      }
      thread_locals_.reserve(dop);
      for (std::size_t i = 0; i < dop; ++i) {
        thread_locals_.emplace_back(channels_.size());
      }
    }

    Result<Traits, TaskStatus> operator()(const TaskContext<Traits>& task_ctx, TaskId task_id) {
      const ThreadId thread_id = static_cast<ThreadId>(task_id);
      if (cancelled_.load()) {
        return Result<Traits, TaskStatus>(TaskStatus::Cancelled());
      }

      bool all_finished = true;
      bool all_unfinished_blocked = true;
      std::vector<std::shared_ptr<Resumer>> blocked_resumers;

      OpResult<Traits> last_op_result(OpOutput<Traits>::PipeSinkNeedsMore());

      auto& tl = thread_locals_[thread_id];
      for (std::size_t channel_id = 0; channel_id < channels_.size(); ++channel_id) {
        if (tl.finished[channel_id]) {
          continue;
        }

        if (auto& resumer = tl.resumers[channel_id]; resumer) {
          if (resumer->IsResumed()) {
            resumer.reset();
          } else {
            blocked_resumers.push_back(resumer);
            all_finished = false;
            continue;
          }
        }

        last_op_result = channels_[channel_id](task_ctx, thread_id);
        if (!last_op_result.ok()) {
          cancelled_.store(true);
          return Result<Traits, TaskStatus>(last_op_result.status());
        }

        auto& out = last_op_result.ValueOrDie();
        if (out.IsFinished()) {
          assert(!out.GetBatch().has_value());
          tl.finished[channel_id] = true;
        } else {
          all_finished = false;
        }

        if (out.IsBlocked()) {
          tl.resumers[channel_id] = out.GetResumer();
          blocked_resumers.push_back(tl.resumers[channel_id]);
        } else {
          all_unfinished_blocked = false;
        }

        if (!out.IsFinished() && !out.IsBlocked()) {
          break;
        }
      }

      if (all_finished) {
        return Result<Traits, TaskStatus>(TaskStatus::Finished());
      }

      if (all_unfinished_blocked && !blocked_resumers.empty()) {
        auto awaiter_r = task_ctx.awaiter_factory(std::move(blocked_resumers));
        if (!awaiter_r.ok()) {
          cancelled_.store(true);
          return Result<Traits, TaskStatus>(awaiter_r.status());
        }
        auto awaiter = std::move(awaiter_r).ValueOrDie();
        return Result<Traits, TaskStatus>(TaskStatus::Blocked(std::move(awaiter)));
      }

      if (!last_op_result.ok()) {
        cancelled_.store(true);
        return Result<Traits, TaskStatus>(last_op_result.status());
      }

      const auto& out = last_op_result.ValueOrDie();
      if (out.IsPipeYield()) {
        return Result<Traits, TaskStatus>(TaskStatus::Yield());
      }
      if (out.IsCancelled()) {
        cancelled_.store(true);
        return Result<Traits, TaskStatus>(TaskStatus::Cancelled());
      }
      return Result<Traits, TaskStatus>(TaskStatus::Continue());
    }

   private:
    struct ThreadLocal {
      explicit ThreadLocal(std::size_t num_channels)
          : finished(num_channels, false), resumers(num_channels) {}

      std::vector<bool> finished;
      std::vector<std::shared_ptr<Resumer>> resumers;
    };

    std::vector<Channel> channels_;
    std::vector<ThreadLocal> thread_locals_;

    std::atomic_bool cancelled_;
  };

  std::size_t dop_;
  std::shared_ptr<Runtime> runtime_;
};

namespace detail {
template <OpenPipelineTraits Traits>
class PipelineCompiler;
}  // namespace detail

/**
 * @brief A compiled pipeline segment.
 *
 * A `PipelineExecSegment` is a small-step, multiplexed runtime over one or more channels
 * (sources feeding the shared sink). Segments are executed in sequence order by the host.
 */
template <OpenPipelineTraits Traits>
class PipelineExecSegment {
 public:
  const std::string& Name() const noexcept { return name_; }

  std::size_t Dop() const noexcept { return dop_; }

  std::vector<SourceExec<Traits>> SourceExecs() const {
    std::vector<SourceExec<Traits>> source_execs;
    source_execs.reserve(channels_.size());
    for (const auto& ch : channels_) {
      auto* src = ch.source_op;
      SourceExec<Traits> tasks;
      tasks.frontend = src->Frontend();
      tasks.backend = src->Backend();
      source_execs.push_back(std::move(tasks));
    }
    return source_execs;
  }

  opl::PipeExec<Traits> PipeExec() const { return opl::PipeExec<Traits>(channels_, sink_op_, dop_); }

 private:
  PipelineExecSegment(std::string name,
                      std::vector<typename Pipeline<Traits>::Channel> channels,
                      std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_keepalive,
                      SinkOp<Traits>* sink_op,
                      std::size_t dop)
      : name_(std::move(name)),
        channels_(std::move(channels)),
        implicit_sources_keepalive_(std::move(implicit_sources_keepalive)),
        sink_op_(sink_op),
        dop_(dop) {}

  friend class detail::PipelineCompiler<Traits>;

  std::string name_;
  std::vector<typename Pipeline<Traits>::Channel> channels_;
  std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_keepalive_;
  SinkOp<Traits>* sink_op_;
  std::size_t dop_;
};

/**
 * @brief A compiled pipeline execution plan.
 *
 * A `PipelineExec` contains:
 * - one `SinkExec` (lifecycle task groups for the sink)
 * - an ordered sequence of `PipelineExecSegment`s
 *
 * The host is responsible for orchestrating ordering between segment/source/sink task groups.
 */
template <OpenPipelineTraits Traits>
class PipelineExec {
 public:
  PipelineExec(std::string name,
               SinkExec<Traits> sink,
               std::vector<PipelineExecSegment<Traits>> segments,
               std::size_t dop)
      : name_(std::move(name)),
        sink_(std::move(sink)),
        segments_(std::move(segments)),
        dop_(dop) {}

  const std::string& Name() const noexcept { return name_; }
  std::size_t Dop() const noexcept { return dop_; }

  const SinkExec<Traits>& Sink() const noexcept { return sink_; }
  const std::vector<PipelineExecSegment<Traits>>& Segments() const noexcept { return segments_; }

 private:
  std::string name_;

  SinkExec<Traits> sink_;
  std::vector<PipelineExecSegment<Traits>> segments_;

  std::size_t dop_;
};

namespace detail {

/**
 * @brief Internal compiler that splits a `Pipeline` into an ordered list of `PipelineExecSegment`s.
 *
 * Splitting rule (current):
 * - Only pipe implicit sources (`PipeOp::ImplicitSource()`) create segment boundaries.
 * - When a pipe provides an implicit source, the downstream pipe chain becomes a new
 *   channel rooted at that implicit source in a later segment.
 */
template <OpenPipelineTraits Traits>
class PipelineCompiler {
 public:
  PipelineCompiler(const Pipeline<Traits>& pipeline, std::size_t dop)
      : pipeline_(pipeline), dop_(dop) {}

  PipelineExec<Traits> Compile() && {
    ExtractTopology();
    SortTopology();
    auto segments = BuildSegments();

    SinkExec<Traits> sink;
    sink.frontend = pipeline_.Sink()->Frontend();
    sink.backend = pipeline_.Sink()->Backend();

    return PipelineExec<Traits>(
        pipeline_.Name(), std::move(sink), std::move(segments), dop_);
  }

 private:
  void ExtractTopology() {
    std::unordered_map<PipeOp<Traits>*, SourceOp<Traits>*> pipe_source_map;

    for (auto& channel : pipeline_.Channels()) {
      std::size_t id = 0;
      topology_.emplace(channel.source_op,
                        std::pair<std::size_t, typename Pipeline<Traits>::Channel>{id++,
                                                                                   channel});
      sources_keep_order_.push_back(channel.source_op);

      for (std::size_t i = 0; i < channel.pipe_ops.size(); ++i) {
        auto* pipe = channel.pipe_ops[i];

        if (pipe_source_map.count(pipe) == 0) {
          if (auto implicit_source_up = pipe->ImplicitSource(); implicit_source_up) {
            auto* implicit_source = implicit_source_up.get();
            pipe_source_map.emplace(pipe, implicit_source);

            typename Pipeline<Traits>::Channel new_channel{
                implicit_source,
                std::vector<PipeOp<Traits>*>(channel.pipe_ops.begin() + i + 1,
                                             channel.pipe_ops.end())};

            topology_.emplace(
                implicit_source,
                std::pair<std::size_t, typename Pipeline<Traits>::Channel>{
                    id++, std::move(new_channel)});
            sources_keep_order_.push_back(implicit_source);
            implicit_sources_keepalive_.emplace(implicit_source,
                                                std::move(implicit_source_up));
          }
        } else {
          auto* implicit_source = pipe_source_map[pipe];
          if (topology_[implicit_source].first < id) {
            topology_[implicit_source].first = id++;
          }
        }
      }
    }
  }

  void SortTopology() {
    for (auto* source : sources_keep_order_) {
      auto& segment_info = topology_[source];
      if (implicit_sources_keepalive_.count(source) > 0) {
        segments_[segment_info.first].first.push_back(
            std::move(implicit_sources_keepalive_[source]));
      }
      segments_[segment_info.first].second.push_back(std::move(segment_info.second));
    }
  }

  std::vector<PipelineExecSegment<Traits>> BuildSegments() {
    std::vector<PipelineExecSegment<Traits>> segment_execs;

    if (segments_.empty()) {
      return segment_execs;
    }

    segment_execs.reserve(segments_.size());

    for (auto& [id, segment_info] : segments_) {
      auto implicit_sources = std::move(segment_info.first);
      auto pipeline_channels = std::move(segment_info.second);

      auto name = "PipelineExecSegment" + std::to_string(id) + "(" + pipeline_.Name() + ")";
      segment_execs.push_back(PipelineExecSegment<Traits>(std::move(name),
                                                          std::move(pipeline_channels),
                                                          std::move(implicit_sources),
                                                          pipeline_.Sink(),
                                                          dop_));
    }

    return segment_execs;
  }

  const Pipeline<Traits>& pipeline_;
  std::size_t dop_;

  std::unordered_map<SourceOp<Traits>*, std::pair<std::size_t, typename Pipeline<Traits>::Channel>>
      topology_;
  std::vector<SourceOp<Traits>*> sources_keep_order_;
  std::unordered_map<SourceOp<Traits>*, std::unique_ptr<SourceOp<Traits>>>
      implicit_sources_keepalive_;

  std::map<std::size_t,
           std::pair<std::vector<std::unique_ptr<SourceOp<Traits>>>,
                     std::vector<typename Pipeline<Traits>::Channel>>>
      segments_;
};

}  // namespace detail

template <OpenPipelineTraits Traits>
PipelineExec<Traits> Compile(const Pipeline<Traits>& pipeline, std::size_t dop) {
  return detail::PipelineCompiler<Traits>(pipeline, dop).Compile();
}

}  // namespace opl
