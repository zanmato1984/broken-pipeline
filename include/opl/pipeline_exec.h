#pragma once

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <opl/concepts.h>
#include <opl/operator.h>
#include <opl/pipeline.h>
#include <opl/task.h>

namespace opl {

namespace detail {

template <OpenPipelineTraits Traits>
struct ChannelSpec {
  SourceOp<Traits>* source_op = nullptr;
  std::vector<PipeOp<Traits>*> pipe_ops;
  SinkOp<Traits>* sink_op = nullptr;
};

template <OpenPipelineTraits Traits>
class PipelineTask;

}  // namespace detail

template <OpenPipelineTraits Traits>
struct SourceExec {
  SourceOp<Traits>* op = nullptr;
  std::vector<TaskGroup<Traits>> frontend;
  std::optional<TaskGroup<Traits>> backend;
};

template <OpenPipelineTraits Traits>
struct SinkExec {
  SinkOp<Traits>* op = nullptr;
  std::vector<TaskGroup<Traits>> frontend;
  std::optional<TaskGroup<Traits>> backend;
};

/**
 * @brief Encapsulation of the task group that runs a stage pipe task.
 *
 * A `PipeExec` wraps the internal small-step stage driver (`detail::PipelineTask`) into a
 * schedulable `TaskGroup`.
 */
template <OpenPipelineTraits Traits>
class PipeExec {
 public:
  PipeExec(std::string name,
           std::string desc,
           std::shared_ptr<detail::PipelineTask<Traits>> pipeline_task,
           std::size_t dop)
      : name_(std::move(name)),
        desc_(std::move(desc)),
        pipeline_task_(std::move(pipeline_task)),
        task_group_(MakePipeTaskGroup(name_, desc_, pipeline_task_, dop)) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  const opl::TaskGroup<Traits>& TaskGroup() const noexcept { return task_group_; }

  Result<Traits, TaskStatus> operator()(const TaskContext<Traits>& task_ctx, TaskId task_id) {
    return (*pipeline_task_)(task_ctx, task_id);
  }

 private:
  static opl::TaskGroup<Traits> MakePipeTaskGroup(
      const std::string& name,
      const std::string& desc,
      const std::shared_ptr<detail::PipelineTask<Traits>>& pipeline_task,
      std::size_t dop) {
    Task<Traits> task(
        name,
        desc,
        [pipeline_task](const TaskContext<Traits>& ctx, TaskId task_id) {
          return (*pipeline_task)(ctx, task_id);
        });
    return opl::TaskGroup<Traits>(name, desc, std::move(task), dop);
  }

  std::string name_;
  std::string desc_;
  std::shared_ptr<detail::PipelineTask<Traits>> pipeline_task_;
  opl::TaskGroup<Traits> task_group_;
};

/**
 * @brief A compiled pipeline stage.
 *
 * A `Stage` is a small-step, multiplexed runtime over one or more channels
 * (sources feeding the shared sink). Stages are executed in sequence order by the host.
 */
template <OpenPipelineTraits Traits>
class Stage {
 public:
  Stage(std::string name,
        std::vector<detail::ChannelSpec<Traits>> channels,
        std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources,
        std::size_t dop)
      : name_(std::move(name)),
        desc_(Explain(channels)),
        dop_(dop),
        pipeline_task_(std::make_shared<detail::PipelineTask<Traits>>(channels,
                                                                      dop_,
                                                                      std::move(implicit_sources))),
        pipe_(name_, desc_, pipeline_task_, dop_) {
    CollectSources(channels);
  }

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  std::size_t Dop() const noexcept { return dop_; }

  const std::vector<SourceExec<Traits>>& Sources() const noexcept { return sources_; }
  const PipeExec<Traits>& Pipe() const noexcept { return pipe_; }

 private:
  static std::string Explain(const std::vector<detail::ChannelSpec<Traits>>& channels) {
    std::stringstream ss;
    for (std::size_t i = 0; i < channels.size(); ++i) {
      if (i > 0) {
        ss << '\n';
      }
      ss << "Channel" << i << ": " << channels[i].source_op->Name() << " -> ";
      for (std::size_t j = 0; j < channels[i].pipe_ops.size(); ++j) {
        ss << channels[i].pipe_ops[j]->Name() << " -> ";
      }
      ss << channels[i].sink_op->Name();
    }
    return ss.str();
  }

  static TaskGroup<Traits> KeepAliveTaskGroup(TaskGroup<Traits> group,
                                              const std::shared_ptr<detail::PipelineTask<Traits>>&
                                                  pipeline_task) {
    auto original_task = group.GetTask();
    Task<Traits> task(
        original_task.Name(),
        original_task.Desc(),
        [pipeline_task, original_task](const TaskContext<Traits>& ctx, TaskId task_id) {
          (void)pipeline_task;
          return original_task(ctx, task_id);
        },
        original_task.Hint());

    std::optional<Continuation<Traits>> cont = std::nullopt;
    if (group.GetContinuation().has_value()) {
      auto original_cont = *group.GetContinuation();
      cont = Continuation<Traits>(
          original_cont.Name(),
          original_cont.Desc(),
          [pipeline_task, original_cont](const TaskContext<Traits>& ctx) {
            (void)pipeline_task;
            return original_cont(ctx);
          },
          original_cont.Hint());
    }

    typename TaskGroup<Traits>::NotifyFinishFunc notify =
        [pipeline_task, group](const TaskContext<Traits>& ctx) -> Status<Traits> {
      (void)pipeline_task;
      return group.NotifyFinish(ctx);
    };

    return TaskGroup<Traits>(group.Name(),
                             group.Desc(),
                             std::move(task),
                             group.NumTasks(),
                             std::move(cont),
                             std::move(notify));
  }

  void CollectSources(const std::vector<detail::ChannelSpec<Traits>>& channels) {
    std::vector<SourceOp<Traits>*> seen_sources;
    for (const auto& ch : channels) {
      auto* src = ch.source_op;
      bool already_seen = false;
      for (auto* seen : seen_sources) {
        if (seen == src) {
          already_seen = true;
          break;
        }
      }
      if (already_seen) {
        continue;
      }
      seen_sources.push_back(src);

      SourceExec<Traits> tasks;
      tasks.op = src;

      tasks.frontend = src->Frontend();
      for (auto& tg : tasks.frontend) {
        tg = KeepAliveTaskGroup(std::move(tg), pipeline_task_);
      }

      tasks.backend = src->Backend();
      if (tasks.backend.has_value()) {
        *tasks.backend = KeepAliveTaskGroup(std::move(*tasks.backend), pipeline_task_);
      }

      sources_.push_back(std::move(tasks));
    }
  }

  std::string name_;
  std::string desc_;

  std::vector<SourceExec<Traits>> sources_;

  std::size_t dop_;
  std::shared_ptr<detail::PipelineTask<Traits>> pipeline_task_;
  PipeExec<Traits> pipe_;
};

/**
 * @brief A compiled pipeline execution plan.
 *
 * A `PipelineExec` contains:
 * - one `SinkExec` (lifecycle task groups for the sink)
 * - an ordered sequence of `Stage` objects (each with its own `PipeExec` + `SourceExec`s)
 *
 * The host is responsible for orchestrating ordering between stage/source/sink task groups.
 */
template <OpenPipelineTraits Traits>
class PipelineExec {
 public:
  PipelineExec(std::string name,
               std::string desc,
               SinkExec<Traits> sink,
               std::vector<Stage<Traits>> stages,
               std::size_t dop)
      : name_(std::move(name)),
        desc_(std::move(desc)),
        sink_(std::move(sink)),
        stages_(std::move(stages)),
        dop_(dop) {}

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }
  std::size_t Dop() const noexcept { return dop_; }

  const SinkExec<Traits>& Sink() const noexcept { return sink_; }
  const std::vector<Stage<Traits>>& Stages() const noexcept { return stages_; }

 private:
  std::string name_;
  std::string desc_;

  SinkExec<Traits> sink_;
  std::vector<Stage<Traits>> stages_;

  std::size_t dop_;
};

namespace detail {

/**
 * @brief Internal small-step pipeline task for one compiled stage.
 *
 * This is the direct analogue of Ara's `PipelineTask`: it multiplexes across N channels
 * (sources feeding the same sink), handles drain/yield/blocked semantics, and maintains
 * per-thread (per-lane) state.
 */
template <OpenPipelineTraits Traits>
class PipelineTask {
 public:
  class Channel {
   public:
    Channel(const ChannelSpec<Traits>& channel,
            std::size_t channel_id,
            std::size_t dop,
            std::atomic_bool& cancelled)
        : channel_id_(channel_id),
          dop_(dop),
          source_(channel.source_op->Source()),
          pipes_(channel.pipe_ops.size()),
          sink_(channel.sink_op->Sink()),
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
    std::size_t dop_;

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

  PipelineTask(const std::vector<ChannelSpec<Traits>>& channels,
               std::size_t dop,
               std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_keepalive)
      : implicit_sources_keepalive_(std::move(implicit_sources_keepalive)),
        dop_(dop),
        cancelled_(false) {
    channels_.reserve(channels.size());
    for (std::size_t i = 0; i < channels.size(); ++i) {
      channels_.emplace_back(channels[i], i, dop_, cancelled_);
    }
    thread_locals_.reserve(dop_);
    for (std::size_t i = 0; i < dop_; ++i) {
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

  std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_keepalive_;
  std::size_t dop_;

  std::vector<Channel> channels_;
  std::vector<ThreadLocal> thread_locals_;

  std::atomic_bool cancelled_;
};

/**
 * @brief Internal compiler that splits a `Pipeline` into an ordered list of `Stage`s.
 *
 * Splitting rule (current):
 * - Only pipe implicit sources (`PipeOp::ImplicitSource()`) create stage boundaries.
 * - When a pipe provides an implicit source, the downstream pipe chain becomes a new
 *   channel rooted at that implicit source in a later stage.
 */
template <OpenPipelineTraits Traits>
class PipelineCompiler {
 public:
  PipelineCompiler(const Pipeline<Traits>& pipeline, std::size_t dop)
      : pipeline_(pipeline), dop_(dop) {}

  PipelineExec<Traits> Compile() && {
    ExtractTopology();
    SortTopology();
    auto stages = BuildStages();

    SinkExec<Traits> sink;
    sink.op = pipeline_.Sink();
    sink.frontend = sink.op->Frontend();
    sink.backend = sink.op->Backend();

    return PipelineExec<Traits>(pipeline_.Name(), pipeline_.Desc(), std::move(sink), std::move(stages), dop_);
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
      auto& stage_info = topology_[source];
      if (implicit_sources_keepalive_.count(source) > 0) {
        stages_[stage_info.first].first.push_back(
            std::move(implicit_sources_keepalive_[source]));
      }
      stages_[stage_info.first].second.push_back(std::move(stage_info.second));
    }
  }

  std::vector<Stage<Traits>> BuildStages() {
    std::vector<Stage<Traits>> stage_execs;

    if (stages_.empty()) {
      return stage_execs;
    }

    stage_execs.reserve(stages_.size());

    for (auto& [id, stage_info] : stages_) {
      auto sources_keepalive = std::move(stage_info.first);
      auto pipeline_channels = std::move(stage_info.second);

      std::vector<ChannelSpec<Traits>> stage_channels(pipeline_channels.size());
      std::transform(
          pipeline_channels.begin(),
          pipeline_channels.end(),
          stage_channels.begin(),
          [&](auto& channel) -> ChannelSpec<Traits> {
            return {channel.source_op, std::move(channel.pipe_ops), pipeline_.Sink()};
          });

      auto name = "Stage" + std::to_string(id) + "(" + pipeline_.Name() + ")";
      stage_execs.emplace_back(std::move(name),
                               std::move(stage_channels),
                               std::move(sources_keepalive),
                               dop_);
    }

    return stage_execs;
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
      stages_;
};

}  // namespace detail

template <OpenPipelineTraits Traits>
PipelineExec<Traits> Compile(const Pipeline<Traits>& pipeline, std::size_t dop) {
  return detail::PipelineCompiler<Traits>(pipeline, dop).Compile();
}

}  // namespace opl
