#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <opl/concepts.h>
#include <opl/op.h>
#include <opl/task.h>

namespace opl {

/**
 * @brief Generic small-step pipeline runtime + stage metadata.
 *
 * `PipelineExec` is a reference engine for driving one compiled pipeline stage.
 *
 * Key properties:
 * - **Small-step**: one invocation performs bounded work and returns `TaskStatus` to let a
 *   scheduler decide when/where to run the next step.
 * - **Explicit flow control**: operators return `OpOutput` values that encode
 *   "needs more input", "has more output", "blocked", "yield", etc.
 * - **Multiplexing across channels**: a stage may contain multiple channels
 *   (multiple sources feeding the same sink). A single task instance tries channels in
 *   order and, when one channel blocks, it can continue to make progress on others.
 *
 * Threading model:
 * - A `TaskGroup` typically schedules `dop` independent instances of this task.
 * - Each task instance uses its `TaskId` as a `ThreadId` index into per-lane state.
 * - The scheduler must not execute the same task instance concurrently.
 */
template <OpenPipelineTraits Traits>
class PipelineExec {
 public:
  struct Channel {
    SourceOp<Traits>* source_op;
    std::vector<PipeOp<Traits>*> pipe_ops;
    SinkOp<Traits>* sink_op;
  };

  PipelineExec(std::string name,
               std::vector<Channel> channels,
               std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources,
               std::size_t dop,
               bool include_sink_task_groups)
      : name_(std::move(name)),
        desc_(Explain(channels)),
        channels_(std::move(channels)),
        dop_(dop),
        runtime_(std::make_shared<Runtime>(channels_, dop_, std::move(implicit_sources))),
        chain_task_group_(MakeChainTaskGroup(name_, desc_, runtime_, dop_)) {
    CollectSourceTaskGroups();
    if (include_sink_task_groups) {
      CollectSinkTaskGroups();
    }
  }

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  const std::vector<Channel>& Channels() const noexcept { return channels_; }

  const std::vector<TaskGroup<Traits>>& SourceFrontendTaskGroups() const noexcept {
    return source_frontend_task_groups_;
  }
  const std::vector<TaskGroup<Traits>>& SourceBackendTaskGroups() const noexcept {
    return source_backend_task_groups_;
  }
  const std::vector<TaskGroup<Traits>>& SinkFrontendTaskGroups() const noexcept {
    return sink_frontend_task_groups_;
  }
  const std::optional<TaskGroup<Traits>>& SinkBackendTaskGroup() const noexcept {
    return sink_backend_task_group_;
  }
  const TaskGroup<Traits>& ChainTaskGroup() const noexcept { return chain_task_group_; }

  Result<Traits, TaskStatus> operator()(const TaskContext<Traits>& task_ctx, TaskId task_id) {
    return (*runtime_)(task_ctx, task_id);
  }

 private:
  class Runtime;

  static std::string Explain(const std::vector<Channel>& channels) {
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

  static TaskGroup<Traits> MakeChainTaskGroup(const std::string& name,
                                              const std::string& desc,
                                              const std::shared_ptr<Runtime>& runtime,
                                              std::size_t dop) {
    Task<Traits> task(
        name,
        desc,
        [runtime](const TaskContext<Traits>& ctx, TaskId task_id) {
          return (*runtime)(ctx, task_id);
        });
    return TaskGroup<Traits>(name, desc, std::move(task), dop);
  }

  static TaskGroup<Traits> KeepAliveTaskGroup(TaskGroup<Traits> group,
                                              const std::shared_ptr<Runtime>& runtime) {
    auto original_task = group.GetTask();
    Task<Traits> task(
        original_task.Name(),
        original_task.Desc(),
        [runtime, original_task](const TaskContext<Traits>& ctx, TaskId task_id) {
          (void)runtime;
          return original_task(ctx, task_id);
        },
        original_task.Hint());

    std::optional<Continuation<Traits>> cont = std::nullopt;
    if (group.GetContinuation().has_value()) {
      auto original_cont = *group.GetContinuation();
      cont = Continuation<Traits>(
          original_cont.Name(),
          original_cont.Desc(),
          [runtime, original_cont](const TaskContext<Traits>& ctx) {
            (void)runtime;
            return original_cont(ctx);
          },
          original_cont.Hint());
    }

    typename TaskGroup<Traits>::NotifyFinishFunc notify =
        [runtime, group](const TaskContext<Traits>& ctx) -> Status<Traits> {
      (void)runtime;
      return group.NotifyFinish(ctx);
    };

    return TaskGroup<Traits>(group.Name(),
                             group.Desc(),
                             std::move(task),
                             group.NumTasks(),
                             std::move(cont),
                             std::move(notify));
  }

  void CollectSourceTaskGroups() {
    std::unordered_set<SourceOp<Traits>*> seen_sources;
    for (const auto& ch : channels_) {
      auto* src = ch.source_op;
      if (!seen_sources.insert(src).second) {
        continue;
      }

      auto fe = src->Frontend();
      for (auto& tg : fe) {
        source_frontend_task_groups_.push_back(KeepAliveTaskGroup(std::move(tg), runtime_));
      }

      if (auto be = src->Backend(); be.has_value()) {
        source_backend_task_groups_.push_back(KeepAliveTaskGroup(std::move(*be), runtime_));
      }
    }
  }

  void CollectSinkTaskGroups() {
    if (channels_.empty()) {
      return;
    }

    auto* sink = channels_[0].sink_op;

    auto fe = sink->Frontend();
    for (auto& tg : fe) {
      sink_frontend_task_groups_.push_back(KeepAliveTaskGroup(std::move(tg), runtime_));
    }

    if (auto be = sink->Backend(); be.has_value()) {
      sink_backend_task_group_ = KeepAliveTaskGroup(std::move(*be), runtime_);
    }
  }

  class Runtime {
   public:
    Runtime(const std::vector<Channel>& channels,
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

    Result<Traits, TaskStatus> operator()(const TaskContext<Traits>& task_ctx,
                                          TaskId task_id) {
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
        auto awaiter_r = task_ctx.any_awaiter_factory(std::move(blocked_resumers));
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
    class ChannelRuntime {
     public:
      ChannelRuntime(const Channel& channel, std::size_t channel_id, std::size_t dop,
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
            return Pipe(task_ctx,
                        thread_id,
                        drain_id + 1,
                        std::move(drain_out.GetBatch()));
          }
        }

        return OpResult<Traits>(OpOutput<Traits>::Finished());
      }

     private:
      OpResult<Traits> Pipe(const TaskContext<Traits>& task_ctx, ThreadId thread_id,
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

      OpResult<Traits> Sink(const TaskContext<Traits>& task_ctx, ThreadId thread_id,
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

    struct ThreadLocal {
      explicit ThreadLocal(std::size_t num_channels)
          : finished(num_channels, false), resumers(num_channels) {}

      std::vector<bool> finished;
      std::vector<std::shared_ptr<Resumer>> resumers;
    };

    std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_keepalive_;
    std::size_t dop_;

    std::vector<ChannelRuntime> channels_;
    std::vector<ThreadLocal> thread_locals_;

    std::atomic_bool cancelled_;
  };

  std::string name_;
  std::string desc_;
  std::vector<Channel> channels_;

  std::vector<TaskGroup<Traits>> source_frontend_task_groups_;
  std::vector<TaskGroup<Traits>> source_backend_task_groups_;
  std::vector<TaskGroup<Traits>> sink_frontend_task_groups_;
  std::optional<TaskGroup<Traits>> sink_backend_task_group_;

  std::size_t dop_;
  std::shared_ptr<Runtime> runtime_;
  TaskGroup<Traits> chain_task_group_;
};

}  // namespace opl
