#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <openpipeline/concepts.h>
#include <openpipeline/op/op.h>
#include <openpipeline/pipeline/detail/physical_pipeline.h>
#include <openpipeline/task/task.h>
#include <openpipeline/task/task_status.h>

namespace openpipeline::pipeline::detail {

template <OpenPipelineTraits Traits>
class PipelineTask {
 public:
  using TaskId = typename Traits::TaskId;
  using ThreadId = typename Traits::ThreadId;

  PipelineTask(std::shared_ptr<const PhysicalPipeline<Traits>> pipeline, std::size_t dop)
      : name_("Task of " + pipeline->Name()),
        desc_(pipeline->Desc()),
        pipeline_(std::move(pipeline)),
        dop_(dop),
        cancelled_(false) {
    channels_.reserve(pipeline_->Channels().size());
    for (std::size_t i = 0; i < pipeline_->Channels().size(); ++i) {
      channels_.emplace_back(*pipeline_, i, dop_, cancelled_);
    }
    thread_locals_.reserve(dop_);
    for (std::size_t i = 0; i < dop_; ++i) {
      thread_locals_.emplace_back(channels_.size());
    }
  }

  const std::string& Name() const noexcept { return name_; }
  const std::string& Desc() const noexcept { return desc_; }

  Result<Traits, task::TaskStatus> operator()(const task::TaskContext<Traits>& task_ctx,
                                              TaskId task_id) {
    const ThreadId thread_id = static_cast<ThreadId>(task_id);
    if (cancelled_.load()) {
      return Traits::Ok(task::TaskStatus::Cancelled());
    }

    bool all_finished = true;
    bool all_unfinished_blocked = true;
    task::Resumers blocked_resumers;

    op::OpResult<Traits> last_op_result = Traits::Ok(op::OpOutput<Traits>::PipeSinkNeedsMore());

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
      if (!Traits::IsOk(last_op_result)) {
        cancelled_.store(true);
        return Traits::template ErrorFrom<task::TaskStatus>(last_op_result);
      }

      auto& out = Traits::Value(last_op_result);
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
      return Traits::Ok(task::TaskStatus::Finished());
    }

    if (all_unfinished_blocked && !blocked_resumers.empty()) {
      auto awaiter_r = task_ctx.any_awaiter_factory(std::move(blocked_resumers));
      if (!Traits::IsOk(awaiter_r)) {
        cancelled_.store(true);
        return Traits::template ErrorFrom<task::TaskStatus>(awaiter_r);
      }
      auto awaiter = Traits::Take(std::move(awaiter_r));
      return Traits::Ok(task::TaskStatus::Blocked(std::move(awaiter)));
    }

    if (!Traits::IsOk(last_op_result)) {
      cancelled_.store(true);
      return Traits::template ErrorFrom<task::TaskStatus>(last_op_result);
    }

    const auto& out = Traits::Value(last_op_result);
    if (out.IsPipeYield()) {
      return Traits::Ok(task::TaskStatus::Yield());
    }
    if (out.IsCancelled()) {
      cancelled_.store(true);
      return Traits::Ok(task::TaskStatus::Cancelled());
    }
    return Traits::Ok(task::TaskStatus::Continue());
  }

 private:
  class Channel {
   public:
    Channel(const PhysicalPipeline<Traits>& pipeline, std::size_t channel_id, std::size_t dop,
            std::atomic_bool& cancelled)
        : channel_id_(channel_id),
          dop_(dop),
          source_(pipeline.Channels()[channel_id].source_op->Source()),
          pipes_(pipeline.Channels()[channel_id].pipe_ops.size()),
          sink_(pipeline.Channels()[channel_id].sink_op->Sink()),
          thread_locals_(dop),
          cancelled_(cancelled) {
      const auto& pipe_ops = pipeline.Channels()[channel_id].pipe_ops;
      for (std::size_t i = 0; i < pipe_ops.size(); ++i) {
        pipes_[i] = {pipe_ops[i]->Pipe(), pipe_ops[i]->Drain()};
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

    op::OpResult<Traits> operator()(const task::TaskContext<Traits>& task_ctx,
                                   ThreadId thread_id) {
      if (cancelled_.load()) {
        return Traits::Ok(op::OpOutput<Traits>::Cancelled());
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
        if (!Traits::IsOk(src_r)) {
          cancelled_.store(true);
          return src_r;
        }
        auto& src_out = Traits::Value(src_r);
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
        return Traits::Ok(op::OpOutput<Traits>::Finished());
      }

      for (; tl.draining < tl.drains.size(); ++tl.draining) {
        const auto drain_id = tl.drains[tl.draining];
        auto& drain_fn = pipes_[drain_id].second;
        assert(static_cast<bool>(drain_fn));

        auto drain_r = drain_fn(task_ctx, thread_id);
        if (!Traits::IsOk(drain_r)) {
          cancelled_.store(true);
          return drain_r;
        }

        auto& drain_out = Traits::Value(drain_r);

        if (tl.yield) {
          assert(drain_out.IsPipeYieldBack());
          tl.yield = false;
          return Traits::Ok(op::OpOutput<Traits>::PipeYieldBack());
        }

        if (drain_out.IsPipeYield()) {
          assert(!tl.yield);
          tl.yield = true;
          return Traits::Ok(op::OpOutput<Traits>::PipeYield());
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

      return Traits::Ok(op::OpOutput<Traits>::Finished());
    }

   private:
    op::OpResult<Traits> Pipe(const task::TaskContext<Traits>& task_ctx, ThreadId thread_id,
                              std::size_t pipe_id,
                              std::optional<typename Traits::Batch> input) {
      auto& tl = thread_locals_[thread_id];

      for (std::size_t i = pipe_id; i < pipes_.size(); ++i) {
        auto pipe_r = pipes_[i].first(task_ctx, thread_id, std::move(input));
        if (!Traits::IsOk(pipe_r)) {
          cancelled_.store(true);
          return pipe_r;
        }

        auto& out = Traits::Value(pipe_r);

        if (tl.yield) {
          assert(out.IsPipeYieldBack());
          tl.pipe_stack.push_back(i);
          tl.yield = false;
          return Traits::Ok(op::OpOutput<Traits>::PipeYieldBack());
        }

        if (out.IsPipeYield()) {
          assert(!tl.yield);
          tl.pipe_stack.push_back(i);
          tl.yield = true;
          return Traits::Ok(op::OpOutput<Traits>::PipeYield());
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

        return Traits::Ok(op::OpOutput<Traits>::PipeSinkNeedsMore());
      }

      return Sink(task_ctx, thread_id, std::move(input));
    }

    op::OpResult<Traits> Sink(const task::TaskContext<Traits>& task_ctx, ThreadId thread_id,
                              std::optional<typename Traits::Batch> input) {
      auto sink_r = sink_(task_ctx, thread_id, std::move(input));
      if (!Traits::IsOk(sink_r)) {
        cancelled_.store(true);
        return sink_r;
      }
      auto& out = Traits::Value(sink_r);
      assert(out.IsPipeSinkNeedsMore() || out.IsBlocked());
      if (out.IsBlocked()) {
        thread_locals_[thread_id].sinking = true;
      }
      return sink_r;
    }

    std::size_t channel_id_;
    std::size_t dop_;

    op::PipelineSource<Traits> source_;
    std::vector<std::pair<op::PipelinePipe<Traits>, op::PipelineDrain<Traits>>> pipes_;
    op::PipelineSink<Traits> sink_;

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
    std::vector<task::ResumerPtr> resumers;
  };

  std::string name_;
  std::string desc_;

  std::shared_ptr<const PhysicalPipeline<Traits>> pipeline_;
  std::size_t dop_;

  std::vector<Channel> channels_;
  std::vector<ThreadLocal> thread_locals_;

  std::atomic_bool cancelled_;
};

}  // namespace openpipeline::pipeline::detail

