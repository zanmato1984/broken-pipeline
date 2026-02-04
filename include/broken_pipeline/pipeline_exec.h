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

#include <algorithm>
#include <atomic>
#include <cassert>
#include <coroutine>
#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <broken_pipeline/concepts.h>
#include <broken_pipeline/operator.h>
#include <broken_pipeline/pipeline.h>
#include <broken_pipeline/task.h>

namespace bp {

template <BrokenPipelineTraits Traits>
/// @brief Source-provided task groups collected during compilation.
///
/// Contains the task groups returned by `SourceOp::Frontend()` and `SourceOp::Backend()`.
/// The host decides scheduling order. A common pattern is:
/// - schedule `frontend` before running the pipelinexe(s) that invoke `Source()`
/// - schedule `backend` ahead as an IO-readiness task (if present)
///
/// These task groups are opaque to Broken Pipeline core. A scheduler may route them using
/// `TaskHint` (for example, CPU vs IO pools).
struct SourceExec {
  /// @brief Source frontend task groups.
  std::vector<TaskGroup<Traits>> frontend;

  /// @brief Optional source backend task group.
  std::optional<TaskGroup<Traits>> backend;
};

template <BrokenPipelineTraits Traits>
/// @brief Sink-provided task groups collected during compilation.
///
/// Contains the task groups returned by `SinkOp::Frontend()` and `SinkOp::Backend()`.
/// The host decides scheduling order. A common pattern is:
/// - schedule `backend` ahead as an IO-readiness task (if present)
/// - schedule `frontend` strictly after all pipelinexes feeding the sink have finished
///
/// These task groups are opaque to Broken Pipeline core. A scheduler may route them using
/// `TaskHint` (for example, CPU vs IO pools).
struct SinkExec {
  /// @brief Sink frontend task groups.
  std::vector<TaskGroup<Traits>> frontend;

  /// @brief Optional sink backend task group.
  std::optional<TaskGroup<Traits>> backend;
};

template <BrokenPipelineTraits Traits>
class Pipelinexe;

/// @brief Reference small-step runtime for a pipelinexe.
///
/// `PipeExec::TaskGroup()` returns a `TaskGroup` with `dop` task instances.
///
/// Execution model:
/// - Each task instance represents one execution lane (the reference runtime uses
///   `ThreadId = TaskId`).
/// - Each invocation performs bounded work and can be invoked repeatedly by a scheduler.
/// - Operator callbacks are invoked in a push-style path (source produces, pipes
/// transform,
///   sink consumes). Source output is obtained by the runtime via `Source()`.
///
/// Control propagation:
/// - `TaskStatus::Yield()` is returned when an operator returns `OpOutput::PipeYield()`.
///   On the next invocation after a yield, the runtime re-enters the yielding operator
///   (input=nullopt) and expects `OpOutput::PipeYieldBack()` as the yield-back handshake.
///   This yield/yield-back handshake provides a natural two-phase scheduling point: run
///   the yielded step in an IO-oriented pool, then schedule subsequent work back on a CPU
///   pool.
/// - `TaskStatus::Blocked(awaiter)` is returned when all unfinished channels are blocked;
///   the awaiter is created by `TaskContext::awaiter_factory` from the channels'
///   resumers.
/// - `TaskStatus::Finished()` is returned when all channels are finished.
/// - After an error, subsequent calls return `TaskStatus::Cancelled()`.
template <BrokenPipelineTraits Traits>
class PipeExec {
 public:
  bp::TaskGroup<Traits> TaskGroup() const {
    Task<Traits> task(std::string{},
                      [exec = exec_](const TaskContext<Traits>& ctx, TaskId task_id) {
                        return (*exec)(ctx, task_id);
                      });
    return bp::TaskGroup<Traits>(std::string{}, std::move(task), dop_);
  }

 private:
  PipeExec(const std::vector<typename Pipeline<Traits>::Channel>& channels,
           SinkOp<Traits>* sink_op, std::size_t dop)
      : dop_(dop), exec_(std::make_shared<Exec>(channels, sink_op, dop_)) {}

  friend class Pipelinexe<Traits>;

  class Exec {
   public:
    class Channel {
     public:
      Channel(const typename Pipeline<Traits>::Channel& channel, SinkOp<Traits>* sink_op,
              std::size_t channel_id, std::size_t dop, std::atomic_bool& cancelled)
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

      OpResult<Traits> operator()(const TaskContext<Traits>& task_ctx,
                                  ThreadId thread_id) {
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
            return Pipe(task_ctx, thread_id, drain_id + 1,
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

          assert(out.IsPipeSinkNeedsMore() || out.IsPipeEven() ||
                 out.IsSourcePipeHasMore());

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

    Exec(const std::vector<typename Pipeline<Traits>::Channel>& channels,
         SinkOp<Traits>* sink_op, std::size_t dop)
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
  std::shared_ptr<Exec> exec_;
};

/// @brief Coroutine-based variant of `PipeExec` (prototype).
///
/// This implementation preserves the same external small-step API as `PipeExec`
/// (`TaskGroup` -> `Task` -> `TaskStatus`) but uses a per-(channel,thread) C++20
/// coroutine to hold re-entry state, aiming to simplify the control flow.
///
/// Note:
/// - This runtime does not introduce any threads; it is still driven by repeated
///   synchronous calls from a scheduler.
template <BrokenPipelineTraits Traits>
class PipeExecCoro {
 public:
  bp::TaskGroup<Traits> TaskGroup() const {
    Task<Traits> task(std::string{},
                      [exec = exec_](const TaskContext<Traits>& ctx, TaskId task_id) {
                        return (*exec)(ctx, task_id);
                      });
    return bp::TaskGroup<Traits>(std::string{}, std::move(task), dop_);
  }

 private:
  PipeExecCoro(const std::vector<typename Pipeline<Traits>::Channel>& channels,
               SinkOp<Traits>* sink_op, std::size_t dop)
      : dop_(dop), exec_(std::make_shared<Exec>(channels, sink_op, dop_)) {}

  friend class Pipelinexe<Traits>;

  class Exec {
   public:
    class Channel {
     public:
      Channel(const typename Pipeline<Traits>::Channel& channel, SinkOp<Traits>* sink_op,
              std::size_t channel_id, std::size_t dop, std::atomic_bool& cancelled)
          : channel_id_(channel_id),
            source_(channel.source_op->Source()),
            pipes_(channel.pipe_ops.size()),
            sink_(sink_op->Sink()),
            steppers_(dop),
            cancelled_(cancelled) {
        for (std::size_t i = 0; i < channel.pipe_ops.size(); ++i) {
          pipes_[i] = {channel.pipe_ops[i]->Pipe(), channel.pipe_ops[i]->Drain()};
          if (pipes_[i].second) {
            drains_.push_back(i);
          }
        }
      }

      OpResult<Traits> operator()(const TaskContext<Traits>& task_ctx,
                                  ThreadId thread_id) {
        if (cancelled_.load()) {
          return OpResult<Traits>(OpOutput<Traits>::Cancelled());
        }

        auto& stepper = steppers_[thread_id];
        if (!stepper) {
          stepper = Run(&task_ctx, thread_id);
        }
        return stepper.Next();
      }

     private:
      class Stepper {
       public:
        struct promise_type {
          OpResult<Traits> current =
              OpResult<Traits>(OpOutput<Traits>::PipeSinkNeedsMore());

          Stepper get_return_object() {
            return Stepper(std::coroutine_handle<promise_type>::from_promise(*this));
          }
          std::suspend_always initial_suspend() noexcept { return {}; }
          std::suspend_always final_suspend() noexcept { return {}; }
          std::suspend_always yield_value(OpResult<Traits> value) noexcept {
            current = std::move(value);
            return {};
          }
          void return_void() noexcept {}
          void unhandled_exception() { std::terminate(); }
        };

        using handle_type = std::coroutine_handle<promise_type>;

        Stepper() = default;
        explicit Stepper(handle_type handle) : handle_(handle) {}

        Stepper(Stepper&& other) noexcept
            : handle_(std::exchange(other.handle_, handle_type{})) {}
        Stepper& operator=(Stepper&& other) noexcept {
          if (this == &other) {
            return *this;
          }
          if (handle_) {
            handle_.destroy();
          }
          handle_ = std::exchange(other.handle_, handle_type{});
          return *this;
        }

        Stepper(const Stepper&) = delete;
        Stepper& operator=(const Stepper&) = delete;

        ~Stepper() {
          if (handle_) {
            handle_.destroy();
          }
        }

        explicit operator bool() const noexcept { return static_cast<bool>(handle_); }

        OpResult<Traits> Next() {
          assert(handle_);
          handle_.resume();
          return std::move(handle_.promise().current);
        }

       private:
        handle_type handle_{};
      };

      Stepper Run(const TaskContext<Traits>* task_ctx, ThreadId thread_id) {
        bool source_done = false;
        std::size_t draining = 0;
        std::vector<std::size_t> pipe_stack;

        std::size_t push_pipe_id = 0;
        std::optional<typename Traits::Batch> push_input;
        bool push_requested = false;

        while (true) {
          if (cancelled_.load()) {
            co_yield OpResult<Traits>(OpOutput<Traits>::Cancelled());
            co_return;
          }

          push_requested = false;
          push_input.reset();
          push_pipe_id = 0;

          if (!pipe_stack.empty()) {
            push_pipe_id = pipe_stack.back();
            pipe_stack.pop_back();
            push_input = std::nullopt;
            push_requested = true;
          } else if (!source_done) {
            auto src_r = source_(*task_ctx, thread_id);
            if (!src_r.ok()) {
              cancelled_.store(true);
              co_yield src_r;
              co_return;
            }
            auto& src_out = src_r.ValueOrDie();
            if (src_out.IsBlocked()) {
              co_yield src_r;
              continue;
            }
            if (src_out.IsFinished()) {
              source_done = true;
              if (src_out.GetBatch().has_value()) {
                push_pipe_id = 0;
                push_input = std::move(src_out.GetBatch());
                push_requested = true;
              }
            } else {
              assert(src_out.IsSourcePipeHasMore());
              assert(src_out.GetBatch().has_value());
              push_pipe_id = 0;
              push_input = std::move(src_out.GetBatch());
              push_requested = true;
            }
          } else {
            // Draining.
            while (draining < drains_.size()) {
              const auto drain_id = drains_[draining];
              auto& drain_fn = pipes_[drain_id].second;
              assert(static_cast<bool>(drain_fn));

              auto drain_r = drain_fn(*task_ctx, thread_id);
              if (!drain_r.ok()) {
                cancelled_.store(true);
                co_yield drain_r;
                co_return;
              }
              auto& drain_out = drain_r.ValueOrDie();

              if (drain_out.IsPipeYield()) {
                co_yield OpResult<Traits>(OpOutput<Traits>::PipeYield());

                auto back_r = drain_fn(*task_ctx, thread_id);
                if (!back_r.ok()) {
                  cancelled_.store(true);
                  co_yield back_r;
                  co_return;
                }
                auto& back_out = back_r.ValueOrDie();
                assert(back_out.IsPipeYieldBack());
                co_yield OpResult<Traits>(OpOutput<Traits>::PipeYieldBack());
                continue;
              }

              if (drain_out.IsBlocked()) {
                co_yield drain_r;
                continue;
              }

              assert(drain_out.IsSourcePipeHasMore() || drain_out.IsFinished());
              if (drain_out.GetBatch().has_value()) {
                if (drain_out.IsFinished()) {
                  ++draining;
                }
                push_pipe_id = drain_id + 1;
                push_input = std::move(drain_out.GetBatch());
                push_requested = true;
                break;
              }

              ++draining;
            }

            if (!push_requested) {
              if (draining >= drains_.size()) {
                co_yield OpResult<Traits>(OpOutput<Traits>::Finished());
                co_return;
              }
            }
          }

          if (!push_requested) {
            continue;
          }

          // Push a (possibly null) input through pipe chain into sink.
          std::optional<typename Traits::Batch> input = std::move(push_input);

          bool push_done = false;
          for (std::size_t i = push_pipe_id; i < pipes_.size() && !push_done; ++i) {
            auto& pipe_fn = pipes_[i].first;

            while (true) {
              auto pipe_r = pipe_fn(*task_ctx, thread_id, std::move(input));
              if (!pipe_r.ok()) {
                cancelled_.store(true);
                co_yield pipe_r;
                co_return;
              }
              auto& out = pipe_r.ValueOrDie();

              if (out.IsPipeYield()) {
                co_yield OpResult<Traits>(OpOutput<Traits>::PipeYield());

                auto back_r = pipe_fn(*task_ctx, thread_id, std::nullopt);
                if (!back_r.ok()) {
                  cancelled_.store(true);
                  co_yield back_r;
                  co_return;
                }
                auto& back_out = back_r.ValueOrDie();
                assert(back_out.IsPipeYieldBack());
                co_yield OpResult<Traits>(OpOutput<Traits>::PipeYieldBack());
                input = std::nullopt;
                continue;
              }

              if (out.IsBlocked()) {
                co_yield pipe_r;
                input = std::nullopt;
                continue;
              }

              assert(out.IsPipeSinkNeedsMore() || out.IsPipeEven() ||
                     out.IsSourcePipeHasMore());

              if (out.IsPipeEven() || out.IsSourcePipeHasMore()) {
                if (out.IsSourcePipeHasMore()) {
                  pipe_stack.push_back(i);
                }
                assert(out.GetBatch().has_value());
                input = std::move(out.GetBatch());
                break;
              }

              co_yield OpResult<Traits>(OpOutput<Traits>::PipeSinkNeedsMore());
              push_done = true;
              break;
            }
          }

          if (push_done) {
            continue;
          }

          // Sink.
          while (true) {
            auto sink_r = sink_(*task_ctx, thread_id, std::move(input));
            if (!sink_r.ok()) {
              cancelled_.store(true);
              co_yield sink_r;
              co_return;
            }
            auto& out = sink_r.ValueOrDie();
            assert(out.IsPipeSinkNeedsMore() || out.IsBlocked());
            if (out.IsBlocked()) {
              co_yield sink_r;
              input = std::nullopt;
              continue;
            }

            co_yield sink_r;
            break;
          }
        }
      }

      std::size_t channel_id_;

      PipelineSource<Traits> source_;
      std::vector<std::pair<PipelinePipe<Traits>, PipelineDrain<Traits>>> pipes_;
      PipelineSink<Traits> sink_;

      std::vector<std::size_t> drains_;
      std::vector<Stepper> steppers_;

      std::atomic_bool& cancelled_;
    };

    Exec(const std::vector<typename Pipeline<Traits>::Channel>& channels,
         SinkOp<Traits>* sink_op, std::size_t dop)
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
  std::shared_ptr<Exec> exec_;
};

namespace detail {
template <BrokenPipelineTraits Traits>
class PipelineCompiler;
}  // namespace detail

/// @brief A compiled pipelinexe (one executable stage).
///
/// A `Pipelinexe` ("pipelinexe") is a small-step, multiplexed runtime over one or more
/// channels (sources feeding the shared sink).
///
/// Notes:
/// - A pipelinexe may contain multiple channels feeding the same sink. This is useful for
///   union-all-like fan-in operators.
/// - A pipelinexe keeps implicit sources returned from `PipeOp::ImplicitSource()` alive
/// for
///   the duration of the compiled plan.
///
/// Pipelinexes are executed in sequence order by the host.
template <BrokenPipelineTraits Traits>
class Pipelinexe {
 public:
  const std::string& Name() const noexcept { return name_; }

  std::size_t Dop() const noexcept { return dop_; }

  // Introspection helpers (primarily for unit tests / tooling).
  const std::vector<typename Pipeline<Traits>::Channel>& Channels() const noexcept {
    return channels_;
  }

  std::size_t NumImplicitSources() const noexcept {
    return implicit_sources_keepalive_.size();
  }

  /// @brief Returns source-provided frontend/backend task groups for each channel source.
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

  /// @brief Returns the reference runtime that runs this pipelinexe.
  bp::PipeExec<Traits> PipeExec() const {
    return bp::PipeExec<Traits>(channels_, sink_op_, dop_);
  }

  /// @brief Returns the coroutine-based runtime prototype that runs this pipelinexe.
  bp::PipeExecCoro<Traits> PipeExecCoro() const {
    return bp::PipeExecCoro<Traits>(channels_, sink_op_, dop_);
  }

 private:
  Pipelinexe(std::string name, std::vector<typename Pipeline<Traits>::Channel> channels,
             std::vector<std::unique_ptr<SourceOp<Traits>>> implicit_sources_keepalive,
             SinkOp<Traits>* sink_op, std::size_t dop)
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

/// @brief A compiled pipeline execution plan.
///
/// A `PipelineExec` contains:
/// - one `SinkExec` (sink frontend/backend task groups)
/// - an ordered sequence of Pipelinexes (`Pipelinexe`)
///
/// The host is responsible for orchestrating ordering between pipelinexes and the
/// source/sink frontend/backend task groups.
template <BrokenPipelineTraits Traits>
class PipelineExec {
 public:
  const std::string& Name() const noexcept { return name_; }
  std::size_t Dop() const noexcept { return dop_; }

  const SinkExec<Traits>& Sink() const noexcept { return sink_; }
  const std::vector<Pipelinexe<Traits>>& Pipelinexes() const noexcept {
    return pipelinexes_;
  }

 private:
  PipelineExec(std::string name, SinkExec<Traits> sink,
               std::vector<Pipelinexe<Traits>> pipelinexes, std::size_t dop)
      : name_(std::move(name)),
        sink_(std::move(sink)),
        pipelinexes_(std::move(pipelinexes)),
        dop_(dop) {}

  friend class detail::PipelineCompiler<Traits>;

  std::string name_;

  SinkExec<Traits> sink_;
  std::vector<Pipelinexe<Traits>> pipelinexes_;

  std::size_t dop_;
};

namespace detail {

/// @brief Internal compiler that splits a `Pipeline` into an ordered list of
/// Pipelinexes (`Pipelinexe`).
///
/// Splitting rule (current):
/// - Only pipe implicit sources (`PipeOp::ImplicitSource()`) create pipelinexe
/// boundaries.
/// - When a pipe provides an implicit source, the downstream pipe chain becomes a new
///   channel rooted at that implicit source in a later pipelinexe.
///
/// Note:
/// - `SinkOp::ImplicitSource()` is intentionally not used by this compiler. It is a
///   host-orchestration hook for chaining a sink's output into a subsequent pipeline
///   stage (for example, an aggregation sink emitting group-by results).
template <BrokenPipelineTraits Traits>
class PipelineCompiler {
 public:
  PipelineCompiler(const Pipeline<Traits>& pipeline, std::size_t dop)
      : pipeline_(pipeline), dop_(dop) {}

  PipelineExec<Traits> Compile() && {
    ExtractTopology();
    SortTopology();
    auto pipelinexes = BuildPipelinexes();

    SinkExec<Traits> sink;
    sink.frontend = pipeline_.Sink()->Frontend();
    sink.backend = pipeline_.Sink()->Backend();

    return PipelineExec<Traits>(pipeline_.Name(), std::move(sink), std::move(pipelinexes),
                                dop_);
  }

 private:
  void ExtractTopology() {
    std::unordered_map<PipeOp<Traits>*, SourceOp<Traits>*> pipe_source_map;

    for (auto& channel : pipeline_.Channels()) {
      std::size_t id = 0;
      topology_.emplace(
          channel.source_op,
          std::pair<std::size_t, typename Pipeline<Traits>::Channel>{id++, channel});
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

            topology_.emplace(implicit_source,
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
      auto& pipelinexe_info = topology_[source];
      if (implicit_sources_keepalive_.count(source) > 0) {
        pipelinexe_infos_[pipelinexe_info.first].first.push_back(
            std::move(implicit_sources_keepalive_[source]));
      }
      pipelinexe_infos_[pipelinexe_info.first].second.push_back(
          std::move(pipelinexe_info.second));
    }
  }

  std::vector<Pipelinexe<Traits>> BuildPipelinexes() {
    std::vector<Pipelinexe<Traits>> pipelinexes;

    if (pipelinexe_infos_.empty()) {
      return pipelinexes;
    }

    pipelinexes.reserve(pipelinexe_infos_.size());

    for (auto& [id, pipelinexe_info] : pipelinexe_infos_) {
      auto implicit_sources = std::move(pipelinexe_info.first);
      auto pipeline_channels = std::move(pipelinexe_info.second);

      auto name = "pipelinexe" + std::to_string(id) + "(" + pipeline_.Name() + ")";
      pipelinexes.push_back(
          Pipelinexe<Traits>(std::move(name), std::move(pipeline_channels),
                             std::move(implicit_sources), pipeline_.Sink(), dop_));
    }

    return pipelinexes;
  }

  const Pipeline<Traits>& pipeline_;
  std::size_t dop_;

  std::unordered_map<SourceOp<Traits>*,
                     std::pair<std::size_t, typename Pipeline<Traits>::Channel>>
      topology_;
  std::vector<SourceOp<Traits>*> sources_keep_order_;
  std::unordered_map<SourceOp<Traits>*, std::unique_ptr<SourceOp<Traits>>>
      implicit_sources_keepalive_;

  std::map<std::size_t, std::pair<std::vector<std::unique_ptr<SourceOp<Traits>>>,
                                  std::vector<typename Pipeline<Traits>::Channel>>>
      pipelinexe_infos_;
};

}  // namespace detail

/// @brief Compile a `Pipeline` into a `PipelineExec` plan.
///
/// The compiled plan:
/// - collects source and sink frontend/backend task groups (as opaque units for the host)
/// - splits the pipeline into an ordered list of pipelinexes on
/// `PipeOp::ImplicitSource()`
/// - owns implicit sources returned from `PipeOp::ImplicitSource()` to keep them alive
///
/// This function does not call `SinkOp::ImplicitSource()`.
template <BrokenPipelineTraits Traits>
PipelineExec<Traits> Compile(const Pipeline<Traits>& pipeline, std::size_t dop) {
  return detail::PipelineCompiler<Traits>(pipeline, dop).Compile();
}

}  // namespace bp
