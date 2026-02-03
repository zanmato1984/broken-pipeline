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

#include "arrow_traits.h"

#include <broken_pipeline/task.h>

#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <memory>

namespace bp_test {

namespace {

class NoopAwaiter final : public Awaiter {};

}  // namespace

TEST(BrokenPipelineTaskTest, BasicTask) {
  Task task("BasicTask",
            [](const TaskContext&, TaskId) { return TaskStatus::Finished(); });

  TaskContext ctx;
  auto res = task(ctx, 0);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished()) << res->ToString();
}

TEST(BrokenPipelineTaskTest, BasicContinuation) {
  Continuation cont("BasicContinuation",
                    [](const TaskContext&) { return TaskStatus::Finished(); });

  TaskContext ctx;
  auto res = cont(ctx);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished()) << res->ToString();
}

TEST(BrokenPipelineTaskTest, TaskStatus) {
  EXPECT_TRUE(TaskStatus::Continue().IsContinue());
  EXPECT_TRUE(TaskStatus::Yield().IsYield());
  EXPECT_TRUE(TaskStatus::Finished().IsFinished());
  EXPECT_TRUE(TaskStatus::Cancelled().IsCancelled());

  auto awaiter = std::make_shared<NoopAwaiter>();
  auto blocked = TaskStatus::Blocked(awaiter);
  EXPECT_TRUE(blocked.IsBlocked());
  EXPECT_EQ(blocked.GetAwaiter().get(), awaiter.get());

  EXPECT_EQ(TaskStatus::Continue().ToString(), "CONTINUE");
  EXPECT_EQ(TaskStatus::Yield().ToString(), "YIELD");
  EXPECT_EQ(TaskStatus::Finished().ToString(), "FINISHED");
  EXPECT_EQ(TaskStatus::Cancelled().ToString(), "CANCELLED");
  EXPECT_EQ(blocked.ToString(), "BLOCKED");
}

TEST(BrokenPipelineTaskTest, BasicTaskGroup) {
  Task task("T", [](const TaskContext&, TaskId) { return TaskStatus::Finished(); });
  Continuation cont("C", [](const TaskContext&) { return TaskStatus::Finished(); });

  TaskGroup tg("G", task, /*num_tasks=*/1, cont);
  ASSERT_EQ(tg.Name(), "G");
  ASSERT_EQ(tg.NumTasks(), 1);
  ASSERT_TRUE(tg.Continuation().has_value());
  ASSERT_EQ(tg.Task().Name(), "T");
  ASSERT_EQ(tg.Continuation()->Name(), "C");
}

TEST(BrokenPipelineTaskTest, TaskHintDefaultsToCpu) {
  Task t("T", [](const TaskContext&, TaskId) { return TaskStatus::Finished(); });
  EXPECT_EQ(t.Hint().type, TaskHint::Type::CPU);
}

}  // namespace bp_test
