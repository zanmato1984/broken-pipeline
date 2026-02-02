#include "arrow_traits.h"

#include <broken_pipeline/task.h>

#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <memory>

namespace broken_pipeline_test {

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
  ASSERT_TRUE(tg.GetContinuation().has_value());
  ASSERT_EQ(tg.GetTask().Name(), "T");
  ASSERT_EQ(tg.GetContinuation()->Name(), "C");
}

TEST(BrokenPipelineTaskTest, TaskHintDefaultsToCpu) {
  Task t("T", [](const TaskContext&, TaskId) { return TaskStatus::Finished(); });
  EXPECT_EQ(t.Hint().type, TaskHint::Type::CPU);
}

}  // namespace broken_pipeline_test
