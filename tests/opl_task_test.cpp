#include <arrow_traits.h>

#include <opl/task.h>

#include <arrow/status.h>
#include <arrow/testing/gtest_util.h>

#include <gtest/gtest.h>

#include <memory>

namespace opl {

namespace {

class NoopAwaiter final : public Awaiter {};

}  // namespace

TEST(OplTaskTest, BasicTask) {
  using O = opl_arrow::Aliases<int>;

  O::Task task("BasicTask",
               [](const O::TaskContext&, O::TaskId) { return O::TaskStatus::Finished(); });

  O::TaskContext ctx;
  auto res = task(ctx, 0);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished()) << res->ToString();
}

TEST(OplTaskTest, BasicContinuation) {
  using O = opl_arrow::Aliases<int>;

  O::Continuation cont(
      "BasicContinuation",
      [](const O::TaskContext&) { return O::TaskStatus::Finished(); });

  O::TaskContext ctx;
  auto res = cont(ctx);
  ASSERT_TRUE(res.ok());
  ASSERT_TRUE(res->IsFinished()) << res->ToString();
}

TEST(OplTaskTest, TaskStatus) {
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

TEST(OplTaskTest, BasicTaskGroup) {
  using O = opl_arrow::Aliases<int>;

  O::Task task("T", [](const O::TaskContext&, O::TaskId) { return O::TaskStatus::Finished(); });
  O::Continuation cont("C", [](const O::TaskContext&) { return O::TaskStatus::Finished(); });

  O::TaskGroup tg("G", task, /*num_tasks=*/1, cont);
  ASSERT_EQ(tg.Name(), "G");
  ASSERT_EQ(tg.NumTasks(), 1);
  ASSERT_TRUE(tg.GetContinuation().has_value());
  ASSERT_EQ(tg.GetTask().Name(), "T");
  ASSERT_EQ(tg.GetContinuation()->Name(), "C");
}

TEST(OplTaskTest, TaskHintDefaultsToCpu) {
  using O = opl_arrow::Aliases<int>;

  O::Task t("T", [](const O::TaskContext&, O::TaskId) { return O::TaskStatus::Finished(); });
  EXPECT_EQ(t.Hint().type, O::TaskHint::Type::CPU);
}

}  // namespace opl
