#include "blocking_queue.h"

#include <thread>

#include <gtest/gtest.h>

namespace xyz::graph::core::common {

// The fixture for testing class LogTest.
class BlockingQueueTest : public ::testing::Test {
 protected:
  BlockingQueueTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(BlockingQueueTest, PushShouldAlwaysReturnImmediately) {
  BlockingQueue<int> queue;
  queue.Push(1);
  queue.Push(2);
  queue.Push(3);
  EXPECT_EQ(queue.Size(), 3);
}

TEST_F(BlockingQueueTest, PopShouldBlockIfQueueIsEmpty) {
  BlockingQueue<int> queue;
  std::thread t([&queue]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    queue.Push(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    queue.Push(2);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    queue.Push(3);
  });
  EXPECT_EQ(queue.PopOrWait(), 1);
  EXPECT_EQ(queue.PopOrWait(), 2);
  EXPECT_EQ(queue.PopOrWait(), 3);
  t.join();
}

TEST_F(BlockingQueueTest, PopShouldReturnImmediatelyIfQueueIsNotEmpty) {
  BlockingQueue<int> queue;
  queue.Push(1);
  queue.Push(2);
  queue.Push(3);
  EXPECT_EQ(queue.PopOrWait(), 1);
  EXPECT_EQ(queue.PopOrWait(), 2);
  EXPECT_EQ(queue.PopOrWait(), 3);
}

TEST_F(BlockingQueueTest, PopShouldReturnImmediatelyIfQueueIsNotEmptyAfterPush) {
  BlockingQueue<int> queue;
  queue.Push(1);
  EXPECT_EQ(queue.PopOrWait(), 1);
  queue.Push(2);
  queue.Push(3);
  EXPECT_EQ(queue.PopOrWait(), 2);
  EXPECT_EQ(queue.PopOrWait(), 3);
}

}  // namespace xyz::graph::core::common
