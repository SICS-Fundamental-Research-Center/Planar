#include "thread_pool.h"

#include <random>

#include <folly/experimental/TestUtil.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "util/atomic.h"

namespace sics::graph::core::common {

// The fixture for testing class TheadPoolTest.
class TheadPoolTest : public ::testing::Test {
 protected:
  TheadPoolTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(TheadPoolTest, CountShouldEqualsToK) {
  auto parallelism = std::thread::hardware_concurrency();
  auto thread_pool = ThreadPool(parallelism);
  auto task_package = TaskPackage();

  int k = random() % (1 << 5);
  int count = 0;
  for (int i = 0; i < k; i++) {
    auto task = std::bind([&count]() {
      sics::graph::core::util::atomic::WriteAdd(&count, 1);
      return;
    });
    task_package.push_back(task);
  }
  thread_pool.SubmitSync(task_package);
  EXPECT_EQ(k, count);
}

}  // namespace sics::graph::core::common
