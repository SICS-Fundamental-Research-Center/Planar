#include "scheduler/scheduler.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "components/discharger.h"
#include "components/executor.h"
#include "components/loader.h"

namespace xyz::graph::core::scheduler {

class SchedulerTest : public ::testing::Test {
 protected:
  SchedulerTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

// TODO: test scheduler
class MockScheduler : public Scheduler {
 public:
  MOCK_METHOD(void, Start, (), ());
};


TEST_F(SchedulerTest, StartFuncTest) {}

}  // namespace xyz::graph::core::scheduler