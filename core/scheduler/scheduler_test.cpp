#include "scheduler/scheduler.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "components/discharger.h"
#include "components/executer.h"
#include "components/loader.h"

namespace sics::graph::core::scheduler {

class SchedulerTest : public ::testing::Test {
 protected:
  SchedulerTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

class MockScheduler : public Scheduler {
 public:
  MOCK_METHOD(void, Start, (), ());
};


TEST_F(SchedulerTest, StartFuncTest) {}

}  // namespace sics::graph::core::scheduler