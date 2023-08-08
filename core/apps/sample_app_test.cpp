#include <gtest/gtest.h>

#include "apps/sample_app.h"
#include "util/pointer_cast.h"

namespace sics::graph::core::apps {

class SampleAppTest : public ::testing::Test {
 protected:
  SampleAppTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(SampleAppTest, SampleAppShouldBeInitiatedByFactory) {
  auto app = util::pointer_upcast<apis::PIE<DummyGraph>, SampleApp>(
      apis::PIEAppFactory<DummyGraph>::Create("SampleApp"));
  EXPECT_NE(app, nullptr);
  DummyGraph graph;
  EXPECT_EQ(graph.get_status(), "initialized");
  app->PEval(&graph);
  EXPECT_EQ(graph.get_status(), "PEval");
  app->IncEval(&graph);
  EXPECT_EQ(graph.get_status(), "IncEval");
  app->Assemble(&graph);
  EXPECT_EQ(graph.get_status(), "Assemble");
}

}  // namespace sics::graph::core::common
