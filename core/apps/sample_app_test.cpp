#include "apps/sample_app.h"

#include <gtest/gtest.h>

#include <vector>

#include "apis/planar_app_factory.h"
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
  apis::PlanarAppFactory<DummyGraph> factory(nullptr);
  DummyGraph graph;
  auto app = util::pointer_upcast<apis::PlanarAppBase<DummyGraph>, SampleApp>(
      factory.Create("SampleApp", &graph));
  EXPECT_NE(app, nullptr);
  EXPECT_EQ(graph.get_status(), "initialized");
  app->PEval();
  EXPECT_EQ(graph.get_status(), "PEval");
  app->IncEval();
  EXPECT_EQ(graph.get_status(), "IncEval");
  app->Assemble();
  EXPECT_EQ(graph.get_status(), "Assemble");
}

TEST_F(SampleAppTest, FactoryCreationShouldFailIfAppIsNotRegistered) {
  apis::PlanarAppFactory<DummyGraph> factory(nullptr);
  DummyGraph graph;
  EXPECT_EQ(nullptr, factory.Create("NotRegisteredApp", nullptr));
  auto app = factory.Create("SampleApp", &graph);
  EXPECT_NE(app, nullptr);
}

}  // namespace sics::graph::core::apps
