#include "apps/sample_app.h"

#include <gtest/gtest.h>

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
  DummyGraph graph;
  update_stores::BspUpdateStore<DummyGraph::VertexData, DummyGraph::EdgeData>
      update_store;
}

TEST_F(SampleAppTest, FactoryCreationShouldFailIfAppIsNotRegistered) {
  DummyGraph graph;
  update_stores::BspUpdateStore<DummyGraph::VertexData, DummyGraph::EdgeData>
      update_store;
}

}  // namespace sics::graph::core::apps
