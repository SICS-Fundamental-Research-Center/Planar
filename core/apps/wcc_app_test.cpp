#include "apps/wcc_app.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/multithreading/thread_pool.h"

namespace sics::graph::core::apps {

class WccAppTest : public ::testing::Test {
 protected:
  WccAppTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(WccAppTest, ParallelVertexOperatorTest) {
  // TODO: test later when logic of wcc is finished
//  TestGraph grph;
//  update_stores::BspUpdateStore<TestGraph::VertexData, TestGraph::EdgeData>
//      update_store;
//  common::ThreadPool pool(1);
//
//  WCCApp app(&pool, &update_store, &graph);
//  app.PEval();
//  EXPECT_EQ(graph.get_status(), "PEval");
}

}  // namespace sics::graph::core::apps