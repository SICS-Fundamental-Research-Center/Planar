#include "data_structures/graph/mutable_csr_graph.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/multithreading/thread_pool.h"
#include "data_structures/graph_metadata.h"

namespace xyz::graph::core::test {
class MutableCSRGraphTest : public ::testing::Test {
 public:
  using MutableCSRGraph = data_structures::graph::MutableCSRGraph<int, int>;

 protected:
  MutableCSRGraphTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
};

TEST_F(MutableCSRGraphTest, ConstructorTest) {
  auto subgraphmetadata = data_structures::SubgraphMetadata();
  auto a =
      data_structures::graph::MutableCSRGraph<int, int>(&subgraphmetadata, 10);
  auto c = std::make_unique<MutableCSRGraphTest::MutableCSRGraph>(
      &subgraphmetadata, 10);
}

TEST_F(MutableCSRGraphTest, TestUpdateOutOffsetNewFunction) {
  auto subgraphmetadata = data_structures::SubgraphMetadata();
  subgraphmetadata.num_vertices = 8;
  auto parallelism = 3;
  auto graph =
      data_structures::graph::MutableCSRGraph<int, int>(&subgraphmetadata, 10);
  common::ThreadPool runner(parallelism);
  // init pointer
  graph.parallelism_ = parallelism;
  graph.out_degree_base_new_ = new uint32_t[subgraphmetadata.num_vertices];
  graph.out_offset_base_new_ =
      new common::EdgeIndex[subgraphmetadata.num_vertices];

  for (int i = 0; i < subgraphmetadata.num_vertices; i++) {
    graph.out_degree_base_new_[i] = i + 1;
    graph.out_offset_base_new_[i] = 0;
  }

  graph.UpdateOutOffsetBaseNew(&runner);

  common::EdgeIndex res[8] = {0, 1, 3, 6, 10, 15, 21, 28};
  for (int i = 0; i < subgraphmetadata.num_vertices; i++) {
    EXPECT_EQ(res[i], graph.out_offset_base_new_[i]);
  }

  // delete new-ed memory
  delete[] graph.out_degree_base_new_;
  delete[] graph.out_offset_base_new_;
}

}  // namespace xyz::graph::core::test