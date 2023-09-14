#include "data_structures/graph/immutable_csr_graph.h"

#include <gtest/gtest.h>

#include "data_structures/graph_metadata.h"

using ImmutableCSRGraph =
    sics::graph::core::data_structures::graph::ImmutableCSRGraph;

namespace sics::graph::core::test {

class ImmutableCSRGraphTest : public ::testing::Test {
 private:
 protected:
  ImmutableCSRGraphTest() {}
};

TEST_F(ImmutableCSRGraphTest, TestDeserialize4Subgraph_0) {
  auto subgraphmetadata = data_structures::SubgraphMetadata();
  auto graph = data_structures::graph::ImmutableCSRGraph(subgraphmetadata);
  auto graph_ptr = std::make_unique<ImmutableCSRGraph>(subgraphmetadata);
}

}  // namespace sics::graph::core::test
