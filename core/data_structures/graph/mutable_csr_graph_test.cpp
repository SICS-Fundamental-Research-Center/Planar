#include "data_structures/graph/mutable_csr_graph.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "data_structures/graph_metadata.h"

namespace sics::graph::core::test {
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
  auto a = data_structures::graph::MutableCSRGraph<int, int>(&subgraphmetadata);
  auto c =
      std::make_unique<MutableCSRGraphTest::MutableCSRGraph>(&subgraphmetadata);
}

}  // namespace sics::graph::core::test