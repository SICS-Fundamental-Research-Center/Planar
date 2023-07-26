#include "immutable_csr_graph.h"

#include <gtest/gtest.h>

#include "common/multithreading/mock_task_runner.h"
#include "data_structures/graph/immutable_csr_graph_config.h"
#include "io/reader.h"
#include "serialized_immutable_csr_graph.h"
#include "util/logging.h"

using ImmutableCSRGraph =
    sics::graph::core::data_structures::graph::ImmutableCSRGraph;
using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using Reader = sics::graph::core::io::Reader;
using VertexID = sics::graph::core::common::VertexID;
using ImmutableCSRGraphConfig =
    sics::graph::core::data_structures::graph::ImmutableCSRGraphConfig;

#define SUBGRAPH_0_PATH "../../../input/small_graph_part/0"
#define SUBGRAPH_1_PATH "../../../input/small_graph_part/1"

namespace sics::graph::core::test {
class SerializableImmutableCSRTest : public ::testing::Test {
 protected:
  // SerializableImmutableCSRTest() = default;
  // ~SerializableImmutableCSRTest() override = default;
  SerializableImmutableCSRTest() {
    // Create a Reader object.
    reader_ = Reader();
    // Initialize a Serialized object.
    serialized_immutable_csr_ = new SerializedImmutableCSR();
    // Initialize test config 0.
    config_0_ = {
      28,  // num_vertex
      27,  // max_vertex
      61,  // sum_in_degree
      68,  // sum_out_degree
    };
    // Initialize test config 1.
    config_1_ = {
      28,  // num_vertex
      55,  // max_vertex
      39,  // sum_in_degree
      32,  // sum_out_degree
    };
  }

  ~SerializableImmutableCSRTest() override {
    delete serialized_immutable_csr_;
  };

  Reader reader_;
  SerializedImmutableCSR* serialized_immutable_csr_;

  ImmutableCSRGraphConfig config_0_;
  ImmutableCSRGraphConfig config_1_;
};

TEST_F(SerializableImmutableCSRTest, TestDeserialize4Subgraph0) {
  ImmutableCSRGraph serializable_immutable_csr(0, std::move(config_0_));

  // Read a subgraph
  reader_.ReadSubgraph(SUBGRAPH_0_PATH, serialized_immutable_csr_);

  // Deserialize
  common::MockTaskRunner runner;
  serializable_immutable_csr.Deserialize(runner,
                                         std::move(*serialized_immutable_csr_));
}

TEST_F(SerializableImmutableCSRTest, TestDeserialize4Subgraph1) {
  ImmutableCSRGraph serializable_immutable_csr(1, std::move(config_1_));

  // Read a subgraph
  reader_.ReadSubgraph(SUBGRAPH_1_PATH, serialized_immutable_csr_);

  // Deserialize
  common::MockTaskRunner runner;
  serializable_immutable_csr.Deserialize(runner,
                                         std::move(*serialized_immutable_csr_));
}

}  // namespace sics::graph::core::test