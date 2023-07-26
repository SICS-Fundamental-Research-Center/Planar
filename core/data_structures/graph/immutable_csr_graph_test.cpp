#include "immutable_csr_graph.h"

#include <gtest/gtest.h>

#include "common/multithreading/mock_task_runner.h"
#include "io/reader.h"
#include "serialized_immutable_csr_graph.h"
#include "util/logging.h"

using SerializableImmutableCSR =
    sics::graph::core::data_structures::graph::ImmutableCSRGraph;
using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSRGraph;
using Reader = sics::graph::core::io::Reader;
using VertexID = sics::graph::core::common::VertexID;

#define SUBGRAPH_1_PATH "../../../input/small_graph_part/1"
#define SUBGRAPH_0_PATH "../../../input/small_graph_part/0"

namespace sics::graph::core::test {
class SerializableImmutableCSRTest : public ::testing::Test {
 protected:
  SerializableImmutableCSRTest() = default;
  ~SerializableImmutableCSRTest() override = default;
};

TEST_F(SerializableImmutableCSRTest, TestParseCSR) {
  // Create a Reader object
  Reader reader;

  // initialize a Serialized object
  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();

  // initialize a Serializable object
  SerializableImmutableCSR serializable_immutable_csr(1);

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_1_PATH, serialized_immutable_csr);

  // Deserialize
  common::MockTaskRunner runner;
  serializable_immutable_csr.Deserialize(runner,
                                         std::move(*serialized_immutable_csr));
}

}  // namespace sics::graph::core::test