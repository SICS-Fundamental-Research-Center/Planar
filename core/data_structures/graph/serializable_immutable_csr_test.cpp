#include "serializable_immutable_csr.h"
#include "common/multithreading/mock_task_runner.h"
#include "common/types.h"
#include "io/reader.h"
#include "serialized_immutable_csr.h"
#include "util/logging.h"
#include <gtest/gtest.h>

using SerializableImmutableCSR =
    sics::graph::core::data_structures::graph::SerializableImmutableCSR;
using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSR;
using Reader = sics::graph::core::io::Reader;
using VertexID = sics::graph::core::common::VertexID;

#define ALIGNMENT_FACTOR (double)64.0

#define SUBGRAPH_1_PATH "../../../input/small_graph_part/1"

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
  SerializableImmutableCSR serializable_immutable_csr(0, 4847570);

  // Read a subgraph
  reader.ReadSubgraph(SUBGRAPH_1_PATH, serialized_immutable_csr);

  // Deserialize
  common::MockTaskRunner runner;
  serializable_immutable_csr.Deserialize(runner,
                                         std::move(*serialized_immutable_csr));
}

}  // namespace sics::graph::core::test