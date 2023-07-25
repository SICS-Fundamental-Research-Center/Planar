#include "serializable_immutable_csr.h"
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

#define CONFIG_PATH "../../../input/test_dir/config.yaml"

namespace sics::graph::core::test {
class SerializableImmutableCSRTest : public ::testing::Test {
 protected:
  SerializableImmutableCSRTest() = default;
  ~SerializableImmutableCSRTest() override = default;
};

TEST_F(SerializableImmutableCSRTest, TestParseCSR) {
  Reader reader;
  Reader reader("/home/baiwc/workspace/graph-systems/input/test/config.yaml");

  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();
  // SerializableImmutableCSR serializable_immutable_csr(0, 4847570);

  reader.SetPointer(
      serialized_immutable_csr);  // pass the address of the object

  reader.ReadBinFile("/data/shared/soclive/workspace3/minigraph_data/0.bin");

  uint8_t* a =
      serialized_immutable_csr->get_csr_buffer().front().front().Get(4846609);
  uint32_t* a_uint32 = reinterpret_cast<uint32_t*>(a);
  for (std::size_t i = 0; i < 10; i++) {
    std::cout << "Element " << i << ": " << a_uint32[i] << std::endl;
  }

  // serialized_immutable_csr->check_ptr();

  //   serializable_immutable_csr.ParseSubgraphCSR(
  //       serialized_immutable_csr->get_csr_buffer().front());

  //   reader.ReadBinFile(
  //       "/home/baiwc/workspace/graph-systems/input/test/0/0_data.bin");

  LOG_INFO(
      "size of buffer:",
      serialized_immutable_csr->get_csr_buffer().front().front().GetSize());

  int num_vertex = 4847571;
  int max_vertex = 4847570;
  int num_edge = 137987546;

  //   int num_vertex = 4846609;
  //   int num_edge = 136950782;
  //   int max_vertex = 4847570;
  int aligned_num_vertex =
      ceil((float)max_vertex / ALIGNMENT_FACTOR) * ALIGNMENT_FACTOR;

  // Total size:
  //
  //   size_t size_globalid = sizeof(VID_T) * num_vertexes;
  //   size_t size_in_edges = sizeof(VID_T) * sum_in_edges;
  //   size_t size_out_edges = sizeof(VID_T) * sum_out_edges;
  //   size_t size_localid_by_globalid = sizeof(VID_T) *
  //   this->get_aligned_max_vid();
  //   size_t size_indegree = sizeof(size_t) * num_vertexes;
  //   size_t size_outdegree = sizeof(size_t) * num_vertexes;
  //   size_t size_in_offset = sizeof(size_t) * num_vertexes;
  //   size_t size_out_offset = sizeof(size_t) * num_vertexes;

  int expected_size =
      sizeof(size_t) * 4 * num_vertex +
      sizeof(VertexID) * (aligned_num_vertex + 2 * num_vertex + num_edge);

  LOG_INFO("expected size:", expected_size);

  //   reader.ReadYaml("/home/baiwc/workspace/graph-systems/input/test/0/0.yaml");
}

}  // namespace sics::graph::core::test