#include "serializable_immutable_csr.h"
#include "io/reader.h"
#include "serialized_immutable_csr.h"
#include "util/logging.h"
#include <gtest/gtest.h>

using SerializableImmutableCSR =
    sics::graph::core::data_structures::graph::SerializableImmutableCSR;
using SerializedImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSR;
using Reader = sics::graph::core::io::Reader;

namespace sics::graph::core::test {
class SerializableImmutableCSRTest : public ::testing::Test {
 protected:
  SerializableImmutableCSRTest() = default;
  ~SerializableImmutableCSRTest() override = default;
};

TEST_F(SerializableImmutableCSRTest, TestParseCSR) {
  Reader reader("/home/baiwc/workspace/graph-systems/input/test/config.yaml");
  SerializedImmutableCSR* serialized_immutable_csr =
      new SerializedImmutableCSR();
  SerializableImmutableCSR serializable_immutable_csr(0, 100);

  reader.SetPointer(
      serialized_immutable_csr);  // pass the address of the object

  // reader.ReadBinFile(
  //     "/home/baiwc/workspace/graph-systems/input/test/0/0_data.bin");

  reader.ReadYaml("/home/baiwc/workspace/graph-systems/input/test/0/0.yaml");

  //   serializable_immutable_csr.ParseSubgraphCSR(
  //       serialized_immutable_csr.get_csr_buffer().front());
}

}  // namespace sics::graph::core::test