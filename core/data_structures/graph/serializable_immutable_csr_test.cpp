#include "serializable_immutable_csr.h"
#include "util/logging.h"
#include <gtest/gtest.h>

using SerializableImmutableCSR =
    sics::graph::core::data_structures::graph::SerializedImmutableCSR;

namespace sics::graph::core::test {

class SerializableImmutableCSRTest : public ::testing::Test {
 protected:
  SerializableImmutableCSRTest() = default;
  ~SerializableImmutableCSRTest() override = default;
};

TEST_F(SerializableImmutableCSRTest, InitTest) {
  // SerializableImmutableCSR serializable_immutable_csr;
  EXPECT_STRNE("hello", "world");
}
}  // namespace sics::graph::core::test