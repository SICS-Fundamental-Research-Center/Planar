#include "io/csr_writer.h"

#include <gtest/gtest.h>

#include "common/multithreading/thread_pool.h"
#include "data_structures/graph/mutable_csr_graph.h"

namespace sics::grapg::core::io {

class CSRWriterTest : public ::testing::Test {
 protected:
  CSRWriterTest() = default;
  ~CSRWriterTest() override = default;
};

// TODO: test write mutable csrgraph

}  // namespace sics::grapg::core::io