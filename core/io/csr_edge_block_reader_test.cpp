#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "common/multithreading/thread_pool.h"
#include "io/mutable_csr_writer.h"
#include "liburing.h"

namespace sics::graph::core::io {

class CSREdgeBlockTest : public ::testing::Test {
 protected:
  CSREdgeBlockTest() = default;
  ~CSREdgeBlockTest() override = default;
};

// Test for io_uring
TEST_F(CSREdgeBlockTest, IO_URING) {

}

}  // namespace sics::graph::core::io