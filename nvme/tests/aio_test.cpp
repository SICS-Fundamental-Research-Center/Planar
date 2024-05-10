#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <cstdlib>
#include <iostream>

namespace sics::graph::core::tests {

class AioTest : public ::testing::Test {
 protected:
  AioTest() {
    // Suppress death test warnings.
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  }
  std::string data_dir = TEST_DATA_DIR;
};

TEST_F(AioTest, AsynIOUsingLinuxAio) {
//  int fd = open("aio_test.txt", O_CREAT | O_RDWR, 0644);
//  ASSERT_NE(fd, -1);
//
//  struct aiocb cb;
//  memset(&cb, 0, sizeof(struct aiocb));
//  cb.aio_fildes = fd;
//  cb.aio_offset = 0;
//  //  cb.aio_buf = (void*)malloc(1024);
//  cb.aio_buf = malloc(1024);
//  cb.aio_nbytes = 1024;
//
//  int ret = aio_read(&cb);
//  ASSERT_EQ(ret, 0);
//
//  ret = aio_suspend(reinterpret_cast<const aiocb* const*>(&cb), 1, nullptr);
//  ASSERT_EQ(ret, 0);
//
//  ret = aio_return(&cb);
//  ASSERT_EQ(ret, 1024);
//
//  close(fd);
//  free((void*)cb.aio_buf);
//  remove("aio_test.txt");
}

}  // namespace sics::graph::core::tests
