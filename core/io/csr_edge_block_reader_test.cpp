#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <inttypes.h>
#include <liburing.h>
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

struct io_data {
  common::GraphID gid;
  common::BlockID block_id;
  size_t size;
  char* addr;
};

void Read(io_uring* ring) {
  auto path =
      "/data/shared/graph-systems/csr_bin/test100_prismix_2/graphs/0_blocks/"
      "blocks.yaml";
  auto fd = open(path, O_RDONLY);
  struct stat sb;
  if (fstat(fd, &sb) < 0) {
    LOG_FATAL("Error at fstat");
  }
  io_data* data = (io_data*)malloc(sizeof(io_data) + sb.st_size);
  data->size = sb.st_size;
  data->gid = 0;
  data->block_id = 0;
  data->addr = (char*)(data + 1);
  struct io_uring_sqe* sqe;
  sqe = io_uring_get_sqe(ring);
  if (!sqe) {
    free(data->addr);
    free(data);
    LOG_FATAL("Error at get sqe");
  }
  io_uring_prep_read(sqe, fd, data->addr, data->size, 0);
  io_uring_sqe_set_data(sqe, data);
  auto ret = io_uring_submit(ring);
  if (ret < 0) {
    std::cout << "error" << std::endl;
  }
}

bool Catch(io_uring* ring) {
  struct io_uring_cqe* cqe;
  bool flag = false;
  while (true) {
    auto ret = io_uring_peek_cqe(ring, &cqe);
    if (ret != 0) {
      break;
    }
    io_data* data = (io_data*)io_uring_cqe_get_data(cqe);
    auto size = cqe->res;
    if (cqe->res != data->size) {
//      LOGF_FATAL("Read error in edge block, size is not fit! error code {}",
//                 cqe->res);
      return true;
    }
    // Set address and <gid, bid> entry for notification.
    //      graphs_->at(data->gid).SetSubBlock(data->block_id,
    //      (uint32_t*)data->addr);
    io_uring_cqe_seen(ring, cqe);
    flag = true;
    free(data);
  }
  return flag;
}

// Test for io_uring
TEST_F(CSREdgeBlockTest, IO_URING) {
  io_uring ring;
  auto ret = io_uring_queue_init(QD, &ring, 0);
  if (ret < 0) {
    LOGF_FATAL("queue_init: {}", ret);
  }
  while (true) {
    Read(&ring);

    if (Catch(&ring)) break;
  }
}

// cat with liburing

#define QUEUE_DEPTH 1
#define BLOCK_SZ 1024

struct file_info {
  off_t file_sz;
//  struct iovec iovecs[]; /* Referred by readv/writev */
  char* buf;
};

/*
 * Returns the size of the file whose open file descriptor is passed in.
 * Properly handles regular file and block devices as well. Pretty.
 * */

off_t get_file_size(int fd) {
  struct stat st;

  if (fstat(fd, &st) < 0) {
    perror("fstat");
    return -1;
  }

  if (S_ISBLK(st.st_mode)) {
    unsigned long long bytes;
    if (ioctl(fd, BLKGETSIZE64, &bytes) != 0) {
      perror("ioctl");
      return -1;
    }
    return bytes;
  } else if (S_ISREG(st.st_mode))
    return st.st_size;

  return -1;
}

/*
 * Output a string of characters of len length to stdout.
 * We use buffered output here to be efficient,
 * since we need to output character-by-character.
 * */
void output_to_console(char* buf, int len) {
  while (len--) {
    fputc(*buf++, stdout);
  }
}

/*
 * Wait for a completion to be available, fetch the data from
 * the readv operation and print it to the console.
 * */

int get_completion_and_print(struct io_uring* ring) {
  struct io_uring_cqe* cqe;
  int ret = io_uring_wait_cqe(ring, &cqe);
  if (ret < 0) {
    perror("io_uring_wait_cqe");
    return 1;
  }
  if (cqe->res < 0) {
    fprintf(stderr, "Async readv failed.\n");
    return 1;
  }
  struct file_info* fi = (file_info*)io_uring_cqe_get_data(cqe);
  int blocks = (int)fi->file_sz / BLOCK_SZ;
  if (fi->file_sz % BLOCK_SZ) blocks++;
  for (int i = 0; i < blocks; i++)
//    output_to_console((char*)fi->iovecs[i].iov_base, fi->iovecs[i].iov_len);

  io_uring_cqe_seen(ring, cqe);
  return 0;
}

/*
 * Submit the readv request via liburing
 * */
int submit_read_request(char* file_path, struct io_uring* ring) {
  int file_fd = open(file_path, O_RDONLY);
  if (file_fd < 0) {
    perror("open");
    return 1;
  }
  off_t file_sz = get_file_size(file_fd);
  off_t bytes_remaining = file_sz;

  struct file_info* fi = (file_info*)malloc(sizeof(file_info) + file_sz);

  char* buff = (char*)malloc(file_sz);
  fi->buf = buff;
  if (!buff) {
    fprintf(stderr, "Unable to allocate memory.\n");
    return 1;
  }


  /* Get an SQE */
  struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
  /* Setup a readv operation */
  io_uring_prep_read(sqe, file_fd,  fi->buf, file_sz, 0);
  /* Set user data */
  io_uring_sqe_set_data(sqe, fi);
  /* Finally, submit the request */
  io_uring_submit(ring);

  return 0;
}

TEST_F(CSREdgeBlockTest, cat) {
  struct io_uring ring;
  char* path = "/data/shared/graph-systems/csr_bin/test100_prismix_2/graphs/0_blocks/blocks.yaml";

  /* Initialize io_uring */
  io_uring_queue_init(QUEUE_DEPTH, &ring, 0);

  int ret = submit_read_request(path, &ring);
  if (ret) {
    fprintf(stderr, "Error reading file: %s\n", path);
  }
  get_completion_and_print(&ring);

  /* Call the clean-up function. */
  io_uring_queue_exit(&ring);
}

}  // namespace sics::graph::core::io