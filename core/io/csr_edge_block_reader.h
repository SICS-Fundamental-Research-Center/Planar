#ifndef GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER_H_
#define GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER_H_

#include <fcntl.h>
#include <liburing.h>
#include <sys/ioctl.h>

#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "common/blocking_queue.h"
#include "common/config.h"
#include "data_structures/buffer.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"

#define QD 8

namespace sics::graph::core::io {

struct io_data {
  common::GraphID gid;
  common::BlockID block_id;
  size_t size;
  uint32_t* addr;
};

// io_uring to read
class CSREdgeBlockReader {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

 public:
  CSREdgeBlockReader() = default;
  CSREdgeBlockReader(const std::string& root_path) : root_path_(root_path) {
    auto ret = io_uring_queue_init(QD, &ring_, 0);
    if (ret < 0) {
      LOGF_FATAL("queue_init: {}", ret);
    }
  }
  void Init(const std::string& root_path,
            data_structures::TwoDMetadata& metadata) {
    // copy the root path
    root_path_ = root_path;
    auto ret = io_uring_queue_init(QD, &ring_, 0);
    if (ret < 0) {
      LOGF_FATAL("queue_init: {}", ret);
    }
    blocks_addr_.resize(metadata.num_blocks);
    for (uint32_t i = 0; i < metadata.num_blocks; i++) {
      auto num = metadata.blocks.at(i).num_sub_blocks;
      blocks_addr_.at(i).resize(num, nullptr);
    }
  }

  static int get_file_size(int fd, off_t* size) {
    struct stat st;
    if (fstat(fd, &st) < 0) return -1;
    if (S_ISREG(st.st_mode)) {
      *size = st.st_size;
      return 0;
    } else if (S_ISBLK(st.st_mode)) {
      unsigned long long bytes;
      if (ioctl(fd, BLKGETSIZE64, &bytes) != 0) return -1;
      *size = bytes;
      return 0;
    }

    return -1;
  }

  void Read(common::GraphID gid, std::vector<common::BlockID> blocks_to_read) {
    for (int i = 0; i < blocks_to_read.size(); i++) {
      auto path = root_path_ + "/graphs/" + std::to_string(gid) + "_blocks" +
                  std::to_string(blocks_to_read.at(i)) + ".bin";
      auto fd = open(path.c_str(), O_RDONLY);
      struct stat sb;
      if (fstat(fd, &sb) < 0) {
        LOG_FATAL("Error at fstat");
      }
      io_data* data = (io_data*)malloc(sb.st_size + sizeof(io_data));
      data->size = sb.st_size;
      data->gid = gid;
      data->block_id = blocks_to_read.at(i);
      data->addr = (uint32_t*)data + 1;
      struct io_uring_sqe* sqe;
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe) {
        free(data);
        LOG_FATAL("Error at get sqe");
      }
      io_uring_prep_read(sqe, fd, data->addr, data->size, 0);
      io_uring_sqe_set_data(sqe, data);
    }
    auto ret = io_uring_submit(&ring_);
    if (ret < 0) {
      LOG_FATAL("Error at submit sqes");
    }
  }

  std::vector<common::BlockID> GetBlockReady() {
    struct io_uring_cqe* cqe;
    std::vector<common::BlockID> block_ready;
    while (true) {
      auto ret = io_uring_peek_cqe(&ring_, &cqe);
      if (ret != 0) {
        break;
      }
      io_data* data = (io_data*)io_uring_cqe_get_data(cqe);
      if (cqe->res != data->size * 4) {
        LOG_FATAL("Read error in edge block, size is not fit!");
      }
      block_ready.push_back(data->block_id);
      // address
      blocks_addr_.at(data->gid).at(data->block_id) = data->addr;
    }
    return block_ready;
  }

  size_t SizeOfReadNow() { return read_size_; }

  // TODO: use queue to replace vector?
  std::vector<common::BlockID> GetReadBlocks() {}

  // Get the block address for iterate edges.
  common::VertexID* GetBlockAddr(common::GraphID gid, common::BlockID bid) {
    return blocks_addr_.at(gid).at(bid);
  }

  // Release the block address after using.
  void ReleaseBlockAddr(common::GraphID gid, common::BlockID bid) {
    blocks_addr_.at(gid).at(bid) = nullptr;
  }

 private:
  struct io_uring ring_;
  std::string root_path_;

  std::vector<std::vector<common::VertexID*>> blocks_addr_;
  size_t read_size_ = 0;  // use MB
};

}  // namespace sics::graph::core::io

#endif  // GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER_H_
