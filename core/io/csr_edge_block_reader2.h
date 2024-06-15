#ifndef GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER2_H_
#define GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER2_H_

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
#include "data_structures/graph/mutable_block_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"
#include "scheduler/edge_buffer2.h"

namespace sics::graph::core::io {

struct io_data {
  common::GraphID gid;
  common::BlockID block_id;
  size_t size;
  size_t offset;
  char* addr;
};

std::vector<std::vector<int>> Fds;

// io_uring to read
class CSREdgeBlockReader2 {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

 public:
  CSREdgeBlockReader2() = default;
  ~CSREdgeBlockReader2() { io_uring_queue_exit(&ring_); }

  void Init(const std::string& root_path,
            data_structures::TwoDMetadata* metadata,
            scheduler::EdgeBuffer2* buffer,
            std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs) {
    // copy the root path
    root_path_ = root_path;
    buffer_ = buffer;
    graphs_ = graphs;
    auto ret = io_uring_queue_init(QD, &ring_, 0);
    if (ret < 0) {
      LOGF_FATAL("queue_init: {}", ret);
    }
    blocks_addr_.resize(metadata->num_blocks);
    Fds.resize(metadata->num_blocks);
    for (uint32_t i = 0; i < metadata->num_blocks; i++) {
      auto num = metadata->blocks.at(i).num_sub_blocks;
      blocks_addr_.at(i).resize(num, nullptr);
      Fds.at(i).resize(num, 0);
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
      auto bid = blocks_to_read.at(i);
      auto path = root_path_ + "graphs/" + std::to_string(gid) + "_blocks/" +
                  std::to_string(bid) + ".bin";
      Fds[gid][bid] = open(path.c_str(), O_RDONLY);
      auto fd = Fds[gid][bid];
      struct stat sb;
      if (fstat(fd, &sb) < 0) {
        LOG_FATAL("Error at fstat");
      }
      io_data* data = (io_data*)malloc(sizeof(io_data));
      data->size = sb.st_size;
      data->gid = gid;
      data->block_id = bid;
      data->addr = (char*)graphs_->at(gid).ApplySubBlockBuffer(bid);
      data->offset = 0;
      struct io_uring_sqe* sqe;
      sqe = io_uring_get_sqe(&ring_);
      if (!sqe) {
        free(data);
        graphs_->at(gid).Release(bid);
        LOG_FATAL("Error at get sqe");
      }
      io_uring_prep_read(sqe, fd, data->addr, data->size, data->offset);
      io_uring_sqe_set_data(sqe, data);
    }
    auto ret = io_uring_submit(&ring_);
    if (ret < 0) {
      LOG_FATAL("Error at submit sqes");
    }
  }

  void Read(io_data* data) {
    auto fd = Fds[data->gid][data->block_id];
    struct io_uring_sqe* sqe;
    sqe = io_uring_get_sqe(&ring_);
    if (!sqe) {
      free(data);
      LOG_FATAL("Error at get sqe");
    }
    io_uring_prep_read(sqe, fd, data->addr, data->size, data->offset);
    io_uring_sqe_set_data(sqe, data);
    auto ret = io_uring_submit(&ring_);
    if (ret < 0) {
      LOG_FATAL("Error at submit sqes for read short");
    }
  }

  size_t GetBlockReady() {
    struct io_uring_cqe* cqe;
    size_t num_cqe = 0;
    std::string ids = "";
    while (true) {
      auto ret = io_uring_peek_cqe(&ring_, &cqe);
      if (ret != 0) {
        break;
      }
      io_data* data = (io_data*)io_uring_cqe_get_data(cqe);
      if (cqe->res != data->size) {
        auto size = cqe->res;
        if (cqe->res > 0) {
          data->addr = data->addr + size;
          data->size = data->size - size;
          if (data->size < 0) {
            LOG_FATAL("size less 0");
          }
          data->offset = data->offset + cqe->res;
          io_uring_cqe_seen(&ring_, cqe);
          Read(data);
          continue;
        } else {
          LOGF_FATAL(
              "Read error in edge block, size is not fit! error code {}, "
              "blockID {}",
              cqe->res, data->block_id);
        }
      }
      // Set address and <gid, bid> entry for notification.
      //      graphs_->at(data->gid).SetSubBlock(data->block_id,
      //      (uint32_t*)data->addr);
      buffer_->PushOneEdgeBlock(data->gid, data->block_id);
      io_uring_cqe_seen(&ring_, cqe);
      ids += std::to_string(data->block_id) + " ";
      close(Fds[data->gid][data->block_id]);
      free(data);
      num_cqe++;
    }
    if (ids != "") {
      LOGF_INFO("Read blocks: {}", ids);
    }
    return num_cqe;
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

  bool IsFinish() { return !io_uring_cq_has_overflow(&ring_); }

 private:
  struct io_uring ring_;
  std::string root_path_;

  common::GraphID current_gid_;
  std::queue<io_data*> reload_ids_;

  scheduler::EdgeBuffer2* buffer_;
  std::vector<std::vector<common::VertexID*>> blocks_addr_;  // to delete.
  std::vector<data_structures::graph::MutableBlockCSRGraph>* graphs_;

  size_t read_size_ = 0;  // use MB
};

}  // namespace sics::graph::core::io

#endif  // GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER2_H_
