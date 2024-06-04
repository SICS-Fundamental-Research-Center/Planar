#ifndef GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER_H_
#define GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER_H_

#include <filesystem>
#include <fstream>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "common/blocking_queue.h"
#include "common/config.h"
#include "data_structures/buffer.h"
#include "data_structures/graph/serialized_mutable_csr_graph.h"
#include "data_structures/graph_metadata.h"
#include "data_structures/serialized.h"
#include "io/reader_writer.h"

namespace sics::graph::core::io {

// io_uring to read
class CSREdgeBlockReader {
 private:
  using OwnedBuffer = sics::graph::core::data_structures::OwnedBuffer;
  using Serialized = sics::graph::core::data_structures::Serialized;

 public:
  CSREdgeBlockReader() = default;
  CSREdgeBlockReader(const std::string& root_path) : root_path_(root_path) {}
  void Init(const std::string& root_path) {
    // copy the root path
    root_path_ = root_path;
  }

  void Read(std::vector<common::BlockID> blocks_to_read) {}

  size_t SizeOfReadNow() { return read_size_; }

  // TODO: use queue to replace vector?
  std::vector<common::BlockID> GetReadBlocks() {}

 private:
  std::string root_path_;
  size_t read_size_ = 0;  // use MB
};

}  // namespace sics::graph::core::io

#endif  // GRAPH_SYSTEMS_CORE_IO_CSR_EDGE_BLOCK_READER_H_
